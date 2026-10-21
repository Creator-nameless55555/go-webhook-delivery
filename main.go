package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"sync"
	"time"
)

const (
	maxAttempts  = 3
	requestLimit = 1 << 20 // 1 MB
)

type Event struct {
	ID        string         `json:"id"`
	Type      string         `json:"type"`
	TargetURL string         `json:"target_url"`
	Data      map[string]any `json:"data"`
}

type DeliveryService struct {
	client *http.Client

	mu        sync.Mutex
	delivered map[string]bool
	inFlight  map[string]chan struct{}
}

func NewDeliveryService() *DeliveryService {
	transport := &http.Transport{
		DialContext: safeDialContext,
	}

	return &DeliveryService{
		client: &http.Client{
			Timeout:   3 * time.Second,
			Transport: transport,
			CheckRedirect: func(
				req *http.Request,
				via []*http.Request,
			) error {
				return http.ErrUseLastResponse
			},
		},
		delivered: make(map[string]bool),
		inFlight:  make(map[string]chan struct{}),
	}
}

func main() {
	service := NewDeliveryService()

	mux := http.NewServeMux()
	mux.HandleFunc("POST /webhooks", service.handleWebhook)
	mux.HandleFunc("POST /test-receiver", handleTestReceiver)

	server := &http.Server{
		Addr:              ":8080",
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	log.Println("Webhook Delivery Service listening on :8080")

	if err := server.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}

func (s *DeliveryService) handleWebhook(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, requestLimit)
	defer r.Body.Close()

	var event Event
	decoder := json.NewDecoder(r.Body)

	if err := decoder.Decode(&event); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}

	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		http.Error(w, "request must contain one JSON object", http.StatusBadRequest)
		return
	}

	if strings.TrimSpace(event.ID) == "" ||
		strings.TrimSpace(event.Type) == "" ||
		!validURL(event.TargetURL) {
		http.Error(w, "id, type and valid target_url are required", http.StatusBadRequest)
		return
	}

	log.Printf("Received event: id=%s type=%s", event.ID, event.Type)

	if err := s.deliverOnce(r.Context(), event); err != nil {
		log.Printf("Delivery failed: id=%s error=%v", event.ID, err)
		http.Error(w, "delivery failed", http.StatusBadGateway)
		return
	}

	writeJSON(w, http.StatusAccepted, map[string]string{
		"status":   "delivered",
		"event_id": event.ID,
	})
}

func (s *DeliveryService) deliverOnce(ctx context.Context, event Event) error {
	for {
		s.mu.Lock()

		if s.delivered[event.ID] {
			s.mu.Unlock()
			return nil
		}

		if pending, exists := s.inFlight[event.ID]; exists {
			s.mu.Unlock()

			select {
			case <-pending:
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		pending := make(chan struct{})
		s.inFlight[event.ID] = pending
		s.mu.Unlock()

		err := s.deliver(ctx, event)

		s.mu.Lock()
		if err == nil {
			s.delivered[event.ID] = true
		}
		delete(s.inFlight, event.ID)
		close(pending)
		s.mu.Unlock()

		return err
	}
}

func (s *DeliveryService) deliver(ctx context.Context, event Event) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("encode event: %w", err)
	}

	var lastErr error

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if attempt > 1 {
			delay := time.Duration(1<<(attempt-2)) * 500 * time.Millisecond
			timer := time.NewTimer(delay)

			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			}
		}

		req, err := http.NewRequestWithContext(
			ctx,
			http.MethodPost,
			event.TargetURL,
			bytes.NewReader(payload),
		)
		if err != nil {
			return fmt.Errorf("create request: %w", err)
		}

		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Event-ID", event.ID)

		resp, err := s.client.Do(req)
		if err != nil {
			lastErr = err
			log.Printf("Attempt %d/%d failed: %v", attempt, maxAttempts, err)
			continue
		}

		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			log.Printf("Delivered event %s on attempt %d", event.ID, attempt)
			return nil
		}

		lastErr = fmt.Errorf("receiver returned HTTP %d", resp.StatusCode)
		log.Printf("Attempt %d/%d failed: %v", attempt, maxAttempts, lastErr)

		if resp.StatusCode < 500 {
			return lastErr
		}
	}

	return fmt.Errorf("all attempts failed: %w", lastErr)
}

func validURL(rawURL string) bool {
	u, err := url.ParseRequestURI(rawURL)
	return err == nil &&
		(u.Scheme == "http" || u.Scheme == "https") &&
		u.Hostname() != "" &&
		u.User == nil
}

func isPublicIP(ip netip.Addr) bool {
	if !ip.IsValid() {
		return false
	}

	ip = ip.Unmap()

	return ip.IsGlobalUnicast() &&
		!ip.IsPrivate() &&
		!ip.IsLoopback() &&
		!ip.IsLinkLocalUnicast() &&
		!ip.IsLinkLocalMulticast() &&
		!ip.IsMulticast() &&
		!ip.IsUnspecified()
}

func safeDialContext(
	ctx context.Context,
	network string,
	address string,
) (net.Conn, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, fmt.Errorf("invalid target address: %w", err)
	}

	if ip, err := netip.ParseAddr(host); err == nil {
		if !isPublicIP(ip) {
			return nil, fmt.Errorf("target IP is not public")
		}

		return (&net.Dialer{}).DialContext(ctx, network, address)
	}

	ips, err := net.DefaultResolver.LookupNetIP(ctx, "ip", host)
	if err != nil {
		return nil, fmt.Errorf("resolve target host: %w", err)
	}

	if len(ips) == 0 {
		return nil, fmt.Errorf("target host has no IP addresses")
	}

	for _, ip := range ips {
		if !isPublicIP(ip) {
			return nil, fmt.Errorf("target resolves to a non-public IP")
		}
	}

	var lastErr error

	for _, ip := range ips {
		conn, err := (&net.Dialer{}).DialContext(
			ctx,
			network,
			net.JoinHostPort(ip.String(), port),
		)
		if err == nil {
			return conn, nil
		}

		lastErr = err
	}

	return nil, fmt.Errorf("connect to target: %w", lastErr)
}

func handleTestReceiver(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, requestLimit))
	if err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	log.Printf("Receiver got event: %s", body)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("received"))
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
