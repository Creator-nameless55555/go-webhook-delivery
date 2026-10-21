package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func newTestDeliveryService() *DeliveryService {
	return &DeliveryService{
		client: &http.Client{
			Timeout: 3 * time.Second,
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

func TestWebhookDeliveryAndIdempotency(t *testing.T) {
	var requests atomic.Int32

	receiver := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			requests.Add(1)

			if r.Header.Get("X-Event-ID") != "evt_test" {
				t.Errorf("unexpected event ID header")
			}

			w.WriteHeader(http.StatusOK)
		},
	))
	defer receiver.Close()

	service := newTestDeliveryService()

	event := Event{
		ID:        "evt_test",
		Type:      "payment.completed",
		TargetURL: receiver.URL,
		Data:      map[string]any{"amount": 1500},
	}

	body, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}

	send := func() *httptest.ResponseRecorder {
		req := httptest.NewRequest(
			http.MethodPost,
			"/webhooks",
			strings.NewReader(string(body)),
		)
		rec := httptest.NewRecorder()

		service.handleWebhook(rec, req)
		return rec
	}

	first := send()
	if first.Code != http.StatusAccepted {
		t.Fatalf("first request: got %d, want %d; body: %s",
			first.Code, http.StatusAccepted, first.Body.String())
	}

	second := send()
	if second.Code != http.StatusAccepted {
		t.Fatalf("duplicate request: got %d, want %d; body: %s",
			second.Code, http.StatusAccepted, second.Body.String())
	}

	if got := requests.Load(); got != 1 {
		t.Fatalf("receiver got %d requests, want 1", got)
	}
}

func TestWebhookRejectsInvalidJSON(t *testing.T) {
	service := newTestDeliveryService()

	req := httptest.NewRequest(
		http.MethodPost,
		"/webhooks",
		strings.NewReader("{invalid"),
	)
	rec := httptest.NewRecorder()

	service.handleWebhook(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("got %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestWebhookRequiresTargetURL(t *testing.T) {
	service := newTestDeliveryService()

	body := `{"id":"evt_test","type":"payment.completed","data":{}}`
	req := httptest.NewRequest(
		http.MethodPost,
		"/webhooks",
		strings.NewReader(body),
	)
	rec := httptest.NewRecorder()

	service.handleWebhook(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("got %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestValidURL(t *testing.T) {
	tests := []struct {
		name string
		url  string
		want bool
	}{
		{"HTTPS URL", "https://example.com/webhook", true},
		{"HTTP URL", "http://example.com/webhook", true},
		{"Missing scheme", "example.com/webhook", false},
		{"FTP URL", "ftp://example.com/file", false},
		{"Missing host", "http:///webhook", false},
		{"User credentials", "http://user:pass@example.com", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := validURL(tt.url); got != tt.want {
				t.Fatalf("validURL(%q) = %v, want %v", tt.url, got, tt.want)
			}
		})
	}
}

func TestIsPublicIP(t *testing.T) {
	tests := []struct {
		ip   string
		want bool
	}{
		{"8.8.8.8", true},
		{"1.1.1.1", true},
		{"127.0.0.1", false},
		{"10.0.0.1", false},
		{"192.168.1.1", false},
		{"169.254.1.1", false},
		{"::1", false},
		{"fc00::1", false},
		{"2606:4700:4700::1111", true},
	}

	for _, tt := range tests {
		t.Run(tt.ip, func(t *testing.T) {
			ip := netip.MustParseAddr(tt.ip)
			if got := isPublicIP(ip); got != tt.want {
				t.Fatalf("isPublicIP(%s) = %v, want %v", tt.ip, got, tt.want)
			}
		})
	}
}
func TestDeliveryRetriesUntilSuccess(t *testing.T) {
	var requests atomic.Int32

	receiver := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			attempt := requests.Add(1)

			if attempt < 2 {
				http.Error(w, "temporary failure", http.StatusInternalServerError)
				return
			}

			w.WriteHeader(http.StatusOK)
		},
	))
	defer receiver.Close()

	service := newTestDeliveryService()
	event := Event{
		ID:        "evt_retry",
		Type:      "payment.completed",
		TargetURL: receiver.URL,
		Data:      map[string]any{"amount": 1500},
	}

	err := service.deliver(context.Background(), event)
	if err != nil {
		t.Fatalf("expected successful delivery after retry, got: %v", err)
	}

	if got := requests.Load(); got != 2 {
		t.Fatalf("got %d requests, want 2", got)
	}
}

func TestDeliveryDoesNotRetry4xx(t *testing.T) {
	var requests atomic.Int32

	receiver := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			requests.Add(1)
			http.Error(w, "bad request", http.StatusBadRequest)
		},
	))
	defer receiver.Close()

	service := newTestDeliveryService()
	event := Event{
		ID:        "evt_4xx",
		Type:      "payment.completed",
		TargetURL: receiver.URL,
	}

	err := service.deliver(context.Background(), event)
	if err == nil {
		t.Fatal("expected delivery error for HTTP 400")
	}

	if got := requests.Load(); got != 1 {
		t.Fatalf("got %d requests, want 1", got)
	}
}

func TestDeliveryStopsAfterMaxAttempts(t *testing.T) {
	var requests atomic.Int32

	receiver := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			requests.Add(1)
			http.Error(w, "server error", http.StatusInternalServerError)
		},
	))
	defer receiver.Close()

	service := newTestDeliveryService()
	event := Event{
		ID:        "evt_5xx",
		Type:      "payment.completed",
		TargetURL: receiver.URL,
	}

	err := service.deliver(context.Background(), event)
	if err == nil {
		t.Fatal("expected delivery error after all attempts")
	}

	if got := requests.Load(); got != maxAttempts {
		t.Fatalf("got %d requests, want %d", got, maxAttempts)
	}
}

func TestDeliveryDoesNotFollowRedirect(t *testing.T) {
	var targetRequests atomic.Int32

	target := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			targetRequests.Add(1)
			w.WriteHeader(http.StatusOK)
		},
	))
	defer target.Close()

	var receiverRequests atomic.Int32

	receiver := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			receiverRequests.Add(1)
			http.Redirect(w, r, target.URL, http.StatusFound)
		},
	))
	defer receiver.Close()

	service := newTestDeliveryService()
	event := Event{
		ID:        "evt_redirect",
		Type:      "payment.completed",
		TargetURL: receiver.URL,
	}

	err := service.deliver(context.Background(), event)
	if err == nil {
		t.Fatal("expected delivery error for HTTP 302")
	}

	if got := receiverRequests.Load(); got != 1 {
		t.Fatalf("receiver got %d requests, want 1", got)
	}

	if got := targetRequests.Load(); got != 0 {
		t.Fatalf("redirect target got %d requests, want 0", got)
	}
}

func TestConcurrentDeliverySameID(t *testing.T) {
	var requests atomic.Int32

	receiver := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			requests.Add(1)
			time.Sleep(100 * time.Millisecond)
			w.WriteHeader(http.StatusOK)
		},
	))
	defer receiver.Close()

	service := newTestDeliveryService()
	event := Event{
		ID:        "evt_concurrent",
		Type:      "payment.completed",
		TargetURL: receiver.URL,
		Data:      map[string]any{"amount": 1500},
	}

	const workers = 10

	start := make(chan struct{})
	errs := make(chan error, workers)

	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()
			<-start
			errs <- service.deliverOnce(context.Background(), event)
		}()
	}

	close(start)
	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			t.Errorf("deliverOnce returned error: %v", err)
		}
	}

	if got := requests.Load(); got != 1 {
		t.Fatalf("receiver got %d requests, want 1", got)
	}
}
