[![Go CI](https://github.com/Creator-nameless55555/go-webhook-delivery/actions/workflows/ci.yml/badge.svg)](https://github.com/Creator-nameless55555/go-webhook-delivery/actions)

# Go Webhook Delivery

A lightweight webhook delivery service written in Go.

It accepts events over HTTP and delivers them to external endpoints with automatic retries, request timeouts, SSRF protections, and in-memory idempotency.

## Features

- HTTP API for accepting webhook events
- JSON request validation and a 1 MB request body limit
- Automatic retries with exponential backoff
- HTTP client timeout of 3 seconds
- Retries on network errors and HTTP 5xx responses
- No retries on HTTP 4xx or 3xx responses
- In-memory idempotency for successfully delivered event IDs
- Coordination of concurrent requests with the same event ID
- URL validation and SSRF protections
- Redirects disabled for outbound requests
- Tests using Go's `httptest` package

## Requirements

- Go 1.26 or later

## Getting Started

Start the server:

```bash
go run .
```

The API listens on:

`http://localhost:8080`

Keep the server running in the terminal.

## API

### `POST /webhooks`

Accepts a webhook event and delivers it to the specified target URL.

Example request:

```json
{
  "id": "evt_001",
  "type": "payment.completed",
  "target_url": "https://your-webhook-endpoint.example/webhook",
  "data": {
    "order_id": "123",
    "amount": 1500
  }
}
```

Send a request from PowerShell:

```powershell
$body = @{
    id = "evt_001"
    type = "payment.completed"
    target_url = "https://your-webhook-endpoint.example/webhook"
    data = @{
        order_id = "123"
        amount = 1500
    }
} | ConvertTo-Json -Depth 5

Invoke-RestMethod `
    -Uri "http://localhost:8080/webhooks" `
    -Method Post `
    -ContentType "application/json" `
    -Body $body
```

Replace the example target URL with a reachable public HTTP or HTTPS endpoint that you control.

Example successful response (`202 Accepted`):

```json
{
  "status": "delivered",
  "event_id": "evt_001"
}
```

The service returns `400 Bad Request` for invalid requests and `502 Bad Gateway` when delivery fails.

### `POST /test-receiver`

A local endpoint that logs the received request body and returns `200 OK`.

**Note:** The production outbound HTTP client blocks loopback and private IP addresses. Therefore, this local test receiver cannot be used as a delivery target through the production client. Automated tests use a separate HTTP client to support local test servers.

## Delivery Behavior

- Maximum attempts: 3
- HTTP client timeout: 3 seconds per request
- Retry delays: 500 ms, then 1 second
- Retries on network errors and HTTP 5xx responses
- Does not retry HTTP 4xx or 3xx responses
- Sends the event ID in the `X-Event-ID` header
- Does not follow redirects
- Delivery is synchronous: the API response is sent after delivery succeeds or fails

## Security

The service implements basic SSRF protections for outbound webhook requests:

- Accepts only HTTP and HTTPS target URLs
- Rejects URLs containing user credentials
- Rejects non-public destination IP addresses
- Resolves hostnames before dialing and connects to the resolved address
- Rejects redirects to reduce the risk of redirect-based SSRF bypasses
- Limits incoming request bodies to 1 MB

These protections reduce SSRF risk but are not a substitute for a complete production security review. Authentication, rate limiting, monitoring, and deployment-specific network controls are not included.

## Idempotency

Successfully delivered event IDs are stored in memory.

Repeated events with the same ID do not trigger another delivery during the current process lifetime. Concurrent requests with the same ID are coordinated to avoid duplicate deliveries when delivery succeeds.

The idempotency state is lost when the service restarts. Failed deliveries are not permanently recorded as delivered and may be attempted again by a subsequent request.

## Testing

Run all tests without using cached results:

```bash
go test -count=1 ./...
```

Run the race detector:

```bash
go test -race -count=1 ./...
```

Run static analysis:

```bash
go vet ./...
```

Measure statement coverage:

```bash
go test -cover ./...
```

## Limitations

- Delivery and idempotency state are stored in memory and lost on restart.
- Events are delivered synchronously.
- Persistent queues and dead-letter handling are not implemented.
- Authentication and rate limiting are not implemented.
- Retry delays are fixed rather than configurable.
- Idempotency is process-local and is not shared across multiple service instances.
- SSRF protections require further review before use in security-critical production environments.

## Tech Stack

- Go
- `net/http`
- `encoding/json`
- `net/netip`
- `sync`
- `testing`
- `net/http/httptest`
