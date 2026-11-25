## Overview

[![CI](https://github.com/pranay1498/courier/actions/workflows/ci.yml/badge.svg)](https://github.com/pranay1498/courier/actions/workflows/ci.yml)

Courier is a distributed multi-channel notification delivery platform built with Spring Boot, Apache Kafka, Redis, and PostgreSQL that guarantees at-least-once delivery across Email, SMS, and Webhook channels with idempotent processing via Redis SETNX deduplication.

## Architecture
![Architecture](docs/architecture.png)

## Tech Stack
| Technology | Version | Usage                                                                                            |
|---|---|--------------------------------------------------------------------------------------------------|
| Spring Boot | 3.2.0 | REST API, dependency injection, Kafka integration                                                |
| Apache Kafka | 7.6.0 (Confluent) | Async event streaming, at-least-once delivery, DLQ                                               |
| Redis | 7 | Rate limiting (Sorted Sets + Lua), deduplication (SETNX), status cache (Hash), Pub/Sub broadcast |
| PostgreSQL | 15 | Persistent notification records, delivery attempt history                                        |
| Docker Compose | - | Single-command local environment                                                                 |
| Micrometer + Prometheus | - | Delivery counters and latency percentiles                                                        |
| Springdoc OpenAPI | 2.3.0 | Auto-generated Swagger UI                                                                        |
| Testcontainers | 1.19.3 | Redis integration tests against real Redis                                                       |
| Mailpit | latest | Local SMTP capture - view delivered mail at :8025                                                |
| k6 | - | Load testing - API throughput and end-to-end delivery latency                                   |
| Spring Security | 6.2.0 | API key authentication, role-based path authorization                            |


## Getting Started

### Prerequisites
- Docker Desktop
- Java 21
- Maven 3.8+

### Run everything with one command

```bash
git clone https://github.com/pranay/courier.git
cd courier
mvn clean package -DskipTests
docker-compose up -d
```

Wait ~30 seconds for all services to start, then send your first notification:

```bash
curl -X POST http://localhost:8080/api/v1/notifications \
  -H "Content-Type: application/json" \
  -H "X-API-Key: dev-client-key-change-me" \
  -d '{
    "correlationId": "my-first-notification",
    "channel": "WEBHOOK",
    "recipient": "https://webhook.site/your-unique-url",
    "content": "Hello from Courier"
  }'
```

Swagger UI available at:
- courier-api: http://localhost:8080/swagger-ui.html
- courier-dashboard: http://localhost:8084/swagger-ui.html

The dashboard listens on 8082 inside its container but is published on host port
8084, because courier-worker reserves the host range 8081-8083 for its replicas.

## Authentication

Every `/api/v1/**` endpoint on both services requires an API key in the
`X-API-Key` header. Actuator health, Swagger UI and the OpenAPI spec stay open
so the project is explorable and Docker healthchecks keep working.

| Role | Can reach | Dev default key |
|---|---|---|
| `CLIENT` | `/api/v1/notifications/**`, `/api/v1/dashboard/**` | `dev-client-key-change-me` |
| `ADMIN` | the above plus `/api/v1/admin/**` | `dev-admin-key-change-me` |

Override the defaults with environment variables:

```bash
COURIER_CLIENT_KEY=... COURIER_ADMIN_KEY=... docker-compose up -d
```

Responses distinguish the two failure modes. A missing or unrecognised key is
**401 Unauthorized** - the server does not know who you are. A valid `CLIENT`
key hitting an admin endpoint is **403 Forbidden** - the server knows exactly
who you are and the answer is no. Retrying a 403 with the same key is futile,
and the status code says so.

Keys are compared with `MessageDigest.isEqual` rather than `String.equals`.
`equals` returns on the first differing byte, so response time leaks how many
leading characters of a guess were correct - enough to recover a key one
character at a time.

The authorization rules end in `denyAll()`, not `permitAll()`. An endpoint
added later and not matched by an earlier rule is closed by default: a
forgotten rule becomes a 403 rather than an open door.

In Swagger UI, click **Authorize** and paste a key (`dev-client-key-change-me` locally)
and "Try it out" sends it on every request.

**Unsafe keys cannot reach production.** The dev defaults are public in this
repository. With `SPRING_PROFILES_ACTIVE=prod`, startup aborts if any key is a
placeholder, shorter than 24 characters, blank or duplicated, or if a role is
mistyped; the error names the client but never the key. In any other profile the
same problems are only logged as warnings, so local runs work without setup.

**What production would do differently.** Keys here come from configuration,
which is appropriate for a single-tenant deployment. A multi-tenant system
would store an Argon2 or bcrypt hash per client in the database, support
rotation with overlapping validity windows, and never retain the plaintext key
after issuance.

### Rate limiting

The rate limiter buckets on the authenticated `clientId`, the one identifier that
is stable across a caller's requests and cannot be chosen freely by the caller.

The limit is **1,000 requests per client, per channel, per rolling minute**,
configurable through `courier.rate-limit.max-requests` or the
`COURIER_RATE_LIMIT_MAX_REQUESTS` environment variable. Redis outages fail open
(requests are allowed) because blocking all notifications is worse than briefly
allowing extra traffic.

## Delivery Channels

Courier delivers through three channels. Each uses a real implementation locally -
no channel is faked with a log statement.

### Email

Email is delivered over SMTP using Spring's `JavaMailSender`. Locally, mail is sent
to [Mailpit](https://github.com/axllent/mailpit), an SMTP server that accepts
messages and stores them instead of relaying them onward. Mailpit speaks the full
SMTP protocol, so the application code is identical to what runs against a real
provider - only the host and port differ.

View delivered mail at **http://localhost:8025** after sending an EMAIL notification.

**Why Mailpit instead of a real provider locally:**
- No credentials required, so the project runs with `docker-compose up` and nothing else
- Works in CI without secrets management
- Every header is inspectable in the web UI (useful for verifying `X-Courier-Event-Id`)
- No rate limits, so retry and DLQ testing doesn't burn through a daily send quota
- Test recipients like `test@example.com` don't bounce

**Using a real SMTP provider:**

No code changes are required. Set these environment variables on `courier-worker`:

| Variable | SendGrid | AWS SES | Gmail |
|---|---|---|---|
| `SPRING_MAIL_HOST` | `smtp.sendgrid.net` | `email-smtp.<region>.amazonaws.com` | `smtp.gmail.com` |
| `SPRING_MAIL_PORT` | `587` | `587` | `587` |
| `SPRING_MAIL_USERNAME` | `apikey` | SES SMTP username | your Gmail address |
| `SPRING_MAIL_PASSWORD` | your API key | SES SMTP password | app password (not account password) |
| `SPRING_MAIL_SMTP_AUTH` | `true` | `true` | `true` |
| `SPRING_MAIL_STARTTLS` | `true` | `true` | `true` |
| `COURIER_MAIL_FROM` | a verified sender address | a verified sender address | your Gmail address |

Additional production requirements not handled by configuration alone:
- **Sender verification** - SendGrid and SES reject mail from unverified addresses or domains
- **SPF, DKIM, DMARC** - DNS records that prevent your mail being classified as spam
- **Bounce handling** - real providers send webhooks on hard bounces; continuing to
  send to invalid addresses damages sender reputation

### SMS

SMS delivery goes through an `SmsProvider` interface rather than calling a vendor SDK
directly. The active implementation is selected by the `courier.sms.provider`
property, so switching vendors is a configuration change rather than a code change.

The default `MockSmsProvider` performs the validation a real provider would:
- Recipient must be valid E.164 format (`+` followed by 1-15 digits, first digit non-zero)
- Content must not be blank
- Content must not exceed 1600 characters (the concatenated SMS limit)
- Calculates and logs the segment count (160 characters per segment)

Invalid input throws `IllegalArgumentException`, which is declared non-retryable in
`KafkaConsumerConfig` - a malformed phone number goes straight to the DLQ after one
attempt rather than burning three pointless retries.

**Adding a real SMS provider:**

Implement `SmsProvider` and annotate the class so it activates on a config value:

```java
@Component
@ConditionalOnProperty(name = "courier.sms.provider", havingValue = "twilio")
public class TwilioSmsProvider implements SmsProvider {

    @Override
    public String send(String recipient, String content) throws DeliveryException {
        // Twilio SDK call - return the provider's message SID
    }

    @Override
    public String getProviderName() {
        return "twilio";
    }
}
```

Then set `courier.sms.provider=twilio`. `SmsDeliveryHandler` is unchanged - it depends
on the interface, not the implementation.

### Webhook

Webhook delivery makes a real HTTP POST to the recipient URL with a JSON payload and
tracing headers (`X-Courier-Event-Id`, `X-Courier-Correlation-Id`, `X-Courier-Timestamp`).
The event ID header lets the receiving system deduplicate on its own side if Courier
retries.

Connection timeout is 5 seconds and read timeout is 10 seconds. These are deliberately
shorter than Kafka's `max.poll.interval.ms` (300 seconds) - an outbound call that hangs
longer than the poll interval would make Kafka assume the consumer is dead and trigger a
consumer group rebalance, stalling every partition.

Response codes are classified by whether retrying could plausibly succeed:

| Response | Classification | Behaviour |
|---|---|---|
| 2xx | Success | Delivered |
| 429, 408 | Transient | Retry up to 3 times, then DLQ |
| Other 4xx | Permanent | Straight to DLQ - a 404 or 400 will fail identically on retry |
| 5xx | Transient | Retry up to 3 times, then DLQ |
| Connection refused / DNS failure / timeout | Transient | Retry up to 3 times, then DLQ |

Test webhook delivery with [webhook.site](https://webhook.site) - paste your unique URL
as the `recipient`. For failure testing, [httpstat.us](https://tools-httpstatus.pickup-services.com/) returns any
status code on request.

## API Reference

### courier-api (port 8080)
| Method | Endpoint | Description |
|---|---|---|
| POST | /api/v1/notifications | Submit a notification for delivery |
| GET | /api/v1/notifications/{id}/status | Get notification status (DB read) |
| POST | /api/v1/admin/notifications/{id}/replay | Replay a DLQ'd notification |

### courier-dashboard (host port 8084)
| Method | Endpoint | Description |
|---|---|---|
| GET | /api/v1/dashboard/notifications/{id} | Get status (Redis-backed) |
| GET | /api/v1/dashboard/notifications/{id}/attempts | Get delivery attempt history |
| GET | /api/v1/dashboard/notifications | Paginated listing with filters |
| GET | /api/v1/dashboard/stats | Delivery statistics per channel |


## Performance

Load tested with [k6](https://k6.io). Scripts live in `load-tests/`.

### Test environment

All numbers below were measured on a single MacBook Air running the full stack
(Kafka, Redis, PostgreSQL, Mailpit and all three services) in Docker Desktop with
~4 GB allocated, with k6 generating load on the same host. The load generator and
the system under test therefore compete for the same CPU - see
[Methodology](#methodology) below.

### API throughput

Ramping arrival rate to 800 req/s over 4 minutes (stages at 128, 256, 480 and
800 req/s, then ramp-down), EMAIL channel, unique `correlationId` per request, one
client key:

| Metric | Result |
|---|---|
| Requests | 86,669 |
| Average rate | 361 req/s (800 req/s reached in the final stage) |
| Latency - median | 1.41 ms |
| Latency - p90 | 3.41 ms |
| Latency - p95 | 5.39 ms |
| Latency - p99 | 17.7 ms |
| Latency - max | 301 ms |
| Failed requests | 0% |
| Dropped iterations | 0 |
| Rate-limiter rejections | 0 (limit raised for the run) |

k6 needed at most 8 of its 2,000 available virtual users, so the generator was not
the constraint.

Pushing the ramp to 2,500 req/s (current code, with API-key auth) averaged
747 req/s: p95 195 ms, but p99 rose to 10.7 s, 1.14% of requests failed and about
25% of scheduled iterations were dropped. The tail collapsing while the median stays
healthy is the saturation signature - past the knee, extra offered load buys nothing
and costs everything.

### End-to-end delivery latency

Constant arrival rate of 20 submissions/s for 2 minutes, measuring wall-clock time
from `POST /api/v1/notifications` until the dashboard reports `DELIVERED`. This
spans the API, Kafka publish and consume, deduplication, the SMTP delivery itself
and both status writes.

| Metric | Result |
|---|---|
| Notifications | 2,401 |
| Delivered | 100% |
| Latency - min | 16 ms |
| Latency - median | 63 ms |
| Latency - p90 | 72 ms |
| Latency - p95 | 77 ms |
| Latency - p99 | 254 ms |
| Latency - max | 1.37 s |

20/s stays well inside the worker's ~141/s capacity, so this measures pipeline
latency rather than queue depth. Submitting faster than the worker delivers would
measure how deep the backlog got, which is a different question.

### Worker throughput

The API accepts faster than the worker delivers. One worker instance, 3 consumer
threads, each EMAIL delivery making a real SMTP round trip:

| Metric | Result |
|---|---|
| Delivery rate | ~141 notifications/s |
| Per consumer thread | ~47/s (~21 ms per SMTP exchange) |
| Peak Kafka lag during test | 59,341 messages |
| Time to drain after load stopped | ~7 minutes |
| Messages lost | 0 |
| Duplicate deliveries | 0 |

This is the async architecture doing its job. The API accepted ~800 req/s at the
front door while the worker delivered at ~141/s; Kafka absorbed the ~59,000-message
difference and the worker drained it completely once load stopped, with no message
loss and no duplicate deliveries. A synchronous design would have rejected or timed
out those requests instead.

Scaling the worker is a configuration change, not a code change - more replicas and
more partitions. Partition count is the hard ceiling on parallelism, so going beyond
3 concurrent consumers means raising the partition count first.

### Methodology

Everything runs on one laptop, so a result is only meaningful once the load
generator has been ruled out as the bottleneck. Several worker replicas plus Kafka,
PostgreSQL, Redis and Mailpit, plus a k6 process driving thousands of virtual users,
do not fit comfortably in 4 GB of Docker memory: pushing harder then lowers
throughput, because more generator pressure leaves less CPU for the application,
which raises latency, which makes k6 need still more virtual users. The tests
therefore run with **one worker replica**.

Details that matter for anyone re-running these:

- **Unique `correlationId` per request.** Reusing one would short-circuit every
  request into the idempotency check and measure an indexed `SELECT` rather than
  the full pipeline.
- **`ramping-arrival-rate`, not `ramping-vus`.** With a fixed VU count each user
  waits for a response before sending again, so offered load drops automatically as
  the system slows and the ceiling is never reached. Arrival rate keeps sending at
  the target regardless and reports what it could not send as `dropped_iterations`.
- **Poll interval sets the latency measurement floor.** The end-to-end test polls
  for status, so a notification delivered just after a poll is not observed until
  the next one. A coarse interval (250 ms) produces a median near the interval and
  a tightly clustered distribution - a measurement grid, not a latency
  distribution. The test polls every 50 ms so the interval stays small relative to
  the latencies being measured.

### Running the tests

The load tests send every request from a single client key, so the default
limit of 1,000/min would reject almost everything. Raise it for the run, with
one worker replica, and restart the API afterwards to restore the default:

```bash
COURIER_RATE_LIMIT_MAX_REQUESTS=10000000 docker-compose up -d courier-api

# afterwards, restore the default limit
docker-compose up -d courier-api
```

The API-throughput and end-to-end numbers were measured against the current code.
The worker-throughput figures come from an earlier build and are pending a refresh.

Without the override the limiter does its job: a 20 req/s run is cut to 2,000
accepted requests over two minutes (1,000/min per window) and the rest receive 429s.

```bash
brew install k6

# API throughput - ramping arrival rate. PEAK is the final-stage req/s.
PEAK=800  k6 run load-tests/api-throughput.js   # sustainable load
PEAK=2500 k6 run load-tests/api-throughput.js   # finds the ceiling (default)

# End-to-end latency - constant rate, measures submission to DELIVERED
k6 run load-tests/end-to-end-latency.js
```

Watch consumer lag during a run to see whether the worker is keeping up:

```bash
while true; do
  clear; date +%T
  docker exec courier-kafka kafka-consumer-groups \
    --bootstrap-server localhost:9092 \
    --group courier-worker-group --describe
  sleep 2
done
```

## Project Structure

```
courier/
├── courier-common/          # Shared entities, DTOs, repositories
├── courier-api/             # REST API - submission and routing (port 8080)
├── courier-worker/          # Kafka consumer - delivery engine (port 8081)
├── courier-dashboard/       # Read API - status and statistics (host port 8084)
├── load-tests/              # k6 throughput and end-to-end latency scripts
├── docker-compose.yml       # Full system: Kafka, Redis, PostgreSQL, all services
├── init.sql                 # PostgreSQL schema
└── docs/
    └── architecture.png
```
    

