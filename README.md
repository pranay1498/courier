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
  -d '{
    "correlationId": "my-first-notification",
    "channel": "WEBHOOK",
    "recipient": "https://webhook.site/your-unique-url",
    "content": "Hello from Courier"
  }'
```

Swagger UI available at:
- courier-api: http://localhost:8080/swagger-ui.html
- courier-dashboard: http://localhost:8082/swagger-ui.html

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

### courier-dashboard (port 8082)
| Method | Endpoint | Description |
|---|---|---|
| GET | /api/v1/dashboard/notifications/{id} | Get status (Redis-backed) |
| GET | /api/v1/dashboard/notifications/{id}/attempts | Get delivery attempt history |
| GET | /api/v1/dashboard/notifications | Paginated listing with filters |
| GET | /api/v1/dashboard/stats | Delivery statistics per channel |


## Project Structure

```
courier/
├── courier-common/          # Shared entities, DTOs, repositories
├── courier-api/             # REST API - submission and routing (port 8080)
├── courier-worker/          # Kafka consumer - delivery engine (port 8081)
├── courier-dashboard/       # Read API - status and statistics (port 8082)
├── docker-compose.yml       # Full system: Kafka, Redis, PostgreSQL, all services
├── init.sql                 # PostgreSQL schema
└── docs/
    └── architecture.png
```
    

