# Courier - Design Decisions

This document explains the key architectural and implementation decisions made
during the development of Courier. Each decision covers the problem, the options
considered, the choice made, and the tradeoffs accepted.

---

## 1. Save to DB Before Publishing to Kafka

**Problem:** The API must persist the notification and publish it to Kafka. The
order of these two operations determines what happens on failure.

**Options considered:**
- Publish to Kafka first, then save to DB
- Save to DB first, then publish to Kafka

**Decision:** Save to DB first with status `PENDING`. Transition to `QUEUED`
only inside the publish callback, after the broker acknowledges.

**Reasoning:** If Kafka publish happens first and the DB save fails, a Kafka
message exists for a notification that has no DB record. The worker consumes the
event, tries to find the notification by ID, and finds nothing — there is no way
to know what the notification was or who to deliver it to.

Saving first means the record always exists before anything else happens. If the
publish fails, the notification sits in `PENDING` and `ReconciliationJob`
re-publishes it after 5 minutes with a fresh `eventId`. The DLQ replay endpoint
depends on the same guarantee.

The timing of the `QUEUED` transition matters. `kafkaTemplate.send()` is
asynchronous, so at response time the API does not know whether the broker
accepted the message. Setting `QUEUED` right after calling `publish()` would mark
a notification `QUEUED` even when the publish failed - invisible to a reconciler
looking for stuck `PENDING` rows, and silently lost. So the transition happens in
the broker-ack callback: status reflects what is confirmed, not what was attempted.

The transition uses `UPDATE ... WHERE id = ? AND status = 'PENDING'` rather than
a blind write, because the worker can consume the message and set `PROCESSING`
before the API's publish callback fires. Without the guard, the callback would
roll the status backwards.

The publish itself is deferred until after the database transaction commits
(`TransactionSynchronization.afterCommit`). Publishing inside the transaction
lets two things go wrong: the worker can consume the event before its row is
visible, and the broker-ack callback can run its `PENDING -> QUEUED` update
against a row that has not committed yet, matching zero rows and leaving the
notification `PENDING` until the reconciler re-publishes it — a duplicate
delivery. If the publish then fails, the committed `PENDING` row is exactly what
the reconciler looks for.

Both the submit path and the admin replay path hand events to a single
`EventDispatcher`, which owns these rules (publish after commit, `QUEUED` only on
the broker ack, failure leaves the row `PENDING`), so they cannot drift apart.

**Alternatives considered:** an outbox table written in the same transaction and
relayed to Kafka (strongest guarantee, more moving parts), and
`@TransactionalEventListener(AFTER_COMMIT)` (equivalent to the approach above, less
explicit).

**Tradeoff accepted:** The 202 response returns `PENDING` rather than
`QUEUED`. This is less satisfying to read but more honest — the API genuinely
does not know yet. There is also a brief window where the notification is in
Kafka but still `PENDING` in the DB, because the callback has not fired. A
reconciler running in that window could re-publish a message that was in fact
delivered successfully. The 5-minute threshold makes this effectively
impossible, and consumer-side deduplication would not help since the
re-published event carries a new `eventId`.

---

## 2. Manual Kafka Offset Commits

**Problem:** Kafka tracks which messages each consumer group has processed via
committed offsets. The commit strategy determines what happens when a consumer
crashes mid-processing.

**Options considered:**
- Auto-commit (default): Kafka commits the offset on a timer (every 5 seconds)
  regardless of whether the consumer finished processing
- Manual commit: the application calls `ack.acknowledge()` explicitly after
  successful processing

**Decision:** Manual commit with `AckMode.MANUAL_IMMEDIATE`.

**Reasoning:** With auto-commit, if the consumer processes a message and crashes
before the 5-second timer fires, Kafka redelivers the message - which is correct.
But if the consumer crashes after the timer commits the offset but before it
finishes processing, the message is permanently lost. Auto-commit provides
no delivery guarantee.

With manual commit, `ack.acknowledge()` is called only after `deliveryService.deliver()`
succeeds. If the process crashes at any point before the ack, Kafka redelivers.
Combined with Redis SETNX deduplication, this gives effectively-once processing:
at-least-once from Kafka, deduplicated to exactly-once at the application level.

**Tradeoff accepted:** Manual commits require careful exception handling - any
caught exception must be rethrown so the error handler fires. Swallowing an
exception and returning normally would commit the offset, silently losing the message.

---

## 3. Redis SETNX for Deduplication

**Problem:** Kafka guarantees at-least-once delivery - the same message can be
delivered more than once (consumer crash before offset commit, rebalance). A
notification delivered twice is worse than one delivered late.

**Options considered:**
- DB unique constraint check before delivery
- Redis SETNX (SET if Not eXists)

**Decision:** Redis SETNX on `eventId` with a 24-hour TTL, called after successful
delivery.

**Reasoning:** A DB unique constraint check requires two operations: SELECT (check)
then INSERT/UPDATE (act). Between these two operations, a concurrent thread can
perform the same check and both proceed. SETNX is a single atomic operation - it
checks existence and sets the value simultaneously. Only one caller wins; all others
find the key already set.

`eventId` is used as the dedup key (not `correlationId`) because these handle
different layers: `correlationId` is the client's idempotency key checked at the
API layer to prevent duplicate submissions. `eventId` is generated per Kafka publish
and handles Kafka-level redelivery at the consumer layer.

The dedup key is set only after successful delivery, not at the start of processing.
Setting it at the start would block all retries - a failed first attempt would mark
the event as "processed" and subsequent retry attempts would be skipped as duplicates.

**Tradeoff accepted:** If the consumer crashes after delivery but before calling
`markProcessed()`, the dedup key is never set. Kafka redelivers, the consumer
processes again, and the notification is delivered twice. This is an edge case that
cannot be fully eliminated without Kafka transactions. The window is milliseconds
wide and acceptable for a notification system.

---

## 4. Lua Script for Rate Limiting

**Problem:** The rate limiter must check the request count in the current window
and either allow or deny - atomically. A sliding window rate limiter requires three
Redis operations: remove stale entries, count current entries, add this request.

**Options considered:**
- Three separate Redis commands
- A Lua script executed atomically

**Decision:** Lua script loaded via `SCRIPT LOAD` and executed via `EVALSHA`.

**Reasoning:** Redis executes Lua scripts atomically - no other command runs between
the script's operations. Without this, two concurrent requests could both read the
count as 9 (under the limit of 10), both add themselves, and both be allowed through
- even though the true count is now 11. This race condition cannot be prevented
with separate Redis commands.

A sliding window is used over a fixed window because fixed windows have a
vulnerability at the boundary: a client can send the full quota at the end of one
window and the full quota at the start of the next, effectively doubling the allowed
rate in any given period. A sliding window counts requests in a rolling time window,
eliminating this gap.

The Lua script is loaded once at startup (as a Spring `@Bean`) and cached by Redis
as a SHA1 hash. All subsequent calls use `EVALSHA` - faster than sending the full
script on every request.

What the limiter keys on matters as much as how it counts. The key must be stable
across a caller's requests, so a per-request value such as `correlationId` is
unusable: every request would open a fresh, empty bucket and the limiter could never
fire. The key is the authenticated `clientId` (see Decision 10). The limit itself is configuration
(`courier.rate-limit.max-requests`, default 1,000 per client per channel per
minute), so tests can use a tiny limit and load tests can raise it.

**Tradeoff accepted:** If Redis is unavailable, the rate limiter fails open -
requests are allowed through. This is deliberate: blocking all notifications because
the rate limiter is down is worse than allowing some extra traffic temporarily.

---

## 5. Separate Kafka Topics Per Channel

**Problem:** Notifications need to be routed to different delivery handlers
(Email, SMS, Webhook). They could share one topic or use separate topics.

**Options considered:**
- Single topic with a channel field in the message
- Separate topic per channel (`courier.email`, `courier.sms`, `courier.webhook`)

**Decision:** Separate topic per channel, each with 3 partitions.

**Reasoning:** Webhook delivery involves an HTTP call to an external endpoint that
can be slow (timeouts, retries). If webhooks share a topic with emails, a slow
webhook endpoint blocks the partition thread - emails queue up behind webhook
timeouts. Separate topics mean slow webhooks only slow down the webhook consumer;
email delivery is unaffected.

Partition count determines delivery parallelism. With 3 partitions and concurrency
set to 3 in the consumer config, three messages from the same topic are processed
simultaneously by three threads, one per partition. A fourth consumer thread would
sit idle - Kafka never assigns more consumers than partitions.

**Tradeoff accepted:** Three topics means the consumer subscribes to all three
(`@KafkaListener(topics = {"courier.email", "courier.sms", "courier.webhook"})`).
Adding a new channel requires a new topic, a new `NewTopic` bean, and a new delivery
handler. This is intentional - the cost of adding a channel should be visible.

---

## 6. Strategy Pattern for Delivery Handlers

**Problem:** The worker must route each notification to the correct delivery
implementation based on channel. As channels grow, the routing logic must stay
maintainable.

**Options considered:**
- `if-else` or `switch` in the consumer
- Strategy Pattern with a handler registry

**Decision:** `ChannelDeliveryHandler` interface with one implementation per channel,
auto-discovered by Spring and registered in a `Map<Channel, Handler>` at startup.

**Reasoning:** With `if-else`, adding a new channel means modifying the consumer
class - a class that already works. The Strategy Pattern keeps the consumer closed
for modification: adding `PushDeliveryHandler` for push notifications requires only
a new class annotated with `@Component`. Spring injects it automatically via
`List<ChannelDeliveryHandler>` in `DeliveryService`'s constructor. Zero changes to
existing code.

`Collectors.toMap()` throws `IllegalStateException` if two handlers return the same
channel. This catches the programming error of registering two handlers for the same
channel at startup, not at runtime when a message is being processed.

**Tradeoff accepted:** The handler is looked up by channel on every message. This
is a `HashMap.get()` - O(1), negligible overhead.

---

## 7. Async DB Writes from the Worker

**Problem:** The consumer thread must update notification status in both Redis and
PostgreSQL after each delivery. DB writes are slower than Redis writes. If the DB
write blocks the consumer thread, throughput drops.

**Options considered:**
- Synchronous DB write on the consumer thread
- Async DB write on a separate thread pool

**Decision:** Redis updated synchronously, DB updated asynchronously via
`@Async("dbUpdateExecutor")` on a dedicated `ThreadPoolTaskExecutor`.

**Reasoning:** Redis is the primary read path for status queries - the dashboard
checks Redis first and falls back to DB on a miss. Updating Redis synchronously
ensures the status is immediately visible to any query after delivery. The DB write
can happen moments later without affecting the client's view of the system.

If the DB write were synchronous, every delivery would block the consumer thread
for the duration of a network round-trip to PostgreSQL. Under load, this reduces
the number of messages processed per second and risks Kafka's `max.poll.interval.ms`
being exceeded - which triggers a consumer group rebalance.

**Tradeoff accepted:** Between the Redis write and the DB write, there is a brief
window where Redis and DB are inconsistent. If the async DB write fails silently,
the inconsistency persists until the next status update overwrites it. This is
acceptable because Redis serves all reads - clients never see the stale DB value.
A production system would add a reconciliation job to detect and fix persistent
Redis-DB mismatches.

---

## 8. DLQ Replay With a New eventId

**Problem:** When a notification exhausts all retries and moves to DLQ, it must
be possible to re-deliver it after fixing the underlying issue (bad webhook URL,
email provider outage). The replay must not be treated as a duplicate by the worker.

**Decision:** On replay, generate a fresh `eventId` and clear the original event's
dedup key before re-publishing.

**Reasoning:** The `DlqConsumer` calls `markProcessed(eventId)` to set the dedup
key for the original event. This prevents the main consumer from reprocessing the
DLQ message on worker restart. When replay is triggered via the API, a new `eventId`
is generated. The dedup check uses `eventId` - the new ID has no existing dedup
key, so the worker processes it as a fresh event.

If the original `eventId` were reused and its dedup key still existed in Redis
(within the 24-hour TTL), the worker would detect it as a duplicate and skip it
silently. The notification would appear to be replayed (moving on to QUEUED)
but would never actually be delivered.

The replay also resets `status` to `PENDING`, clears `errorMessage`, resets
`retryCount` to 0, and tags `sourceService` as `"courier-api-replay"` so replay
events are distinguishable in logs and metrics from original events.

Replay publishes through the same `EventDispatcher` as a normal submit: the event
is sent after the transaction commits, and the row moves from `PENDING` to `QUEUED`
only when the broker acknowledges it. A failed publish therefore leaves the row
`PENDING` and the reconciler retries it, rather than looking successful to the
operator.

**Tradeoff accepted:** Each replay creates a new `eventId`, so the delivery attempt
history shows attempts from both the original delivery and the replay under the same
`notificationId`. This is correct and intentional - the full audit trail is visible.

### Decision 9: Classifying failures as retryable or permanent

Not every delivery failure is worth retrying. A webhook returning 503 means the
endpoint is temporarily broken and may recover - retrying is correct. A webhook
returning 404 means the endpoint does not exist; retrying three times wastes six
seconds and produces four identical DLQ-bound failures instead of one.

The same applies to input validation. A phone number missing its country code will
never become valid on retry.

`KafkaConsumerConfig` declares `IllegalArgumentException` and `IllegalStateException`
as non-retryable via `errorHandler.addNotRetryableExceptions()`. Delivery handlers
throw `DeliveryException` for transient failures (retry) and `IllegalArgumentException`
for permanent ones (straight to DLQ). This keeps the classification decision in the
handler that has the context to make it, while the retry mechanics stay in the Kafka
error handler.

---

## 10. API-Key Authentication

**Problem:** The API accepts notification submissions and exposes admin operations
(DLQ replay). Callers must be identified - for authorization, and because the rate
limiter needs a stable per-caller identifier (see Decision 4) - and admin
operations must be restricted to a stronger credential.

**Options considered:**
- No authentication
- Static API keys in a request header
- OAuth2 client-credentials with short-lived JWTs
- HMAC-signed requests
- Mutual TLS

**Decision:** Static API keys in an `X-API-Key` header, two roles (`CLIENT`,
`ADMIN`), enforced by a stateless Spring Security filter chain.

**Reasoning:** Courier's callers are backend services, not people, in a single-tenant
deployment. API keys give each caller an identity with the fewest moving parts - no
auth server, no token endpoint, no certificate authority. The identity is what the
rate limiter and audit logs need; the credential type is secondary to that.

Several choices make the simple mechanism hold up:

- **Authenticate and authorize are separate.** `ApiKeyAuthFilter` only establishes
  who the caller is. Which paths that caller may reach lives in `SecurityConfig`, so
  a new endpoint is covered by the authorization rules without touching the filter.
- **The principal is the `clientId`, never the key.** Rate limiting, logging and the
  MDC all see a stable identifier, and the secret goes no further than the filter.
- **Constant-time comparison.** Keys are compared with `MessageDigest.isEqual`.
  `String.equals` returns at the first differing byte, so response time leaks how
  many leading characters of a guess were right.
- **401 versus 403.** An unknown or missing key is 401 (who are you); a valid
  `CLIENT` key on an admin endpoint is 403 (known, and not permitted). Retrying a 403
  with the same key is futile and the status code says so.
- **Fail closed.** The rules end in `denyAll()`. An endpoint added later and not
  matched by an earlier rule is closed by default.
- **CSRF disabled, deliberately.** CSRF defends against a browser silently attaching
  an ambient credential (a cookie). There are no cookies or sessions here - every
  request carries its own header - so there is nothing to ride on.
- **Unsafe configuration cannot reach production.** The dev defaults
  (`...-change-me`) are public in the repository. With the `prod` profile active,
  startup aborts if any key is a placeholder, shorter than 24 characters, blank or
  duplicated, or if a role is mistyped. Error messages name the client, never the key.

**Tradeoff accepted:** Keys are long-lived with no expiry, live in configuration in
plaintext, and rotating one means a redeploy. There are no per-endpoint scopes. The
startup check only enforces under the `prod` profile - a deployment that forgets to
set it gets a logged warning, not a refusal. Refusing unless a `dev` profile is
active would be safer, but it forces every local run and test to opt in.

The upgrade path is OAuth2 client-credentials with short-lived JWTs: Courier becomes
a resource server, the filter is replaced by signature validation, and `clientId`
comes from the token's `client_id` claim. The controller and rate limiter do not
change, because they only ever saw the `clientId`.
