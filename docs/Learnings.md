## What I Learned

- **Spring proxy self-invocation:** Spring applies annotations like `@Async` and
  `@Transactional` by wrapping your bean in a proxy. When an external caller
  invokes a method, the call passes through the proxy and the annotation fires.
  When a method calls another method on the same class (`this.method()`), it
  bypasses the proxy entirely - the annotation is silently ignored. I hit this
  with `StatusTrackingService` calling its own `updateStatusInDb()` method, which
  caused `TransactionRequiredException` at runtime. The fix was extracting the
  `@Async @Transactional` method into a separate `NotificationStatusUpdater` bean
  so the call goes through that bean's proxy.

- **Deduplication key timing matters:** My first implementation set the Redis dedup
  key at the start of processing (on first check). This blocked all retries - a
  failed first attempt would permanently prevent the message from being processed
  again. The correct approach is to set the key only after successful delivery, so
  retries are allowed until one succeeds.

- **`orElse` vs `orElseGet` for idempotency:** Java's `Optional.orElse(method())`
  evaluates the argument eagerly - the method runs regardless of whether the
  Optional is empty. Using `orElse(processNewNotification(request))` meant a new
  notification was being created even for duplicate requests. Switching to
  `orElseGet(() -> processNewNotification(request))` evaluates the supplier lazily,
  only when the Optional is actually empty.

- **JPQL bulk updates bypass Hibernate lifecycle hooks:** The `@UpdateTimestamp`
  annotation only fires when Hibernate manages the entity through `save()`. JPQL
  `UPDATE` queries bypass the entity lifecycle entirely and go straight to SQL.
  This meant `updatedAt` in the DB was always behind the timestamp written to
  Redis. The fix was to capture `Instant.now()` once and explicitly pass it to
  both the Redis write and the JPQL query parameter.

- **At-least-once delivery is the default, not exactly-once:** Kafka guarantees
  messages are delivered at least once. A consumer that crashes after processing
  but before committing its offset will receive the same message again on restart.
  Building idempotent consumers (via Redis SETNX deduplication) is the standard
  pattern for handling this - simpler and more portable than Kafka's exactly-once
  semantics (transactions), which add significant complexity and performance
  overhead.