# Performance and Reliability Guidelines

This guide captures the optimization tactics that should accompany the ongoing refactor. Apply these practices incrementally as components are extracted.

## Database

- **Eliminate N+1 queries** by preferring JOINs and batch fetches for related entities (e.g., chats and files). Reuse prepared statements where available.
- **Index frequently filtered columns** (chat id, message id, download status, timestamps, and composite chatId+status) to accelerate pagination and aggregation.
- **Cache hot reads** such as settings, chat lists, and aggregation snapshots with short TTLs; invalidate caches on writes through the `FileCommandRepository` façade.
- **Return Futures only** for repository calls and use `CompositeFuture` when parallelizing multi-entity fetches to avoid blocking event loops.

## Download Pipeline

- **Queue with priorities** in `DownloadEngine`, enforcing configurable concurrency limits and backpressure before dispatching TDLib downloads.
- **Retry with exponential backoff** and circuit breaking for repeated failures; capture last error for observability.
- **Throttle bandwidth** per download or globally where supported to protect system stability on constrained hosts.
- **Disk-space safety**: verify available space before starting large downloads and abort gracefully when thresholds are breached.
- **Stream large files** using chunked I/O to keep memory use low; prefer buffer pooling where Vert.x or TDLib integration allows.

## Automation Rules

- **Strategy-based rules** (size, type, time window, chat, keyword) must be composable and ordered by priority for deterministic scheduling.
- **Rule evaluation** should be cheap; short-circuit when a high-priority rule matches and reuse parsed rule artifacts to minimize allocations.

## Observability

- **Structured logging** with concise context (telegramId, chatId, file uniqueId) to aid debugging.
- **Metrics endpoints** should remain lightweight; avoid dynamic allocations or DB calls in health/metrics handlers.
- **Download state tracking** in `DownloadStateManager` should persist progress snapshots to tolerate restarts and support pause/resume.

## Testing and Safeguards

- **Unit tests for repositories** should assert batched query behavior and cache invalidation.
- **Load-oriented tests** should validate queue throughput, concurrency ceilings, and retry/backoff timing.
- **Resource-usage thresholds** (memory, disk, bandwidth) deserve regression tests where feasible to prevent accidental degradations.
