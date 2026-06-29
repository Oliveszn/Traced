# TRADEOFFS.md

## Storage Design

I chose an in-memory `map[string][]Span` protected by a `sync.RWMutex`. The key is `trace_id`; the value is a flat slice of every span received for that trace.

**Why not a database?** The verifier runs immediately after a 60-second emit cycle against a server on the same machine. Round-trip latency to Postgres or Redis would add overhead without any durability benefit, if the server restarts, the 30-minute window would rebuild itself within minutes. In-memory is the right call at this scale.

**Why a flat slice per trace, not a tree?** The tree structure only matters at read time (for the dashboard's span detail view). Building and maintaining a tree on every write is wasted work when 99% of requests are writes. Instead, every span is appended to the slice unconditionally, and the tree is implicit in the `parent_span_id` relationships. At read time, the client already receives the flat list and can reconstruct the tree from parent references.

**Why not index by span_id?** The only lookups we do are by `trace_id` (list all spans for a trace) or full scan (list all traces). A secondary index on `span_id` would cost memory and mutex contention with no query benefit for the required API surface.

## Eviction

A background goroutine ticks every 30 seconds and calls `store.Evict()`. Evict holds a write lock, iterates every trace, and removes spans whose `start_time` is older than `now - window`. If all spans in a trace are removed, the trace key is deleted from the map.

Eviction also happens at ingest: spans outside the window are dropped before being stored. This is cheap (one comparison per span) and prevents the map from ever accumulating stale data between eviction ticks.

The 30-second tick interval is a deliberate trade-off. A shorter interval (e.g. 1 second) reduces the staleness window but increases lock contention under write-heavy load. With a 30-minute window, data that is 30m 29s old is not meaningfully different from data that is 30m 00s old for the dashboard's purposes.

## Concurrency

All writes go through a `sync.Mutex` (exclusive lock). All reads go through a `sync.RWMutex` read lock, so concurrent readers don't block each other. The only time readers and writers block each other is when a write is actually in progress.

**Trade-off:** A single mutex is a global bottleneck. Under 20 concurrent workers, this is fine, each write holds the lock for microseconds (a map insert and a slice append). Under very high load (see below), the mutex becomes contended and goroutines queue up waiting for it.

An alternative is sharding: partition `traces` into N sub-maps, each with its own mutex, and route each `trace_id` to a shard via hash. This reduces contention by a factor of N. I chose not to implement sharding because the default load (20 workers, 5 req/s each = 100 req/s) does not saturate a single mutex on modern hardware.

## What Breaks First

Under 10x default load (200 workers, 500 req/s sustained):

The mutex becomes the primary bottleneck. Go's runtime will queue goroutines waiting for `sync.Mutex.Lock()`. Memory usage grows proportionally with ingest rate, at 500 req/s with batch size 10, that's 5,000 spans/s, or ~18M spans over 30 minutes. Each span is roughly 300 bytes (with tags), so peak memory approaches ~5 GB. The server will OOM before the mutex saturates.

The second failure: Go's `net/http` default server queue is not bounded. Under sustained 500 req/s with slow mutex acquisition, the accept queue fills and the OS starts dropping new TCP connections. Clients see connection refused rather than 503.

## What I'd Do Differently

**Sharded mutex map.** Partition traces into 64 shards. Reduces lock contention by ~64x with minimal code complexity.

**Span size cap.** Reject or truncate spans with oversized tag maps to prevent memory exhaustion attacks.

**Structured logging.** Replace `log.Printf` with `slog` (Go 1.21+) for JSON output, making eviction and ingest metrics queryable.

**Graceful shutdown.** The current server stops immediately on SIGINT. A proper implementation would call `server.Shutdown(ctx)` with a deadline, allowing in-flight requests to complete before the process exits.

**Persistent storage option.** For a production collector, spans should be written to an append-only log (e.g. a time-series store or Kafka) before the in-memory index is updated, so a restart doesn't lose the current window. The in-memory map would become a read cache, rebuilt from the log on startup.
