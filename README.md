# connpool

connpool is a thread safe, bounded connection pool for net.Conn.

It is built for Go services that need connection reuse, hard concurrency limits, context aware dialing, and explicit shutdown behavior.

## Features

- Bounded pool size
- Context aware Acquire and factory calls
- Idle timeout cleanup
- Maximum connection lifetime
- Validate hook before a connection is handed out
- Reset hook before a connection is returned to idle
- Discard path for broken connections
- CloseContext for bounded shutdown waits
- Snapshot stats for basic observability
- Hot Acquire and Release path with zero allocations in steady state

## Install

```bash
go get github.com/mohsanabbas/connpool
```

## Try It

Run the included end-to-end example with one command. No local Go toolchain required — Docker builds and runs everything.

```bash
docker compose up --build
```

This spins up a Valkey container and a Go program that:

- Opens a pool capped at 5 connections
- Runs 10 concurrent workers, each doing a `SET` then `GET` over raw RESP
- Validates each idle connection with a `PING` before handing it out
- Runs a second sequential wave with no new dials to confirm reuse

**How to read the output**

Each log line shows the local TCP port used for the operation. Because the pool is capped at 5, each port appears exactly twice in wave 1 — proof that 10 workers shared 5 sockets, not 10.

```
dialed new connection 172.18.0.3:54321       ← exactly 5 of these
[worker-0] port=172.18.0.3:54321  SET ...    ← first borrow
[worker-5] port=172.18.0.3:54321  SET ...    ← same socket, reused
...
--- wave 2: sequential reuse proof ---
[reuse-0]  port=172.18.0.3:54321  SET ...    ← still same socket, no new dial
```

The final `stats` line confirms zero leaked connections: `Idle:5 InUse:0`.

**Tear down**

```bash
docker compose down
```

## Usage

```go
package main

import (
    "context"
    "net"
    "time"

    "github.com/mohsanabbas/connpool"
)

func main() {
    dialer := &net.Dialer{Timeout: 2 * time.Second}

    pool := connpool.NewPool(
        10,
        30*time.Second,
        func(ctx context.Context) (net.Conn, error) {
            return dialer.DialContext(ctx, "tcp", "127.0.0.1:8080")
        },
        connpool.WithMaxLifetime(5*time.Minute),
    )
    defer func() {
        shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
        defer cancel()
        _ = pool.CloseContext(shutdownCtx)
    }()

    ctx := context.Background()
    conn, err := pool.Acquire(ctx)
    if err != nil {
        panic(err)
    }

    if err := useConn(conn); err != nil {
        _ = pool.Discard(conn)
        return
    }

    if err := pool.Release(conn); err != nil {
        panic(err)
    }
}

func useConn(net.Conn) error {
    return nil
}
```

## API

- NewPool creates a pool with a maximum size, idle timeout, context aware factory, and optional settings.
- Acquire gets a connection or waits until one is available or the context is canceled.
- Release returns a healthy connection to the pool.
- Discard removes a broken connection from the pool and closes it.
- Close shuts the pool down and waits without a timeout for borrowed connections to be returned.
- CloseContext starts shutdown and returns if the provided context expires while waiting.
- Stats returns a point in time snapshot of pool state.
- Underlying returns the factory-created connection behind a pooled connection.

## Options

- WithValidator checks a connection before Acquire returns it.
- WithReset prepares a connection for reuse before Release returns it to idle.
- WithMaxLifetime retires old connections instead of reusing them forever.

## Use Cases

connpool is a good fit when you own the protocol behavior and need to reuse expensive net.Conn values.

- Custom TCP clients
- Custom TLS clients
- Internal RPC clients
- Long running workers that call the same backend repeatedly
- Gateways or proxies that need a hard cap on upstream connections
- Protocol clients that do not already provide their own pool
- Services that need backpressure instead of unbounded dials

For HTTP, database/sql, Redis, gRPC, and other mature clients, prefer the pooling already built into those clients unless you have a specific reason to manage raw net.Conn values yourself.

## Production Notes

Use a context aware factory. The factory receives the same context passed to Acquire, so dials and handshakes can stop when callers time out.

Release only healthy connections. If a request leaves unread protocol data, sees an I O error, or otherwise makes the connection unsafe to reuse, call Discard instead.

Use WithReset or WithValidator when the protocol has session state, deadlines, authentication state, or health checks that must be cleaned up between borrowers.

Use CloseContext during service shutdown when you need a bounded wait. If CloseContext times out, shutdown continues in the background and a later Close call can wait for completion.

Close is a convenience method for callers that want to wait until shutdown is fully complete. It can wait forever if a borrowed connection is leaked or a factory call never returns.

## Errors

- ErrPoolClosed is returned when Acquire is called after shutdown starts.
- ErrReleaseForeignConn is returned when Release or Discard gets a connection not owned by the pool.
- ErrReleaseNotInUse is returned when Release gets an owned connection that is not currently in use.
- ErrFactoryReturnedNilConn is returned when the factory returns nil without an error.
- ErrFactoryReturnedManagedConn is returned when the factory returns a connection already managed by a pool.

## Benchmarks

Command:

```bash
go test -run '^$' -bench 'BenchmarkPool' -benchmem -benchtime=500ms -count=5 .
```

Environment:

- goos: darwin
- goarch: arm64
- cpu: Apple M5
- Go: 1.26.2

These benchmarks use the default pool without validator or reset hooks enabled.

Acquire plus Release:

| Scenario | Time per op | Approx throughput | Allocations |
| --- | --- | --- | --- |
| max1 procs1 | about 92 to 94 ns | about 10.7 to 10.8 million ops per second | 0 allocs/op |
| max4 procs4 | about 316 to 323 ns | about 3.1 to 3.2 million ops per second | 0 allocs/op |
| max16 procs8 | about 293 to 326 ns | about 3.1 to 3.4 million ops per second | 0 allocs/op |
| max64 procs16 | about 339 to 346 ns | about 2.9 million ops per second | 0 allocs/op |

Lock contention by parallelism:

| Scenario | Time per op | Approx throughput | Allocations |
| --- | --- | --- | --- |
| parallel 1 | about 313 to 325 ns | about 3.1 to 3.2 million ops per second | 0 allocs/op |
| parallel 2 | about 353 to 360 ns | about 2.8 million ops per second | 0 allocs/op |
| parallel 4 | about 427 to 439 ns | about 2.3 million ops per second | 0 allocs/op |
| parallel 8 | about 510 to 525 ns | about 1.9 to 2.0 million ops per second | 0 allocs/op |
| parallel 16 | about 618 to 653 ns | about 1.5 to 1.6 million ops per second | 0 allocs/op |
| lock contention at parallel 32 | about 738 to 766 ns | about 1.3 to 1.4 million ops per second | 0 allocs/op |

These benchmarks measure pool bookkeeping only. They do not include network dial time, TLS handshakes, request processing, backend work, or response reads.

In practical terms:

- If a real request takes 1 ms on the wire, a 350 ns checkout and return is about 0.035 percent of that budget.
- Even at heavier contention around 766 ns, pool overhead is about 0.077 percent of a 1 ms request.
- For a 5 ms to 20 ms backend call, pool overhead is usually noise.
- Zero allocations on the hot path means steady state Acquire and Release do not add GC pressure.

The honest takeaway is that connection I O and backend latency should dominate in most production services, while the pool keeps reuse and concurrency limits explicit.
