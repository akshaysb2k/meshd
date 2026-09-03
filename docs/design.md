# Design

## Request lifecycle

A request enters the listener, is matched to a route, resolved to a cluster, balanced onto an endpoint, and forwarded through that endpoint's private connection pool. On the way back, the result feeds the outlier detector.

The commit point matters: nothing is written to the client until an attempt has produced a usable response. That is what makes retrying safe. Once headers go out, the request is no longer replayable.

## Configuration is immutable and versioned

The control plane hands over a whole snapshot. The proxy validates it, builds a new routing table and reconciles clusters, then swaps the table with a single `atomic.Pointer.Store`. In-flight requests keep the table they matched against.

Ordering is deliberate: **validate, build, reconcile, swap**. A snapshot that fails validation leaves the running configuration untouched. A broken config file should never be able to take a proxy down, and an operator should see the error at push time rather than discover it in a log.

### Reconciliation, not replacement

The subtle part is that an endpoint present in both the old and new snapshot keeps its exact `Endpoint` object — same connection pool, same health state, same ejection history. Only genuinely removed endpoints are drained.

Rebuilding everything on each push is the obvious implementation and it is wrong. It discards warm pools and health state, so a routine config change shows up as a latency spike and a burst of 502s. `TestConfigPushPreservesEndpointIdentity` asserts pointer identity across a push specifically to protect this.

Removed endpoints are marked draining **synchronously**, before the swap. If the flag were set inside the drain goroutine there would be a window in which a retired endpoint still received traffic.

## Load balancing

**Smooth weighted round robin** (the nginx algorithm) rather than naive WRR. Naive WRR bunches a heavy endpoint's turns together, which defeats slow start — a ramping endpoint would still get a burst. Smooth WRR interleaves, so a 0.1-weight endpoint gets one request in ten spread evenly.

**Least request** via power-of-two-choices. Scanning for the true minimum is O(n) and a herding hazard: every proxy replica computes the same minimum and stampedes the same backend. Sampling two at random lands within a few percent of optimal at O(1) with no coordination. Sampling must be *without replacement*; with replacement, two candidates means the loaded one still wins a quarter of decisions.

**Ring hash** for session affinity. Each endpoint occupies many points on a 64-bit ring; a key takes the next endpoint clockwise. Removing one of N moves roughly 1/N of keys rather than reshuffling everything.

The hash function needs a real finalizer. FNV-1a alone, on short similar strings like `ep-3#41`, clusters badly: measured over five endpoints at 200 replicas, one owned 32% of the ring and another 6%, and losing one endpoint remapped 64% of keys. FNV-1a plus SplitMix64 gives 21.3%, against a theoretical 20%.

## Health: two mechanisms, deliberately

**Active probing** catches a backend that is down but receiving no traffic. It applies hysteresis — N consecutive failures out, M consecutive successes back — because without it a single dropped probe flaps the cluster. Probe times are jittered so the fleet is not hit in lockstep, which also stops every proxy replica agreeing on a false negative simultaneously.

**Outlier detection** catches the case active probing cannot: a backend whose `/healthz` returns 200 while real requests fail. `TestLyingHealthEndpointIsCaughtOnlyByOutlierDetection` is exactly this scenario.

Two safety valves matter more than the detection rule:

- The ejection window **grows** with each ejection, so a repeat offender stays out longer instead of flapping back every few seconds.
- The ejected fraction is **capped**. A bad deploy that breaks every backend must not cause the detector to eject the whole fleet and take the service to zero.

### Panic threshold

When the healthy fraction falls below a threshold, health status is discarded and every endpoint becomes a candidate.

This looks wrong until you consider the alternative. If 90% of a fleet is marked unhealthy, the health signal is more likely broken than the fleet is — and funnelling all traffic onto the surviving 10% guarantees those die too. Spreading load across degraded backends beats concentrating it on doomed ones.

## Retries

A fixed `max_attempts` is how a partial outage becomes a total one: backends slow, every client retries, offered load multiplies, remaining capacity dies.

The **retry budget** caps concurrent retries as a percentage of the cluster's active requests. It is generous in steady state and clamps hard during an incident, automatically. Under concurrent load the integration test measures 1.23x amplification against a 4x attempt cap.

An important property the simulation makes visible: the budget is a **concurrency gate, not a rate limit**. With strictly sequential requests the active count is always one, each request gets its slot and hands it back, and amplification lands exactly on the attempt cap. Both facts are true and neither test shows both.

Three gates must all allow a retry: the budget, the breaker's retry ceiling, and the backoff timer. Backoff uses **full jitter** — a uniform draw from `[0, min(max, base·2^(n-1))]` — because the goal is decorrelating clients, not guaranteeing a minimum wait.

Retries are restricted to idempotent methods unless a route opts in, and a request whose body exceeded the replay buffer is not retryable at all. Buffering unbounded uploads to enable retries just converts a resilience feature into an OOM.

## Hedging

If the first attempt has not answered within a delay, a second fires; the first usable response wins and the loser is cancelled. This trades a little extra load for a large cut in tail latency — p99 stops being "whichever backend had a GC pause".

Three constraints make it safe: idempotent requests only, the hedge is steered to a *different* endpoint than the original, and it draws from the same budget as a retry so it switches itself off during an incident.

Each attempt owns its own cancellable context. A shared context would mean cancelling the losers also killed the winner's response body mid-stream.

## Circuit breaking

These are **resource limits**, not the classic closed/open/half-open state machine. Per-cluster ceilings on pending requests, concurrent requests, and concurrent retries. Exceeding one sheds immediately rather than queueing.

The state-machine behaviour people usually mean is per-endpoint and lives in outlier detection. Keeping the two separate is worth being precise about.

Shed requests return 503 with a diagnostic reason header, and are **not** retryable — retrying a shed request defeats the purpose of shedding.

## Draining

Two cases.

On **config change**, a removed endpoint stops receiving traffic at the swap, in-flight requests finish under a deadline, then its idle connections close.

On **shutdown**: readiness flips to not-ready first, a delay lets the upstream load balancer notice, then the listener stops accepting, in-flight requests finish, and upstream pools drain. Skipping the readiness step is why nominally graceful deploys still produce connection resets.

## Control plane

Snapshots stream as newline-delimited JSON over a long-lived connection rather than being polled. The useful property is bounded staleness, and polling makes staleness equal to the poll interval even when nothing changes.

Reconnects use full-jitter backoff, so a control plane restart is not immediately followed by every data plane instance reconnecting in lockstep and knocking it over again. A subscriber too slow to keep up is skipped rather than allowed to block the publisher; it receives the current snapshot on reconnect.

## Determinism

`internal/simulation` replaces every source of nondeterminism: the clock is manually advanced, the network is a scripted `RoundTripper`, failures come from a seeded PRNG, requests are synchronous, and worker ordering is sorted rather than map-iteration order.

One deliberate deviation: in simulation, waiting *advances* time rather than blocking on it. A real wait would deadlock, since the only thing that could advance the clock is the driver, which is itself blocked inside the request. Advancing on demand is also the more faithful discrete-event model. The cost is that genuine concurrency between a sleeping request and other work cannot be represented, so hedging is exercised by integration tests instead.

Least-request balancing is likewise not simulated, because its sampling draws from the global random source; simulated clusters use round robin.
