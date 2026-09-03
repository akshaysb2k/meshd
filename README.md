# meshd

An L7 reverse proxy and service mesh data plane, written in Go with **no external dependencies**.

Health checking, outlier ejection, circuit breaking, budgeted retries, request hedging, consistent-hash session affinity, and a streaming control plane that pushes configuration without dropping connections.

Everything is standard library. The Prometheus exposition format, the consistent hash ring, and the deterministic clock are all written here rather than imported.

## What it does under failure

Three backends, 300 rps, 20 seconds. At t=6s one backend begins returning 503 for every request; at t=13s it recovers.

```
requests   6000
success    6000 (100.000%)
errors     0 transport, 0 5xx
p50 4.93ms   p90 5.61ms   p99 6.23ms   p999 11.03ms
```

The per-second timeline shows the entire incident as one blip:

```
second,requests,failures,p50_ms,p99_ms
5,300,0,5.38,5.76
6,300,0,5.39,11.08   <- backend fails; retries absorb it
7,300,0,4.61,5.75    <- ejected, traffic redistributed
...
```

Traffic distribution afterwards tells the rest: the failing backend took 1,066 requests while the healthy two took 2,516 and 2,439, because it was out of rotation for the failure window.

Reproduce with `make demo`.

## Quick start

```bash
make build

# three upstreams
./bin/backend -addr :9001 -name b1 &
./bin/backend -addr :9002 -name b2 &
./bin/backend -addr :9003 -name b3 &

# the proxy
./bin/meshd -config examples/config.json -addr :8080 -admin-addr :9901 &

curl localhost:8080/api/users
curl localhost:9901/clusters      # live endpoint state
curl localhost:9901/metrics       # Prometheus

# break a backend and watch it get ejected
curl "localhost:9002/_control?error_rate=1.0"
```

Or `docker compose -f deploy/docker-compose.yml up`, which also starts the control plane.

## Design

The data plane is a request pipeline; everything interesting hangs off the cluster layer.

```
                        ┌───────────────┐
                        │ control plane │  streaming ndjson
                        └───────┬───────┘
                                │ versioned snapshot
   ┌──────────┐  ┌────────┐  ┌──▼──────────┐  ┌──────────┐
   │ listener │─▶│ router │─▶│ cluster+LB  │─▶│ upstream │─▶ backend
   └──────────┘  └────────┘  └──▲───────▲──┘  └──────────┘
                                │       │
                    ┌───────────┴──┐ ┌──┴──────────────┐
                    │ active probe │ │ outlier ejection│
                    └──────────────┘ └─────────────────┘
```

Configuration is immutable and versioned. A push is validated, a whole new object graph is built, and the routing table is swapped with a single atomic store. In-flight requests keep the snapshot they matched against.

Full detail in [docs/design.md](docs/design.md). Operational guidance in [docs/runbook.md](docs/runbook.md).

## Features

**Load balancing** — smooth weighted round robin, power-of-two-choices least request, and ring hash for session affinity. All three respect slow-start weights.

**Health** — active probing with hysteresis and jitter, plus passive outlier ejection with a growing ejection window and a cap on the ejected fraction. A **panic threshold** discards health status entirely when too much of the fleet looks unhealthy, because at that point the signal is more likely wrong than the fleet is.

**Retries** — a retry *budget* expressed as a percentage of the cluster's active requests, not a fixed attempt count. Plus full-jitter exponential backoff, per-try timeouts, and idempotency enforcement.

**Hedging** — a second attempt fires when the first exceeds a delay; first response wins, loser is cancelled. Steered to a different endpoint and drawn from the same budget as a retry.

**Circuit breaking** — per-cluster concurrency ceilings that shed load rather than queue it.

**Draining** — removed endpoints stop receiving traffic synchronously at the config swap, finish in-flight requests, then close their pools. On shutdown, readiness flips first so the upstream load balancer can react before the listener closes.

## Deterministic simulation

`internal/simulation` runs the entire proxy against a scripted network with no sockets, no goroutines, and no wall clock. Time is a manually advanced fake, failures are drawn from a seeded PRNG, and requests are issued synchronously.

A scenario is therefore a pure function of its seed, so `seed 4471 fails on step 38` is a complete and permanent bug report. Invariants are checked across many seeds:

- no client-visible 5xx while any endpoint is reachable
- the ejected fraction never exceeds `max_ejection_percent`
- amplification stays within the attempt cap
- the same seed replays byte-identically, down to the per-host hit distribution

It also covers a case integration tests cannot reach cleanly: a backend whose `/healthz` returns 200 while every real request fails. Active probing never marks it unhealthy; outlier detection ejects it anyway. That test is why both mechanisms exist.

```bash
make race    # full suite including simulation, race detector on
```

## Bugs the tests found

Worth listing, because none were visible by reading the code.

| Found by | Bug |
|---|---|
| ring hash remap test | FNV-1a alone skewed the hash ring badly — one endpoint owned 32% of the ring, another 6%, and losing one of five remapped **64%** of keys instead of 20%. Adding the SplitMix64 finalizer brought it to 21.3%. |
| least-request distribution test | Power-of-two-choices was sampling **with replacement**, so with two candidates the loaded one still won a quarter of decisions. The algorithm was silently degrading toward random. |
| config reconciliation test | Endpoints were flagged draining inside a goroutine, leaving a window where a removed endpoint still received traffic. |
| circuit breaker test | Shed requests returned 502 and were retryable — so they looked like backend failures on dashboards, and retrying them defeated the shedding. |
| `-race` under config push | `Manager.Apply` mutated a live cluster's picker and breaker while requests read them; the mutex only guarded endpoints. |
| slow-start simulation | An assertion that passed *vacuously* because the endpoint wasn't ramping, it was still unhealthy. |

Two more were found by reading: hedged attempts could land on the same endpoint as the original, and cancelling the shared hedge context would have killed the winner's response body mid-stream.

## Layout

```
cmd/          meshd (data plane), meshcp (control plane), backend, loadgen
internal/
  proxy/      request path, forwarding, admin surface
  cluster/    endpoint state, cluster, config reconciliation
  balancer/   round robin, least request, ring hash
  health/     active prober, outlier detector, supervisor
  retry/      policy, budget, jittered backoff
  breaker/    resource-limit circuit breaking
  router/     specificity-ordered route matching
  xds/        streaming control plane client and server
  config/     versioned snapshots and validation
  clock/      real and deterministic fake clocks
  metrics/    Prometheus registry and text exposition
  simulation/ scripted network and seeded scenario driver
```
