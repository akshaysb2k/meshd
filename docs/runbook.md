# Runbook

## Surfaces

| Endpoint | Purpose |
|---|---|
| `:9901/clusters` | live endpoint state: healthy, ejected, draining, active requests, weight, retry budget |
| `:9901/routes` | compiled routing table in match order |
| `:9901/metrics` | Prometheus |
| `:9901/ready` | 503 once draining begins |
| `:9901/healthz` | liveness |

Every response carries `X-Request-Id`. Proxy-generated errors also carry `X-Proxy-Reason`, and successful responses carry `X-Upstream-Endpoint`.

## Reading a degraded cluster

```bash
curl -s localhost:9901/clusters | jq '.clusters[] | {name, panic_mode, healthy, total, retry_budget}'
```

- `panic_mode: true` — more than half the fleet looks unhealthy, so health status is being ignored and traffic is spread across everything. Treat this as *the health signal is suspect*, not merely "backends are down". Check whether probes can reach the fleet at all before restarting anything.
- `retry_budget.denied` climbing — retries are being suppressed. This is the system working, but it means the cluster is failing enough that amplification control has engaged.
- `ejection_count` high on one endpoint with others at zero — a single bad instance. Ejection windows grow, so it will stay out longer each time.

## Key metrics

```
meshd_requests_total{route,cluster,code}
meshd_request_duration_seconds{route,cluster}
meshd_upstream_attempts_total{cluster,endpoint,outcome}
meshd_retries_total{cluster}
meshd_retries_denied_total{cluster,gate}         # gate: budget | breaker
meshd_breaker_overflow_total{cluster,limit}      # limit: pending | requests | retries
meshd_outlier_ejections_total{cluster,endpoint,reason}
meshd_endpoints_ejected{cluster}
meshd_endpoint_healthy{cluster,endpoint}
meshd_cluster_panic_mode{cluster}
meshd_config_pushes_total{result}                # result: applied | rejected | failed
```

Amplification factor is `rate(meshd_upstream_attempts_total) / rate(meshd_requests_total)`. Sustained above ~2 during an incident means the budget is too loose.

## Symptoms

**Client 503 with `X-Proxy-Reason: breaker_requests`** — load shedding, not backend failure. The cluster hit its concurrency ceiling. Either the backends slowed down or the limit is set below real demand. Check `meshd_breaker_overflow_total` by `limit` to see which ceiling.

**Client 503 with `no_healthy_endpoints`** — every endpoint is unhealthy, ejected, or draining. Check whether a config push removed them (`/clusters` shows `draining: true`).

**Client 504 `timeout`** — the route timeout fired. If `per_try_timeout` is unset, one slow attempt can consume the whole budget and leave no room for a retry.

**Latency spike at deploy time** — check that endpoints were retained rather than rebuilt: `meshd_config_pushes_total{result="applied"}` incrementing alongside a jump in connection setup means reconciliation is not preserving pools.

**Config push had no effect** — check `meshd_config_pushes_total{result="rejected"}` and the proxy log. A rejected snapshot leaves the running config intact by design.

## Deploys

The shutdown sequence is: readiness fails → `ready-delay` elapses → listener stops accepting → in-flight requests finish → upstream pools drain.

Set the orchestrator's termination grace period **above** `drain-timeout + ready-delay`, or the process is killed mid-drain and the guarantee is void. The compose file uses `stop_grace_period: 30s` against a 20s drain.

## Reproducing a simulation failure

Simulation failures name a seed. Scenarios are pure functions of their seed, so:

```bash
go test ./internal/simulation/ -run TestInvariantNoLostRequests -v
```

replays the identical scenario. The failure message includes the full trace: per step, the simulated time and every endpoint's state.

## Chaos drills

The bundled backend can be mutated at runtime:

```bash
curl "localhost:9002/_control?error_rate=1.0"   # every request 503s
curl "localhost:9002/_control?healthy=false"    # /healthz fails, real traffic fine
curl "localhost:9002/_control?blackhole=true"   # accepts and never answers
curl "localhost:9002/_control?latency_ms=500"   # slow, for hedging
curl "localhost:9002/_control?error_rate=0"     # recover
```

`error_rate=1.0` with `healthy` left true is the interesting one: probes pass, real traffic fails, and only outlier detection catches it.
