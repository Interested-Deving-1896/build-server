# build-server

Shared GitHub Actions runner pool. Two components, mix and match per host.

## Architecture

```
GitHub ──webhook──► GATEWAY ──┬── POST /spawn → RUNNER (local, 127.0.0.1:3001)
                              │
                              ├── POST /spawn → RUNNER on box-2
                              │
                              └── POST /spawn → RUNNER on box-N
```

| Component | Role |
|---|---|
| **gateway** | Single GitHub webhook entry. Polls each runner's `/capacity` and dispatches each job to whichever has the most free RAM. Tracks per-job SLA (queued→in_progress latency, alerts on breach). Periodically scans GitHub for orphan queued jobs and synthesises spawns. |
| **runner** | Exposes `/capacity` and `/spawn`. Spawns ephemeral `github-runner` containers, watchdogs them, re-queues failed spawns. RAM-gated scheduler + dockerd semaphore prevent OOM and webhook-stampede. |

**Single-host deploy:** run both on the same machine. Gateway on `:3000`, runner on `:3001`, `WORKERS=http://127.0.0.1:3001`. nginx reverse-proxies port 80 → gateway.

**Multi-host deploy:** one gateway box, N runner boxes. Gateway's `WORKERS=http://runner-1,http://runner-2,...`.

## Scheduler (runner-side)

Single knob: `MIN_FREE_RAM_MB` (default 3072) — how much RAM to always keep free for OS + spike headroom.

```python
avail = mem_available_mb() - inflight_reservation
if avail < MIN_FREE_RAM_MB:                       return False   # refuse
if avail >= 2 * MIN_FREE_RAM_MB:                  return True    # burst
if (now - last_spawn_at) < 15s:                    return False  # throttle (tight band)
return True                                                      # tight ok
```

In-flight reservation: each recent spawn (last 60s) "owes" `MIN_FREE_RAM_MB` worth of allocation in the accounting. Prevents a webhook stampede from racing past the gate before kernel sees real allocation.

Burst is also capped by a docker-daemon semaphore (= vCPU count) so `containers.run()` calls don't pile up at the unix socket.

| MemAvailable (default MIN=3072) | behavior |
|---|---|
| < 3 GB | refuse → next worker / gateway pending queue |
| 3–6 GB | tight, 1 spawn / 15s |
| ≥ 6 GB | burst freely |

## SLA alert (gateway-side)

Measures `queued → in_progress` per `(org, job_id)` from GitHub webhooks. Alerts once via Telegram if the gap exceeds `LATENCY_ALERT_S` (default 300s).

## Runner image per job

Add a `runner-image:` label to `runs-on` to use a custom image:

```yaml
runs-on: [self-hosted, runner-image:ghcr.io/my-org/my-runner:latest]
```

Jobs without this label use `DEFAULT_RUNNER_IMAGE` from the runner's `.env`.

## Setup

### 1. Provision each host

```bash
ssh root@<HOST-IP>
curl -fsSL https://raw.githubusercontent.com/jakwuh/build-server/main/setup.sh | bash
cp /opt/build-server/.env.example /opt/build-server/.env
# Fill in GITHUB_APP_*, WEBHOOK_SECRET (gateway), INTERNAL_SECRET, DEFAULT_RUNNER_IMAGE
```

`setup.sh` installs both `build-server-gateway.service` and `build-server-runner.service`. Disable whichever you don't want for this host.

### 2a. Single-host (gateway + runner on one box)

```bash
echo 'WORKERS=http://127.0.0.1:3001' >> /opt/build-server/.env
systemctl start build-server-runner build-server-gateway
```

### 2b. Multi-host (one gateway, N runners)

On the **gateway** host:
```bash
systemctl disable --now build-server-runner   # optional, if gateway shouldn't also run jobs
echo 'WORKERS=http://runner-1.internal:3001,http://runner-2.internal:3001' >> /opt/build-server/.env
systemctl start build-server-gateway
```

On each **runner** host:
```bash
systemctl disable --now build-server-gateway  # runners don't talk to GitHub
# Remove WEBHOOK_SECRET / WORKERS / LATENCY_ALERT_S / TELEGRAM_* from .env — runner-only.
systemctl start build-server-runner
```

### 3. Connect an org

Point your GitHub App's webhook URL at `http://<GATEWAY-IP>/webhook`. Install the App on each org.

## Tuning

- **Lots of small linux jobs only?** Drop `MIN_FREE_RAM_MB` to ~1024 → higher density.
- **Mostly android/e2e?** Raise to your largest-job peak (e.g. 4096).
- **Host has swap?** Don't lean on it — swap protects from OOM-kill but not from thrash. Keep `MIN_FREE_RAM_MB` realistic.
