# build-server

Shared GitHub Actions runner pool across one or more VPS nodes, with RAM-aware scheduling.

## How it works

```
GitHub ──webhook──► MASTER ──┬── spawn locally (if it has the most free RAM)
                             │
                             ├── POST /spawn → slave-1
                             │
                             └── POST /spawn → slave-N
```

- An org-level GitHub webhook fires `workflow_job.queued` for every queued job
- **Master** is the only node GitHub talks to. It polls every worker's `/capacity` in parallel and dispatches the job to whichever node has the most free RAM (itself included).
- **Slaves** expose only `/capacity` and `/spawn` (authenticated with `INTERNAL_SECRET`). They never see GitHub directly.
- Each chosen worker re-checks its own capacity before accepting (race-safe) and either spawns or returns 503.
- If the entire fleet returns 503, master holds the job in a pending queue, retries every 5s, and gives up after 1h.

### Scheduler

Concurrency on each node is gated by free RAM (`/proc/meminfo MemAvailable`), not a fixed runner count.

| Knob | Default | Meaning |
|---|---|---|
| `MIN_FREE_RAM_MB` | 3072 | Always keep this much free. Below = refuse. |
| `WORST_CASE_RUNNER_MB` | 3072 | Assumed peak per runner (single tunable, no per-image table). |
| `SPAWN_COOLDOWN_S` | 15 | Only applies when headroom < worst-case. Lets MemAvailable catch up. |

```python
avail = mem_available_mb()
if avail < MIN_FREE_RAM_MB:                 return False  # refuse
headroom = avail - MIN_FREE_RAM_MB
if headroom >= WORST_CASE_RUNNER_MB:        return True   # burst
if (now - last_spawn_at) < SPAWN_COOLDOWN_S: return False  # throttle
return True                                                # tight ok
```

| MemAvailable | headroom | behavior |
|---|---|---|
| 12 GB | 9 GB | burst (3 heavy runners in parallel) |
| 6 GB | 3 GB | one more burst |
| 5 GB | 2 GB | 1 spawn / 15s |
| < 3 GB | — | refuse → next worker / queue |

## Runner image per job

Add a `runner-image:` label to `runs-on` to use a custom image:

```yaml
runs-on: [self-hosted, runner-image:ghcr.io/my-org/my-runner:latest]
```

Jobs without this label use `DEFAULT_RUNNER_IMAGE` from `.env`.

## Setup

### 1. Provision each node (master + slaves) identically

```bash
ssh root@<NODE-IP>
curl -fsSL https://raw.githubusercontent.com/jakwuh/build-server/main/setup.sh | bash
cp /opt/build-server/.env.example /opt/build-server/.env
# Edit .env — GITHUB_APP_*, WEBHOOK_SECRET (master), INTERNAL_SECRET (all nodes, identical)
systemctl start build-server
```

Every node needs GitHub App credentials (each mints its own installation token before spawning).

### 2. Wire master ↔ slaves

On master, add to `/opt/build-server/.env`:
```bash
SLAVE_URLS=http://<slave-1-ip>,http://<slave-2-ip>
```
Restart master: `systemctl restart build-server`.

### 3. Connect an org (master only)

```bash
gh api --method POST /orgs/{ORG}/hooks \
  --field name=web \
  --field active=true \
  --field events='["workflow_job"]' \
  --field config[url]="http://<MASTER-IP>/webhook" \
  --field config[content_type]=json \
  --field config[secret]="<WEBHOOK_SECRET from master .env>"
```

Repeat per org. No GitHub config touches slaves — only master receives webhooks.

### 4. Add a new slave later

1. Provision the new node (step 1) with the same `INTERNAL_SECRET`.
2. Append its URL to master's `SLAVE_URLS`.
3. Restart master.

No changes needed in GitHub, no changes on existing slaves.

## Tuning

Defaults are conservative for a 12 GB host with a mix of linux/android jobs.

- **Lots of small linux jobs only?** Drop `MIN_FREE_RAM_MB` and `WORST_CASE_RUNNER_MB` to 1024 each → higher density, faster ramp-up.
- **Mostly android/e2e?** Defaults are fine. If you OOM, raise `WORST_CASE_RUNNER_MB` to your observed peak; the cooldown will kick in earlier.
- **Host has lots of swap?** Don't lean on it — swap protects from OOM-kill but not from thrash. Treat swap as a safety net, keep `MIN_FREE_RAM_MB` realistic.
