import asyncio
import hashlib
import hmac
import logging
import os
import time
from collections import deque

import docker
import jwt as _jwt
import requests as _requests
from fastapi import FastAPI, Header, HTTPException, Request
from github import GithubIntegration

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

app = FastAPI()
docker_client = docker.from_env()

# ---- GitHub App (master only — slaves don't need it, but presence is harmless) ----
_github_app_id_env = os.getenv("GITHUB_APP_ID")
_github_app_key_env = os.getenv("GITHUB_APP_PRIVATE_KEY")
if _github_app_id_env and _github_app_key_env:
    _private_key = _github_app_key_env.replace("\\n", "\n")
    _app_id = int(_github_app_id_env)
    gi: GithubIntegration | None = GithubIntegration(_app_id, _private_key)
else:
    _private_key = ""
    _app_id = 0
    gi = None  # slave-only deployment

# ---------- Real config (env-tunable) ----------
# Capacity gate. Single knob: how much RAM to always keep free for OS + spike
# headroom. Burst threshold is implicitly 2 × this. Heuristic: worst-case
# runner ≈ reserve floor. Bump for android/e2e hosts, drop on tiny VPSes.
MIN_FREE_RAM_MB = int(os.getenv("MIN_FREE_RAM_MB", "3072"))
# Workload-specific upper bound on a single CI job (default 30 min):
RUNNER_JOB_TIMEOUT = int(os.getenv("RUNNER_JOB_TIMEOUT", "1800"))
# User-facing SLA: alert when commit→runner-pickup gap exceeds this:
LATENCY_ALERT_S = int(os.getenv("LATENCY_ALERT_S", "300"))
# Fleet topology + shared internal secret for master↔slave /spawn:
SLAVE_URLS: list[str] = [u.strip().rstrip("/") for u in os.getenv("SLAVE_URLS", "").split(",") if u.strip()]
INTERNAL_SECRET = os.getenv("INTERNAL_SECRET", "")

# ---------- Internal constants (no env) ----------
# Promoted from env to constants — nobody had a realistic per-deployment reason
# to tune these. If a real reason ever appears, promote back to env.
SPAWN_COOLDOWN_S = 15              # throttle in the tight band (avail < 2 × MIN_FREE_RAM_MB)
PENDING_JOB_DEADLINE_S = 3600      # drop locally-queued jobs older than 1h
PENDING_DRAIN_INTERVAL_S = 5       # drainer tick
WORKER_CAPACITY_TIMEOUT_S = 5.0    # HTTP timeout when polling worker /capacity
RUNNER_IDLE_TIMEOUT = 600          # kill runner that hasn't picked up a job in 10 min
RUNNER_POLL_INTERVAL = 15          # spawn_runner watchdog tick
LATENCY_SWEEP_INTERVAL_S = 30      # SLA sweeper tick
LATENCY_TRACK_TTL_S = 3600         # drop SLA tracking entries older than 1h
QUEUE_POLL_INTERVAL_S = 60         # scan GitHub for orphan queued jobs every minute
QUEUE_POLL_AGE_THRESHOLD_S = 60    # skip jobs younger than this (race with real webhook)
QUEUE_POLL_ENABLED = True          # ops kill switch — set False here if poller misbehaves

DEFAULT_IMAGE = os.environ["DEFAULT_RUNNER_IMAGE"]
ALLOWED_ORGS: set[str] = set(filter(None, os.getenv("ALLOWED_ORGS", "").split(",")))

_TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
_TG_CHAT = os.getenv("TELEGRAM_CHAT_ID", "")

_GITHUB_HOSTED = {
    "ubuntu-latest", "ubuntu-24.04", "ubuntu-22.04", "ubuntu-20.04", "ubuntu-18.04",
    "windows-latest", "windows-2022", "windows-2019",
    "macos-latest", "macos-14", "macos-13", "macos-12",
}
_NON_LINUX_OS = {"Windows", "macOS"}

# ---- Local node state ----
# _can_spawn read + _last_spawn_at write are atomic under _state_lock.
_state_lock = asyncio.Lock()
_last_spawn_at: float = 0.0
_active_runners: int = 0
# Pessimistic in-flight RAM accounting: each recent spawn is assumed to "owe"
# MIN_FREE_RAM_MB worth of allocation until COLD_START_WINDOW_S has elapsed
# (long enough for the container + github-runner agent + actual job process to
# materialise in MemAvailable). Prevents a webhook stampede from racing past
# the RAM gate when 100 runners haven't yet started consuming memory.
COLD_START_WINDOW_S = 60
_recent_spawns: deque[float] = deque()

# Cap concurrent docker.containers.run() calls at vCPU count. dockerd serializes
# API requests at the unix socket; calling 80 spawns at once produces 60s
# ReadTimeouts and silently-lost jobs.
MAX_CONCURRENT_DOCKER_SPAWNS = os.cpu_count() or 4
_docker_spawn_sem = asyncio.Semaphore(MAX_CONCURRENT_DOCKER_SPAWNS)

# ---- FIFO of jobs awaiting capacity. Used on every node (drainer below) ----
_pending: list[dict] = []
# SLA tracking: (org, job_id) -> monotonic_time when workflow_job.queued arrived.
# Cleared on workflow_job.in_progress (success: latency computed) or .completed.
# Sweeper alerts on entries older than LATENCY_ALERT_S.
_job_queued_at: dict[tuple[str, int], float] = {}
_job_alerted: set[tuple[str, int]] = set()
_job_lock = asyncio.Lock()

IS_MASTER = bool(SLAVE_URLS) or gi is not None

log.info(
    "Build server starting: role=%s slaves=%d MIN_FREE_RAM=%dMB BURST_AT=%dMB "
    "DOCKER_SEM=%d JOB_TIMEOUT=%ds SLA_ALERT=%ds DEFAULT_IMAGE=%s ALLOWED_ORGS=%s",
    "master" if IS_MASTER else "slave",
    len(SLAVE_URLS), MIN_FREE_RAM_MB, 2 * MIN_FREE_RAM_MB,
    MAX_CONCURRENT_DOCKER_SPAWNS,
    RUNNER_JOB_TIMEOUT, LATENCY_ALERT_S,
    DEFAULT_IMAGE, ALLOWED_ORGS or "unrestricted",
)


def _tg_notify(text: str) -> None:
    if not _TG_TOKEN or not _TG_CHAT:
        return
    try:
        _requests.post(
            f"https://api.telegram.org/bot{_TG_TOKEN}/sendMessage",
            json={"chat_id": _TG_CHAT, "text": text, "parse_mode": "HTML"},
            timeout=10,
        )
    except Exception:
        log.exception("Telegram notification failed")


# ---------- Local capacity / RAM gate ----------

def _mem_available_mb() -> int:
    """MemAvailable from /proc/meminfo, in MB. Returns 0 if unreadable."""
    try:
        with open("/proc/meminfo") as f:
            for line in f:
                if line.startswith("MemAvailable:"):
                    return int(line.split()[1]) // 1024
    except OSError:
        log.exception("Failed to read /proc/meminfo")
    return 0


def _can_spawn_local(now: float) -> tuple[bool, str, int]:
    """Decide whether THIS host can absorb one more runner right now.

    Returns (ok, reason, mem_available_mb). Caller MUST hold _state_lock
    when committing on the basis of this call (it reads _last_spawn_at,
    _recent_spawns).
    """
    # Decay in-flight reservations: spawns older than COLD_START_WINDOW_S have
    # had time to allocate their share into MemAvailable, so trust the kernel
    # number from here on.
    while _recent_spawns and now - _recent_spawns[0] > COLD_START_WINDOW_S:
        _recent_spawns.popleft()

    raw_avail = _mem_available_mb()
    inflight_mb = len(_recent_spawns) * MIN_FREE_RAM_MB
    effective_avail = raw_avail - inflight_mb

    if effective_avail < MIN_FREE_RAM_MB:
        return False, (
            f"ram_low effective={effective_avail}MB raw={raw_avail}MB "
            f"inflight={inflight_mb}MB ({len(_recent_spawns)} spawns) min={MIN_FREE_RAM_MB}MB"
        ), raw_avail
    headroom = effective_avail - MIN_FREE_RAM_MB
    # Burst when there's room for another reserve-worth of allocation
    # (i.e. effective_avail >= 2 × MIN_FREE_RAM_MB).
    if headroom >= MIN_FREE_RAM_MB:
        return True, (
            f"burst effective={effective_avail}MB inflight={inflight_mb}MB headroom={headroom}MB"
        ), raw_avail
    wait = SPAWN_COOLDOWN_S - (now - _last_spawn_at)
    if wait > 0:
        return False, (
            f"cooldown {wait:.0f}s left effective={effective_avail}MB headroom={headroom}MB"
        ), raw_avail
    return True, f"tight effective={effective_avail}MB headroom={headroom}MB", raw_avail


def _local_capacity_snapshot() -> dict:
    now = asyncio.get_event_loop().time()
    ok, reason, avail = _can_spawn_local(now)
    inflight_count = len(_recent_spawns)
    return {
        "active": _active_runners,
        "mem_available_mb": avail,
        "mem_min_free_mb": MIN_FREE_RAM_MB,
        "mem_burst_at_mb": 2 * MIN_FREE_RAM_MB,
        "inflight_spawns": inflight_count,
        "inflight_reserved_mb": inflight_count * MIN_FREE_RAM_MB,
        "can_spawn": ok,
        "status": reason,
    }


# ---------- HTTP endpoints ----------

@app.get("/capacity")
async def capacity():
    snap = _local_capacity_snapshot()
    snap["pending"] = len(_pending)
    snap["docker_sem_in_flight"] = MAX_CONCURRENT_DOCKER_SPAWNS - _docker_spawn_sem._value  # noqa: SLF001
    return snap


@app.post("/webhook")
async def webhook(request: Request):
    """GitHub webhook entry point. Master-only in practice — slaves can have it
    enabled too, but GitHub only POSTs to the master URL."""
    if gi is None:
        raise HTTPException(status_code=503, detail="this node is slave-only, no GitHub App configured")

    body = await request.body()
    _verify_github_signature(body, request.headers.get("X-Hub-Signature-256", ""))

    payload = await request.json()
    action = payload.get("action")
    if "workflow_job" not in payload:
        return {"ok": True}

    installation_id = (payload.get("installation") or {}).get("id")
    org = payload["repository"]["owner"]["login"]
    owner_type = payload["repository"]["owner"].get("type", "Organization")
    repo_full_name = payload["repository"]["full_name"]
    labels: list[str] = payload["workflow_job"].get("labels", [])
    job_id = payload["workflow_job"]["id"]

    # Track latency across queued -> in_progress regardless of org allowlist
    # (the metric reflects platform health, not whether we'd spawn).
    if all(lbl not in _NON_LINUX_OS for lbl in labels) and not all(lbl in _GITHUB_HOSTED for lbl in labels):
        await _track_job_event(org, job_id, action)

    if action != "queued":
        return {"ok": True}

    if ALLOWED_ORGS and org not in ALLOWED_ORGS:
        log.warning("Ignored job from unlisted org=%s job_id=%d", org, job_id)
        return {"ok": True}

    if all(lbl in _GITHUB_HOSTED for lbl in labels):
        return {"ok": True}

    if any(lbl in _NON_LINUX_OS for lbl in labels):
        log.info("Skipped non-Linux job: org=%s job_id=%d labels=%s", org, job_id, labels)
        return {"ok": True}

    if not installation_id:
        installation_id = _resolve_installation_id(org, owner_type)
    if not installation_id:
        log.warning("Could not resolve installation_id for org=%s job_id=%d — skipping", org, job_id)
        return {"ok": True}

    job = {
        "installation_id": installation_id,
        "org": org,
        "owner_type": owner_type,
        "repo_full_name": repo_full_name,
        "labels": labels,
        "job_id": job_id,
        "enqueued_at": asyncio.get_event_loop().time(),
    }

    log.info("Job queued: org=%s job_id=%d labels=%s", org, job_id, labels)
    asyncio.create_task(_route(job))
    return {"ok": True}


@app.post("/spawn")
async def spawn_endpoint(request: Request, x_internal_secret: str = Header(default="")):
    """Internal: master delegates a job here when this node has the most free RAM.
    Returns 503 if local capacity changed in the meantime — caller should retry
    another worker."""
    if not INTERNAL_SECRET:
        raise HTTPException(status_code=500, detail="INTERNAL_SECRET not configured")
    if not hmac.compare_digest(x_internal_secret, INTERNAL_SECRET):
        raise HTTPException(status_code=401, detail="bad internal secret")

    job = await request.json()
    # Re-verify capacity locally — race between capacity poll and arrival here.
    async with _state_lock:
        ok, reason, _ = _can_spawn_local(asyncio.get_event_loop().time())
        if not ok:
            log.info("Spawn rejected: org=%s job_id=%s (%s)", job.get("org"), job.get("job_id"), reason)
            raise HTTPException(status_code=503, detail=reason)
        _commit_spawn(job, reason)
    return {"ok": True}


# ---------- Local spawn bookkeeping ----------

def _commit_spawn(job: dict, reason: str) -> None:
    """Caller MUST hold _state_lock. Schedules spawn_runner as a background task."""
    global _last_spawn_at, _active_runners
    now = asyncio.get_event_loop().time()
    _last_spawn_at = now
    _active_runners += 1
    _recent_spawns.append(now)  # ages out of COLD_START_WINDOW_S automatically in _can_spawn_local
    log.info(
        "Spawning runner locally: org=%s job_id=%s active=%d inflight=%d (%s)",
        job["org"], job["job_id"], _active_runners, len(_recent_spawns), reason,
    )
    asyncio.create_task(spawn_runner(job))


# ---------- Master-side routing ----------

async def _worker_capacity(url: str) -> dict | None:
    """Poll a single worker's /capacity. url='' means local. Returns None on failure."""
    if not url:
        snap = _local_capacity_snapshot()
        snap["url"] = ""
        return snap
    loop = asyncio.get_event_loop()
    try:
        resp = await loop.run_in_executor(
            None,
            lambda: _requests.get(f"{url}/capacity", timeout=WORKER_CAPACITY_TIMEOUT_S),
        )
        if not resp.ok:
            return None
        data = resp.json()
        data["url"] = url
        return data
    except Exception:
        log.warning("Worker %s capacity poll failed", url)
        return None


async def _pick_worker_order() -> list[str]:
    """Poll all workers in parallel, return URLs sorted best-first.
    'Best' = can_spawn=True ranked by mem_available_mb DESC.
    Unreachable workers are dropped. Unable-to-spawn workers come last (best-effort
    fallback in case live state shifts during the request)."""
    targets = [""] + SLAVE_URLS  # "" = local
    results = await asyncio.gather(*[_worker_capacity(t) for t in targets])
    able = [r for r in results if r and r.get("can_spawn")]
    able.sort(key=lambda r: r.get("mem_available_mb", 0), reverse=True)
    unable = [r for r in results if r and not r.get("can_spawn")]
    unable.sort(key=lambda r: r.get("mem_available_mb", 0), reverse=True)
    return [r["url"] for r in able + unable]


async def _dispatch(worker_url: str, job: dict) -> bool:
    """Send job to a worker. worker_url='' means local. Returns True if accepted."""
    if not worker_url:
        async with _state_lock:
            ok, reason, _ = _can_spawn_local(asyncio.get_event_loop().time())
            if not ok:
                return False
            _commit_spawn(job, f"local {reason}")
            return True
    loop = asyncio.get_event_loop()
    try:
        resp = await loop.run_in_executor(
            None,
            lambda: _requests.post(
                f"{worker_url}/spawn",
                json=job,
                headers={"X-Internal-Secret": INTERNAL_SECRET},
                timeout=10,
            ),
        )
        if resp.status_code == 200:
            log.info("Dispatched: org=%s job_id=%s → %s", job["org"], job["job_id"], worker_url)
            return True
        if resp.status_code == 503:
            log.info("Worker %s busy (503): %s", worker_url, resp.text[:200])
            return False
        log.warning("Worker %s rejected: %d %s", worker_url, resp.status_code, resp.text[:200])
        return False
    except Exception:
        log.exception("Dispatch to %s failed", worker_url)
        return False


async def _route(job: dict) -> None:
    """Try each worker in order; if all reject, enqueue locally for the drainer."""
    order = await _pick_worker_order()
    for url in order:
        if await _dispatch(url, job):
            return
    async with _state_lock:
        _pending.append(job)
        log.info(
            "All workers busy, enqueued: org=%s job_id=%s pending=%d",
            job["org"], job["job_id"], len(_pending),
        )


# ---------- Master pending-queue drainer ----------

async def _drain_pending() -> None:
    while True:
        await asyncio.sleep(PENDING_DRAIN_INTERVAL_S)
        try:
            await _drain_once()
        except Exception:
            log.exception("Drain failed")


async def _drain_once() -> None:
    now = asyncio.get_event_loop().time()
    async with _state_lock:
        kept = []
        expired = 0
        for job in _pending:
            if now - job["enqueued_at"] > PENDING_JOB_DEADLINE_S:
                log.warning(
                    "Pending job expired: org=%s job_id=%s waited=%ds",
                    job["org"], job["job_id"], int(now - job["enqueued_at"]),
                )
                expired += 1
            else:
                kept.append(job)
        _pending[:] = kept
        snapshot = list(_pending)
    if not snapshot:
        if expired:
            log.info("Drain tick: expired=%d remaining=0", expired)
        return

    spawned = 0
    for job in snapshot:
        order = await _pick_worker_order()
        accepted = False
        for url in order:
            if await _dispatch(url, job):
                accepted = True
                break
        if accepted:
            async with _state_lock:
                try:
                    _pending.remove(job)
                except ValueError:
                    pass
            spawned += 1
        else:
            # Whole fleet is busy — stop trying this tick.
            break
    if spawned or expired:
        log.info("Drain tick: spawned=%d expired=%d remaining=%d", spawned, expired, len(_pending))


@app.on_event("startup")
async def _start_background_tasks():
    # Drainer runs everywhere — slaves use it to retry their own _pending
    # after a spawn failure (dockerd timeout, image pull error, etc.).
    asyncio.create_task(_drain_pending())
    # SLA tracking is master-only because GitHub webhooks only hit master.
    if IS_MASTER:
        asyncio.create_task(_latency_sweeper())
        if QUEUE_POLL_ENABLED and gi is not None and ALLOWED_ORGS:
            asyncio.create_task(_queue_poller())


# ---------- Queue poller (master only) ----------

async def _queue_poller() -> None:
    """Periodically scan GitHub for queued jobs we haven't seen via webhooks
    and synthesize spawns for them. Self-healing against webhook loss and
    org-pool races where our runners get assigned different jobs."""
    log.info("Queue poller started: every %ds, age>%ds, orgs=%s",
             QUEUE_POLL_INTERVAL_S, QUEUE_POLL_AGE_THRESHOLD_S, ALLOWED_ORGS)
    while True:
        await asyncio.sleep(QUEUE_POLL_INTERVAL_S)
        try:
            await _poll_queued_orphans()
        except Exception:
            log.exception("queue poller iteration failed")


def _gh_get(url: str, itoken: str) -> dict | list | None:
    try:
        r = _requests.get(
            url,
            headers={"Authorization": f"token {itoken}", "Accept": "application/vnd.github+json"},
            timeout=10,
        )
        if not r.ok:
            log.warning("Queue poller GET %s → %d %s", url, r.status_code, r.text[:200])
            return None
        return r.json()
    except Exception:
        log.exception("Queue poller GET %s failed", url)
        return None


def _list_installation_repos(itoken: str) -> list[str]:
    data = _gh_get("https://api.github.com/installation/repositories?per_page=100", itoken)
    if not data:
        return []
    return [r["full_name"] for r in data.get("repositories", [])]


def _list_repo_queued_jobs(itoken: str, repo: str) -> list[dict]:
    """Returns dicts with id, name, labels, created_at, run_id."""
    out: list[dict] = []
    runs = _gh_get(f"https://api.github.com/repos/{repo}/actions/runs?status=queued&per_page=30", itoken)
    if not runs:
        return out
    for run in runs.get("workflow_runs", []):
        jobs = _gh_get(f"https://api.github.com/repos/{repo}/actions/runs/{run['id']}/jobs", itoken)
        if not jobs:
            continue
        for j in jobs.get("jobs", []):
            if j.get("status") == "queued":
                out.append({
                    "id": j["id"],
                    "name": j.get("name", ""),
                    "labels": j.get("labels", []),
                    "created_at": j.get("created_at", ""),
                    "run_id": run["id"],
                })
    return out


def _iso_age_s(iso: str) -> float:
    if not iso:
        return 0.0
    try:
        from datetime import datetime, timezone
        ts = datetime.fromisoformat(iso.replace("Z", "+00:00"))
        return (datetime.now(timezone.utc) - ts).total_seconds()
    except Exception:
        return 0.0


async def _poll_queued_orphans() -> None:
    loop = asyncio.get_event_loop()
    for org in ALLOWED_ORGS:
        try:
            inst_id = await loop.run_in_executor(None, lambda o=org: _resolve_installation_id(o, "Organization"))
            if not inst_id:
                continue
            try:
                itoken = await loop.run_in_executor(None, lambda iid=inst_id: gi.get_access_token(iid).token)
            except Exception:
                log.exception("Queue poller: token fetch failed for org=%s", org)
                continue
            repos = await loop.run_in_executor(None, lambda t=itoken: _list_installation_repos(t))
            for repo in repos:
                jobs = await loop.run_in_executor(None, lambda t=itoken, r=repo: _list_repo_queued_jobs(t, r))
                for j in jobs:
                    age = _iso_age_s(j["created_at"])
                    if age < QUEUE_POLL_AGE_THRESHOLD_S:
                        continue
                    labels = j["labels"]
                    # Skip github-hosted-only jobs (we don't serve those).
                    if all(lbl in _GITHUB_HOSTED for lbl in labels):
                        continue
                    if any(lbl in _NON_LINUX_OS for lbl in labels):
                        continue
                    key = (org, j["id"])
                    async with _job_lock:
                        already_tracked = key in _job_queued_at
                    if already_tracked:
                        continue
                    log.warning(
                        "Queue poller: orphan job org=%s job_id=%s name=%s age=%.0fs labels=%s — synthesizing spawn",
                        org, j["id"], j["name"], age, labels,
                    )
                    fake_job = {
                        "installation_id": inst_id,
                        "org": org,
                        "owner_type": "Organization",
                        "repo_full_name": repo,
                        "labels": labels,
                        "job_id": j["id"],
                        "enqueued_at": asyncio.get_event_loop().time(),
                    }
                    # Register in SLA tracker so the in_progress webhook clears it.
                    async with _job_lock:
                        _job_queued_at.setdefault(key, asyncio.get_event_loop().time())
                    asyncio.create_task(_route(fake_job))
        except Exception:
            log.exception("Queue poller failed for org=%s", org)


async def _requeue_failed(job: dict, reason: str) -> None:
    """Spawn failed (token/dockerd error). Put the job back into _pending so the
    drainer retries it on the next tick. Decrement _active_runners since the
    spawn was committed but never produced a live container."""
    global _active_runners
    async with _state_lock:
        _active_runners -= 1
        _pending.append(job)
        log.warning(
            "Re-queued failed spawn: org=%s job_id=%s reason='%s' pending=%d",
            job["org"], job["job_id"], reason, len(_pending),
        )


# ---------- SLA latency tracking ----------

async def _track_job_event(org: str, job_id: int, action: str) -> None:
    """Record/clear (org, job_id) timestamps from workflow_job webhooks so the
    sweeper can alert on queue→start latency above LATENCY_ALERT_S."""
    key = (org, job_id)
    now = asyncio.get_event_loop().time()
    async with _job_lock:
        if action == "queued":
            # First queued event wins (dedupe duplicate deliveries).
            _job_queued_at.setdefault(key, now)
        elif action == "in_progress":
            queued_at = _job_queued_at.pop(key, None)
            already_alerted = key in _job_alerted
            _job_alerted.discard(key)
            if queued_at is None:
                return
            latency = now - queued_at
            log.info(
                "SLA: org=%s job_id=%s queue_latency=%.1fs",
                org, job_id, latency,
            )
            if latency > LATENCY_ALERT_S and not already_alerted:
                msg = (
                    f"⚠️ <b>Build server SLA</b>\n"
                    f"Job <code>{org}/{job_id}</code> waited "
                    f"<b>{int(latency)}s</b> queued before a runner picked it up."
                )
                asyncio.create_task(asyncio.to_thread(_tg_notify, msg))
        elif action == "completed":
            _job_queued_at.pop(key, None)
            _job_alerted.discard(key)


async def _latency_sweeper() -> None:
    """Periodically check tracked jobs. Alert (once) on entries that have been
    queued > LATENCY_ALERT_S without an in_progress event. Expire entries past
    LATENCY_TRACK_TTL_S so the map can't grow unbounded."""
    while True:
        await asyncio.sleep(LATENCY_SWEEP_INTERVAL_S)
        try:
            now = asyncio.get_event_loop().time()
            to_alert: list[tuple[str, int, float]] = []
            async with _job_lock:
                for key in list(_job_queued_at.keys()):
                    age = now - _job_queued_at[key]
                    if age > LATENCY_TRACK_TTL_S:
                        del _job_queued_at[key]
                        _job_alerted.discard(key)
                        continue
                    if age > LATENCY_ALERT_S and key not in _job_alerted:
                        _job_alerted.add(key)
                        org, job_id = key
                        to_alert.append((org, job_id, age))
            for org, job_id, age in to_alert:
                msg = (
                    f"⚠️ <b>Build server SLA</b>\n"
                    f"Job <code>{org}/{job_id}</code> still queued after "
                    f"<b>{int(age)}s</b> — no runner has picked it up yet."
                )
                log.warning("SLA breach: org=%s job_id=%s age=%.0fs", org, job_id, age)
                asyncio.create_task(asyncio.to_thread(_tg_notify, msg))
        except Exception:
            log.exception("latency sweeper failed")


# ---------- GitHub helpers ----------

def _parse_image(labels: list[str]) -> str:
    for lbl in labels:
        if lbl.startswith("runner-image:"):
            return lbl.removeprefix("runner-image:")
    return DEFAULT_IMAGE


def _verify_github_signature(body: bytes, header: str) -> None:
    secret = os.environ["WEBHOOK_SECRET"].encode()
    expected = "sha256=" + hmac.new(secret, body, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(header, expected):
        raise HTTPException(status_code=401, detail="invalid signature")


def _resolve_installation_id(org: str, owner_type: str) -> int | None:
    if gi is None:
        return None
    now = int(time.time())
    token = _jwt.encode({"iat": now - 60, "exp": now + 600, "iss": str(_app_id)}, _private_key, algorithm="RS256")
    segment = "users" if owner_type == "User" else "orgs"
    url = f"https://api.github.com/{segment}/{org}/installation"
    resp = _requests.get(
        url,
        headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"},
        timeout=10,
    )
    if resp.ok:
        return resp.json()["id"]
    log.warning("Installation lookup failed for org=%s: %s", org, resp.text[:200])
    return None


# ---------- Runner lifecycle ----------

async def spawn_runner(job: dict) -> None:
    """Launches the github-runner container and watchdogs it.
    On any exit path, decrements _active_runners under _state_lock."""
    global _active_runners
    installation_id = job["installation_id"]
    org = job["org"]
    owner_type = job["owner_type"]
    repo_full_name = job["repo_full_name"]
    labels = job["labels"]
    job_id = job["job_id"]

    container = None
    try:
        # We need a GitHub installation token to register a runner. Slaves don't
        # have the App; the token must come from the master via the job payload
        # in the future, but for now slaves authenticate with their own App if
        # configured. If neither — fail loudly.
        if gi is None:
            log.error("Cannot spawn: no GitHub App on this node (org=%s job_id=%s)", org, job_id)
            await _requeue_failed(job, "no github app")
            return
        try:
            token = gi.get_access_token(installation_id).token
        except Exception:
            log.exception("Failed to get installation token for installation_id=%s", installation_id)
            await _requeue_failed(job, "token fetch failed")
            return

        image = _parse_image(labels)
        custom_labels = [lbl for lbl in labels if lbl not in _GITHUB_HOSTED]

        if owner_type == "User":
            env = {
                "RUNNER_SCOPE": "repo",
                "REPO_URL": f"https://github.com/{repo_full_name}",
                "ACCESS_TOKEN": token,
                "EPHEMERAL": "true",
                "DISABLE_AUTO_UPDATE": "true",
                "LABELS": ",".join(custom_labels),
            }
        else:
            env = {
                "RUNNER_SCOPE": "org",
                "ORG_NAME": org,
                "ACCESS_TOKEN": token,
                "EPHEMERAL": "true",
                "DISABLE_AUTO_UPDATE": "true",
                "LABELS": ",".join(custom_labels),
            }

        loop = asyncio.get_event_loop()
        # Rate-limit concurrent docker.containers.run() to keep dockerd from
        # serializing 80+ calls into 60s ReadTimeouts.
        try:
            async with _docker_spawn_sem:
                container = await loop.run_in_executor(
                    None,
                    lambda: docker_client.containers.run(
                        image,
                        detach=True,
                        user="root",
                        volumes={"/var/run/docker.sock": {"bind": "/var/run/docker.sock", "mode": "rw"}},
                        environment=env,
                    ),
                )
        except Exception:
            log.exception("Runner start failed: org=%s job_id=%s image=%s — requeueing", org, job_id, image)
            await _requeue_failed(job, "dockerd error")
            return

        start = asyncio.get_event_loop().time()
        job_started_at: float | None = None
        while True:
            await asyncio.sleep(RUNNER_POLL_INTERVAL)
            try:
                await loop.run_in_executor(None, container.reload)
            except docker.errors.NotFound:
                log.info("Runner container gone: org=%s job_id=%s", org, job_id)
                return
            if container.status != "running":
                result = await loop.run_in_executor(None, container.wait)
                log.info(
                    "Runner finished: org=%s job_id=%s exit=%d",
                    org, job_id, result.get("StatusCode", -1),
                )
                break

            now = asyncio.get_event_loop().time()
            elapsed = now - start
            if job_started_at is None:
                try:
                    logs = await loop.run_in_executor(
                        None, lambda: container.logs().decode("utf-8", "replace")
                    )
                except Exception:
                    logs = ""
                if "Running job:" in logs:
                    job_started_at = now
                    log.info("Runner picked up job: org=%s spawned_for=%s", org, job_id)
                elif elapsed > RUNNER_IDLE_TIMEOUT:
                    # Wasted spawn — org pool gave this job (or others queued in
                    # the same burst) to a different runner before this one
                    # registered. No alert: see SLA tracker for user-facing
                    # latency. Just reclaim the container.
                    log.info(
                        "Runner idle %ds without a job: org=%s spawned_for=%s — killing (likely lost org-pool race)",
                        RUNNER_IDLE_TIMEOUT, org, job_id,
                    )
                    await loop.run_in_executor(None, container.kill)
                    break
            elif now - job_started_at > RUNNER_JOB_TIMEOUT:
                log.warning(
                    "Job exceeded %ds: org=%s job_id=%s — killing",
                    RUNNER_JOB_TIMEOUT, org, job_id,
                )
                await loop.run_in_executor(None, container.kill)
                break
    except Exception:
        log.exception("Runner wait failed: org=%s job_id=%s", org, job_id)
    finally:
        # _active_runners was +1 in _commit_spawn. If we failed BEFORE creating
        # a container, _requeue_failed already did the -1. Only decrement here
        # when we got far enough to actually start a container.
        if container is not None:
            try:
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, lambda: container.remove(force=True))
            except Exception:
                pass
            async with _state_lock:
                _active_runners -= 1
