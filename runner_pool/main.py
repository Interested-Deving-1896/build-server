import asyncio
import hashlib
import hmac
import logging
import os
import time

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

# ---- Scheduler knobs (apply to every worker) ----
# There is no fixed runner cap. Capacity is gated by free RAM.
MIN_FREE_RAM_MB = int(os.getenv("MIN_FREE_RAM_MB", "3072"))
WORST_CASE_RUNNER_MB = int(os.getenv("WORST_CASE_RUNNER_MB", "3072"))
SPAWN_COOLDOWN_S = int(os.getenv("SPAWN_COOLDOWN_S", "15"))

# ---- Master-only knobs ----
# Pending queue lives on master. Slaves never queue — they accept or 503.
PENDING_JOB_DEADLINE_S = int(os.getenv("PENDING_JOB_DEADLINE_S", "3600"))
PENDING_DRAIN_INTERVAL_S = int(os.getenv("PENDING_DRAIN_INTERVAL_S", "5"))
# Comma-separated URLs of slave workers. Master itself is always included as "local".
# Empty (default) → standalone, no slaves.
SLAVE_URLS: list[str] = [u.strip().rstrip("/") for u in os.getenv("SLAVE_URLS", "").split(",") if u.strip()]
# Capacity poll timeout (seconds) when picking a worker.
WORKER_CAPACITY_TIMEOUT_S = float(os.getenv("WORKER_CAPACITY_TIMEOUT_S", "2.0"))
# Shared secret for master ↔ slave /spawn calls. Required when SLAVE_URLS is set
# or when this node may receive /spawn (i.e. always, defensively).
INTERNAL_SECRET = os.getenv("INTERNAL_SECRET", "")

# ---- Runner watchdog knobs ----
RUNNER_IDLE_TIMEOUT = int(os.getenv("RUNNER_IDLE_TIMEOUT", "600"))
RUNNER_JOB_TIMEOUT = int(os.getenv("RUNNER_JOB_TIMEOUT", "7200"))
RUNNER_POLL_INTERVAL = int(os.getenv("RUNNER_POLL_INTERVAL", "15"))

# SLA: alert if a workflow_job sits in "queued" state longer than this before any
# runner in the org pool picks it up. Measured by the gap between the
# workflow_job.queued and workflow_job.in_progress webhooks for the SAME job_id —
# NOT by what our particular container did. Org-scoped runners mean another
# runner in the pool may legitimately pick up a job we spawned for, and that's
# fine for the end user. What matters is "did the user's commit get its CI in
# reasonable time?"
LATENCY_ALERT_S = int(os.getenv("LATENCY_ALERT_S", "300"))
LATENCY_SWEEP_INTERVAL_S = int(os.getenv("LATENCY_SWEEP_INTERVAL_S", "30"))
# Drop tracking entries older than this so memory doesn't grow unbounded.
LATENCY_TRACK_TTL_S = int(os.getenv("LATENCY_TRACK_TTL_S", "3600"))

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

# ---- Master state (unused on slaves) ----
_pending: list[dict] = []  # FIFO of jobs awaiting capacity across the fleet
# SLA tracking: (org, job_id) -> monotonic_time when workflow_job.queued arrived.
# Cleared on workflow_job.in_progress (success: latency computed) or .completed.
# Sweeper alerts on entries older than LATENCY_ALERT_S.
_job_queued_at: dict[tuple[str, int], float] = {}
_job_alerted: set[tuple[str, int]] = set()
_job_lock = asyncio.Lock()

IS_MASTER = bool(SLAVE_URLS) or gi is not None

log.info(
    "Build server starting: role=%s slaves=%d MIN_FREE_RAM=%dMB WORST_CASE_RUNNER=%dMB "
    "COOLDOWN=%ds IDLE_TIMEOUT=%ds JOB_TIMEOUT=%ds SLA_ALERT=%ds DEFAULT_IMAGE=%s ALLOWED_ORGS=%s",
    "master" if IS_MASTER else "slave",
    len(SLAVE_URLS), MIN_FREE_RAM_MB, WORST_CASE_RUNNER_MB, SPAWN_COOLDOWN_S,
    RUNNER_IDLE_TIMEOUT, RUNNER_JOB_TIMEOUT, LATENCY_ALERT_S, DEFAULT_IMAGE,
    ALLOWED_ORGS or "unrestricted",
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
    when committing on the basis of this call (it reads _last_spawn_at).
    """
    avail = _mem_available_mb()
    if avail < MIN_FREE_RAM_MB:
        return False, f"ram_low avail={avail}MB min={MIN_FREE_RAM_MB}MB", avail
    headroom = avail - MIN_FREE_RAM_MB
    if headroom >= WORST_CASE_RUNNER_MB:
        return True, f"burst headroom={headroom}MB", avail
    wait = SPAWN_COOLDOWN_S - (now - _last_spawn_at)
    if wait > 0:
        return False, f"cooldown {wait:.0f}s left headroom={headroom}MB", avail
    return True, f"tight headroom={headroom}MB", avail


def _local_capacity_snapshot() -> dict:
    now = asyncio.get_event_loop().time()
    ok, reason, avail = _can_spawn_local(now)
    return {
        "active": _active_runners,
        "mem_available_mb": avail,
        "mem_min_free_mb": MIN_FREE_RAM_MB,
        "worst_case_runner_mb": WORST_CASE_RUNNER_MB,
        "can_spawn": ok,
        "status": reason,
    }


# ---------- HTTP endpoints ----------

@app.get("/capacity")
async def capacity():
    snap = _local_capacity_snapshot()
    snap["pending"] = len(_pending) if IS_MASTER else 0
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
    _last_spawn_at = asyncio.get_event_loop().time()
    _active_runners += 1
    log.info(
        "Spawning runner locally: org=%s job_id=%s active=%d (%s)",
        job["org"], job["job_id"], _active_runners, reason,
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
    if IS_MASTER:
        asyncio.create_task(_drain_pending())
        asyncio.create_task(_latency_sweeper())


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
            return
        try:
            token = gi.get_access_token(installation_id).token
        except Exception:
            log.exception("Failed to get installation token for installation_id=%s", installation_id)
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
        try:
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
            log.exception("Runner start failed: org=%s job_id=%s image=%s", org, job_id, image)
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
        if container is not None:
            try:
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, lambda: container.remove(force=True))
            except Exception:
                pass
        async with _state_lock:
            _active_runners -= 1
