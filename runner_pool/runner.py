"""Runner role: accepts /spawn from a gateway, exposes /capacity, spawns the
github-runner container, watchdogs it, and re-queues failed spawns for retry.

Does NOT receive GitHub webhooks or talk to GitHub for queue scanning.
"""
import asyncio
import hmac
import logging
import os
from collections import deque

import docker
from fastapi import FastAPI, Header, HTTPException, Request

from ._common import (
    GITHUB_HOSTED, INTERNAL_SECRET, gi, installation_token, log, mem_available_mb,
)

# ---------- Config ----------
DEFAULT_IMAGE = os.environ["DEFAULT_RUNNER_IMAGE"]
# Capacity gate. Single knob: how much RAM to always keep free for OS + spike.
# Burst threshold is implicitly 2 × this.
MIN_FREE_RAM_MB = int(os.getenv("MIN_FREE_RAM_MB", "3072"))
# Workload-specific upper bound on a single CI job (default 30 min):
RUNNER_JOB_TIMEOUT = int(os.getenv("RUNNER_JOB_TIMEOUT", "1800"))

# ---------- Internal constants ----------
SPAWN_COOLDOWN_S = 15
RUNNER_IDLE_TIMEOUT = 600
RUNNER_POLL_INTERVAL = 15
COLD_START_WINDOW_S = 60       # in-flight RAM reservation decay
PENDING_DRAIN_INTERVAL_S = 5
PENDING_JOB_DEADLINE_S = 3600
# dockerd serializes API requests at the unix socket — cap parallel spawns at
# vCPU count so 80 webhooks don't turn into 80 simultaneous container.run()
# calls and 60s ReadTimeouts.
MAX_CONCURRENT_DOCKER_SPAWNS = os.cpu_count() or 4

# ---------- State ----------
_docker = docker.from_env()
_state_lock = asyncio.Lock()
_last_spawn_at: float = 0.0
_active_runners: int = 0
_recent_spawns: deque[float] = deque()
_pending: list[dict] = []   # failed-spawn retry queue
_docker_spawn_sem = asyncio.Semaphore(MAX_CONCURRENT_DOCKER_SPAWNS)

app = FastAPI()

log.info(
    "Runner starting: MIN_FREE_RAM=%dMB BURST_AT=%dMB DOCKER_SEM=%d "
    "JOB_TIMEOUT=%ds DEFAULT_IMAGE=%s",
    MIN_FREE_RAM_MB, 2 * MIN_FREE_RAM_MB, MAX_CONCURRENT_DOCKER_SPAWNS,
    RUNNER_JOB_TIMEOUT, DEFAULT_IMAGE,
)


# ---------- Capacity / RAM gate ----------

def _can_spawn(now: float) -> tuple[bool, str, int]:
    """Caller MUST hold _state_lock. Returns (ok, reason, mem_available_mb)."""
    while _recent_spawns and now - _recent_spawns[0] > COLD_START_WINDOW_S:
        _recent_spawns.popleft()
    raw_avail = mem_available_mb()
    inflight_mb = len(_recent_spawns) * MIN_FREE_RAM_MB
    effective = raw_avail - inflight_mb
    if effective < MIN_FREE_RAM_MB:
        return False, (
            f"ram_low effective={effective}MB raw={raw_avail}MB "
            f"inflight={inflight_mb}MB ({len(_recent_spawns)} spawns) min={MIN_FREE_RAM_MB}MB"
        ), raw_avail
    headroom = effective - MIN_FREE_RAM_MB
    if headroom >= MIN_FREE_RAM_MB:
        return True, f"burst effective={effective}MB headroom={headroom}MB", raw_avail
    wait = SPAWN_COOLDOWN_S - (now - _last_spawn_at)
    if wait > 0:
        return False, f"cooldown {wait:.0f}s headroom={headroom}MB", raw_avail
    return True, f"tight headroom={headroom}MB", raw_avail


@app.get("/capacity")
async def capacity():
    async with _state_lock:
        ok, reason, avail = _can_spawn(asyncio.get_event_loop().time())
        return {
            "active": _active_runners,
            "pending": len(_pending),
            "mem_available_mb": avail,
            "mem_min_free_mb": MIN_FREE_RAM_MB,
            "mem_burst_at_mb": 2 * MIN_FREE_RAM_MB,
            "inflight_spawns": len(_recent_spawns),
            "inflight_reserved_mb": len(_recent_spawns) * MIN_FREE_RAM_MB,
            "docker_sem_in_flight": MAX_CONCURRENT_DOCKER_SPAWNS - _docker_spawn_sem._value,  # noqa: SLF001
            "can_spawn": ok,
            "status": reason,
        }


@app.post("/spawn")
async def spawn(request: Request, x_internal_secret: str = Header(default="")):
    if not INTERNAL_SECRET:
        raise HTTPException(status_code=500, detail="INTERNAL_SECRET not configured")
    if not hmac.compare_digest(x_internal_secret, INTERNAL_SECRET):
        raise HTTPException(status_code=401, detail="bad internal secret")
    job = await request.json()
    async with _state_lock:
        ok, reason, _ = _can_spawn(asyncio.get_event_loop().time())
        if not ok:
            log.info("Spawn rejected: org=%s job_id=%s (%s)", job.get("org"), job.get("job_id"), reason)
            raise HTTPException(status_code=503, detail=reason)
        _commit_spawn(job, reason)
    return {"ok": True}


# ---------- Spawn bookkeeping ----------

def _commit_spawn(job: dict, reason: str) -> None:
    """Caller MUST hold _state_lock."""
    global _last_spawn_at, _active_runners
    now = asyncio.get_event_loop().time()
    _last_spawn_at = now
    _active_runners += 1
    _recent_spawns.append(now)
    log.info(
        "Spawning runner: org=%s job_id=%s active=%d inflight=%d (%s)",
        job["org"], job["job_id"], _active_runners, len(_recent_spawns), reason,
    )
    asyncio.create_task(_spawn_container(job))


async def _requeue_failed(job: dict, reason: str) -> None:
    global _active_runners
    async with _state_lock:
        _active_runners -= 1
        _pending.append(job)
        log.warning(
            "Re-queued failed spawn: org=%s job_id=%s reason='%s' pending=%d",
            job["org"], job["job_id"], reason, len(_pending),
        )


# ---------- Container lifecycle ----------

def _parse_image(labels: list[str]) -> str:
    for lbl in labels:
        if lbl.startswith("runner-image:"):
            return lbl.removeprefix("runner-image:")
    return DEFAULT_IMAGE


async def _spawn_container(job: dict) -> None:
    global _active_runners
    org = job["org"]
    job_id = job["job_id"]
    labels = job["labels"]
    owner_type = job["owner_type"]
    repo_full_name = job["repo_full_name"]
    installation_id = job["installation_id"]

    container = None
    try:
        if gi is None:
            log.error("Cannot spawn: no GitHub App configured (org=%s job_id=%s)", org, job_id)
            await _requeue_failed(job, "no github app")
            return
        try:
            token = installation_token(installation_id)
        except Exception:
            log.exception("Failed to mint installation token (installation_id=%s)", installation_id)
            await _requeue_failed(job, "token fetch failed")
            return

        image = _parse_image(labels)
        custom_labels = [lbl for lbl in labels if lbl not in GITHUB_HOSTED]
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
            async with _docker_spawn_sem:
                container = await loop.run_in_executor(
                    None,
                    lambda: _docker.containers.run(
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

        # Watchdog loop: track when container picks up *a* job (org-scoped pool
        # means it might not be the one we spawned for, that's fine) and kill
        # runaway containers.
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
                    logs = await loop.run_in_executor(None, lambda: container.logs().decode("utf-8", "replace"))
                except Exception:
                    logs = ""
                if "Running job:" in logs:
                    job_started_at = now
                    log.info("Runner picked up job: org=%s spawned_for=%s", org, job_id)
                elif elapsed > RUNNER_IDLE_TIMEOUT:
                    log.info(
                        "Runner idle %ds without a job: org=%s spawned_for=%s — killing (lost org-pool race)",
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
        # +1 in _commit_spawn was paired here only when container was created.
        # Pre-container failures already -1'd via _requeue_failed.
        if container is not None:
            try:
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, lambda: container.remove(force=True))
            except Exception:
                pass
            async with _state_lock:
                _active_runners -= 1


# ---------- Pending drainer (retry failed spawns) ----------

async def _drain_pending() -> None:
    while True:
        await asyncio.sleep(PENDING_DRAIN_INTERVAL_S)
        try:
            await _drain_once()
        except Exception:
            log.exception("Drain failed")


async def _drain_once() -> None:
    now = asyncio.get_event_loop().time()
    spawned = 0
    expired = 0
    async with _state_lock:
        kept = []
        for job in _pending:
            if now - job.get("enqueued_at", now) > PENDING_JOB_DEADLINE_S:
                log.warning(
                    "Pending job expired: org=%s job_id=%s",
                    job.get("org"), job.get("job_id"),
                )
                expired += 1
            else:
                kept.append(job)
        _pending[:] = kept
        while _pending:
            ok, reason, _ = _can_spawn(asyncio.get_event_loop().time())
            if not ok:
                break
            job = _pending.pop(0)
            _commit_spawn(job, f"drained ({reason})")
            spawned += 1
    if spawned or expired:
        log.info("Drain tick: spawned=%d expired=%d remaining=%d", spawned, expired, len(_pending))


@app.on_event("startup")
async def _start_drainer():
    asyncio.create_task(_drain_pending())
