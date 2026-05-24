"""Gateway role: receives GitHub webhooks, polls every runner's /capacity,
dispatches each job to the runner with the most free RAM, tracks per-job SLA
latency, and periodically scans GitHub for orphaned queued jobs.

Does NOT spawn containers itself — that's the runner's job. Multiple runners
(local + remote) form the pool that the gateway dispatches across.
"""
import asyncio
import hashlib
import hmac
import os

import requests as _requests
from fastapi import FastAPI, HTTPException, Request

from ._common import (
    ALLOWED_ORGS, GITHUB_HOSTED, INTERNAL_SECRET, NON_LINUX_OS, gi, log,
    resolve_installation_id, tg_notify,
)

# ---------- Config ----------
# Runner URLs — at least one (could be 'http://127.0.0.1:3001' for a standalone
# host that runs both gateway and runner on the same machine).
WORKERS: list[str] = [u.strip().rstrip("/") for u in os.getenv("WORKERS", "").split(",") if u.strip()]
# User-facing SLA: alert when commit→runner-pickup gap exceeds this:
LATENCY_ALERT_S = int(os.getenv("LATENCY_ALERT_S", "300"))

# ---------- Internal constants ----------
WORKER_CAPACITY_TIMEOUT_S = 5.0
WORKER_UNREACHABLE_THRESHOLD = 3
PENDING_JOB_DEADLINE_S = 3600
PENDING_DRAIN_INTERVAL_S = 5
LATENCY_SWEEP_INTERVAL_S = 30
LATENCY_TRACK_TTL_S = 3600
QUEUE_POLL_INTERVAL_S = 60
QUEUE_POLL_AGE_THRESHOLD_S = 60

# ---------- State ----------
_pending: list[dict] = []
_pending_lock = asyncio.Lock()

# SLA tracking
_job_queued_at: dict[tuple[str, int], float] = {}
_job_alerted: set[tuple[str, int]] = set()
_job_lock = asyncio.Lock()

# Worker health
_worker_failures: dict[str, int] = {}
_worker_alerted: set[str] = set()

app = FastAPI()

if not WORKERS:
    log.warning("Gateway starting with no WORKERS configured — nothing to dispatch to!")
log.info(
    "Gateway starting: workers=%d (%s) SLA_ALERT=%ds ALLOWED_ORGS=%s",
    len(WORKERS), ",".join(WORKERS) or "<none>",
    LATENCY_ALERT_S, ALLOWED_ORGS or "unrestricted",
)


# ---------- HTTP endpoints ----------

@app.get("/capacity")
async def capacity():
    """Gateway self-status (not actual spawn capacity — that's at each runner)."""
    return {
        "role": "gateway",
        "workers": WORKERS,
        "pending": len(_pending),
        "tracked_jobs": len(_job_queued_at),
        "unreachable_workers": sorted(_worker_alerted),
    }


@app.post("/webhook")
async def webhook(request: Request):
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

    # Track every queued/in_progress/completed for jobs we actually serve.
    if all(lbl not in NON_LINUX_OS for lbl in labels) and not all(lbl in GITHUB_HOSTED for lbl in labels):
        await _track_job_event(org, job_id, action)

    if action != "queued":
        return {"ok": True}
    if ALLOWED_ORGS and org not in ALLOWED_ORGS:
        log.warning("Ignored job from unlisted org=%s job_id=%d", org, job_id)
        return {"ok": True}
    if all(lbl in GITHUB_HOSTED for lbl in labels):
        return {"ok": True}
    if any(lbl in NON_LINUX_OS for lbl in labels):
        log.info("Skipped non-Linux job: org=%s job_id=%d labels=%s", org, job_id, labels)
        return {"ok": True}

    if not installation_id:
        installation_id = resolve_installation_id(org, owner_type)
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


def _verify_github_signature(body: bytes, header: str) -> None:
    secret = os.environ["WEBHOOK_SECRET"].encode()
    expected = "sha256=" + hmac.new(secret, body, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(header, expected):
        raise HTTPException(status_code=401, detail="invalid signature")


# ---------- Routing ----------

async def _worker_capacity(url: str) -> dict | None:
    loop = asyncio.get_event_loop()
    data: dict | None = None
    try:
        resp = await loop.run_in_executor(
            None,
            lambda: _requests.get(f"{url}/capacity", timeout=WORKER_CAPACITY_TIMEOUT_S),
        )
        if resp.ok:
            data = resp.json()
            data["url"] = url
    except Exception:
        log.warning("Worker %s capacity poll failed", url)
    _record_worker_health(url, data is not None)
    return data


def _record_worker_health(url: str, ok: bool) -> None:
    if ok:
        if _worker_failures.get(url, 0) >= WORKER_UNREACHABLE_THRESHOLD and url in _worker_alerted:
            asyncio.create_task(asyncio.to_thread(
                tg_notify,
                f"✅ <b>Build server worker recovered</b>\nWorker <code>{url}</code> is responding again.",
            ))
            _worker_alerted.discard(url)
        _worker_failures[url] = 0
        return
    n = _worker_failures.get(url, 0) + 1
    _worker_failures[url] = n
    if n == WORKER_UNREACHABLE_THRESHOLD and url not in _worker_alerted:
        _worker_alerted.add(url)
        log.warning("Worker %s marked unreachable (n=%d)", url, n)
        asyncio.create_task(asyncio.to_thread(
            tg_notify,
            f"⚠️ <b>Build server worker unreachable</b>\n"
            f"Worker <code>{url}</code> failed {n} consecutive capacity polls. "
            f"Gateway is excluding it from dispatch — fix it or remove from WORKERS.",
        ))


async def _pick_worker_order() -> list[str]:
    """Poll all workers in parallel, return URLs best-first."""
    if not WORKERS:
        return []
    results = await asyncio.gather(*[_worker_capacity(u) for u in WORKERS])
    able = sorted(
        (r for r in results if r and r.get("can_spawn")),
        key=lambda r: r.get("mem_available_mb", 0),
        reverse=True,
    )
    unable = sorted(
        (r for r in results if r and not r.get("can_spawn")),
        key=lambda r: r.get("mem_available_mb", 0),
        reverse=True,
    )
    return [r["url"] for r in able + unable]


async def _dispatch(worker_url: str, job: dict) -> bool:
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
    order = await _pick_worker_order()
    for url in order:
        if await _dispatch(url, job):
            return
    async with _pending_lock:
        _pending.append(job)
        log.info(
            "All workers busy, enqueued: org=%s job_id=%s pending=%d",
            job["org"], job["job_id"], len(_pending),
        )


# ---------- Pending drainer ----------

async def _drain_pending() -> None:
    while True:
        await asyncio.sleep(PENDING_DRAIN_INTERVAL_S)
        try:
            await _drain_once()
        except Exception:
            log.exception("Drain failed")


async def _drain_once() -> None:
    now = asyncio.get_event_loop().time()
    async with _pending_lock:
        kept = []
        expired = 0
        for job in _pending:
            if now - job["enqueued_at"] > PENDING_JOB_DEADLINE_S:
                log.warning("Pending job expired: org=%s job_id=%s", job["org"], job["job_id"])
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
            async with _pending_lock:
                try:
                    _pending.remove(job)
                except ValueError:
                    pass
            spawned += 1
        else:
            break
    if spawned or expired:
        log.info("Drain tick: spawned=%d expired=%d remaining=%d", spawned, expired, len(_pending))


# ---------- SLA latency tracking ----------

async def _track_job_event(org: str, job_id: int, action: str) -> None:
    key = (org, job_id)
    now = asyncio.get_event_loop().time()
    async with _job_lock:
        if action == "queued":
            _job_queued_at.setdefault(key, now)
        elif action == "in_progress":
            queued_at = _job_queued_at.pop(key, None)
            already_alerted = key in _job_alerted
            _job_alerted.discard(key)
            if queued_at is None:
                return
            latency = now - queued_at
            log.info("SLA: org=%s job_id=%s queue_latency=%.1fs", org, job_id, latency)
            if latency > LATENCY_ALERT_S and not already_alerted:
                asyncio.create_task(asyncio.to_thread(
                    tg_notify,
                    f"⚠️ <b>Build server SLA</b>\n"
                    f"Job <code>{org}/{job_id}</code> waited "
                    f"<b>{int(latency)}s</b> queued before a runner picked it up.",
                ))
        elif action == "completed":
            _job_queued_at.pop(key, None)
            _job_alerted.discard(key)


async def _latency_sweeper() -> None:
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
                log.warning("SLA breach: org=%s job_id=%s age=%.0fs", org, job_id, age)
                asyncio.create_task(asyncio.to_thread(
                    tg_notify,
                    f"⚠️ <b>Build server SLA</b>\n"
                    f"Job <code>{org}/{job_id}</code> still queued after "
                    f"<b>{int(age)}s</b> — no runner has picked it up yet.",
                ))
        except Exception:
            log.exception("Latency sweeper failed")


# ---------- Queue poller (orphan recovery) ----------

def _gh_get(url: str, itoken: str):
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
    return [r["full_name"] for r in (data or {}).get("repositories", [])]


def _list_repo_queued_jobs(itoken: str, repo: str) -> list[dict]:
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


async def _queue_poller() -> None:
    log.info("Queue poller started: every %ds, age>%ds, orgs=%s",
             QUEUE_POLL_INTERVAL_S, QUEUE_POLL_AGE_THRESHOLD_S, ALLOWED_ORGS)
    while True:
        await asyncio.sleep(QUEUE_POLL_INTERVAL_S)
        try:
            await _poll_queued_orphans()
        except Exception:
            log.exception("Queue poller iteration failed")


async def _poll_queued_orphans() -> None:
    loop = asyncio.get_event_loop()
    for org in ALLOWED_ORGS:
        try:
            inst_id = await loop.run_in_executor(None, lambda o=org: resolve_installation_id(o, "Organization"))
            if not inst_id:
                continue
            try:
                itoken = await loop.run_in_executor(None, lambda iid=inst_id: gi.get_access_token(iid).token)  # type: ignore[union-attr]
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
                    if all(lbl in GITHUB_HOSTED for lbl in labels):
                        continue
                    if any(lbl in NON_LINUX_OS for lbl in labels):
                        continue
                    key = (org, j["id"])
                    async with _job_lock:
                        already_tracked = key in _job_queued_at
                    if already_tracked:
                        continue
                    log.warning(
                        "Queue poller: orphan job org=%s job_id=%s name=%s age=%.0fs — synthesising spawn",
                        org, j["id"], j["name"], age,
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
                    async with _job_lock:
                        _job_queued_at.setdefault(key, asyncio.get_event_loop().time())
                    asyncio.create_task(_route(fake_job))
        except Exception:
            log.exception("Queue poller failed for org=%s", org)


@app.on_event("startup")
async def _start_background_tasks():
    asyncio.create_task(_drain_pending())
    asyncio.create_task(_latency_sweeper())
    if gi is not None and ALLOWED_ORGS:
        asyncio.create_task(_queue_poller())
