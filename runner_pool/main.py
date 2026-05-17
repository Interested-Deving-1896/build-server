import asyncio
import hashlib
import hmac
import logging
import os
import time

import docker
import jwt as _jwt
import requests as _requests
from fastapi import FastAPI, HTTPException, Request
from github import GithubIntegration

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

app = FastAPI()
docker_client = docker.from_env()

_private_key = os.environ["GITHUB_APP_PRIVATE_KEY"].replace("\\n", "\n")
_app_id = int(os.environ["GITHUB_APP_ID"])
gi = GithubIntegration(_app_id, _private_key)

MAX_RUNNERS = int(os.getenv("MAX_RUNNERS") or os.cpu_count())
# Max seconds a container may run before being killed — prevents idle leaks
# when a runner registers with wrong labels and never receives a job.
RUNNER_TIMEOUT = int(os.getenv("RUNNER_TIMEOUT", "600"))
sem = asyncio.Semaphore(MAX_RUNNERS)
DEFAULT_IMAGE = os.environ["DEFAULT_RUNNER_IMAGE"]
ALLOWED_ORGS: set[str] = set(filter(None, os.getenv("ALLOWED_ORGS", "").split(",")))

# Labels used exclusively by GitHub-hosted runners — skip spawning for these.
_GITHUB_HOSTED = {
    "ubuntu-latest", "ubuntu-24.04", "ubuntu-22.04", "ubuntu-20.04", "ubuntu-18.04",
    "windows-latest", "windows-2022", "windows-2019",
    "macos-latest", "macos-14", "macos-13", "macos-12",
}

# OS labels indicating a platform-specific pre-registered runner — do not spawn.
# "Windows" and "macOS" are the OS labels GitHub assigns when a runner's OS is
# detected; a Linux VPS can never serve these jobs.
_NON_LINUX_OS = {"Windows", "macOS"}

log.info(
    "Build server starting: MAX_RUNNERS=%d RUNNER_TIMEOUT=%ds DEFAULT_IMAGE=%s ALLOWED_ORGS=%s",
    MAX_RUNNERS, RUNNER_TIMEOUT, DEFAULT_IMAGE, ALLOWED_ORGS or "unrestricted",
)


def _parse_image(labels: list[str]) -> str:
    for lbl in labels:
        if lbl.startswith("runner-image:"):
            return lbl.removeprefix("runner-image:")
    return DEFAULT_IMAGE


def _verify_signature(body: bytes, header: str) -> None:
    secret = os.environ["WEBHOOK_SECRET"].encode()
    expected = "sha256=" + hmac.new(secret, body, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(header, expected):
        raise HTTPException(status_code=401, detail="invalid signature")


def _resolve_installation_id(org: str, owner_type: str) -> int | None:
    """Look up the GitHub App installation ID for an org/user via the App API."""
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


@app.post("/webhook")
async def webhook(request: Request):
    body = await request.body()
    _verify_signature(body, request.headers.get("X-Hub-Signature-256", ""))

    payload = await request.json()
    action = payload.get("action")
    if action != "queued" or "workflow_job" not in payload:
        return {"ok": True}

    installation_id = (payload.get("installation") or {}).get("id")
    org = payload["repository"]["owner"]["login"]
    owner_type = payload["repository"]["owner"].get("type", "Organization")
    repo_full_name = payload["repository"]["full_name"]
    labels: list[str] = payload["workflow_job"].get("labels", [])
    job_id = payload["workflow_job"]["id"]

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

    log.info("Job queued: org=%s job_id=%d labels=%s", org, job_id, labels)
    asyncio.create_task(spawn_runner(installation_id, org, owner_type, repo_full_name, labels, job_id))
    return {"ok": True}


async def spawn_runner(
    installation_id: int, org: str, owner_type: str,
    repo_full_name: str, labels: list[str], job_id: int,
):
    async with sem:
        active = MAX_RUNNERS - sem._value  # noqa: SLF001
        log.info("Spawning runner: org=%s job_id=%d active=%d/%d", org, job_id, active, MAX_RUNNERS)
        try:
            token = gi.get_access_token(installation_id).token
        except Exception:
            log.exception("Failed to get installation token for installation_id=%d", installation_id)
            return

        image = _parse_image(labels)
        custom_labels = [lbl for lbl in labels if lbl not in _GITHUB_HOSTED]

        if owner_type == "User":
            # Personal accounts don't have org-level runners — register at repo scope.
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
                    volumes={"/var/run/docker.sock": {"bind": "/var/run/docker.sock", "mode": "rw"}},
                    environment=env,
                ),
            )
        except Exception:
            log.exception("Runner start failed: org=%s job_id=%d image=%s", org, job_id, image)
            return

        try:
            result = await asyncio.wait_for(
                loop.run_in_executor(None, container.wait),
                timeout=RUNNER_TIMEOUT,
            )
            log.info("Runner finished: org=%s job_id=%d exit=%d", org, job_id, result.get("StatusCode", -1))
        except asyncio.TimeoutError:
            log.warning("Runner timed out after %ds: org=%s job_id=%d — killing", RUNNER_TIMEOUT, org, job_id)
            await loop.run_in_executor(None, container.kill)
        except Exception:
            log.exception("Runner wait failed: org=%s job_id=%d", org, job_id)
        finally:
            try:
                await loop.run_in_executor(None, lambda: container.remove(force=True))
            except Exception:
                pass
