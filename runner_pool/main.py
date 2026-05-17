import asyncio
import hashlib
import hmac
import logging
import os

import docker
from fastapi import FastAPI, HTTPException, Request
from github import GithubIntegration

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

app = FastAPI()
docker_client = docker.from_env()

_private_key = os.environ["GITHUB_APP_PRIVATE_KEY"].replace("\\n", "\n")
gi = GithubIntegration(os.environ["GITHUB_APP_ID"], _private_key)

MAX_RUNNERS = int(os.getenv("MAX_RUNNERS") or os.cpu_count())
sem = asyncio.Semaphore(MAX_RUNNERS)
DEFAULT_IMAGE = os.environ["DEFAULT_RUNNER_IMAGE"]
ALLOWED_ORGS: set[str] = set(filter(None, os.getenv("ALLOWED_ORGS", "").split(",")))

# Jobs targeting GitHub-hosted runners only — no point spawning a self-hosted runner
_GITHUB_HOSTED = {
    "ubuntu-latest", "ubuntu-24.04", "ubuntu-22.04", "ubuntu-20.04", "ubuntu-18.04",
    "windows-latest", "windows-2022", "windows-2019",
    "macos-latest", "macos-14", "macos-13", "macos-12",
}

log.info(
    "Build server starting: MAX_RUNNERS=%d DEFAULT_IMAGE=%s ALLOWED_ORGS=%s",
    MAX_RUNNERS, DEFAULT_IMAGE, ALLOWED_ORGS or "unrestricted",
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


@app.post("/webhook")
async def webhook(request: Request):
    body = await request.body()
    _verify_signature(body, request.headers.get("X-Hub-Signature-256", ""))

    payload = await request.json()
    action = payload.get("action")
    log.debug("Webhook received: action=%s event=%s", action, "workflow_job" if "workflow_job" in payload else "other")
    if action != "queued" or "workflow_job" not in payload:
        return {"ok": True}

    installation_id = payload["installation"]["id"]
    org = payload["repository"]["owner"]["login"]
    labels: list[str] = payload["workflow_job"].get("labels", [])
    job_id = payload["workflow_job"]["id"]

    if ALLOWED_ORGS and org not in ALLOWED_ORGS:
        log.warning("Ignored job from unlisted org=%s job_id=%d", org, job_id)
        return {"ok": True}

    if all(lbl in _GITHUB_HOSTED for lbl in labels):
        return {"ok": True}

    log.info("Job queued: org=%s job_id=%d labels=%s", org, job_id, labels)
    asyncio.create_task(spawn_runner(installation_id, org, labels, job_id))
    return {"ok": True}


async def spawn_runner(installation_id: int, org: str, labels: list[str], job_id: int):
    async with sem:
        active = MAX_RUNNERS - sem._value  # noqa: SLF001
        log.info("Spawning runner: org=%s job_id=%d active=%d/%d", org, job_id, active, MAX_RUNNERS)
        try:
            token = gi.get_access_token(installation_id).token
        except Exception:
            log.exception("Failed to get installation token for installation_id=%d", installation_id)
            return

        image = _parse_image(labels)
        custom_labels = [lbl for lbl in labels if not lbl.startswith("runner-image:") and lbl not in _GITHUB_HOSTED]
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
            await loop.run_in_executor(
                None,
                lambda: docker_client.containers.run(
                    image,
                    remove=True,
                    volumes={"/var/run/docker.sock": {"bind": "/var/run/docker.sock", "mode": "rw"}},
                    environment=env,
                ),
            )
            log.info("Runner finished: org=%s job_id=%d", org, job_id)
        except Exception:
            log.exception("Runner failed: org=%s job_id=%d image=%s", org, job_id, image)
