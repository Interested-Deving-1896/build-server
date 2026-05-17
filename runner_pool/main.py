import asyncio
import hashlib
import hmac
import logging
import os

import docker
from fastapi import FastAPI, HTTPException, Request
from github import GithubIntegration

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

app = FastAPI()
docker_client = docker.from_env()

_private_key = os.environ["GITHUB_APP_PRIVATE_KEY"].replace("\\n", "\n")
gi = GithubIntegration(os.environ["GITHUB_APP_ID"], _private_key)

MAX_RUNNERS = int(os.getenv("MAX_RUNNERS") or os.cpu_count())
sem = asyncio.Semaphore(MAX_RUNNERS)
DEFAULT_IMAGE = os.environ["DEFAULT_RUNNER_IMAGE"]

log.info("Build server starting: MAX_RUNNERS=%d DEFAULT_IMAGE=%s", MAX_RUNNERS, DEFAULT_IMAGE)


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
    if action != "queued" or "workflow_job" not in payload:
        return {"ok": True}

    installation_id = payload["installation"]["id"]
    org = payload["repository"]["owner"]["login"]
    labels: list[str] = payload["workflow_job"].get("labels", [])
    job_id = payload["workflow_job"]["id"]

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
        env = {
            "RUNNER_SCOPE": "org",
            "ORG_NAME": org,
            "ACCESS_TOKEN": token,
            "EPHEMERAL": "true",
            "DISABLE_AUTO_UPDATE": "true",
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
