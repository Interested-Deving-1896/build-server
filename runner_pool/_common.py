"""Shared helpers used by both the gateway and the runner roles."""
import logging
import os
import time

import jwt as _jwt
import requests as _requests
from github import GithubIntegration

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ---------- GitHub App ----------
# Both roles need this:
#   - gateway uses it for the queue poller (list installation repos + queued jobs)
#   - runner uses it to mint installation tokens when registering each container
_github_app_id_env = os.getenv("GITHUB_APP_ID")
_github_app_key_env = os.getenv("GITHUB_APP_PRIVATE_KEY")
if _github_app_id_env and _github_app_key_env:
    _private_key = _github_app_key_env.replace("\\n", "\n")
    _app_id = int(_github_app_id_env)
    gi: GithubIntegration | None = GithubIntegration(_app_id, _private_key)
else:
    _private_key = ""
    _app_id = 0
    gi = None


def resolve_installation_id(org: str, owner_type: str) -> int | None:
    """Look up the App installation id for an org (or user account)."""
    if gi is None:
        return None
    now = int(time.time())
    token = _jwt.encode(
        {"iat": now - 60, "exp": now + 600, "iss": str(_app_id)},
        _private_key, algorithm="RS256",
    )
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


def installation_token(installation_id: int) -> str:
    """Mint an installation access token."""
    assert gi is not None, "GitHub App not configured"
    return gi.get_access_token(installation_id).token


def generate_jit_config(
    itoken: str, owner_type: str, org_or_repo: str,
    name: str, labels: list[str], runner_group_id: int = 1,
) -> str:
    """Generate a Just-in-Time runner config for ONE specific job assignment.

    Unlike a long-lived ephemeral runner registration (which joins the org pool
    and runs any matching queued job FIFO), a JIT runner is bound to a single
    job allocation. This eliminates the org-pool race that caused the entire
    class of 'we spawned for X but our container served Y' bugs.

    Returns base64-encoded jitconfig the runner agent uses via `./run.sh
    --jitconfig <token>`.

    https://docs.github.com/en/rest/actions/self-hosted-runners#create-configuration-for-a-just-in-time-runner-for-an-organization
    """
    if owner_type == "User":
        # Repo-scoped JIT for personal accounts (no org runners API).
        url = f"https://api.github.com/repos/{org_or_repo}/actions/runners/generate-jitconfig"
    else:
        url = f"https://api.github.com/orgs/{org_or_repo}/actions/runners/generate-jitconfig"
    resp = _requests.post(
        url,
        headers={"Authorization": f"token {itoken}", "Accept": "application/vnd.github+json"},
        json={
            "name": name,
            "runner_group_id": runner_group_id,
            "labels": labels,
            "work_folder": "_work",
        },
        timeout=10,
    )
    if not resp.ok:
        raise RuntimeError(f"jitconfig generate failed: {resp.status_code} {resp.text[:300]}")
    return resp.json()["encoded_jit_config"]


# ---------- Telegram ----------
_TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
_TG_CHAT = os.getenv("TELEGRAM_CHAT_ID", "")


def tg_notify(text: str) -> None:
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


# ---------- Shared config & label sets ----------
ALLOWED_ORGS: set[str] = set(filter(None, os.getenv("ALLOWED_ORGS", "").split(",")))
INTERNAL_SECRET = os.getenv("INTERNAL_SECRET", "")

GITHUB_HOSTED = {
    "ubuntu-latest", "ubuntu-24.04", "ubuntu-22.04", "ubuntu-20.04", "ubuntu-18.04",
    "windows-latest", "windows-2022", "windows-2019",
    "macos-latest", "macos-14", "macos-13", "macos-12",
}
NON_LINUX_OS = {"Windows", "macOS"}


def mem_available_mb() -> int:
    """MemAvailable from /proc/meminfo, in MB. Returns 0 if unreadable."""
    try:
        with open("/proc/meminfo") as f:
            for line in f:
                if line.startswith("MemAvailable:"):
                    return int(line.split()[1]) // 1024
    except OSError:
        log.exception("Failed to read /proc/meminfo")
    return 0
