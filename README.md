# build-server

Shared GitHub Actions runner pool for self-hosted CI on a single VPS.

## How it works

- An org-level GitHub webhook fires `workflow_job.queued` for every queued job
- The build server spawns an ephemeral Docker container as a self-hosted runner for that org
- Concurrency is bounded by `MAX_RUNNERS` (defaults to `os.cpu_count()`)
- All orgs share the same pool — idle capacity is never wasted on one org while another is busy

## Runner image per job

Add a `runner-image:` label to `runs-on` to use a custom image:

```yaml
runs-on: [self-hosted, runner-image:ghcr.io/my-org/my-runner:latest]
```

Jobs without this label use `DEFAULT_RUNNER_IMAGE` from `.env`.

## Setup

### 1. VPS provisioning

```bash
ssh root@<VPS-IP>
curl -fsSL https://raw.githubusercontent.com/jakwuh/build-server/main/setup.sh | bash
cp /opt/build-server/.env.example /opt/build-server/.env
# Edit .env — set GITHUB_PAT and WEBHOOK_SECRET
systemctl start build-server
systemctl status build-server
```

### 2. Connect an org

Create an org-level webhook (run once per org):

```bash
gh api --method POST /orgs/{ORG}/hooks \
  --field name=web \
  --field active=true \
  --field events='["workflow_job"]' \
  --field config[url]="http://<VPS-IP>/webhook" \
  --field config[content_type]=json \
  --field config[secret]="<WEBHOOK_SECRET from .env>"
```

### 3. Add more orgs

Repeat step 2 for each new org — no server config changes needed.

## Tuning concurrency

The default (`os.cpu_count()`) works well for CPU-bound builds.
For memory-heavy builds: `MAX_RUNNERS=floor(RAM_GB / peak_job_GB)`.
