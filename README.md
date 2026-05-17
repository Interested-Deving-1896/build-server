# build-server

Shared GitHub Actions runner pool for self-hosted CI on a single VPS.

## How it works

- A GitHub App receives `workflow_job.queued` webhooks from any installed org
- For each queued job, an ephemeral Docker container is spawned on the VPS
- Concurrency is bounded by `MAX_RUNNERS` (defaults to `os.cpu_count()`)
- Containers use the `myoung34/github-runner` image by default, or a custom image specified via label

## Runner image per job

To use a custom runner image, add a `runner-image:` label to `runs-on`:

```yaml
runs-on: [self-hosted, runner-image:ghcr.io/my-org/my-runner:latest]
```

The build server parses the `runner-image:` prefix and spawns that image. Jobs without this label use `DEFAULT_RUNNER_IMAGE` from `.env`.

## Setup

### 1. Create the GitHub App

Go to https://github.com/settings/apps/new and configure:

- **Webhook URL**: `http://<VPS-IP>/webhook`
- **Webhook secret**: generate a random string, save to `.env`
- **Organization permissions**: Self-hosted runners → Read & write
- **Subscribe to events**: Workflow jobs

After creation:
- Note the **App ID**
- Generate a **private key** (PEM file)
- Install the App on each org that should use this runner pool

### 2. Provision the VPS

```bash
ssh root@<VPS-IP>
curl -fsSL https://raw.githubusercontent.com/jakwuh/build-server/main/setup.sh | bash
cp /opt/build-server/.env.example /opt/build-server/.env
# Edit .env with your App ID, private key, webhook secret
systemctl start build-server
systemctl status build-server
```

### 3. Add more orgs

Install the GitHub App on the new org — that's it. No server config changes needed.

## Tuning concurrency

The default (`os.cpu_count()`) works well for CPU-bound builds.
For memory-heavy builds, set `MAX_RUNNERS=floor(RAM_GB / peak_job_GB)` in `.env`.
