# build-server

Self-hosted GitHub Actions runner pool on a single host, powered by upstream [actions-runner-controller](https://github.com/actions/actions-runner-controller) (ARC) on k3s. ARC uses GitHub's **Just-in-Time runner configs** — each pod is bound to one specific job, so there is no shared org pool and no spawn race.

This repo provides:
- Deploy scaffolding (`setup.sh`, `scripts/deploy-scale-set.sh`).
- A reference generic runner image at `ghcr.io/jakwuh/actions-runner:latest` (Dockerfile in `runner-image/`). It's an upstream `actions-runner` + the minimum CLI toolkit (`gh`, `aws`, `jq`, `git`, `curl`, `unzip`, `zip`, `xz`, `rsync`, `gnupg`) — everything else (Node, Java, Flutter, Playwright, Android SDK, Python …) is installed on demand by workflow steps via `setup-*` actions, cached through `actions/cache`. Use this for every scale-set unless you have a reason not to.

## Architecture

```
GitHub Actions broker ──long-poll──► ARC listener ─► ARC controller ─► k8s pod (JIT runner) ──► job
```

One **scale-set** per `(GitHub org, runner image)` pair. The scale-set's name becomes the `runs-on:` label workflows use.

## Bootstrap a host

```bash
# Fresh host (24+ vCPU, 16+ GB RAM recommended for production load):
ssh root@<HOST>
bash <(curl -fsSL https://raw.githubusercontent.com/jakwuh/build-server/main/setup.sh)
```

`setup.sh` installs k3s + helm + the ARC controller into namespace `arc-systems`. Then deploy one scale-set per `(org, image)` you need.

## Deploy a scale-set

```bash
APP_ID=<github-app-id> \
INSTALL_ID=<github-app-installation-id> \
ORG=<github-org-or-user> \
NAME=<scale-set-name>           # = the runs-on: label \
IMAGE=ghcr.io/your-org/your-runner:tag \
MAX=20 \
PRIVATE_KEY_FILE=/path/to/app-private-key.pem \
scripts/deploy-scale-set.sh
```

The GitHub App webhook URL is **not used** — ARC pulls from GitHub's runner broker via long-polling with the App credentials.

### The two scale-sets on `bld1`

Written down because the sizing is not the defaults, and re-running the script without
these would quietly shrink the pools and drop miraj's local registry mirror. `PRIVATE_KEY_FILE`
is the **jakwuh-build-server** App PEM; App id `3743839` for both.

```bash
# izi-x org — the product CI pool
APP_ID=3743839 INSTALL_ID=133105803 ORG=izi-x NAME=izi-x-linux MAX=20 \
  CPU_REQUEST=500m MEM_REQUEST=2Gi DIND_CPU_REQUEST=250m DIND_MEM_REQUEST=1Gi \
  IMAGE=ghcr.io/jakwuh/actions-runner:<sha> \
  PRIVATE_KEY_FILE=<app>.pem scripts/deploy-scale-set.sh

# Miraj-OS org — `runs-on: self-hosted`; also pulls through the in-cluster registry cache.
# NAMESPACE is mandatory here: the pool lives in arc-miraj, while the default
# derived from the org would be arc-miraj-os. Deploy without it and you get a
# second scale-set sharing the same GitHub registration instead of an upgrade.
APP_ID=3743839 INSTALL_ID=133143010 ORG=Miraj-OS NAME=self-hosted MAX=8 \
  NAMESPACE=arc-miraj RELEASE=miraj-self-hosted \
  MEM_REQUEST=2Gi DIND_MEM_REQUEST=1Gi \
  REGISTRY_MIRRORS=http://10.43.104.17:5000 \
  IMAGE=ghcr.io/jakwuh/actions-runner:<sha> \
  PRIVATE_KEY_FILE=<app>.pem scripts/deploy-scale-set.sh
```

Pin `IMAGE` to a commit sha, never `:latest` — a scale-set is only rolled when its pod
template changes, so a moving tag means the pool keeps running whatever it pulled first.

## Runner image contract

The reference image (`runner-image/`) and any custom image you want to use must satisfy ARC's DinD container mode:

1. **Base on `ghcr.io/actions/actions-runner:latest`** (or any image that ships the upstream runner layout). That gets you everything below for free.
2. **`/home/runner/{run.sh,config.sh,bin,externals,k8s,env.sh,...}`** must be present. The chart's `init-dind-externals` init container `cp -r`s from `/home/runner/externals`; the runner container `exec`s `/home/runner/run.sh`. Missing either → `Init:Error` or `OCI runtime ... no such file or directory`.
3. **`runner` user must be in a group with GID 123.** The chart hardcodes `DOCKER_GROUP_GID=123` for the dind sidecar, so the docker socket ends up owned `root:123` — the runner needs that group to use it. The upstream image already puts `runner` in `docker:123`. Without it: `permission denied while trying to connect to the docker API at unix:///var/run/docker.sock`.

`myoung34/github-runner` does **not** satisfy any of the above. It has no `run.sh`, no `externals/`, and its `docker` group is GID 500. Don't use it as a base — there's no clean ARC-DinD adapter that doesn't end up being a wrapper image with the missing pieces re-copied in.

## Not in this repo

`setup.sh` gets a fresh box to a working pool, but it cannot produce these. Check them off by
hand when you rebuild or move the host, or the box will come up looking healthy and quietly
serving nothing:

| What | Where it comes from |
|---|---|
| Tailnet membership + the `tag:buildsrv` tag | `tailscale up --authkey` with an auth key from 1Password; the tag is what the tailnet policy grants on |
| Tailnet grants to reach the clusters | the tailnet ACL (`tag:buildsrv` → `tag:k8s-operator`, impersonating a group that RBAC binds inside the target cluster) |
| `github-app` secret in each `arc-*` namespace | GitHub App **jakwuh-build-server** (app id, installation id, private key) — the same App the runner healthcheck mints tokens from |
| `ghcr-pull` secret in each `arc-*` namespace, and the matching `ghcr.io` entry in root's `~/.docker/config.json` | a ghcr read token, for the custom runner image. **It expires, and both copies go dead together.** A dead token does not degrade gracefully: ghcr answers the token endpoint with `403 denied` instead of falling through to the anonymous access a public chart would get, so `helm upgrade … oci://ghcr.io/actions/…` fails for every scale-set even though that chart needs no credentials at all. Found that way on 2026-09-24 with a token issued 2026-05-18. Check with `curl -so /dev/null -w '%{http_code}\n' -H "Authorization: Basic $(jq -r '.auths["ghcr.io"].auth' ~/.docker/config.json)" 'https://ghcr.io/token?scope=repository%3Aactions%2Factions-runner-controller-charts%2Fgha-runner-scale-set%3Apull&service=ghcr.io'` — 200 is healthy, 403 means rotate it in both places |
| `/etc/arc-watchdog/{tg-token,config}` | alerts bot token + chat id; without them the watchdog heals silently |
| Anything izi-x-specific | lives in `izi-x/izi-x-infra`, not here — e.g. `ops/pr-stand-janitor` |

The rule for what belongs where: this repo is the **build server as a machine** — the pool,
the image, the things that keep the pool alive. Anything that knows about a particular
product's clusters, namespaces or databases belongs to that product's repo, even when it
physically runs on this host.

## Self-heal watchdog

`scripts/arc-watchdog.sh` (installed by `setup.sh` as an `arc-watchdog.timer` firing every
3 minutes) exists because ARC can die in ways that produce **no red check anywhere** — jobs
simply queue forever. Both of these happened for real on 2026-08-06/07 after a GitHub Actions
outage and cost ~12 hours:

- the `AutoscalingListener` CR keeps pointing at a deleted `EphemeralRunnerSet`, so the
  listener pod crash-loops on `could not patch ephemeral runner set ... not found`;
- the controller wedges outright (log frozen mid `deleting runner scale set`) and no listener
  is created at all.

The watchdog heals on the second consecutive unhealthy check — deleting the stale listener CR
in the first case, restarting the controller in the second — and announces what it did to
Telegram if `/etc/arc-watchdog/tg-token` (chmod 600) and `TG_CHAT=` in `/etc/arc-watchdog/config`
are present. Without those it heals silently.

**The restart is not a reliable cure.** On 2026-09-24 both pools had no listener from 15:31 to
16:13 UTC and seven restarts, one every six minutes, changed nothing; what brought them back was
a helm upgrade that altered the runner pod template and so forced a fresh EphemeralRunnerSet and
listener. A repeating alert therefore means the repair is *not* working — treat it as a page, not
as a resolution. Before each heal the watchdog now dumps the controller and listener logs, the
CRs, the pods and the events to `/var/lib/arc-watchdog/incident-<ts>-<ns>_<name>/` (last 20 kept),
because `rollout restart` destroys the controller pod and its log, which is why the 2026-09-24
wedge can no longer be explained. It also no longer counts a strike when the API is unreadable:
a starved apiserver is not an absent listener, and restarting the controller against one only
adds a full re-LIST to the queue it is already drowning in.

```bash
systemctl list-timers arc-watchdog.timer arc-runner-janitor.timer
journalctl -u arc-watchdog.service --since -1h
/opt/build-server/arc-watchdog.sh          # run once by hand; silence == healthy
```

`scripts/arc-runner-janitor.sh` (every 5 minutes) covers the neighbouring failure: the dind
sidecar exits while the runner container keeps running, so the pod sits at `1/2 Error`
forever — taking no work, holding its CPU requests. Enough of them and the node hits its
requests ceiling, the next dind's containerd misses its startup window and becomes another
zombie. That loop stalled CI on 2026-08-06 after a reboot: 23 queued runs against a pool that
looked healthy. Deleting the pod is safe — ARC recreates it, GitHub re-assigns the job.

## Operations

```bash
# List scale-sets and pods
kubectl get autoscalingrunnerset -A
kubectl get pods -A | grep -E '^arc-'

# Tail listener logs
kubectl -n arc-systems logs -l app.kubernetes.io/component=runner-scale-set-listener -f

# Bump max runners on an existing release
helm upgrade <release> -n <namespace> --reuse-values --set maxRunners=50 \
  oci://ghcr.io/actions/actions-runner-controller-charts/gha-runner-scale-set
```

## Tuning

- **Per-host capacity**: `maxRunners` per scale-set + pod template resource *requests* decide how many pods the scheduler admits. They do not decide how much a pod may then take — that is the *limits*, and a pool without them will eventually take the node down instead of failing one job. `MEM_LIMIT` / `DIND_MEM_LIMIT` / `CPU_LIMIT` / `DIND_CPU_LIMIT` in `scripts/deploy-scale-set.sh` are not optional tuning. Cap `dind` as hard as the runner: buildkit fans out to every core it can see, and on 2026-09-24 a single pod was taking 14.8 of 24 cores through its uncapped dind while the runner container beside it sat under its own 4-core cap.
- **The control plane does not compete.** `setup.sh` reserves CPU and memory for k3s and the system through `/etc/rancher/k3s/config.yaml`. Without it the apiserver and kine lose to the builds, the node flaps NotReady, and the ARC listeners get evicted — the pool dies with jobs queuing and nothing red anywhere.
- **Never put the job tree in tmpfs.** `emptyDir: { medium: Memory }` is RAM the scheduler cannot account for — it is charged to nobody's request, and `sizeLimit` is per volume, so `maxRunners: 20` with a 16 GiB `work` volume promises 320 GiB on the box. It ends in swap thrash, not in an eviction. `work` and `dind-externals` are node disk; only the 32 KB `dind-sock` stays in memory.
- **Burst latency**: first pull of a runner image is slow (~1-2 min for multi-GB images). Subsequent spawns hit local cache and start in ~30s.
