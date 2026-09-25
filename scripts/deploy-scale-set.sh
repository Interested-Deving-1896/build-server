#!/usr/bin/env bash
# Deploy one ARC scale-set (= one Helm release).
#
# Usage:
#   APP_ID=… PRIVATE_KEY_FILE=path INSTALL_ID=… ORG=… NAME=… [IMAGE=…] [MAX=…] \
#     scripts/deploy-scale-set.sh
#
# Optional, and mandatory when the pool's namespace or Helm release name is not
# the derived default: NAMESPACE, RELEASE. Plus sizing: CPU_REQUEST,
# MEM_LIMIT, DIND_CPU_REQUEST, DIND_MEM_REQUEST, DIND_CPU_LIMIT,
# DIND_MEM_LIMIT, REGISTRY_MIRRORS ("host1 host2", highest priority first).
#
# Prerequisites (once per namespace):
#   kubectl -n arc-<org> create secret docker-registry ghcr-pull \
#     --docker-server=ghcr.io \
#     --docker-username=<github-user> \
#     --docker-password=<PAT with packages:read>
#
# Examples:
#   APP_ID=123 INSTALL_ID=456 ORG=my-org NAME=my-org-linux MAX=8 \
#     PRIVATE_KEY_FILE=/etc/build-server/my-org.pem scripts/deploy-scale-set.sh
set -euo pipefail

: "${APP_ID:?GITHUB_APP_ID required}"
: "${INSTALL_ID:?GITHUB_APP_INSTALLATION_ID required}"
: "${ORG:?ORG required (GitHub org or user)}"
: "${NAME:?NAME required — scale-set name, must match runs-on: label in workflows}"
# No apostrophe in a :? message — inside ${VAR:?word} it opens a single quote
# that never closes, and the whole script dies at parse time.
: "${PRIVATE_KEY_FILE:?PRIVATE_KEY_FILE required — path to the GitHub App PEM file}"
IMAGE="${IMAGE:-ghcr.io/jakwuh/actions-runner:latest}"
MAX="${MAX:-8}"
MIN="${MIN:-1}"
# Per-container CPU/memory requests. These bound the scheduler so it never
# overpacks the node — without them a container is "weightless" → CPU contention
# → dind's managed containerd misses its 15s startup window → dind exits 1,
# runner hangs Running (1/2 Error forever), build times climb. The dind container
# is the one that runs dockerd + that managed containerd (and every
# `docker build`), so it MUST carry its OWN request — a requested runner sitting
# next to a weightless dind still lets the scheduler overpack dind and starve
# containerd at startup. Sized from p90 of live builds (runner 1.6 cores / 0.9Gi).
CPU_REQUEST="${CPU_REQUEST:-1}"
MEM_REQUEST="${MEM_REQUEST:-1.5Gi}"
DIND_CPU_REQUEST="${DIND_CPU_REQUEST:-1}"
DIND_MEM_REQUEST="${DIND_MEM_REQUEST:-1.5Gi}"
# Memory limits are mandatory, not tuning. A request only tells the scheduler how
# many pods fit; it does not stop one of them from eating the box. On 2026-09-24
# bld1 (24 vCPU / 62 GiB) ran 20 runners whose real footprint was 2.3–7.8 GiB per
# pod against a 1.75 GiB request: 58 GiB used, swap thrashing at 50 MB/s, load
# 351. k3s lost to the builds — kine answered in 18–83 s, the apiserver returned
# `Handler timeout`, kubelet reported `PLEG is not healthy`, and the node flapped
# NotReady long enough for the taint manager to evict the ARC listeners and for
# the kernel OOM killer to take arc-gha-rs-controller (exit 137). With no
# listener GitHub has nowhere to place jobs, and CI queues silently — the exact
# outage arc-watchdog was written for, except the watchdog cannot cure it.
# With a limit the offending container is OOM-killed alone and one job goes red.
MEM_LIMIT="${MEM_LIMIT:-6Gi}"
DIND_MEM_LIMIT="${DIND_MEM_LIMIT:-4Gi}"
# CPU limits, for the same reason as the memory ones. dind is where `docker
# build` actually runs, and buildkit fans out to every core it can see: measured
# on bld1 on 2026-09-24, one pod was taking 14.8 of 24 cores while its runner
# container sat under its own 4-core cap — the whole 14.8 was in the uncapped
# dind. A pod ceiling of CPU_LIMIT + DIND_CPU_LIMIT keeps one job from
# monopolising the box while leaving burst room for compile-heavy steps.
CPU_LIMIT="${CPU_LIMIT:-4}"
DIND_CPU_LIMIT="${DIND_CPU_LIMIT:-4}"
# Extra dockerd registry mirrors, highest priority first (space-separated). The
# built-in https://mirror.gcr.io is always appended last.
REGISTRY_MIRRORS="${REGISTRY_MIRRORS:-}"
# Pinned: unpinned, a redeploy of an unchanged scale-set silently moves the pool
# to whatever ARC released since. Must match the controller version setup.sh
# installs — the listener image comes from the controller, the runner spec from
# this chart, and ARC does not support them drifting apart.
CHART_VERSION="${CHART_VERSION:-0.14.2}"

# Namespace defaults to the lowercased org, but the two are not the same fact:
# `githubConfigUrl` must carry the org exactly as GitHub spells it, while the
# namespace is whatever the pool was first created under. bld1 runs the Miraj-OS
# pool in `arc-miraj`, not the `arc-miraj-os` this default would derive — deploy
# it without the override and you get a second, parallel scale-set long-polling
# the same org for the same `runs-on` label instead of an upgrade of the first.
NAMESPACE="${NAMESPACE:-}"
NS="${NAMESPACE:-arc-$(echo "$ORG" | tr '[:upper:]' '[:lower:]')}"
# Same story for the Helm release name: it defaults to the scale-set name, but
# the two are independent and on bld1 they differ — the Miraj-OS pool is the
# release `miraj-self-hosted` serving `runs-on: self-hosted`. Get it wrong and
# helm refuses outright ("cannot be imported into the current release"), which
# is the pleasant failure; the namespace one above fails silently.
RELEASE="${RELEASE:-$NAME}"
export KUBECONFIG=/etc/rancher/k3s/k3s.yaml

PRIV_KEY=$(cat "$PRIVATE_KEY_FILE")

kubectl create namespace "$NS" --dry-run=client -o yaml | kubectl apply -f -
kubectl -n "$NS" delete secret github-app --ignore-not-found
kubectl -n "$NS" create secret generic github-app \
  --from-literal=github_app_id="$APP_ID" \
  --from-literal=github_app_installation_id="$INSTALL_ID" \
  --from-literal=github_app_private_key="$PRIV_KEY"

# Build a full-spec overlay so helm never has to construct partial array
# elements (--set containers[0].x replaces the entire element, losing
# name/image/command/etc. and producing an invalid AutoscalingRunnerSet).
OVERLAY=$(mktemp /tmp/arc-overlay-XXXXXX.yaml)
trap 'rm -f "$OVERLAY"' EXIT

MIRROR_ARGS=""
for mirror in $REGISTRY_MIRRORS https://mirror.gcr.io; do
  MIRROR_ARGS+="
      - --registry-mirror=$mirror"
done

cat > "$OVERLAY" << YAML
minRunners: $MIN
maxRunners: $MAX
listenerTemplate:
  spec:
    # The listener is the only thing that can accept a job from GitHub, and on a
    # single-node cluster there is nowhere to reschedule it — the default 300s
    # NoExecute tolerations only guarantee that a node blip takes the pool
    # offline. bld1 on 2026-09-24: the node flapped NotReady under CI load, the
    # taint manager evicted both listener pods, and the pool then sat without a
    # listener for 42 minutes while jobs queued with no red check anywhere.
    tolerations:
    - { key: node.kubernetes.io/not-ready,   operator: Exists, effect: NoExecute }
    - { key: node.kubernetes.io/unreachable, operator: Exists, effect: NoExecute }
    containers:
    # Requests, so the listener is not BestEffort: that QoS class is what the
    # kernel OOM killer reaches for first, and it is exactly the pod whose death
    # is invisible. Measured usage is 3m CPU / 10Mi.
    - name: listener
      resources:
        requests: { cpu: 50m, memory: 64Mi }
        limits: { memory: 256Mi }
template:
  spec:
    imagePullSecrets:
    - name: ghcr-pull
    initContainers:
    - name: init-dind-externals
      image: $IMAGE
      imagePullPolicy: IfNotPresent
      command: [cp, -r, /home/runner/externals/., /home/runner/tmpDir/]
      volumeMounts:
      - { mountPath: /home/runner/tmpDir, name: dind-externals }
    containers:
    - name: runner
      image: $IMAGE
      imagePullPolicy: IfNotPresent
      command:
      - /bin/bash
      - -c
      - until /usr/bin/docker info >/dev/null 2>&1; do sleep 1; done; exec /home/runner/run.sh
      env:
      - { name: DOCKER_HOST, value: unix:///var/run/docker.sock }
      resources:
        requests:
          cpu: "$CPU_REQUEST"
          memory: $MEM_REQUEST
        limits:
          cpu: "$CPU_LIMIT"
          memory: $MEM_LIMIT
      volumeMounts:
      - { mountPath: /home/runner/_work, name: work }
      - { mountPath: /var/run, name: dind-sock }
    - name: dind
      image: mirror.gcr.io/library/docker:dind
      imagePullPolicy: IfNotPresent
      args:
      - dockerd
      - --host=unix:///var/run/docker.sock
      - --group=123$MIRROR_ARGS
      securityContext:
        privileged: true
      resources:
        requests:
          cpu: "$DIND_CPU_REQUEST"
          memory: $DIND_MEM_REQUEST
        limits:
          cpu: "$DIND_CPU_LIMIT"
          memory: $DIND_MEM_LIMIT
      volumeMounts:
      - { mountPath: /home/runner/_work, name: work }
      - { mountPath: /var/run, name: dind-sock }
      - { mountPath: /home/runner/externals, name: dind-externals }
    volumes:
    # work and dind-externals are node disk, not tmpfs. medium: Memory makes the
    # volume RAM the scheduler cannot see — emptyDir does not enter a pod's
    # memory request, and sizeLimit is per volume, so maxRunners: 20 promised
    # 320 GiB of tmpfs on a 62 GiB box. bld1 held 33.8 GiB of RAM in 72 such
    # volumes on 2026-09-24 while its disk sat 31% used; tmpfs pages can only
    # leave RAM through swap, which is what put the node into thrash and took
    # k3s down with it. The job tree belongs on the disk that has 134 GiB free.
    - { name: work,           emptyDir: { sizeLimit: 16Gi  } }
    - { name: dind-sock,      emptyDir: { medium: Memory, sizeLimit: 256Mi } }
    - { name: dind-externals, emptyDir: { sizeLimit: 1Gi   } }
YAML

helm upgrade --install "$RELEASE" \
  --namespace "$NS" \
  --set githubConfigUrl="https://github.com/$ORG" \
  --set githubConfigSecret=github-app \
  --set "runnerScaleSetName=$NAME" \
  -f "$OVERLAY" \
  --version "$CHART_VERSION" \
  oci://ghcr.io/actions/actions-runner-controller-charts/gha-runner-scale-set

kubectl -n "$NS" get autoscalingrunnerset
