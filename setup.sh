#!/usr/bin/env bash
# Idempotent VPS provisioner. Run as root on the target host.
#
# Two systemd services are installed; you choose which to enable per host:
#
#   build-server-gateway.service  — receives GitHub webhooks, dispatches to
#                                   runners. Listens on 127.0.0.1:3000 (nginx
#                                   reverse-proxies :80 → :3000). Set WORKERS
#                                   in .env to a comma-separated list of
#                                   runner URLs (e.g. http://127.0.0.1:3001
#                                   for a self-contained single-host deploy).
#
#   build-server-runner.service   — accepts /spawn from a gateway, spawns
#                                   ephemeral github-runner containers.
#                                   Listens on 127.0.0.1:3001.
#
# Single-host deploy: enable both. Multi-host: gateway on one box, runner on
# each of N worker boxes.
set -euo pipefail

INSTALL_DIR=/opt/build-server
REPO_URL=https://github.com/jakwuh/build-server.git

echo "=== Installing system dependencies ==="
apt-get update -qq
apt-get install -y -qq python3 python3-venv python3-pip nginx git

echo "=== Installing / updating build-server ==="
if [ -d "$INSTALL_DIR/.git" ]; then
  git -C "$INSTALL_DIR" pull --ff-only
else
  git clone "$REPO_URL" "$INSTALL_DIR"
fi

echo "=== Setting up Python venv ==="
python3 -m venv "$INSTALL_DIR/venv"
"$INSTALL_DIR/venv/bin/pip" install --quiet --upgrade pip
"$INSTALL_DIR/venv/bin/pip" install --quiet -r "$INSTALL_DIR/runner_pool/requirements.txt"

echo "=== Installing systemd units ==="
cp "$INSTALL_DIR/systemd/build-server-gateway.service" /etc/systemd/system/
cp "$INSTALL_DIR/systemd/build-server-runner.service"  /etc/systemd/system/
systemctl daemon-reload
# Enable both by default — disable whichever you don't want for this host.
systemctl enable build-server-gateway build-server-runner

echo "=== Configuring nginx reverse proxy (→ gateway on :3000) ==="
cat > /etc/nginx/sites-available/build-server << 'EOF'
server {
    listen 80;
    server_name _;

    location / {
        proxy_pass http://127.0.0.1:3000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Hub-Signature-256 $http_x_hub_signature_256;
        proxy_read_timeout 30s;
    }
}
EOF
ln -sf /etc/nginx/sites-available/build-server /etc/nginx/sites-enabled/build-server
rm -f /etc/nginx/sites-enabled/default
nginx -t && systemctl enable --now nginx && systemctl reload nginx

echo
echo "=== Done ==="
echo "Next:"
echo "  1. cp $INSTALL_DIR/.env.example $INSTALL_DIR/.env and fill in credentials."
echo "  2. systemctl start build-server-runner build-server-gateway"
echo "  3. (single-host) ensure .env has WORKERS=http://127.0.0.1:3001"
echo
echo "Webhook URL: http://$(curl -s ifconfig.me)/webhook"
