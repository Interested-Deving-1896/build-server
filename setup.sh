#!/usr/bin/env bash
# Idempotent VPS provisioner for the build server.
# Run as root on the target VPS.
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

echo "=== Installing systemd service ==="
cp "$INSTALL_DIR/systemd/build-server.service" /etc/systemd/system/build-server.service
systemctl daemon-reload
systemctl enable build-server

echo "=== Configuring nginx reverse proxy ==="
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

echo ""
echo "=== Done ==="
echo "Next: copy .env.example to /opt/build-server/.env and fill in credentials,"
echo "then: systemctl start build-server && systemctl status build-server"
echo ""
echo "Webhook URL: http://$(curl -s ifconfig.me)/webhook"
