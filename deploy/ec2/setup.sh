#!/bin/sh
# Install the Hyderabad publisher on the EC2 box. Run from the project root:
#   sudo sh deploy/ec2/setup.sh
# /etc/airport-eye.env must already exist. This script does not create the password.
set -eu

ROOT="$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)"
install_root=/opt/airport-eye
service_user="${SUDO_USER:-ec2-user}"

if [ "$(id -u)" -ne 0 ]; then
  echo "Run with sudo from the project root." >&2
  exit 1
fi
if [ ! -f /etc/airport-eye.env ]; then
  echo "Create /etc/airport-eye.env from deploy/ec2/airport-eye.env.example first." >&2
  exit 1
fi
if ! command -v python3.11 >/dev/null 2>&1; then
  echo "python3.11 is not installed." >&2
  exit 1
fi

mkdir -p "$install_root"
# Copy the app. Skip the Mac virtualenv, generated output, and secrets.
tar -C "$ROOT" --exclude .venv --exclude output --exclude .poc --exclude __pycache__ \
  -cf - pyproject.toml src config deploy | tar -C "$install_root" -xf -
chown -R "$service_user:$service_user" "$install_root"

sudo -u "$service_user" python3.11 -m venv "$install_root/.venv"
sudo -u "$service_user" "$install_root/.venv/bin/pip" install --upgrade pip
sudo -u "$service_user" "$install_root/.venv/bin/pip" install -e "$install_root"

python_bin="$(command -v python3.11)"
sed "s/^User=.*/User=${service_user}/" "$ROOT/deploy/ec2/airport-eye-pax.service" \
  > /etc/systemd/system/airport-eye-pax.service
sed -e "s/^User=.*/User=${service_user}/" \
    -e "s|^ExecStart=.*|ExecStart=${python_bin} ${install_root}/deploy/ec2/publisher_status.py --host 127.0.0.1 --port 8787|" \
    "$ROOT/deploy/ec2/airport-eye-status.service" \
  > /etc/systemd/system/airport-eye-status.service
chmod 600 /etc/airport-eye.env
systemctl daemon-reload
systemctl enable --now airport-eye-pax.service airport-eye-status.service
systemctl --no-pager --full status airport-eye-pax.service airport-eye-status.service
