#!/bin/sh
set -e

SERVICE=modbus-reader

if command -v systemctl >/dev/null 2>&1; then
  systemctl daemon-reload
  systemctl enable --now "${SERVICE}.service" || true
  systemctl restart "${SERVICE}.service" || true
fi

exit 0