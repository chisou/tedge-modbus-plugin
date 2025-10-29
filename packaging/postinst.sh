#!/bin/sh
set -e

SERVICE=tedge-modbus-service

if command -v systemctl >/dev/null 2>&1; then
  systemctl daemon-reload
  systemctl enable --now myapp.service || true
  systemctl restart "${SERVICE}.service" || true
fi

exit 0