#!/bin/sh

set -e

SERVICE=tedge-modbus-plugin

if [ "$1" = "remove" ]; then
  if command -v systemctl >/dev/null 2>&1; then
    systemctl stop "${SERVICE}.service" || true
    systemctl disable "${SERVICE}.service" || true
  fi
fi

exit 0