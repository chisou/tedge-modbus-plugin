#!/usr/bin/env bash

# clean dist
if [ -d dist ]; then
    rm -rf dist
fi
mkdir -p dist

# install local copy of pymodbus
if [ -d modbus_reader/pymodbus ]; then
  rm -rf modbus_reader/pymodbus
fi
pip3 install pymodbus==3.11.3 --target modbus_reader/pymodbus

# build using nfpm
docker run --rm \
  -v $(pwd):/workspace \
  -w /workspace \
  -e VERSION=$(git describe --tags) \
  ghcr.io/goreleaser/nfpm package \
  --config nfpm.yaml \
  --packager deb \
  --target ./dist/
