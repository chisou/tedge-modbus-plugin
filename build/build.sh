#!/usr/bin/env bash

# clean dist
if [ -d dist ]; then
    rm -rf dist
fi
mkdir -p dist

docker run --rm \
  -v $(pwd):/workspace \
  -w /workspace \
  -e VERSION=$(git describe --tags) \
  ghcr.io/goreleaser/nfpm package \
  --config nfpm.yaml \
  --packager deb \
  --target ./dist/
