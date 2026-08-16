#!/usr/bin/env bash
set -euo pipefail

: "${DSH_CLOUD_CUBEMASTERCLI:?path to cubemastercli is required}"
: "${DSH_CLOUD_CUBE_MASTER_HOST:?Cube master host is required}"
: "${DSH_CLOUD_CUBE_MASTER_PORT:?Cube master port is required}"
: "${DSH_CLOUD_CUBE_TEMPLATE_IMAGE:?registry image is required}"

exec "$DSH_CLOUD_CUBEMASTERCLI" \
  -a "$DSH_CLOUD_CUBE_MASTER_HOST" \
  -p "$DSH_CLOUD_CUBE_MASTER_PORT" \
  tpl create-from-image \
  --image "$DSH_CLOUD_CUBE_TEMPLATE_IMAGE" \
  --writable-layer-size "${DSH_CLOUD_CUBE_WRITABLE_LAYER_SIZE:-8G}" \
  --expose-port 49984 \
  --probe 49984 \
  --probe-path /health/live \
  --cpu "${DSH_CLOUD_CUBE_CPU_MILLIS:-2000}" \
  --memory "${DSH_CLOUD_CUBE_MEMORY_MB:-2048}" \
  --with-cube-ca=false \
  --allow-internet-access \
  --detach \
  --json
