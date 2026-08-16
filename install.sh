#!/usr/bin/env bash
set -euo pipefail
umask 077

readonly repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly compose_file="${repository_root}/deploy/production/compose.yaml"
readonly environment_file="${repository_root}/deploy/production/production.env"
readonly example_file="${repository_root}/deploy/production/production.env.example"

log() { printf '[dsh-cloud] %s\n' "$*"; }
fail() { printf '[dsh-cloud] ERROR: %s\n' "$*" >&2; exit 1; }

command -v docker >/dev/null 2>&1 || fail 'Docker Engine with Compose is required'
docker compose version >/dev/null 2>&1 || fail 'Docker Compose v2 is required'
command -v openssl >/dev/null 2>&1 || fail 'openssl is required'

if [[ ! -f "${environment_file}" ]]; then
  cp -- "${example_file}" "${environment_file}"
  chmod 0600 "${environment_file}"
  sed -i \
    -e "s|^POSTGRES_PASSWORD=$|POSTGRES_PASSWORD=$(openssl rand -hex 24)|" \
    -e "s|^DSH_CLOUD_TOOL_BROKER_TOKEN=$|DSH_CLOUD_TOOL_BROKER_TOKEN=$(openssl rand -hex 32)|" \
    -e "s|^DSH_CLOUD_SANDBOX_ENCRYPTION_KEY=$|DSH_CLOUD_SANDBOX_ENCRYPTION_KEY=$(openssl rand -base64 32 | tr -d '\n')|" \
    "${environment_file}"
  log "created ${environment_file}; add the DeepSeek and Cube values, then rerun"
  exit 2
fi

required=(
  POSTGRES_PASSWORD DSH_CLOUD_TOOL_BROKER_TOKEN DSH_CLOUD_SANDBOX_ENCRYPTION_KEY
  DEEPSEEK_API_KEY DSH_CLOUD_CUBE_API_URL DSH_CLOUD_CUBE_API_KEY
  DSH_CLOUD_CUBE_CONTROL_NETWORK DSH_CLOUD_CUBE_TEMPLATE_ID
  DSH_CLOUD_CUBE_PROXY_NODE_IP DSH_CLOUD_CUBE_EGRESS_PROXY_IP
)
for name in "${required[@]}"; do
  value="$(sed -n "s/^${name}=//p" "${environment_file}" | tail -n 1)"
  [[ -n "${value}" ]] || fail "${name} is empty in ${environment_file}"
done

case "${1:-up}" in
  up)
    docker compose --env-file "${environment_file}" -f "${compose_file}" up -d --build --wait
    log "ready at http://$(sed -n 's/^DSH_CLOUD_HTTP_BIND=//p' "${environment_file}"):$(sed -n 's/^DSH_CLOUD_HTTP_PORT=//p' "${environment_file}")"
    ;;
  check)
    docker compose --env-file "${environment_file}" -f "${compose_file}" config --quiet
    log 'configuration is valid'
    ;;
  down)
    docker compose --env-file "${environment_file}" -f "${compose_file}" down
    ;;
  *) fail 'usage: ./install.sh [up|check|down]' ;;
esac
