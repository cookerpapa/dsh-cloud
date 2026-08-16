#!/usr/bin/env bash
# CubeSandbox v0.6 binary Volume Plugin over a pre-mounted POSIX filesystem.
set -euo pipefail

readonly BASE_DIR="/data/cube-shared/volume"
readonly VOLUME_ID_PATTERN='^dsh-[0-9a-f]{48}$'

operation=""
volume_id=""
name=""
sandbox_id=""
namespace=""
ref_count=""
volume_base_dir="$BASE_DIR"
private_data=""
metadata=""

fail() {
  printf '%s\n' '{"error":"dsh-cloud POSIX volume operation failed"}'
  exit 1
}

while (($# > 0)); do
  (($# >= 2)) || fail
  case "$1" in
    --op) operation="$2" ;;
    --volume-id) volume_id="$2" ;;
    --name) name="$2" ;;
    --sandbox-id) sandbox_id="$2" ;;
    --namespace) namespace="$2" ;;
    --ref-count) ref_count="$2" ;;
    --volume-base-dir) volume_base_dir="$2" ;;
    --private-data) private_data="$2" ;;
    --metadata) metadata="$2" ;;
    *) fail ;;
  esac
  shift 2
done

[[ "$volume_id" =~ $VOLUME_ID_PATTERN ]] || fail
[[ "$volume_base_dir" == "$BASE_DIR" ]] || fail
readonly volume_path="$BASE_DIR/dsh-cloud-posix-$volume_id"
readonly workspace_path="$volume_path/workspace"

safe_root() {
  [[ -d "$BASE_DIR" && ! -L "$BASE_DIR" ]] || fail
}

safe_volume() {
  [[ -d "$volume_path" && ! -L "$volume_path" ]] || fail
  [[ "$(realpath "$volume_path")" == "$(realpath "$BASE_DIR")/dsh-cloud-posix-$volume_id" ]] || fail
}

safe_workspace() {
  [[ -d "$workspace_path" && ! -L "$workspace_path" ]] || fail
  [[ "$(realpath "$workspace_path")" == "$(realpath "$volume_path")/workspace" ]] || fail
}

case "$operation" in
  create)
    [[ -z "$name" || "$name" == "$volume_id" ]] || fail
    safe_root
    [[ ! -L "$volume_path" && ! -L "$workspace_path" ]] || fail
    mkdir -p -- "$workspace_path"
    chmod 0700 -- "$volume_path" "$workspace_path"
    chown 1000:1000 -- "$volume_path" "$workspace_path"
    printf '%s\n' '{"token":"","private_data":"dsh-cloud-posix-v1","error":""}'
    ;;
  attach)
    [[ -n "$sandbox_id" && -n "$namespace" && "$ref_count" =~ ^[0-9]+$ ]] || fail
    [[ -z "$private_data" || "$private_data" == "dsh-cloud-posix-v1" ]] || fail
    safe_root
    safe_volume
    safe_workspace
    printf '{"host_path":"%s","metadata":{"driver":"dsh-cloud-posix-v1"},"error":""}\n' "$workspace_path"
    ;;
  detach)
    [[ -n "$sandbox_id" && -n "$namespace" && "$ref_count" =~ ^[0-9]+$ ]] || fail
    [[ -z "$metadata" || "$metadata" == '{"driver":"dsh-cloud-posix-v1"}' ]] || fail
    printf '%s\n' '{"error":""}'
    ;;
  destroy)
    safe_root
    if [[ ! -e "$volume_path" ]]; then
      printf '%s\n' '{"error":""}'
      exit 0
    fi
    safe_volume
    safe_workspace
    find -P "$workspace_path" -mindepth 1 -delete || fail
    rmdir -- "$workspace_path" "$volume_path" || fail
    printf '%s\n' '{"error":""}'
    ;;
  *) fail ;;
esac
