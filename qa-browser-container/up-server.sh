#!/bin/sh

set -eu

container_dir="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
repository_dir="$(dirname -- "${container_dir}")"
deploy_key="${repository_dir}/src/use_cases/secrets/ifx_registry_deploy_key"
runner_instance_id="${QA_BROWSER_RUNNER_INSTANCE_ID:-qa-browser-server-$(hostname)}"

if [ ! -f "${deploy_key}" ]; then
    echo "Missing IFX Registry deploy key: ${deploy_key}" >&2
    exit 2
fi

cd "${container_dir}"
QA_BROWSER_RUNNER_INSTANCE_ID="${runner_instance_id}" \
IFX_REGISTRY_SSH_KEY="${deploy_key}" \
exec docker compose up -d --build "$@"
