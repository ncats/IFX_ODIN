#!/usr/bin/env bash
set -euo pipefail

cd /home/kelleherkj/IFX_ODIN
source .venv/bin/activate

export PYTHONPATH=/home/kelleherkj/IFX_ODIN

python scripts/pubmed_mirror/main.py update
