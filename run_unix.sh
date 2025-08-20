#!/usr/bin/env bash
set -euo pipefail
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
PORT=${PORT:-8899}
python -m uvicorn app.main:app --host 0.0.0.0 --port "$PORT"
