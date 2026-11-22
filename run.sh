#!/usr/bin/env bash
# One command start: installs deps (first run) and launches on http://localhost:8000
cd "$(dirname "$0")"
python3 -c "import fastapi, uvicorn, neo_api_client" 2>/dev/null || pip install -r requirements.txt
exec python3 -m uvicorn backend.main:app --host 0.0.0.0 --port "${PORT:-8000}"
