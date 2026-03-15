#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$SCRIPT_DIR/../.."
OUT="$ROOT/apps/desktop/resources/backend"

echo "[build-python] Installing dependencies..."
pip install pyinstaller -r "$ROOT/backend/requirements.txt"

echo "[build-python] Building executable..."
pyinstaller "$ROOT/backend/app.py" \
  --onefile \
  --name server \
  --distpath "$OUT" \
  --workpath /tmp/pyinstaller-build \
  --specpath /tmp/pyinstaller-spec

echo "[build-python] Done. Binary at $OUT/server"
