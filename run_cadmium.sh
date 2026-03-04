#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BACKEND_DIR="$ROOT_DIR/backend"
FRONTEND_DIR="$ROOT_DIR/frontend"
VENV_DIR="$ROOT_DIR/.venv"
if [[ ! -d "$VENV_DIR" ]]; then
  VENV_DIR="$BACKEND_DIR/.venv"
fi

if ! command -v python3 >/dev/null 2>&1; then
  echo "python3 is required but not found."
  exit 1
fi

if ! command -v npm >/dev/null 2>&1; then
  echo "npm is required but not found."
  exit 1
fi

cleanup() {
  if [[ -n "${BACKEND_PID:-}" ]] && kill -0 "$BACKEND_PID" >/dev/null 2>&1; then
    echo "Stopping backend (pid $BACKEND_PID)..."
    kill "$BACKEND_PID" >/dev/null 2>&1 || true
  fi
}

trap cleanup EXIT INT TERM

echo "Setting up backend..."
if [[ ! -d "$VENV_DIR" ]]; then
  python3 -m venv "$VENV_DIR"
fi

"$VENV_DIR/bin/pip" install -r "$BACKEND_DIR/requirements.txt"

PLAYWRIGHT_MARKER="$VENV_DIR/.playwright_chromium_installed"
if [[ ! -f "$PLAYWRIGHT_MARKER" ]]; then
  echo "Installing Playwright Chromium (one-time)..."
  "$VENV_DIR/bin/playwright" install chromium
  touch "$PLAYWRIGHT_MARKER"
fi

echo "Setting up frontend..."
if [[ ! -d "$FRONTEND_DIR/node_modules" ]]; then
  (cd "$FRONTEND_DIR" && npm install)
fi

echo "Starting backend at http://localhost:8000 ..."
(
  cd "$BACKEND_DIR"
  exec "$VENV_DIR/bin/uvicorn" app.main:app --reload --port 8000
) &
BACKEND_PID=$!

sleep 2
if ! kill -0 "$BACKEND_PID" >/dev/null 2>&1; then
  echo "Backend failed to start."
  exit 1
fi

echo "Starting frontend at http://localhost:5173 ..."
cd "$FRONTEND_DIR"
exec npm run dev
