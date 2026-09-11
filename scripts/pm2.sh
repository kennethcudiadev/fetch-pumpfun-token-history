#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

mkdir -p logs data/.state

if ! command -v pm2 >/dev/null 2>&1; then
  echo "PM2 not found. Install: npm install -g pm2"
  exit 1
fi

case "${1:-}" in
  scan)
    pm2 start ecosystem.config.cjs --only pump-scan
    ;;
  fetch)
    pm2 start ecosystem.config.cjs --only pump-fetch
    ;;
  stop)
    pm2 stop pump-scan pump-fetch 2>/dev/null || true
    ;;
  logs)
    pm2 logs "${2:-}"
    ;;
  status)
    pm2 status
    ;;
  *)
    echo "Usage: $0 {scan|fetch|stop|logs|status}"
    echo "Settings: config.json (target_wallet, lookback_hours, helius_keys)"
    exit 1
    ;;
esac
