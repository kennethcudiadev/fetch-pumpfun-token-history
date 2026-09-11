#!/usr/bin/env python3
"""
Step 3 — Open the local chart viewer.

Usage:
  python serve.py

Opens:
  http://localhost:<viewer_port>
"""

from __future__ import annotations

import http.server
import json
import os
import socket
import socketserver
import sys
import threading
import time
import urllib.parse
import webbrowser
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "pumpfun_history"))

from project_config import get_viewer_port  # noqa: E402

DATA_DIR = ROOT / "data"


def _data_wallet_dirs() -> list[str]:
    if not DATA_DIR.is_dir():
        return []
    return sorted(
        p.name
        for p in DATA_DIR.iterdir()
        if p.is_dir() and p.name not in {"backtest"} and not p.name.startswith(".")
    )


class Handler(http.server.SimpleHTTPRequestHandler):
    def end_headers(self) -> None:
        self.send_header("Cache-Control", "no-store")
        super().end_headers()

    def do_GET(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/api/data-wallets":
            payload = json.dumps(_data_wallet_dirs()).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return

        parts = parsed.path.split("/")
        # /data/<wallet>/<mint>.json → serve from whichever wallet folder has the file
        if (
            len(parts) >= 4
            and parts[1] == "data"
            and parts[-1].endswith(".json")
            and parts[2] != "backtest"
        ):
            wallet, fname = parts[2], parts[-1]
            direct = DATA_DIR / wallet / fname
            if not direct.is_file():
                for alt in _data_wallet_dirs():
                    cand = DATA_DIR / alt / fname
                    if cand.is_file():
                        self.path = f"/data/{alt}/{fname}"
                        if parsed.query:
                            self.path += f"?{parsed.query}"
                        break
        super().do_GET()


class ViewerServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


def _wait_until_listening(host: str, port: int, timeout: float = 5.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.2):
                return True
        except OSError:
            time.sleep(0.05)
    return False


def _default_wallet() -> str:
    """Wallet folder that actually has a scan manifest, so the first page load is usable."""
    for name in reversed(_data_wallet_dirs()):
        if (DATA_DIR / f"{name}.json").is_file():
            return name
    return ""


def main() -> None:
    port = get_viewer_port()
    host = "127.0.0.1"
    os.chdir(ROOT)
    wallet = _default_wallet()
    url = f"http://{host}:{port}/"
    if wallet:
        url += "?" + urllib.parse.urlencode({"wallet": wallet})

    try:
        httpd = ViewerServer((host, port), Handler)
    except OSError as exc:
        print(f"Cannot bind {url} ({exc})", file=sys.stderr)
        print("If a viewer is already running, stop it and try again.", file=sys.stderr)
        raise SystemExit(1) from exc

    print(f"Serving viewer from {ROOT}")
    print(f"Open {url}")
    print("Press Ctrl+C to stop.")

    thread = threading.Thread(target=httpd.serve_forever, name="viewer-http", daemon=True)
    thread.start()
    if not _wait_until_listening(host, port):
        print(f"Server did not start on {url}", file=sys.stderr)
        httpd.shutdown()
        raise SystemExit(1)

    try:
        webbrowser.open(url)
    except Exception:
        pass

    try:
        thread.join()
    except KeyboardInterrupt:
        print("\nStopping.")
    finally:
        httpd.shutdown()
        httpd.server_close()


if __name__ == "__main__":
    main()
