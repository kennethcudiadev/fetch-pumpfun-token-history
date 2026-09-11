"""Pytest configuration."""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PUMPFUN_DIR = ROOT / "pumpfun_history"
if str(PUMPFUN_DIR) not in sys.path:
    sys.path.insert(0, str(PUMPFUN_DIR))
