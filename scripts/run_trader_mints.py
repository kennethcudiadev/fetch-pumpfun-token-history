#!/usr/bin/env python3
"""Deprecated — use project-root scan.py."""

import runpy
import sys
from pathlib import Path

print("Use: python scan.py", file=sys.stderr)
runpy.run_path(str(Path(__file__).resolve().parent.parent / "scan.py"), run_name="__main__")
