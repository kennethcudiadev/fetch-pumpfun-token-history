#!/usr/bin/env python3
"""Deprecated — use project-root fetch.py."""

import runpy
import sys
from pathlib import Path

print("Use: python fetch.py", file=sys.stderr)
runpy.run_path(str(Path(__file__).resolve().parent.parent / "fetch.py"), run_name="__main__")
