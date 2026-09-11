#!/usr/bin/env python3
"""Redirect to project-root serve.py."""

from __future__ import annotations

import runpy
from pathlib import Path

runpy.run_path(str(Path(__file__).resolve().parent.parent / "serve.py"), run_name="__main__")
