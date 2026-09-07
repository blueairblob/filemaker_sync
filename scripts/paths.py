#!/usr/bin/env python3
"""
paths.py — translate config.toml's export path between Windows and WSL.

This pipeline runs from two different places against the same config.toml:
native Windows Python (python.exe, for FileMaker-side extraction -- see
CLAUDE.md) and WSL Python directly (for Postgres/Storage-only work, per this
project's own documented pattern). A single path value can't be valid on both
without translation -- Windows needs "C:/dev/..." and WSL needs the
"/mnt/c/dev/..." mount-point form for the exact same real directory.

config.toml's [export].path is kept in its native Windows form (that's what
filemaker_extract.py's own file-writing needs, running under python.exe) --
resolve_export_path() below translates it to a WSL path when the caller is
running under WSL/Linux, and passes it through unchanged on native Windows.

Confirmed broken/unusable before this existed (2026-09-07): the previously
committed [export].path ("/dev/RAT_Trains_Project/Migration/exports") was not
a valid path on *either* system -- not a real Windows path (no drive letter)
and not a real WSL path either ("/dev/" is Linux's device-file namespace).
"""
from __future__ import annotations
import os
import re


def resolve_export_path(raw_path: str) -> str:
    """Translate a Windows-style path (e.g. "C:/dev/foo") to its WSL mount-point
    equivalent ("/mnt/c/dev/foo") when running under Linux/WSL. Passed through
    unchanged on native Windows, and unchanged if it's already a Linux-style
    path (no drive letter) -- e.g. running against a real Linux host one day."""
    m = re.match(r"^([A-Za-z]):[/\\](.*)$", raw_path)
    if os.name != "nt" and m:
        drive = m.group(1).lower()
        rest = m.group(2).replace("\\", "/")
        return f"/mnt/{drive}/{rest}"
    return raw_path
