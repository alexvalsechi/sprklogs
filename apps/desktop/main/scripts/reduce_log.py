#!/usr/bin/env python3
"""
Desktop reducer bridge.

Reads a ZIP file path and outputs a reduced report as text.
This script reuses backend LogReducer to keep parity with server behavior.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Ensure the directory that contains the 'backend' package is on sys.path.
# Walk up the directory tree so the same script works both when installed
# (resources/scripts/ → resources/ → backend/) and in the dev repo
# (apps/desktop/main/scripts/ → … → project_root/ → backend/).
_backend_root = next(
    (p for p in Path(__file__).resolve().parents if (p / "backend" / "__init__.py").is_file()),
    None,
)
if _backend_root is None:
    raise ImportError(
        "Cannot locate the 'backend' package.  "
        "In the packaged app, ensure electron-builder copies backend/ under "
        "resources/.  In dev, run from the project root."
    )
if str(_backend_root) not in sys.path:
    sys.path.insert(0, str(_backend_root))

from backend.services.log_reducer import LogReducer


def main() -> int:
    parser = argparse.ArgumentParser(description="Reduce Spark ZIP locally for desktop flow")
    parser.add_argument("--zip", required=True, help="Path to ZIP file")
    parser.add_argument("--out", required=True, help="Path to output reduced report")
    parser.add_argument("--compact", action="store_true", help="Use compact reducer output")
    parser.add_argument(
        "--format",
        choices=["md", "json"],
        default="md",
        help="Output format: md (markdown, default) or json (structured)",
    )
    args = parser.parse_args()

    zip_path = Path(args.zip)
    if not zip_path.exists():
        raise FileNotFoundError(f"ZIP not found: {zip_path}")

    reducer = LogReducer(output_format=args.format, compact=args.compact)
    summary, reduced_report = reducer.reduce(zip_path.read_bytes())

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(reduced_report, encoding="utf-8")

    # Emit full summary as JSON on stdout so Electron can capture structured data.
    import json
    print(json.dumps(summary.model_dump()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
