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

# Ensure project root is importable when script is executed directly by Electron.
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.services.log_reducer import LogReducer


def main() -> int:
    parser = argparse.ArgumentParser(description="Reduce Spark ZIP locally for desktop flow")
    parser.add_argument("--zip", required=True, help="Path to ZIP file")
    parser.add_argument("--out", required=True, help="Path to output reduced report")
    parser.add_argument("--compact", action="store_true", help="Use compact reducer output")
    args = parser.parse_args()

    zip_path = Path(args.zip)
    if not zip_path.exists():
        raise FileNotFoundError(f"ZIP not found: {zip_path}")

    reducer = LogReducer(output_format="md", compact=args.compact)
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
