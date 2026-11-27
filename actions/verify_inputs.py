#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Read-only integration check for the exact F04 v2.0.1 base/increment loaders."""
from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd


def load_f04_module(path: Path):
    spec = importlib.util.spec_from_file_location("sso_f04_v201", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"無法載入 F04 source: {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", required=True)
    parser.add_argument("--increments", required=True)
    parser.add_argument("--signal-date", default=None)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    source_path = Path(__file__).with_name("11.py")
    base_path = Path(args.base)
    increment_dir = Path(args.increments)
    output_path = Path(args.output)
    if not source_path.is_file() or not base_path.is_file() or not increment_dir.is_dir():
        raise FileNotFoundError("F04 source、已驗證基礎檔或增量資料夾不存在")

    signal_date = args.signal_date or datetime.now(ZoneInfo("Asia/Taipei")).date().isoformat()
    f04 = load_f04_module(source_path)

    # Use exact production parsing, universe filters, duplicate/conflict
    # checks, and history calendar validation. No ranking/API/state functions.
    base, base_meta = f04.load_base(base_path)
    increment_files = f04.discover_increment_files(str(increment_dir))
    increments, increment_manifest, increment_meta = f04.load_increment_data(base, increment_files)

    history = pd.concat([base, increments], ignore_index=True) if not increments.empty else base.copy()
    history = history[history["日期"] < pd.Timestamp(signal_date)].copy()
    history = history.drop_duplicates(["市場", "股票代號", "日期"], keep="last")

    validation = f04.validate_history_sessions(
        history, base_meta["universe_securities"], signal_date
    )
    previous_sessions = validation["expected_previous_market_sessions"]
    session_rows = []
    for day in previous_sessions:
        day_frame = history[history["日期"] == pd.Timestamp(day)]
        securities = int(day_frame[["市場", "股票代號"]].drop_duplicates().shape[0]) if not day_frame.empty else 0
        coverage = securities / base_meta["universe_securities"] if base_meta["universe_securities"] else 0.0
        session_rows.append({
            "date": day,
            "valid_ohlc_securities": securities,
            "coverage": coverage,
            "pass": coverage >= f04.MIN_HISTORY_DAILY_COVERAGE,
        })

    file_rows = []
    for path in increment_files:
        file_rows.append({
            "name": path.name,
            "bytes": path.stat().st_size,
            "sha256": f04.sha256_file(path),
        })

    load_pass = (
        base_meta["sha256"] == f04.BASE_SHA256
        and increment_meta["files"] == len(increment_files)
    )
    status = "DATA_INPUT_PASS" if load_pass and validation.get("pass") else (
        "DATA_LOAD_PASS_HISTORY_BLOCKED" if load_pass else "DATA_INPUT_FAILED"
    )
    result = {
        "status": status,
        "signal_date_checked": signal_date,
        "base": {
            "rows": base_meta["rows"],
            "universe_securities": base_meta["universe_securities"],
            "date_min": base_meta["date_min"],
            "date_max": base_meta["date_max"],
            "sha256": base_meta["sha256"],
            "bytes": base_path.stat().st_size,
        },
        "increments": {
            "file_count": len(increment_files),
            "rows_after_production_filters_and_dedup": increment_meta["rows"],
            "latest_date": increment_meta["latest_date"],
            "unique_dates": increment_meta.get("dates", []),
            "files": file_rows,
        },
        "history_validation": validation,
        "previous_sessions": session_rows,
        "actions_not_performed": [
            "F04 ranking",
            "Top10 freeze",
            "Fugle Snapshot Quotes",
            "Fugle minute candles",
            "cumulative state writes",
            "GitHub audit data upload",
            "live orders",
        ],
        "read_only": True,
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({
        "status": status,
        "signal_date_checked": signal_date,
        "base_rows": base_meta["rows"],
        "universe_securities": base_meta["universe_securities"],
        "increment_file_count": len(increment_files),
        "increment_rows": increment_meta["rows"],
        "increment_latest_date": increment_meta["latest_date"],
        "history_validation_pass": validation.get("pass"),
        "previous_sessions": session_rows,
        "read_only": True,
    }, ensure_ascii=False))
    return 0 if status == "DATA_INPUT_PASS" else 2


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"[INPUT VALIDATION FATAL] {type(exc).__name__}: {exc}", file=sys.stderr)
        raise
