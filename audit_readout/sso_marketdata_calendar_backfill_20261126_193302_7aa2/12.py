# -*- coding: utf-8 -*-
"""SSO_Market_Data_Continuation_TWSE_TPEX_v1.5.18｜臨時休市核驗與近30日行情補查

執行方式：在 Google Colab 上傳本檔後執行：%run /content/12.py
本程式讀取 v1.5.10 已封存的官方行情函數，不執行其舊主流程；
所有新資料只寫入新的 v1.5.18 目錄，不覆寫或刪除任何舊資料。
額外完全沿用 11.py 的 v2.31 基準篩選規則，重建固定 2,435 組 (市場,代號) 股票池，逐日檢查最近三個交易日有效 OHLC 覆蓋率；每日至少 90% 才可供 11.py 的 F04 前瞻程序使用。
"""
from __future__ import annotations

import ast
import base64
import contextlib
import io
import linecache
import csv
import hashlib
import json
import math
import os
import re
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
import uuid
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path

import requests

VERSION = "SSO_Market_Data_Continuation_TWSE_TPEX_v1.5.18"
REPO = "CHLINX/sso"
EXPECTED_MAIN_SHA = "aaabc2433fb5df2bb68af06461178305d6bd30d1"
BASE_SCRIPT_GIT_BLOB_SHA = "033cd9694d80e3b233b6020e2aaf371f757f709f"
BASE_BRANCH = "audit/sso-marketdata-v1510-seal-20261112_180751_3bd3"
BASE_SCRIPT_RELATIVE = (
    "audit_readout/sso_marketdata_v1510_20261112_180751_3bd3/"
    "SSO_Market_Data_Continuation_TWSE_TPEX_v1.5.10.py"
)
BASE_RAW_URL = (
    "https://raw.githubusercontent.com/CHLINX/sso/"
    + BASE_BRANCH + "/" + BASE_SCRIPT_RELATIVE
)

# v4.48 已以官方全市場行情確認至少一市場有交易的八個星期六。
# 不因星期六而自動認定休市；每個市場仍須分別通過官方行情 API 驗證。
KNOWN_MAKEUP_SATURDAYS = {
    date(2016, 1, 30),
    date(2016, 6, 4),
    date(2016, 9, 10),
    date(2017, 2, 18),
    date(2017, 6, 3),
    date(2017, 9, 30),
    date(2018, 3, 31),
    date(2018, 12, 22),
}

TWSE_HOLIDAY_URL = (
    "https://www.twse.com.tw/rwd/zh/holidaySchedule/holidaySchedule"
)

# 每次執行依台北時間動態計算：17:00 前截止昨天，17:00 起納入當日。
# 以截止日往前回看 30 個日曆日；不使用固定起訖日期，也不回掃全部歷史日期。
LOOKBACK_CALENDAR_DAYS = 30
SAME_DAY_GATE_HOUR = 17


def resolve_scan_window(now: datetime) -> tuple[date, date, bool]:
    """Return (start, end, includes_today) under the 17:00 Asia/Taipei same-day gate."""
    if not isinstance(now, datetime) or now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("scan window requires a timezone-aware Taipei datetime")
    includes_today = now.hour >= SAME_DAY_GATE_HOUR
    end_day = now.date() if includes_today else now.date() - timedelta(days=1)
    start_day = end_day - timedelta(days=LOOKBACK_CALENDAR_DAYS - 1)
    if (end_day - start_day).days + 1 != LOOKBACK_CALENDAR_DAYS:
        raise AssertionError("rolling scan window length mismatch")
    return start_day, end_day, includes_today


def test_same_day_gate_policy() -> dict:
    """Network-free contract test for the 16:59/17:00 same-day boundary."""
    before = datetime.fromisoformat("2026-11-18T16:59:00+08:00")
    at_gate = datetime.fromisoformat("2026-11-18T17:00:00+08:00")
    after = datetime.fromisoformat("2026-11-18T18:59:44+08:00")
    start_b, end_b, include_b = resolve_scan_window(before)
    start_g, end_g, include_g = resolve_scan_window(at_gate)
    start_a, end_a, include_a = resolve_scan_window(after)
    assert end_b == date(2026, 11, 17) and include_b is False
    assert end_g == date(2026, 11, 18) and include_g is True
    assert end_a == date(2026, 11, 18) and include_a is True
    assert (end_b - start_b).days + 1 == LOOKBACK_CALENDAR_DAYS
    assert (end_g - start_g).days + 1 == LOOKBACK_CALENDAR_DAYS
    assert (end_a - start_a).days + 1 == LOOKBACK_CALENDAR_DAYS
    try:
        resolve_scan_window(datetime(2026, 11, 18, 18, 0))
    except ValueError:
        pass
    else:
        raise AssertionError("naive datetime must not be accepted")
    return {
        "status": "PASS",
        "before_gate_end": end_b.isoformat(),
        "at_gate_end": end_g.isoformat(),
        "after_gate_end": end_a.isoformat(),
        "lookback_calendar_days": LOOKBACK_CALENDAR_DAYS,
        "gate_hour_taipei": SAME_DAY_GATE_HOUR,
    }

# 已核驗的年度日曆以外「臨時休市」公告。
# 2026-10-23：薔琵颱風，TWSE 集中市場及 TPEx 櫃買市場均全日休市。
# 年度開休市日曆沒有收錄該臨時公告，因此必須另設明確事件層；
# 僅這些有公開公告證據的日期可排除，不以 API 空表自行推定休市。
SPECIAL_CLOSED_SESSIONS = {
    date(2026, 10, 23): {
        "markets": ("TWSE", "TPEX"),
        "name": "薔琵颱風臨時休市",
        "reason": "2026-10-22 公告：2026-10-23 集中交易市場及櫃檯買賣市場全日休市；後續 2026-10-26 為例行補假。",
        "source_urls": [
            "https://www.cna.com.tw/news/afe/202610220388.aspx",
            "https://www.knews.com.tw/news/B650912BDC1E5A2A91FE108A84816BB8",
        ],
    },
}


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with Path(path).open("rb") as f:
        for block in iter(lambda: f.read(4 * 1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def git_blob_sha(data: bytes) -> str:
    return hashlib.sha1(b"blob " + str(len(data)).encode() + b"\0" + data).hexdigest()


def get_base_script(root: Path) -> tuple[str, str]:
    candidates = sorted(
        root.glob("v1.5.10_*/SSO_Market_Data_Continuation_TWSE_TPEX_v1.5.10.py"),
        key=lambda p: (p.stat().st_mtime_ns, str(p)),
    )
    source = None
    source_origin = ""
    if candidates:
        candidate = candidates[-1]
        data = candidate.read_bytes()
        if git_blob_sha(data) == BASE_SCRIPT_GIT_BLOB_SHA:
            source = data.decode("utf-8-sig")
            source_origin = str(candidate)
    if source is None:
        response = requests.get(BASE_RAW_URL, timeout=(20, 120))
        response.raise_for_status()
        data = response.content
        if git_blob_sha(data) != BASE_SCRIPT_GIT_BLOB_SHA:
            raise RuntimeError("下載的 v1.5.10 原始程式 Git blob SHA 不符，停止執行。")
        source = data.decode("utf-8-sig")
        source_origin = BASE_RAW_URL
    return source, source_origin


def load_v1510_helpers(source: str) -> dict:
    tree = ast.parse(source)
    kept = []
    removed_entry = False
    for node in tree.body:
        # 移除原檔案最末端的 RESULT_V1510 = run_v1510()，只載入函數與常數。
        targets = getattr(node, "targets", []) if isinstance(node, ast.Assign) else []
        assigned_result = any(
            isinstance(t, ast.Name) and t.id == "RESULT_V1510"
            for target in targets
            for t in (target,) if isinstance(t, ast.Name)
        )
        value = getattr(node, "value", None)
        calls_run = (
            isinstance(value, ast.Call)
            and isinstance(value.func, ast.Name)
            and value.func.id == "run_v1510"
        )
        if assigned_result and calls_run:
            removed_entry = True
            continue
        kept.append(node)
    if not removed_entry:
        raise RuntimeError("v1.5.10 執行入口格式與封存版不符，拒絕猜測式載入。")
    tree.body = kept
    ast.fix_missing_locations(tree)
    namespace = {"__name__": "sso_v1510_helpers"}
    exec(compile(tree, "SSO_Market_Data_Continuation_TWSE_TPEX_v1.5.10.py", "exec"), namespace)
    required = [
        "ROOT", "TZ", "mount_drive", "parse_date", "market_norm", "find_col",
        "read_header", "normalize_csv_rows", "canonical_records_sha256",
        "parse_payload", "validate_records", "make_session", "download_and_validate",
        "verify_all_forward_records", "write_csv", "ensure_columns", "record_key",
        "load_baseline_and_schema", "version_dirs", "log", "CORE_REQUIRED_FIELDS",
        "COMPARE_FIELDS", "ALIASES", "MIN_ROWS_ABSOLUTE",
        "read_csv_rows", "make_mapping", "normalize_value",
    ]
    missing = [name for name in required if name not in namespace]
    if missing:
        raise RuntimeError("v1.5.10 基礎函數缺少必要介面：" + ", ".join(missing))

    # v1.5.10 normalize_csv_rows 原始版本以「證券代號」單欄判斷重複。
    # 這對單一交易日檔案尚可，但全市場跨日增量及逐股 overlay 會把同一代號
    # 在不同交易日的正常資料誤判為重複。此處只修正唯一鍵判斷，其他欄位
    # 別名、數值正規化、必要欄位及市場一致性檢查仍沿用 v1.5.10 函數。
    def normalize_csv_rows_natural_key(path, market=None, required_fields=None):
        columns, rows = namespace["read_csv_rows"](path)
        mapping = namespace["make_mapping"](columns)
        required = (
            required_fields
            if required_fields is not None
            else namespace["CORE_REQUIRED_FIELDS"]
        )
        missing_fields = [field for field in required if mapping.get(field) is None]
        if missing_fields:
            raise RuntimeError(
                f"{Path(path).name} 缺少必要欄位：{missing_fields}"
            )

        records = []
        for row in rows:
            record = {}
            for field in namespace["COMPARE_FIELDS"]:
                column = mapping.get(field)
                value = row.get(column) if column is not None else None
                record[field] = namespace["normalize_value"](field, value)
            records.append(record)

        if any(not str(record.get("instrument_code") or "").strip() for record in records):
            raise RuntimeError(f"{Path(path).name} 存在空白證券代號。")
        if any(not record.get("trade_date") for record in records):
            raise RuntimeError(f"{Path(path).name} 存在空白或無效交易日期。")
        if any(record.get("market") not in ("TWSE", "TPEX") for record in records):
            raise RuntimeError(f"{Path(path).name} 存在空白或無效市場別。")

        natural_keys = [namespace["record_key"](record) for record in records]
        if len(natural_keys) != len(set(natural_keys)):
            raise RuntimeError(
                f"{Path(path).name} 存在重複自然鍵值（市場＋交易日＋證券代號）。"
            )

        if market is not None:
            wrong_market = [
                record["instrument_code"]
                for record in records
                if record["market"] != market
            ]
            if wrong_market:
                raise RuntimeError(
                    f"{Path(path).name} 市場別錯誤：{wrong_market[:10]}"
                )

        return columns, records

    namespace["normalize_csv_rows"] = normalize_csv_rows_natural_key
    return namespace


def classify_holiday_calendar(payload: dict, year: int, parse_date) -> tuple[set, set, list]:
    """回傳 (official_closed_dates, explicit_open_dates, parsed_rows)。"""
    rows = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(rows, list) or not rows:
        raise RuntimeError(f"TWSE {year} 年官方開休市日曆沒有有效 data 列。")
    closed, explicit_open, parsed = set(), set(), []
    open_tokens = ("補行交易", "補行開市", "開始交易", "最後交易", "恢復交易")
    closed_tokens = ("市場無交易", "不交易", "休市", "放假", "補假", "停止交易")
    for row in rows:
        if not isinstance(row, (list, tuple)) or len(row) < 2:
            continue
        day = parse_date(row[0])
        if day is None or day.year != year:
            continue
        name = str(row[1] or "").strip()
        detail = str(row[2] or "").strip() if len(row) > 2 else ""
        joined = name + " " + detail
        is_open = any(token in joined for token in open_tokens)
        is_closed = any(token in joined for token in closed_tokens)
        if is_open:
            explicit_open.add(day)
            closed.discard(day)
        elif is_closed:
            closed.add(day)
        parsed.append({"trade_date": day.isoformat(), "name": name, "description": detail,
                       "calendar_class": "EXPLICIT_OPEN" if is_open else ("EXPLICIT_CLOSED" if is_closed else "INFO_ONLY")})
    if not parsed:
        raise RuntimeError(f"TWSE {year} 年日曆回應未包含 {year} 年日期，可能忽略歷史年份參數。")
    return closed, explicit_open, parsed


def fetch_official_calendars(session, years, parse_date, logs):
    calendars = {}
    for year in years:
        last_error = None
        payload_ok = None
        # 官方端點以 date 指定年份；兼容 YYYY0101 / YYYY 兩種已公開查詢形式。
        for date_arg in (f"{year}0101", str(year)):
            try:
                response = session.get(
                    TWSE_HOLIDAY_URL,
                    params={"date": date_arg, "response": "json"},
                    timeout=(20, 60),
                )
                response.raise_for_status()
                payload = response.json()
                closed, explicit_open, parsed = classify_holiday_calendar(payload, year, parse_date)
                payload_ok = {
                    "year": year,
                    "requested_date": date_arg,
                    "response_date": payload.get("date"),
                    "title": payload.get("title", ""),
                    "rows": parsed,
                    "closed_dates": sorted(d.isoformat() for d in closed),
                    "explicit_open_dates": sorted(d.isoformat() for d in explicit_open),
                    "source_url": response.url,
                }
                break
            except Exception as exc:
                last_error = f"{type(exc).__name__}: {exc}"
        if payload_ok is None:
            raise RuntimeError(f"無法取得/解析 TWSE {year} 年官方日曆：{last_error}")
        calendars[year] = payload_ok
        logs.append(f"[CALENDAR PASS] {year}：rows={len(payload_ok['rows'])} closed={len(payload_ok['closed_dates'])} explicit_open={len(payload_ok['explicit_open_dates'])}")
        time.sleep(0.15)
    return calendars


def build_expected_sessions(start: date, end: date, calendars: dict) -> tuple[set, dict]:
    closed, explicit_open = set(), set(KNOWN_MAKEUP_SATURDAYS)
    for year_data in calendars.values():
        closed.update(date.fromisoformat(x) for x in year_data["closed_dates"])
        explicit_open.update(date.fromisoformat(x) for x in year_data["explicit_open_dates"])
    # 僅套用已核驗公告且涵蓋兩市場的臨時休市日；API 空資料本身不會加入此集合。
    special_closed = {
        day for day, item in SPECIAL_CLOSED_SESSIONS.items()
        if set(item.get("markets", ())) == {"TWSE", "TPEX"}
    }
    closed.update(special_closed)
    expected = set()
    evidence = {}
    cur = start
    while cur <= end:
        if cur in explicit_open:
            expected.add(cur)
            evidence[cur] = "TWSE official calendar / v4.48 verified make-up Saturday"
        elif cur.weekday() < 5 and cur not in closed:
            expected.add(cur)
            evidence[cur] = "TWSE official holiday schedule + weekday trading rule"
        cur += timedelta(days=1)
    return expected, {d.isoformat(): evidence[d] for d in sorted(expected)}


def scan_csv_date_counts(path: Path, helper: dict, counts: dict, audit: list,
                         date_start: date | None = None, date_end: date | None = None,
                         chunksize=250_000):
    import pandas as pd
    path = Path(path)
    try:
        header = list(pd.read_csv(path, nrows=0, encoding="utf-8-sig").columns)
        date_col = helper["find_col"](header, helper["ALIASES"]["trade_date"])
        market_col = helper["find_col"](header, helper["ALIASES"]["market"])
        if not date_col or not market_col:
            audit.append({"path": str(path), "status": "SKIP_SCHEMA", "reason": "缺少日期或市場欄位"})
            return
        total_rows = 0
        local = defaultdict(int)
        for chunk in pd.read_csv(path, usecols=[date_col, market_col], dtype=str,
                                 chunksize=chunksize, encoding="utf-8-sig", low_memory=False):
            total_rows += len(chunk)
            grp = chunk.groupby([market_col, date_col], dropna=False).size()
            for (raw_market, raw_date), number in grp.items():
                market = helper["market_norm"](raw_market)
                day = helper["parse_date"](raw_date)
                if market in ("TWSE", "TPEX") and day is not None:
                    if date_start is not None and day < date_start:
                        continue
                    if date_end is not None and day > date_end:
                        continue
                    local[(market, day)] += int(number)
        # 同日跨多版本/重複封存採最大單檔列數，不把副本加總成虛高覆蓋。
        for key, number in local.items():
            counts[key] = max(counts.get(key, 0), number)
        audit.append({"path": str(path), "status": "PASS", "rows_scanned": total_rows,
                      "market_date_groups_in_window": len(local),
                      "window_start": date_start.isoformat() if date_start else None,
                      "window_end": date_end.isoformat() if date_end else None,
                      "bytes": path.stat().st_size,
                      "mtime": datetime.fromtimestamp(path.stat().st_mtime).isoformat()})
    except Exception as exc:
        audit.append({"path": str(path), "status": "ERROR", "reason": f"{type(exc).__name__}: {exc}"})
        raise


def scan_existing_database(helper: dict, logs: list,
                           window_start: date, window_end: date) -> tuple[dict, list, date, date]:
    """唯讀掃描既有資料，但只統計最近 30 日窗口內的市場／日期覆蓋。"""
    if window_start > window_end:
        raise ValueError(f"日期窗口錯誤：{window_start} > {window_end}")
    if (window_end - window_start).days + 1 != LOOKBACK_CALENDAR_DAYS:
        raise ValueError("日期窗口不是設定的最近 30 個日曆日。")

    root = Path(helper["ROOT"])
    project_root = root.parent
    main_candidates = sorted(project_root.glob(
        "看股最速適_官方行情完整證券代號重建_v4_16_*/官方行情總表_完整證券代號_v4.17.csv"),
        key=lambda p: (p.stat().st_mtime_ns, str(p)))
    if not main_candidates:
        raise FileNotFoundError("找不到 v4.17 官方完整證券代號主行情檔；不會用不完整資料推定未知營業日。")
    main_db = main_candidates[-1]
    counts = {}
    audit = []
    logs.append(f"[DATABASE SCAN] 主資料庫：{main_db}")
    logs.append(f"[DATABASE WINDOW] 僅統計 {window_start} 至 {window_end}，共 {LOOKBACK_CALENDAR_DAYS} 個日曆日。")
    scan_csv_date_counts(main_db, helper, counts, audit, window_start, window_end)
    if not audit or audit[0].get("status") != "PASS":
        raise RuntimeError("v4.17 主資料庫日期／市場欄位未通過掃描，禁止推定缺漏日期。")
    if int(audit[0].get("rows_scanned", 0)) < 4_000_000:
        raise RuntimeError(f"v4.17 主資料庫讀取列數異常：{audit[0].get('rows_scanned')}；預期超過 4,000,000 列。")

    dirs = helper["version_dirs"]()
    # 逐版增量只掃標準化增量 CSV；僅統計窗口內日期。
    delta_files = sorted({p for d in dirs for p in d.glob("**/全市場標準化增量_*.csv")}, key=str)
    for path in delta_files:
        scan_csv_date_counts(path, helper, counts, audit, window_start, window_end)
    # 已封存的全日修正層也納入窗口覆蓋判斷。
    correction_files = sorted({p for d in dirs for p in d.glob("修正_TWSE_*.csv")} |
                              {p for d in dirs for p in d.glob("修正_TPEX_*.csv")}, key=str)
    for path in correction_files:
        scan_csv_date_counts(path, helper, counts, audit, window_start, window_end)

    # 不要求窗口內一定已有資料：若整段尚未接續，後續應把窗口內預期交易日列為補查。
    return counts, audit, window_start, window_end


def recover_unsealed_v1514_increments(root: Path, start_date: date, end_date: date,
                                      counts: dict, helper: dict, logs: list):
    """回收 v1.5.14 在 round-trip 失敗前已寫出的增量，僅重建 overlay，不重複追加標準增量。

    v1.5.14 的失敗點發生在標準化 CSV 寫出後、overlay/manifest 寫出前。
    這些 CSV 已經由官方下載器逐日通過 OHLC 等檢查；本函數仍以修正後的
    自然鍵正規化器重新讀取，逐市場／交易日再次驗證，通過才作為 overlay 回收。
    舊資料夾只讀，不修改、不刪除；新版本只輸出補齊的 overlay 與回收稽核表。
    """
    root = Path(root)
    candidates = sorted(root.glob("v1.5.14_*"), key=lambda p: (p.stat().st_mtime_ns, p.name))
    recovered = {}
    audit_rows = []
    recovered_sources = set()

    for run_dir in candidates:
        # 有 manifest 的 v1.5.14 目錄視為已封存，不在此重建。
        if (run_dir / "artifact_sha256_manifest.json").is_file():
            continue
        csv_files = sorted(run_dir.glob("**/全市場標準化增量_*.csv"))
        for csv_path in csv_files:
            _, parsed = helper["normalize_csv_rows"](
                csv_path, market=None, required_fields=helper["CORE_REQUIRED_FIELDS"]
            )
            scoped = []
            for record in parsed:
                d = helper["parse_date"](record.get("trade_date"))
                if d is not None and start_date <= d <= end_date:
                    scoped.append(record)
            grouped = defaultdict(list)
            for record in scoped:
                grouped[(record["market"], helper["parse_date"](record["trade_date"]))].append(record)

            for (market, day), group in sorted(grouped.items(), key=lambda x: (x[0][1], x[0][0])):
                if len(group) != counts.get((market, day), 0):
                    audit_rows.append({
                        "source_file": str(csv_path), "trade_date": day.isoformat(),
                        "market": market, "rows": len(group), "status": "SKIP_NOT_MAX_COVERAGE",
                        "canonical_sha256": helper["canonical_records_sha256"](group),
                        "reason": f"此檔 {len(group)} 列；資料庫覆蓋統計為 {counts.get((market, day), 0)} 列。",
                    })
                    continue

                passed, stats = helper["validate_records"](group, market, day, None)
                if not passed:
                    raise RuntimeError(
                        f"未封存 v1.5.14 增量回收驗證失敗：{csv_path.name} {market} {day}；"
                        + "; ".join(stats.get("validation_errors", []))
                    )

                for record in group:
                    key = helper["record_key"](record)
                    if key in recovered:
                        old_sha = helper["canonical_records_sha256"]([recovered[key]])
                        new_sha = helper["canonical_records_sha256"]([record])
                        if old_sha != new_sha:
                            raise RuntimeError(
                                f"未封存 v1.5.14 來源間出現自然鍵資料衝突：{key}"
                            )
                        continue
                    recovered[key] = record

                recovered_sources.add(str(csv_path))
                audit_rows.append({
                    "source_file": str(csv_path), "trade_date": day.isoformat(),
                    "market": market, "rows": len(group), "status": "RECOVERED_AND_REVALIDATED",
                    "canonical_sha256": helper["canonical_records_sha256"](group),
                    "reason": "自然鍵、欄位及逐日行情品質重新驗證通過；僅重建逐股 overlay，不重複追加標準增量。",
                })

    recovered_records = sorted(
        recovered.values(),
        key=lambda r: (r["trade_date"], r["market"], r["instrument_code"])
    )
    if recovered_records:
        helper["verify_all_forward_records"](recovered_records)
        logs.append(
            f"[UNSEALED V1.5.14 RECOVERY PASS] rows={len(recovered_records)}; "
            f"files={len(recovered_sources)}; overlay-only=True"
        )
    else:
        logs.append("[UNSEALED V1.5.14 RECOVERY] no eligible unsealed increment rows found")
    return recovered_records, audit_rows, sorted(recovered_sources)


def local_expected_rows(market: str, day: date, expected_sessions: list, counts: dict):
    try:
        idx = expected_sessions.index(day)
    except ValueError:
        return None
    candidates = []
    for distance in range(1, 11):
        for j in (idx - distance, idx + distance):
            if 0 <= j < len(expected_sessions):
                n = counts.get((market, expected_sessions[j]), 0)
                if n >= 500:
                    candidates.append(n)
        if len(candidates) >= 6:
            break
    return int(statistics.median(candidates)) if candidates else None


def build_daily_summary(expected_by_market: dict, counts: dict, helper: dict, partial_ratio=0.55):
    rows = []
    partial = []
    for market, dates in expected_by_market.items():
        ordered = sorted(dates)
        for day in ordered:
            n = counts.get((market, day), 0)
            expected = local_expected_rows(market, day, ordered, counts)
            if n == 0:
                status, reason = "MISSING_EXPECTED_SESSION", "官方日曆預期交易日，但資料庫無該市場行情列"
            elif expected is not None and n < max(500, expected * partial_ratio):
                status, reason = "POSSIBLE_PARTIAL_DAY", f"既有列數 {n} 低於鄰近日行情中位數 {expected} 的 {partial_ratio:.0%}"
                partial.append({"trade_date": day.isoformat(), "market": market, "rows": n,
                                "neighbor_median_rows": expected, "reason": reason})
            else:
                status, reason = "COVERED", "資料庫已有該市場日期行情"
            rows.append({"trade_date": day.isoformat(), "market": market, "expected_session": True,
                         "existing_rows": n, "neighbor_median_rows": expected,
                         "status": status, "reason": reason})
    return rows, partial


def make_synthetic_payload(n=501):
    fields = ["證券代號", "證券名稱", "開盤價", "最高價", "最低價", "收盤價", "成交股數", "成交金額", "漲跌價差", "成交筆數"]
    data = []
    for i in range(1, n + 1):
        data.append([f"{i:04d}", f"TEST{i}", "10.00", "11.00", "9.00", "10.50", "1000", "10500", "0.50", "100"])
    return {"stat": "OK", "date": "20261112", "title": "unit-test", "tables": [{"title": "每日收盤行情", "fields": fields, "data": data}]}


def make_empty_payload():
    return {"stat": "OK", "date": "20261023", "title": "unit-test-empty", "tables": []}


class FakeResponse:
    def __init__(self, payload):
        self._payload = payload
        self.content = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.status_code = 200
        self.headers = {"Content-Type": "application/json; charset=utf-8"}
        self.url = "https://unit.test/market"
    def json(self):
        return self._payload
    def raise_for_status(self):
        return None


class FakeSession:
    def __init__(self, payload):
        self.payload = payload
    def get(self, *args, **kwargs):
        return FakeResponse(self.payload)


def download_legacy_tpex(session, day: date, expected_rows, out_dir: Path, helper: dict, logs: list):
    """使用 v4.48 曾成功核驗的 TPEx 官方全市場日行情端點。"""
    roc_year = day.year - 1911
    url = "https://www.tpex.org.tw/web/stock/aftertrading/otc_quotes_no1430/stk_wn1430_result.php"
    params = {"l": "zh-tw", "d": f"{roc_year:03d}/{day:%m/%d}", "se": "EW", "o": "json"}
    meta = {"trade_date": day.isoformat(), "market": "TPEX", "query_type": "EW_LEGACY_OFFICIAL",
            "url": url, "params": params, "http_status": None, "response_sha256": "",
            "raw_gzip_sha256": "", "raw_file": "", "table_title": "", "status": "ERROR",
            "validation": {}, "error": ""}
    try:
        response = session.get(url, params=params, timeout=(25, 120))
        meta["http_status"] = response.status_code
        raw = response.content
        meta["response_sha256"] = sha256_bytes(raw)
        raw_dir = Path(out_dir) / "raw" / "TPEX"
        raw_dir.mkdir(parents=True, exist_ok=True)
        raw_path = raw_dir / f"{day:%Y%m%d}_legacy.json.gz"
        compressed = __import__("gzip").compress(raw, compresslevel=6, mtime=0)
        raw_path.write_bytes(compressed)
        meta["raw_gzip_sha256"] = sha256_bytes(compressed)
        meta["raw_file"] = raw_path.relative_to(out_dir).as_posix()
        if __import__("gzip").decompress(compressed) != raw:
            raise RuntimeError("TPEx legacy raw GZIP round-trip 失敗")
        response.raise_for_status()
        payload = response.json()
        records, parse_status, parse_error, title, parse_stats = helper["parse_payload"](payload, "TPEX", day)
        meta["table_title"] = title
        meta["parse_stats"] = parse_stats
        if parse_status == "NO_DATA":
            meta["status"] = "WAITING_FOR_OFFICIAL_DATA"
            meta["error"] = f"{parse_error}；legacy endpoint 無資料，仍須由另一官方端點交叉重查。"
            return [], meta
        passed, stats = helper["validate_records"](records, "TPEX", day, expected_rows)
        stats["parse_stats"] = parse_stats
        meta["validation"] = stats
        invalid_limit = max(10, int(parse_stats.get("table_rows", 0) * 0.05))
        if parse_stats.get("invalid_code_rows", 0) > invalid_limit:
            passed = False
            stats["validation_errors"].append("TPEx legacy 原始表格不合法代號過多")
        if not passed:
            meta["status"] = "REJECTED"
            meta["error"] = "; ".join(stats["validation_errors"])
            return [], meta
        meta["status"] = "PASS"
        meta["error"] = ""
        logs.append(f"[TPEX LEGACY PASS] {day} rows={len(records)} type=EW")
        return records, meta
    except Exception as exc:
        meta["status"] = "ERROR"
        meta["error"] = f"{type(exc).__name__}: {exc}"
        logs.append(f"[TPEX LEGACY ERROR] {day}: {meta['error']}")
        return [], meta


def download_market_day(session, market: str, day: date, expected_rows, out_dir: Path, helper: dict, logs: list):
    """上市走 v1.5.10 官方端點；上櫃先走 v4.48 驗證過的 legacy，再走 v1.5.10 新端點。"""
    if market == "TWSE":
        return helper["download_and_validate"](session, market, day, expected_rows, out_dir, logs)
    records, legacy_meta = download_legacy_tpex(session, day, expected_rows, out_dir, helper, logs)
    if legacy_meta.get("status") == "PASS":
        return records, legacy_meta
    # fallback：保留 legacy 原始回應，再嘗試 v1.5.10 新站 daily endpoint。
    modern_records, modern_meta = helper["download_and_validate"](session, "TPEX", day, expected_rows, out_dir, logs)
    modern_meta["legacy_attempt"] = {"status": legacy_meta.get("status"), "http_status": legacy_meta.get("http_status"),
                                     "error": legacy_meta.get("error"), "raw_file": legacy_meta.get("raw_file")}
    if modern_meta.get("status") == "PASS":
        return modern_records, modern_meta
    # 兩個官方端點皆明確回報無資料時，列為 WAITING；不能把 0 rows 當品質拒絕或自行推定休市。
    if (legacy_meta.get("status") == "WAITING_FOR_OFFICIAL_DATA"
            and modern_meta.get("status") == "WAITING_FOR_OFFICIAL_DATA"):
        modern_meta["status"] = "WAITING_FOR_OFFICIAL_DATA"
        modern_meta["error"] = (
            "TPEx legacy 與 modern 官方端點均回傳無行情資料；"
            "保留原始回應並列待確認，不寫入增量、不推定休市。"
        )
        modern_meta["legacy_attempt"] = {
            "status": legacy_meta.get("status"),
            "http_status": legacy_meta.get("http_status"),
            "error": legacy_meta.get("error"),
            "raw_file": legacy_meta.get("raw_file"),
        }
        return [], modern_meta
    modern_meta["error"] = ("legacy: " + str(legacy_meta.get("error", "")) +
                             " | modern: " + str(modern_meta.get("error", "")))
    return [], modern_meta


def run_function_self_tests(helper: dict, temp_dir: Path, logs: list):
    tests = []
    def check(name, fn):
        try:
            fn()
            tests.append({"test": name, "status": "PASS", "error": ""})
            logs.append(f"[FUNCTION TEST PASS] {name}")
        except Exception as exc:
            tests.append({"test": name, "status": "FAIL", "error": f"{type(exc).__name__}: {exc}"})
            logs.append(f"[FUNCTION TEST FAIL] {name}: {type(exc).__name__}: {exc}")
            raise

    def test_date_and_market():
        assert helper["parse_date"]("2016-01-30") == date(2016, 1, 30)
        assert helper["parse_date"]("105/01/30") == date(2016, 1, 30)
        assert helper["market_norm"]("上市") == "TWSE"
        assert helper["market_norm"]("上櫃") == "TPEX"

    def test_recent_window_rule():
        end_day = date(2026, 11, 13)
        start_day = end_day - timedelta(days=LOOKBACK_CALENDAR_DAYS - 1)
        assert (end_day - start_day).days + 1 == LOOKBACK_CALENDAR_DAYS
        assert start_day == date(2026, 10, 15)
        assert end_day == date(2026, 11, 13)
        payload = {"stat": "ok", "data": [
            ["2026-10-26", "臺灣光復暨金門古寧頭大捷紀念日", "適逢星期日，於10月26日（星期一）補假。"],
        ]}
        closed, opened, parsed = classify_holiday_calendar(payload, 2026, helper["parse_date"])
        sessions, evidence = build_expected_sessions(start_day, end_day, {2026: {
            "closed_dates": sorted(d.isoformat() for d in closed),
            "explicit_open_dates": sorted(d.isoformat() for d in opened),
        }})
        assert date(2026, 10, 26) not in sessions
        assert date(2026, 10, 23) not in sessions, "薔琵颱風臨時休市日不可列為預期交易日"
        assert date(2026, 10, 22) in sessions
        assert date(2026, 10, 27) in sessions
        assert date(2026, 11, 13) in sessions
        assert all(start_day <= d <= end_day for d in sessions)

    def test_database_scan_window_filter():
        import csv
        path = temp_dir / "window_scan_test.csv"
        with path.open("w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["trade_date", "market"])
            writer.writeheader()
            writer.writerows([
                {"trade_date": "2026-10-14", "market": "TWSE"},
                {"trade_date": "2026-10-15", "market": "TWSE"},
                {"trade_date": "2026-11-13", "market": "TWSE"},
                {"trade_date": "2026-11-14", "market": "TWSE"},
            ])
        counts, audit = {}, []
        scan_csv_date_counts(path, helper, counts, audit, date(2026, 10, 15), date(2026, 11, 13))
        assert counts == {("TWSE", date(2026, 10, 15)): 1, ("TWSE", date(2026, 11, 13)): 1}, counts
        assert audit[0]["status"] == "PASS"
        assert audit[0]["market_date_groups_in_window"] == 2

    def test_multi_date_natural_key_roundtrip():
        # 同一代號跨交易日、跨市場均是合法資料；同市場同日同代號才是重複。
        sample = []
        for trade_day, market in (("2026-11-16", "TWSE"),
                                  ("2026-11-17", "TWSE"),
                                  ("2026-11-16", "TPEX")):
            sample.append({
                "trade_date": trade_day, "market": market,
                "instrument_code": "2330", "instrument_name": "測試股票",
                "open": 100.0, "high": 102.0, "low": 99.0, "close": 101.0,
                "volume": 1000, "trade_value": 101000,
                "price_change": 1.0, "transactions": 10, "source": "UNIT_TEST",
            })
        multi_path = temp_dir / "natural_key_multi_date.csv"
        helper["write_csv"](multi_path, sample, helper["COMPARE_FIELDS"])
        parsed = helper["normalize_csv_rows"](
            multi_path, market=None, required_fields=helper["CORE_REQUIRED_FIELDS"]
        )[1]
        assert len(parsed) == 3, len(parsed)

        duplicate_path = temp_dir / "natural_key_duplicate.csv"
        helper["write_csv"](duplicate_path, sample + [dict(sample[0])], helper["COMPARE_FIELDS"])
        try:
            helper["normalize_csv_rows"](
                duplicate_path, market=None, required_fields=helper["CORE_REQUIRED_FIELDS"]
            )
        except RuntimeError as exc:
            assert "重複自然鍵值" in str(exc), str(exc)
        else:
            raise AssertionError("相同市場／交易日／證券代號的重複資料未被拒絕")

    def test_calendar_rules():
        payload = {"stat": "ok", "data": [
            ["2016-01-01", "中華民國開國紀念日", "依規定放假1日。"],
            ["2016-01-04", "新年度開始交易日", "開始交易。"],
            ["2016-01-30", "補行交易日", "公告補行交易。"],
        ]}
        closed, opened, parsed = classify_holiday_calendar(payload, 2016, helper["parse_date"])
        assert date(2016, 1, 1) in closed
        assert date(2016, 1, 4) in opened
        assert date(2016, 1, 30) in opened
        assert len(parsed) == 3

    def test_payload_validator_roundtrip():
        payload = make_synthetic_payload()
        records, status, error, title, parse_stats = helper["parse_payload"](payload, "TWSE", date(2026, 11, 12))
        assert status == "PARSED" and len(records) == 501, (status, error, len(records))
        passed, stats = helper["validate_records"](records, "TWSE", date(2026, 11, 12), None)
        assert passed, stats["validation_errors"]
        helper["verify_all_forward_records"](records)
        out = temp_dir / "function_roundtrip.csv"
        helper["write_csv"](out, records, helper["COMPARE_FIELDS"])
        result = helper["verify_csv_roundtrip"](out, records, market="TWSE", day=date(2026, 11, 12))
        assert result["rows"] == 501

    def test_download_function_twse_and_tpex():
        # 這裡刻意使用 FakeSession 假資料驗函數介面，不是真實行情下載；
        # 收起底層下載器的成功訊息，避免把 501 筆測試資料誤認為正式行情。
        payload = make_synthetic_payload()
        test_logs = []  # 假資料測試訊息不可混入正式 run.log。
        with contextlib.redirect_stdout(io.StringIO()):
            for market in ("TWSE", "TPEX"):
                records, meta = download_market_day(
                    FakeSession(payload), market, date(2026, 11, 12), None, temp_dir, helper, test_logs
                )
                assert meta["status"] == "PASS", (market, meta)
                assert len(records) == 501, (market, len(records))
                assert Path(temp_dir, meta["raw_file"]).is_file()
        # 兩個官方端點均無資料，應維持 WAITING，不可變成 REJECTED。
        with contextlib.redirect_stdout(io.StringIO()):
            empty_records, empty_meta = download_market_day(
                FakeSession(make_empty_payload()), "TPEX", date(2026, 10, 23), 893, temp_dir, helper, test_logs
            )
        assert empty_records == []
        assert empty_meta["status"] == "WAITING_FOR_OFFICIAL_DATA", empty_meta
        assert not any("501" in line and "DOWNLOAD + CHECK PASS" in line for line in logs)

    check("日期與市場正規化", test_date_and_market)
    check("近30日窗口邊界與2026休市日", test_recent_window_rule)
    check("資料庫掃描僅納入窗口內日期", test_database_scan_window_filter)
    check("跨日／跨市場自然鍵與重複鍵防護", test_multi_date_natural_key_roundtrip)
    check("官方開休市日曆分類", test_calendar_rules)
    check("payload解析／OHLC驗證／CSV round-trip", test_payload_validator_roundtrip)
    check("官方下載函數介面（TWSE + TPEx 假資料）", test_download_function_twse_and_tpex)
    return tests


def write_json(path: Path, obj):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2, default=str), encoding="utf-8")


def write_csv_rows(path: Path, rows: list, fields: list):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def make_manifest(out_dir: Path):
    items = []
    for path in sorted(out_dir.rglob("*")):
        if not path.is_file() or path.name in {"artifact_sha256_manifest.json", "GitHub上傳結果.json"} or path.suffix.lower() == ".zip":
            continue
        items.append({"path": path.relative_to(out_dir).as_posix(), "bytes": path.stat().st_size,
                      "sha256": sha256_file(path)})
    obj = {"version": VERSION, "created_at_taipei": datetime.now().astimezone().isoformat(),
           "file_count": len(items), "self_excluded": True, "zip_excluded": True, "files": items}
    write_json(out_dir / "artifact_sha256_manifest.json", obj)
    return obj


def make_zip(out_dir: Path, manifest: dict):
    import zipfile
    zip_path = out_dir / f"{out_dir.name}.zip"
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
        for path in sorted(out_dir.rglob("*")):
            if not path.is_file() or path == zip_path or path.name == "GitHub上傳結果.json" or path.suffix.lower() == ".zip":
                continue
            zf.write(path, path.relative_to(out_dir).as_posix())
    with zipfile.ZipFile(zip_path, "r") as zf:
        bad = zf.testzip()
        if bad:
            raise RuntimeError(f"ZIP CRC 失敗：{bad}")
        names = set(zf.namelist())
        for item in manifest["files"]:
            if item["path"] not in names:
                raise RuntimeError(f"ZIP 缺檔：{item['path']}")
            if sha256_bytes(zf.read(item["path"])) != item["sha256"]:
                raise RuntimeError(f"ZIP 內 SHA 不符：{item['path']}")
    return zip_path


def get_github_token():
    token = (os.environ.get("GITHUB_SSO_TOKEN", "") or os.environ.get("GITHUB_TOKEN", "")).strip()
    if not token:
        try:
            from google.colab import userdata
            token = str(userdata.get("GITHUB_SSO_TOKEN") or userdata.get("GITHUB_TOKEN") or "").strip()
        except Exception:
            token = ""
    if not token:
        raise RuntimeError("找不到 Colab Secret GITHUB_SSO_TOKEN 或 GITHUB_TOKEN；Drive 結果仍保留，GitHub 上傳標示失敗。")
    return token


def git_audit_upload(out_dir: Path, run_id: str, logs: list):
    token = get_github_token()
    work = Path(tempfile.mkdtemp(prefix="sso_v1517_git_"))
    askpass = work / "askpass.sh"
    askpass.write_text('#!/bin/sh\ncase "$1" in *Username*) echo x-access-token ;; *) echo "$SSO_GITHUB_TOKEN" ;; esac\n', encoding="utf-8")
    askpass.chmod(0o700)
    env = os.environ.copy()
    env.update({"GIT_ASKPASS": str(askpass), "GIT_TERMINAL_PROMPT": "0", "SSO_GITHUB_TOKEN": token})

    def git(*args, check=True):
        result = subprocess.run(["git", *args], cwd=work, env=env, text=True,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if check and result.returncode != 0:
            # 不把 token 放入錯誤訊息。
            raise RuntimeError(f"git {' '.join(args[:2])} 失敗：{result.stderr[-1500:]}")
        return result.stdout.strip(), result.stderr.strip(), result.returncode

    git("init")
    git("config", "user.name", "SSO Audit Bot")
    git("config", "user.email", "sso-audit@users.noreply.github.com")
    git("remote", "add", "origin", f"https://github.com/{REPO}.git")
    git("fetch", "--depth=1", "origin", "main")
    main_before, _, _ = git("rev-parse", "FETCH_HEAD")
    if main_before != EXPECTED_MAIN_SHA:
        raise RuntimeError(f"GitHub main 與既定保護 SHA 不同：{main_before}；停止上傳，不碰 main。")
    branch = f"audit/sso-marketdata-calendar-backfill-{run_id}"
    git("checkout", "-b", branch, "FETCH_HEAD")
    prefix = f"audit_readout/sso_marketdata_calendar_backfill_{run_id}"
    dest = work / prefix
    dest.mkdir(parents=True, exist_ok=True)
    for src in out_dir.rglob("*"):
        if src.is_file():
            rel = src.relative_to(out_dir)
            target = dest / rel
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, target)
    git("add", prefix)
    git("commit", "-m", f"SSO v1.5.18 same-day gate official calendar backfill {run_id}")
    git("push", "origin", f"HEAD:refs/heads/{branch}")
    commit_sha, _, _ = git("rev-parse", "HEAD")
    git("fetch", "--depth=1", "origin", f"refs/heads/{branch}:refs/remotes/origin/{branch}")
    branch_head, _, _ = git("rev-parse", f"refs/remotes/origin/{branch}")
    if branch_head != commit_sha:
        raise RuntimeError("GitHub audit branch head 回讀不一致。")
    readback_count = 0
    for local in out_dir.rglob("*"):
        if not local.is_file():
            continue
        rel = f"{prefix}/{local.relative_to(out_dir).as_posix()}"
        # 二進位 ZIP 以 bytes 回讀，逐檔比對 SHA256。
        raw = subprocess.run(["git", "show", f"refs/remotes/origin/{branch}:{rel}"], cwd=work,
                             env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if raw.returncode != 0 or sha256_bytes(raw.stdout) != sha256_file(local):
            raise RuntimeError(f"GitHub 逐檔回讀 SHA 不一致：{rel}")
        readback_count += 1
    git("fetch", "--depth=1", "origin", "main")
    main_after = subprocess.run(["git", "ls-remote", "origin", "refs/heads/main"], cwd=work,
                                env=env, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True).stdout.split()[0]
    if main_after != main_before or main_after != EXPECTED_MAIN_SHA:
        raise RuntimeError(f"GitHub main 前後 SHA 不一致：{main_before} -> {main_after}")
    result = {"status": "PASS", "branch": branch, "commit_sha": commit_sha,
              "branch_head_sha": branch_head, "main_before": main_before,
              "main_after": main_after, "main_unchanged": True,
              "file_readback_count": readback_count,
              "url": f"https://github.com/{REPO}/tree/{branch}/{prefix}"}
    logs.append(f"[GITHUB PASS] branch={branch}; files={readback_count}; main unchanged={main_after}")
    return result



# F04 H0 forward-test bridge: exact frozen v2.31 universe used by 11.py.
F04_BASE_PATH = Path(
    "/content/drive/MyDrive/看股最速適/分析重跑_v1.2_20261026_155023/"
    "SSO_研究指標獨立驗證與前瞻分析_v2.31_20261116_121536/"
    "相對價格因子與前瞻報酬研究資料_v2.31.csv.gz"
)
F04_BASE_SHA256 = "113a4f7cd5c94d7bbd3cb78427963dde2a6c249d66a25a66fab2c408958de71c"
F04_EXPECTED_UNIVERSE_SIZE = 2435
F04_EXPECTED_BASE_MAX_DATE = "2026-11-06"
F04_MIN_SESSION_COVERAGE = 0.90
F04_PRICE_FIELDS = ("open", "high", "low", "close")


def _f04_normalize_market(value):
    s = str(value).strip().upper().replace(" ", "")
    if s in {"TWSE", "TSE", "上市", "上市市場", "1", "1.0", "T"}:
        return "TWSE"
    if s in {"TPEX", "TPEx".upper(), "OTC", "上櫃", "上櫃市場", "2", "2.0", "O"}:
        return "TPEX"
    return None


def _f04_normalize_code(value):
    return re.sub(r"\.(TW|TWO)$", "", re.sub(r"\.0$", "", str(value).strip().upper()))


def summarize_f04_session_coverage(universe, source_records, required_dates):
    """Count unique, valid (market,symbol) natural keys for the frozen F04 universe."""
    by_date = {str(d): {} for d in required_dates}
    conflicts = []
    for row in source_records:
        day = str(row.get("trade_date", ""))[:10]
        if day not in by_date:
            continue
        market = _f04_normalize_market(row.get("market"))
        code = _f04_normalize_code(row.get("instrument_code"))
        if not market or not code or (market, code) not in universe:
            continue
        try:
            px = {k: float(row.get(k)) for k in F04_PRICE_FIELDS}
        except (TypeError, ValueError):
            continue
        if not all(math.isfinite(v) and v > 0 for v in px.values()):
            continue
        if px["high"] < max(px["open"], px["close"], px["low"]):
            continue
        if px["low"] > min(px["open"], px["close"], px["high"]):
            continue
        key = (market, code)
        old = by_date[day].get(key)
        if old is not None and any(abs(old[k] - px[k]) > 1e-9 for k in F04_PRICE_FIELDS):
            conflicts.append({"date": day, "market": market, "symbol": code,
                              "existing": old, "incoming": px})
        else:
            by_date[day][key] = px
    rows = []
    for day in required_dates:
        day = str(day)
        n = len(by_date[day])
        coverage = n / len(universe) if universe else 0.0
        rows.append({"trade_date": day, "valid_ohlc_universe_keys": n,
                     "frozen_universe_size": len(universe), "coverage": coverage,
                     "minimum_required_coverage": F04_MIN_SESSION_COVERAGE,
                     "pass": coverage >= F04_MIN_SESSION_COVERAGE})
    return rows, conflicts


def build_f04_frozen_universe(base, expected_size=F04_EXPECTED_UNIVERSE_SIZE,
                              expected_max_date=F04_EXPECTED_BASE_MAX_DATE):
    """Reproduce 11.py load_base eligibility exactly before counting (market, symbol)."""
    import pandas as pd
    required = ["日期", "市場", "股票代號", "影子開盤價", "影子收盤價"]
    missing = [c for c in required if c not in base.columns]
    if missing:
        raise RuntimeError(f"F04 基準檔缺少 11.py 必要欄位：{missing}")

    x = base.copy()
    raw_rows = int(len(x))
    raw_pair_count = int(x[["市場", "股票代號"]].drop_duplicates().shape[0])
    x["日期"] = pd.to_datetime(x["日期"], errors="coerce").dt.normalize()
    x["影子開盤價"] = pd.to_numeric(x["影子開盤價"], errors="coerce")
    x["影子收盤價"] = pd.to_numeric(x["影子收盤價"], errors="coerce")
    x["市場"] = x["市場"].map(_f04_normalize_market)
    x["股票代號"] = x["股票代號"].map(_f04_normalize_code)

    # Must match 11.py load_base(): valid date/market/symbol/open/close,
    # non-empty symbol, and strictly positive shadow open and close.
    before_valid_filter = len(x)
    x = x.dropna(subset=["日期", "市場", "股票代號", "影子開盤價", "影子收盤價"]).copy()
    x = x[(x["股票代號"] != "") & (x["影子開盤價"] > 0) & (x["影子收盤價"] > 0)].copy()
    valid_rows = int(len(x))

    # Match the integrity checks in 11.py as well: no repeated security/date row
    # and a pinned last date. Do not silently drop or repair duplicates.
    if x.duplicated(["市場", "股票代號", "日期"]).any():
        dup = x.loc[x.duplicated(["市場", "股票代號", "日期"], keep=False),
                    ["市場", "股票代號", "日期"]].head(10)
        raise RuntimeError("F04 v2.31 基準資料存在重複 (市場,股票代號,日期)：" +
                           dup.to_dict(orient="records").__repr__())
    max_date = str(x["日期"].max().date()) if not x.empty else None
    if max_date != expected_max_date:
        raise RuntimeError(f"F04 v2.31 基準資料末日異常：{max_date}; expected={expected_max_date}")

    universe = set(zip(x["市場"].astype(str), x["股票代號"].astype(str)))
    diagnostics = {
        "raw_rows": raw_rows,
        "raw_market_symbol_pairs_before_11py_filters": raw_pair_count,
        "rows_after_11py_filters": valid_rows,
        "rows_excluded_by_11py_filters": int(raw_rows - valid_rows),
        "market_symbol_pairs_after_11py_filters": len(universe),
        "expected_market_symbol_pairs": int(expected_size),
        "base_max_date": max_date,
        "filter_contract": "Same as 11.py load_base: parse date; normalize market/code; require date, market, symbol, shadow open and close; symbol non-empty; shadow open/close > 0; reject duplicate (market,symbol,date).",
    }
    if len(universe) != expected_size:
        raise RuntimeError("F04 固定股票池數量不符（已套用 11.py 完整基準篩選）：" +
                           json.dumps(diagnostics, ensure_ascii=False))
    return universe, diagnostics


def validate_f04_three_session_history(root: Path, expected_union, helper, logs):
    """Independently reconstruct the last three official sessions from pinned base + increments."""
    import pandas as pd
    base_path = F04_BASE_PATH
    if not base_path.is_file():
        raise FileNotFoundError(f"F04 固定 v2.31 基礎檔不存在：{base_path}")
    actual_sha = sha256_file(base_path)
    if actual_sha != F04_BASE_SHA256:
        raise RuntimeError(f"F04 v2.31 基礎檔 SHA256 不符：{actual_sha}")
    base = pd.read_csv(
        base_path, compression="infer",
        usecols=["日期", "市場", "股票代號", "影子開盤價", "影子收盤價"],
        dtype={"市場": "string", "股票代號": "string"}, low_memory=False,
    )
    universe, universe_diagnostics = build_f04_frozen_universe(base)
    logs.append("[F04 BASE UNIVERSE] " + json.dumps(universe_diagnostics, ensure_ascii=False))

    required_dates = [d.isoformat() for d in sorted(expected_union)[-3:]]
    if len(required_dates) != 3:
        raise RuntimeError(f"官方日曆無法提供前三個交易日：{required_dates}")
    records = []
    source_files = []
    # Only standard full-market increment files are read; all are read-only.
    for path in sorted(root.rglob("全市場標準化增量_*.csv*"), key=lambda p: str(p)):
        if not path.is_file() or not (path.suffix.lower() == ".csv" or path.name.lower().endswith(".csv.gz")):
            continue
        _, parsed = helper["normalize_csv_rows"](
            path, market=None, required_fields=helper["CORE_REQUIRED_FIELDS"]
        )
        selected = [r for r in parsed if str(r.get("trade_date", ""))[:10] in set(required_dates)]
        if selected:
            records.extend(selected)
        source_files.append({"path": str(path), "sha256": sha256_file(path),
                             "bytes": path.stat().st_size,
                             "matching_required_rows": len(selected)})

    session_rows, conflicts = summarize_f04_session_coverage(universe, records, required_dates)
    latest_matches = session_rows[-1]["trade_date"] == required_dates[-1]
    passed = latest_matches and all(x["pass"] for x in session_rows) and not conflicts
    result = {
        "status": "PASS" if passed else "BLOCKED",
        "base_path": str(base_path), "base_sha256": actual_sha,
        "frozen_universe_size": len(universe),
        "frozen_universe_diagnostics": universe_diagnostics,
        "required_previous_market_sessions": required_dates,
        "latest_history_date": session_rows[-1]["trade_date"] if session_rows else None,
        "minimum_daily_coverage": F04_MIN_SESSION_COVERAGE,
        "session_checks": session_rows,
        "natural_key_price_conflicts": conflicts,
        "increment_files_scanned": len(source_files),
        "increment_sources": source_files,
        "pass": passed,
        "note": "股票池先完全沿用 11.py load_base 的有效日期、市場、代號、正值影子開盤／收盤篩選，再以固定 v2.31 的 2,435 組 (market,symbol) 為分母；每日只納入有效正值 OHLC；跨檔同自然鍵同價去重，價格衝突即阻擋。",
    }
    logs.append("[F04 HISTORY GATE] " + json.dumps({
        "required_dates": required_dates,
        "coverage": [round(x["coverage"], 6) for x in session_rows],
        "pass": passed, "conflicts": len(conflicts),
    }, ensure_ascii=False))
    return result


def test_f04_base_universe_matches_11py_filters():
    import pandas as pd
    rows = []
    for i in range(5):
        rows.append({"日期": "2026-11-06", "市場": "TWSE", "股票代號": f"{i:04d}",
                     "影子開盤價": 10.0, "影子收盤價": 10.5})
    # Three additional market/code pairs are deliberately ineligible under 11.py's loader.
    rows.extend([
        {"日期": "2026-11-06", "市場": "TWSE", "股票代號": "9001", "影子開盤價": None, "影子收盤價": 10.0},
        {"日期": "2026-11-06", "市場": "TPEX", "股票代號": "9002", "影子開盤價": 10.0, "影子收盤價": None},
        {"日期": "2026-11-06", "市場": "TPEX", "股票代號": "9003", "影子開盤價": 0.0, "影子收盤價": 10.0},
    ])
    universe, diag = build_f04_frozen_universe(pd.DataFrame(rows), expected_size=5)
    assert len(universe) == 5
    assert diag["raw_market_symbol_pairs_before_11py_filters"] == 8
    assert diag["market_symbol_pairs_after_11py_filters"] == 5
    return {"status": "PASS", "raw_pairs": 8, "eligible_pairs": 5,
            "excluded_rows": diag["rows_excluded_by_11py_filters"]}


def test_f04_coverage_gate():
    universe = {(m, f"{i:04d}") for m in ("TWSE", "TPEX") for i in range(10)}
    rows = []
    for m, code in sorted(universe):
        rows.append({"trade_date": "2026-11-23", "market": m, "instrument_code": code,
                     "open": 10, "high": 11, "low": 9, "close": 10.5})
    summary, conflicts = summarize_f04_session_coverage(universe, rows, ["2026-11-23"])
    assert summary[0]["pass"] and summary[0]["valid_ohlc_universe_keys"] == 20
    assert not conflicts
    duplicate = dict(rows[0]); duplicate["close"] = 10.8
    _, conflicts = summarize_f04_session_coverage(universe, rows + [duplicate], ["2026-11-23"])
    assert len(conflicts) == 1
    return {"status": "PASS", "unique_coverage": summary[0]["coverage"],
            "conflict_detected": len(conflicts) == 1}

def resolve_current_script_source() -> tuple[bytes, str]:
    """Locate the actual uploaded/executed 12.py for a complete audit archive.

    Colab may execute uploaded code through a temporary /tmp/ipykernel_*/...py
    wrapper, so __file__ is not always the original uploaded file. Prefer a real
    file that carries this exact version signature; then fall back to the source
    cached by Python's linecache. Never archive a similarly named but stale file.
    """
    expected_version_line = f'VERSION = "{VERSION}"'

    def is_this_program(raw: bytes) -> bool:
        try:
            text = raw.decode("utf-8-sig")
        except UnicodeDecodeError:
            return False
        return (
            expected_version_line in text
            and "def validate_f04_three_session_history" in text
            and "def test_f04_base_universe_matches_11py_filters" in text
            and "def run():" in text
        )

    candidates: list[Path] = []
    raw_file = globals().get("__file__")
    if raw_file:
        try:
            candidates.append(Path(str(raw_file)).expanduser())
        except Exception:
            pass

    # sys.argv[0] may be a kernel -f JSON argument; only consider Python files.
    argv0 = sys.argv[0] if sys.argv else ""
    if argv0 and str(argv0).lower().endswith(".py"):
        candidates.append(Path(argv0).expanduser())

    cwd = Path.cwd()
    candidates.extend([
        cwd / "12.py",
        Path("/content/12.py"),
        Path("/mnt/data/12.py"),
    ])
    # Colab uploads may suffix a repeated upload as "12 (1).py".
    for folder in (cwd, Path("/content")):
        try:
            candidates.extend(sorted(folder.glob("12*.py"), key=lambda q: (q.name, str(q))))
        except Exception:
            pass

    seen: set[str] = set()
    for candidate in candidates:
        try:
            resolved = candidate.resolve()
            key = str(resolved)
            if key in seen or not resolved.is_file():
                continue
            seen.add(key)
            raw = resolved.read_bytes()
            if is_this_program(raw):
                return raw, str(resolved)
        except Exception:
            continue

    # Some notebook runners remove the temporary source file after compiling it,
    # but Python may still retain the exact source lines in linecache.
    linecache_keys = []
    if raw_file:
        linecache_keys.append(str(raw_file))
    if argv0 and str(argv0).lower().endswith(".py"):
        linecache_keys.append(str(argv0))
    for key in linecache_keys:
        try:
            lines = linecache.getlines(key)
            if not lines:
                continue
            raw = "".join(lines).encode("utf-8")
            if is_this_program(raw):
                return raw, f"linecache:{key}"
        except Exception:
            continue

    searched = ", ".join(seen) if seen else "no readable Python source candidates"
    raise RuntimeError(
        "無法定位本次執行的 v1.5.18 原始程式；已檢查 __file__、sys.argv[0]、"
        f"工作目錄及 /content 的 12*.py，且未找到版本簽章相符的檔案。候選：{searched}。"
    )


def run():
    # 先透過封存 v1.5.10 函數載入器建立穩定介面。
    root_hint = Path("/content/drive/MyDrive/看股最速適/SSO_Market_Data_Continuation_TWSE_TPEX_v1")
    if not Path("/content/drive/MyDrive").exists():
        from google.colab import drive
        drive.mount("/content/drive", force_remount=False)
    if not root_hint.exists():
        raise FileNotFoundError(f"SSO 資料根目錄不存在：{root_hint}")
    source, source_origin = get_base_script(root_hint)
    helper = load_v1510_helpers(source)
    helper["mount_drive"]()
    root = Path(helper["ROOT"])
    now = datetime.now(helper["TZ"])
    run_id = now.strftime("%Y%m%d_%H%M%S") + "_" + uuid.uuid4().hex[:4]
    out_dir = root / f"v1.5.18_同日閘門與官方日曆核驗_{run_id}"
    out_dir.mkdir(parents=True, exist_ok=False)
    logs = []
    logs.append(f"Version={VERSION}")
    logs.append(f"Run time={now.isoformat()}")
    logs.append(f"v1.5.10 base source={source_origin}")
    logs.append("[SAFETY] v4.17、v2.31與既有增量全部唯讀；只寫入新的 v1.5.18 輸出目錄；不掃描巡田最速適；不推送 main。")

    # 先驗函數介面/解析/驗證/寫入函數，避免正式請求前才發現函數錯誤。
    temp_test_dir = Path(tempfile.mkdtemp(prefix="sso_v1517_function_test_"))
    function_tests = run_function_self_tests(helper, temp_test_dir, logs)
    f04_base_test = test_f04_base_universe_matches_11py_filters()
    function_tests.append({"test": "F04基準股票池與11.py篩選口徑一致", "status": "PASS", "error": ""})
    logs.append("[F04 BASE FILTER TEST PASS] " + json.dumps(f04_base_test, ensure_ascii=False))
    f04_test = test_f04_coverage_gate()
    function_tests.append({"test": "F04固定股票池三日覆蓋／衝突偵測", "status": "PASS", "error": ""})
    logs.append("[F04 COVERAGE TEST PASS] " + json.dumps(f04_test, ensure_ascii=False))
    shutil.rmtree(temp_test_dir, ignore_errors=True)

    # 依 17:00 台北時間閘門決定是否納入當日；當日仍須通過官方日曆與逐市場行情驗證。
    target_start_date, target_end_date, includes_today = resolve_scan_window(now)
    gate_result = test_same_day_gate_policy()
    function_tests.append({"test": "same_day_17_00_taipei_gate", "status": "PASS", "error": ""})
    logs.append("[SAME-DAY GATE TEST PASS] " + json.dumps(gate_result, ensure_ascii=False))
    logs.append(
        f"[SAME-DAY GATE] Taipei={now.isoformat()}; gate=17:00; "
        f"includes_today={includes_today}; target_end={target_end_date}"
    )
    counts, source_audit, start_date, end_date = scan_existing_database(
        helper, logs, target_start_date, target_end_date
    )
    logs.append(f"[DATABASE WINDOW PASS] {start_date} .. {end_date}; market-date groups={len(counts)}")

    session = helper["make_session"]()
    years = list(range(start_date.year, end_date.year + 1))
    calendars = fetch_official_calendars(session, years, helper["parse_date"], logs)
    expected_union, calendar_evidence = build_expected_sessions(start_date, end_date, calendars)
    expected_by_market = {m: sorted(expected_union) for m in ("TWSE", "TPEX")}
    coverage_rows, partial_suspects = build_daily_summary(expected_by_market, counts, helper)

    # 失敗的 v1.5.14 已有標準 CSV，但未完成逐股 overlay；重新驗證後只補 overlay。
    recovered_records, recovery_audit_rows, recovery_sources = recover_unsealed_v1514_increments(
        root, start_date, end_date, counts, helper, logs
    )

    missing_tasks = []
    for market in ("TWSE", "TPEX"):
        for day in expected_by_market[market]:
            if counts.get((market, day), 0) == 0:
                missing_tasks.append((market, day, "MISSING_EXPECTED_SESSION"))

    for special_day, special_item in sorted(SPECIAL_CLOSED_SESSIONS.items()):
        if start_date <= special_day <= end_date:
            logs.append(f"[SPECIAL CLOSED PASS] {special_day} {special_item['name']}；市場={','.join(special_item['markets'])}；不建立補抓任務。")
    logs.append(f"[CALENDAR RESULT] expected sessions={len(expected_union)}; missing market-days={len(missing_tasks)}; partial suspects={len(partial_suspects)}")
    logs.append("[CALENDAR POLICY] 僅依年度官方日曆或有證據的臨時休市公告排除；API空資料本身一律不推定休市。")

    # 只對官方日曆預期交易、但資料庫缺列的日期補抓；每市場獨立判斷。
    api_meta = []
    added_records = []
    task_rows = []
    unresolved = []
    raw_dir_root = out_dir / "raw"
    for index, (market, day, reason) in enumerate(missing_tasks, 1):
        expected_rows = local_expected_rows(market, day, expected_by_market[market], counts)
        logs.append(f"[BACKFILL {index}/{len(missing_tasks)}] {market} {day}; local baseline={expected_rows}")
        records, meta = download_market_day(
            session, market, day, expected_rows, out_dir, helper, logs
        )
        api_meta.append(meta)
        task_rows.append({
            "trade_date": day.isoformat(), "market": market,
            "calendar_evidence": calendar_evidence.get(day.isoformat(), ""),
            "reason": reason, "api_status": meta.get("status"),
            "rows": len(records), "http_status": meta.get("http_status"),
            "error": meta.get("error", ""), "raw_file": meta.get("raw_file", ""),
            "response_sha256": meta.get("response_sha256", ""),
        })
        if meta.get("status") == "PASS":
            # 只有資料庫原先完全沒有該市場/日期時才允許 append，避免日資料重複或半天資料混併。
            if counts.get((market, day), 0) != 0:
                raise RuntimeError(f"補抓前資料庫已有 {market} {day} 列，禁止 append 重複日資料。")
            added_records.extend(records)
        else:
            unresolved.append({"market": market, "trade_date": day.isoformat(),
                               "status": meta.get("status"), "error": meta.get("error", "")})
        time.sleep(0.35)

    helper["verify_all_forward_records"](added_records)
    added_records.sort(key=lambda r: (r["trade_date"], r["market"], r["instrument_code"]))

    # 寫入既有 v1.5.x 可自動辨識的標準化增量檔名；不覆寫 v4.17 或任何舊增量。
    dirs = helper["version_dirs"]()
    _, _, standard_columns, overlay_columns = helper["load_baseline_and_schema"](dirs)
    if added_records:
        added_dates = sorted({r["trade_date"] for r in added_records})
        std_path = out_dir / f"全市場標準化增量_{added_dates[0].replace('-', '')}_{added_dates[-1].replace('-', '')}.csv"
    else:
        std_path = out_dir / f"全市場標準化增量_無新增_{run_id}.csv"
    helper["write_csv"](std_path, added_records, standard_columns)
    roundtrip = helper["normalize_csv_rows"](std_path, market=None, required_fields=helper["CORE_REQUIRED_FIELDS"])[1]
    if len(roundtrip) != len(added_records):
        raise RuntimeError("標準化增量 round-trip 列數不一致。")
    if helper["canonical_records_sha256"](roundtrip) != helper["canonical_records_sha256"](added_records):
        raise RuntimeError("標準化增量 round-trip canonical SHA 不一致。")
    logs.append(f"[INCREMENT ROUNDTRIP PASS] rows={len(roundtrip)}")

    f04_history_validation = validate_f04_three_session_history(root, expected_union, helper, logs)
    write_json(out_dir / "F04前三個交易日日線連續性.json", f04_history_validation)
    write_csv_rows(out_dir / "F04前三個交易日覆蓋率.csv", f04_history_validation["session_checks"],
                   ["trade_date", "valid_ohlc_universe_keys", "frozen_universe_size", "coverage",
                    "minimum_required_coverage", "pass"])

    # 逐股 overlay，沿用 v1.5.x 官方歷史行情 overlay 命名結構。
    overlay_groups = defaultdict(list)
    overlay_source_records = recovered_records + added_records
    overlay_natural_keys = [helper["record_key"](r) for r in overlay_source_records]
    if len(overlay_natural_keys) != len(set(overlay_natural_keys)):
        raise RuntimeError("回收舊增量與本次新增行情在市場＋交易日＋證券代號上重複。")
    for row in overlay_source_records:
        overlay_groups[(row["market"], row["instrument_code"])].append(row)
    overlay_index = []
    for (market, code), rows in sorted(overlay_groups.items()):
        suffix = "TWSE" if market == "TWSE" else "TPEx"
        target = out_dir / "overlay" / "官方歷史行情" / f"{code}_{suffix}.csv"
        rows.sort(key=lambda r: r["trade_date"])
        helper["write_csv"](target, rows, overlay_columns[market])
        check = helper["normalize_csv_rows"](target, market=market, required_fields=helper["CORE_REQUIRED_FIELDS"])[1]
        if helper["canonical_records_sha256"](check) != helper["canonical_records_sha256"](rows):
            raise RuntimeError(f"逐股 overlay round-trip SHA 不一致：{target.name}")
        overlay_index.append({"market": market, "instrument_code": code,
                              "overlay_file": target.relative_to(out_dir).as_posix(),
                              "rows": len(rows), "sha256": sha256_file(target)})

    # 每日市場日曆與缺口結果輸出。
    calendar_rows = []
    closed_by_year = {}
    for year, item in calendars.items():
        closed_by_year[year] = item["closed_dates"]
    for market in ("TWSE", "TPEX"):
        for day in expected_by_market[market]:
            calendar_rows.append({"trade_date": day.isoformat(), "market": market,
                                  "calendar_expected_open": True,
                                  "calendar_evidence": calendar_evidence.get(day.isoformat(), ""),
                                  "database_rows_before": counts.get((market, day), 0),
                                  "api_backfill_status": next((x["api_status"] for x in task_rows
                                      if x["market"] == market and x["trade_date"] == day.isoformat()), "NOT_REQUIRED")})
    for day, item in sorted(SPECIAL_CLOSED_SESSIONS.items()):
        if start_date <= day <= end_date:
            for market in item.get("markets", ()):
                calendar_rows.append({
                    "trade_date": day.isoformat(), "market": market,
                    "calendar_expected_open": False,
                    "calendar_evidence": f"SPECIAL_CLOSED｜{item['name']}｜{item['reason']}｜{' ; '.join(item['source_urls'])}",
                    "database_rows_before": counts.get((market, day), 0),
                    "api_backfill_status": "OFFICIAL_SPECIAL_CLOSED_SKIP",
                })
    calendar_rows.sort(key=lambda r: (r["trade_date"], r["market"]))
    write_csv_rows(out_dir / "官方交易日曆與資料庫覆蓋.csv", calendar_rows,
                   ["trade_date", "market", "calendar_expected_open", "calendar_evidence", "database_rows_before", "api_backfill_status"])
    write_csv_rows(out_dir / "未知營業日補充查詢結果.csv", task_rows,
                   ["trade_date", "market", "calendar_evidence", "reason", "api_status", "rows", "http_status", "error", "raw_file", "response_sha256"])
    write_csv_rows(out_dir / "疑似不完整交易日清單.csv", partial_suspects,
                   ["trade_date", "market", "rows", "neighbor_median_rows", "reason"])
    write_csv_rows(out_dir / "逐股增量覆蓋索引.csv", overlay_index,
                   ["market", "instrument_code", "overlay_file", "rows", "sha256"])
    write_csv_rows(out_dir / "前次未封存增量回收核驗.csv", recovery_audit_rows,
                   ["source_file", "trade_date", "market", "rows", "status", "canonical_sha256", "reason"])

    calendar_obj = {
        "version": VERSION, "start_date": start_date.isoformat(), "end_date": end_date.isoformat(),
        "source": "TWSE official holidaySchedule annual JSON; market-by-market daily OHLC validation",
        "scope_calendar_days": LOOKBACK_CALENDAR_DAYS,
        "years": calendars,
        "known_v448_makeup_saturdays": sorted(d.isoformat() for d in KNOWN_MAKEUP_SATURDAYS),
        "verified_special_closed_sessions": [
            {"trade_date": d.isoformat(), **item}
            for d, item in sorted(SPECIAL_CLOSED_SESSIONS.items())
            if start_date <= d <= end_date
        ],
        "policy": "週一至週五先依官方日曆排除明示休市日；另僅依有公開公告證據的臨時休市事件排除；補行星期六納入候選；API 空資料、HTTP錯誤或解析失敗仍不得自行推定休市。",
    }
    write_json(out_dir / "官方市場開休市日曆.json", calendar_obj)
    write_json(out_dir / "資料庫來源掃描紀錄.json", {
        "main_database": source_audit[0] if source_audit else {},
        "source_files": source_audit,
        "market_date_group_count_in_window": len(counts),
        "window_start": start_date.isoformat(), "window_end": end_date.isoformat(),
        "window_calendar_days": LOOKBACK_CALENDAR_DAYS,
    })
    write_json(out_dir / "官方API查詢索引.json", api_meta)
    write_json(out_dir / "函數自我測試.json", {"status": "PASS", "tests": function_tests})

    # 本版本資料只含全日缺漏的新增列；部分日異常另外列出並阻擋 ready，不做未經核對的覆寫。
    ready = (len(unresolved) == 0 and len(partial_suspects) == 0
             and all(x["status"] == "PASS" for x in function_tests)
             and f04_history_validation.get("pass") is True)
    report = {
        "version": VERSION, "run_id": run_id, "run_time_taipei": now.isoformat(),
        "status": "SUCCESS" if ready else "PARTIAL_BLOCKED",
        "source_base_version": "v1.5.10", "source_base_origin": source_origin,
        "check_window": {"start": start_date.isoformat(), "end": end_date.isoformat(),
                         "calendar_days": LOOKBACK_CALENDAR_DAYS,
                         "end_rule": "台北時間17:00前截止昨天；17:00起納入當日",
                         "same_day_gate_hour_taipei": SAME_DAY_GATE_HOUR,
                         "same_day_included": includes_today,
                         "execution_date_taipei": now.date().isoformat()},
        "official_calendar_years": years,
        "expected_session_dates": len(expected_union),
        "lookback_calendar_days": LOOKBACK_CALENDAR_DAYS,
        "verified_special_closed_sessions": [
            {"trade_date": d.isoformat(), **item}
            for d, item in sorted(SPECIAL_CLOSED_SESSIONS.items())
            if start_date <= d <= end_date
        ],
        "expected_market_days": len(expected_union) * 2,
        "missing_market_days_found": len(missing_tasks),
        "backfill_rows": len(added_records),
        "recovered_unsealed_v1514_rows": len(recovered_records),
        "recovered_unsealed_v1514_sources": recovery_sources,
        "overlay_rows_total_including_recovery": len(overlay_source_records),
        "backfill_by_market": {m: sum(r["market"] == m for r in added_records) for m in ("TWSE", "TPEX")},
        "backfill_trade_dates": sorted({r["trade_date"] for r in added_records}),
        "unresolved_market_days": unresolved,
        "partial_day_suspects": partial_suspects,
        "f04_forward_history_validation": f04_history_validation,
        "function_tests": function_tests,
        "increment_file": std_path.name,
        "downstream_ready": ready,
        "integration": {
            "standardized_increment_saved_under_v1_5_17": True,
            "future_v1_5_x_version_dirs_glob_can_discover_file": True,
            "per_stock_overlay_written": True,
            "old_v4_17_database_modified": False,
            "old_v1_5_x_files_modified": False,
            "any_existing_file_deleted": False,
            "partial_day_overwrite_performed": False,
            "unknown_no_data_days_misclassified_as_holidays": False,
        },
        "note": "17:00前不納入當日；17:00起納入當日。當日官方預期開市但 API 空表、HTTP/解析失敗或行情疑似不完整時，保持 PARTIAL_BLOCKED，不假裝完成；重新執行會重新掃描及補查。",
    }
    write_json(out_dir / "v1.5.18_同日閘門與官方日曆核驗報告.json", report)
    (out_dir / "README.md").write_text(
        f"# {VERSION}\n\n"
        f"狀態：{report['status']}\n\n"
        f"檢查區間（最近30日）：{start_date} 至 {end_date}\n\n"
        f"預期交易日：{len(expected_union)}；缺漏市場日：{len(missing_tasks)}；新增行情：{len(added_records)} 列。\n\n"
        f"未解決日期：{len(unresolved)}；疑似不完整日：{len(partial_suspects)}。\n\n"
        f"F04 前三交易日連續性：{f04_history_validation['status']}；覆蓋率：{[round(x['coverage']*100,2) for x in f04_history_validation['session_checks']]}%。\n\n"
        "本版本納入 2026-10-23 薔琵颱風雙市場臨時休市公告；該日不建立行情補抓任務。\n"
        "新標準化增量存於本 v1.5.18 目錄，檔名符合原 v1.5.x 資料夾掃描規則；舊主檔及舊增量均不覆寫。\n"
        "同日資料閘門：台北時間17:00前截止昨天；17:00起納入當日。\n"
        "當日行情若尚未完整公布，會列為未解決／疑似不完整並阻擋下游，不會當作成功。\n"
        "官方 API 空表或錯誤本身不等於休市；未有公告證據者仍列待確認。\n",
        encoding="utf-8",
    )

    # 封存實際執行程式。Colab 的 __file__ 可能是暫存 ipykernel 路徑，
    # 因此依版本簽章尋找真正上傳的 12.py，必要時回退到 linecache。
    script_source_bytes, script_source_origin = resolve_current_script_source()
    archived_script = out_dir / "12.py"
    archived_script.write_bytes(script_source_bytes)
    script_source_sha256 = sha256_bytes(script_source_bytes)
    report["source_archive"] = {
        "status": "PASS",
        "source_origin": script_source_origin,
        "file": archived_script.name,
        "sha256": script_source_sha256,
        "bytes": len(script_source_bytes),
    }
    write_json(out_dir / "v1.5.18_同日閘門與官方日曆核驗報告.json", report)
    logs.append(
        f"[SOURCE ARCHIVE PASS] origin={script_source_origin}; "
        f"sha256={script_source_sha256}; bytes={len(script_source_bytes)}"
    )

    manifest = make_manifest(out_dir)
    zip_path = make_zip(out_dir, manifest)
    logs.append(f"[ZIP PASS] {zip_path.name}; sha256={sha256_file(zip_path)}")
    (out_dir / "run.log").write_text("\n".join(logs) + "\n", encoding="utf-8")
    # run.log 建立後更新 manifest/ZIP，確保 log 亦納入封存。
    manifest = make_manifest(out_dir)
    zip_path = make_zip(out_dir, manifest)
    report["zip"] = {"file": zip_path.name, "sha256": sha256_file(zip_path), "bytes": zip_path.stat().st_size}
    write_json(out_dir / "v1.5.18_同日閘門與官方日曆核驗報告.json", report)
    manifest = make_manifest(out_dir)
    zip_path = make_zip(out_dir, manifest)

    github_result = {"status": "NOT_RUN"}
    try:
        github_result = git_audit_upload(out_dir, run_id, logs)
    except Exception as exc:
        github_result = {"status": "FAILED", "error": f"{type(exc).__name__}: {exc}"}
        logs.append(f"[GITHUB WARNING] {github_result['error']}")
    write_json(out_dir / "GitHub上傳結果.json", github_result)
    (out_dir / "run.log").write_text("\n".join(logs) + "\n", encoding="utf-8")

    print("\n" + "=" * 88)
    print(f"{VERSION}：{report['status']}")
    print(f"檢查區間（最近30日）：{start_date} 至 {end_date}")
    print(f"官方預期交易日：{len(expected_union)}")
    print(f"待補市場日：{len(missing_tasks)}")
    print(f"新增行情：{len(added_records):,} 列")
    print(f"未解決市場日：{len(unresolved)}")
    print(f"疑似不完整日：{len(partial_suspects)}")
    print(f"函數測試：{sum(x['status']=='PASS' for x in function_tests)}/{len(function_tests)} PASS")
    print(f"F04 前三個交易日覆蓋率：{[round(x['coverage']*100,2) for x in f04_history_validation['session_checks']]}%")
    print(f"F04 前置資料閘門：{f04_history_validation['status']}")
    print(f"下游可用：{ready}")
    print(f"輸出資料夾：{out_dir}")
    print(f"ZIP：{zip_path}")
    print(f"GitHub：{github_result}")
    print("=" * 88)
    return report


if __name__ == "__main__":
    RESULT_V1518 = run()
