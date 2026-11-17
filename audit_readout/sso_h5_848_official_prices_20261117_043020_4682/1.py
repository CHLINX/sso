# -*- coding: utf-8 -*-
"""SSO｜848 檔凍結候選 H5 官方行情補抓 v1.0.2

用途：
1. 讀取固定 SHA256 的 848 檔凍結候選清單。
2. 使用已封存且 SHA 固定的 SSO v1.5.10 官方行情解析／驗證函數。
3. 抓取訊號日 2026-11-06，以及 H5 入場／出場日 2026-11-09 至 2026-11-13。
4. TWSE 使用 MI_INDEX / ALLBUT0999；TPEx 優先使用 type=EW 官方舊端點，
   失敗時改用 v1.5.10 的 TPEx afterTrading/otc type=EW 端點。
5. 以 v1.5.9 已封存 2026-11-11 雙市場修正檔作為該日最終修正層，並核對 OHLCV。
6. 優先重用前次已下載之官方原始 GZIP（唯讀）；輸出全市場六日官方行情、848 檔入出場價格配對表、報告及 ZIP。
7. 只推送到 CHLINX/sso 新 audit 分支及 audit_readout/ 路徑；驗證 main SHA 不變。

重要：
- 不覆寫、搬移、刪除任何 Drive 舊資料。
- 不將整段行情盲目 append 到既有增量庫，避免重複自然鍵。
- 848 檔價格配對表的報酬欄為價格層初算值；公司行動／signal_entry_mismatch
  尚須由正式 H5 audit adapter 核驗，因此本程式不宣稱已通過模型的正式 entry_trade_valid。
- 若官方資料未取得、驗證未過、修正比對不符或 main SHA 不符，程式會阻擋 downstream_ready。

Colab：上傳本檔後直接執行，或在儲存格執行 %run /content/1.py。
"""
from __future__ import annotations

import ast
import csv
import gzip
import hashlib
import json
import math
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import uuid
import zipfile
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import requests

VERSION = "SSO_H5_848_Official_Price_Backfill_v1.0.2"
REPO = "CHLINX/sso"
EXPECTED_MAIN_SHA = "aaabc2433fb5df2bb68af06461178305d6bd30d1"

# v1.5.10 已封存的官方行情解析器。只載入函數，不執行舊版主流程。
BASE_BRANCH = "audit/sso-marketdata-v1510-seal-20261112_180751_3bd3"
BASE_RELATIVE = (
    "audit_readout/sso_marketdata_v1510_20261112_180751_3bd3/"
    "SSO_Market_Data_Continuation_TWSE_TPEX_v1.5.10.py"
)
BASE_RAW_URL = f"https://raw.githubusercontent.com/{REPO}/{BASE_BRANCH}/{BASE_RELATIVE}"
BASE_GIT_BLOB_SHA = "033cd9694d80e3b233b6020e2aaf371f757f709f"

PROJECT_ROOT = Path("/content/drive/MyDrive/看股最速適")
CANDIDATE_CSV = PROJECT_ROOT / (
    "推薦結果/非前瞻參考/SSO_main_v5_20261116_114722_cef3a1/"
    "SSO_v2.66_H5_2026-11-06_凍結假設符合候選.csv"
)
EXPECTED_CANDIDATE_SHA256 = "eea433b9f5242079d2db15256890e136a4a98364c73b62de508ea4ca95e11d1a"
EXPECTED_CANDIDATE_COUNT = 848

SIGNAL_DATE = date(2026, 11, 6)
ENTRY_DATE = date(2026, 11, 9)   # H5 t+1 open
EXIT_DATE = date(2026, 11, 13)   # H5 t+5 close
FETCH_DATES = [
    SIGNAL_DATE,
    date(2026, 11, 9),
    date(2026, 11, 10),
    date(2026, 11, 11),
    date(2026, 11, 12),
    EXIT_DATE,
]
MARKETS = ("TWSE", "TPEX")

# v2.50/v2.51 的既有成本設定；價格配對報酬僅供後續正式 audit 使用。
SLIPPAGE_GENERAL = 0.001
SLIPPAGE_STRESS = 0.002
FEE_RATE = 0.001425
SELL_TAX_RATE = 0.003

TWSE_HOLIDAY_URL = "https://www.twse.com.tw/rwd/zh/holidaySchedule/holidaySchedule"


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


def write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2, default=str), encoding="utf-8")


def write_csv(path: Path, rows: list[dict], columns: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k) for k in columns})


def log(message: str, logs: list[str]) -> None:
    line = f"[{datetime.now().astimezone().strftime('%Y-%m-%d %H:%M:%S %z')}] {message}"
    print(line, flush=True)
    logs.append(line)


def ensure_drive() -> None:
    if not Path("/content/drive/MyDrive").exists():
        try:
            from google.colab import drive
        except Exception as exc:
            raise RuntimeError("此程式需在 Google Colab 執行，且需掛載 Google Drive。") from exc
        drive.mount("/content/drive", force_remount=False)
    if not PROJECT_ROOT.exists():
        raise FileNotFoundError(f"找不到專案根目錄：{PROJECT_ROOT}")


def load_base_helpers() -> tuple[dict, str]:
    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0 (compatible; SSO-H5-Official-Audit/1.0)",
                            "Accept": "application/json,text/plain,*/*"})
    response = session.get(BASE_RAW_URL, timeout=(20, 120))
    response.raise_for_status()
    raw = response.content
    actual_blob = git_blob_sha(raw)
    if actual_blob != BASE_GIT_BLOB_SHA:
        raise RuntimeError(f"v1.5.10 封存程式 Git blob SHA 不符：{actual_blob}")
    source = raw.decode("utf-8-sig")
    tree = ast.parse(source)
    kept = []
    removed_entry = False
    for node in tree.body:
        targets = getattr(node, "targets", []) if isinstance(node, ast.Assign) else []
        assigned_result = any(
            isinstance(t, ast.Name) and t.id == "RESULT_V1510"
            for target in targets for t in (target,) if isinstance(t, ast.Name)
        )
        value = getattr(node, "value", None)
        calls_run = (isinstance(value, ast.Call) and isinstance(value.func, ast.Name)
                     and value.func.id == "run_v1510")
        if assigned_result and calls_run:
            removed_entry = True
            continue
        kept.append(node)
    if not removed_entry:
        raise RuntimeError("v1.5.10 封存程式執行入口與預期不符；停止載入。")
    tree.body = kept
    ast.fix_missing_locations(tree)
    namespace = {"__name__": "sso_v1510_h5_helpers"}
    exec(compile(tree, "SSO_Market_Data_Continuation_TWSE_TPEX_v1.5.10.py", "exec"), namespace)
    required = [
        "ROOT", "TZ", "mount_drive", "parse_date", "market_norm", "safe_num",
        "read_csv_rows", "read_header", "make_mapping", "normalize_csv_rows",
        "canonical_records_sha256", "parse_payload", "validate_records",
        "download_and_validate", "make_session", "load_baseline_and_schema",
        "version_dirs", "write_csv", "verify_csv_roundtrip", "COMPARE_FIELDS",
        "CORE_REQUIRED_FIELDS", "ALIASES", "load_v159_corrections",
        "normalize_value",
    ]
    missing = [k for k in required if k not in namespace]
    if missing:
        raise RuntimeError("v1.5.10 缺少必要函數：" + ", ".join(missing))
    return namespace, BASE_RAW_URL


def fetch_official_calendar(helper: dict, logs: list) -> dict:
    """取得 2026 年 TWSE 官方開休市日曆；API 空回應不視為休市。"""
    session = helper["make_session"]()
    last_error = None
    payload = None
    used_url = None
    used_params = None
    for date_arg in ("20261101", "2026"):
        try:
            params = {"date": date_arg, "response": "json"}
            r = session.get(TWSE_HOLIDAY_URL, params=params, timeout=(20, 60))
            r.raise_for_status()
            p = r.json()
            rows = p.get("data") if isinstance(p, dict) else None
            if not isinstance(rows, list) or not rows:
                raise RuntimeError("官方日曆沒有有效 data 列")
            parsed = []
            closed = set()
            opened = set()
            open_tokens = ("補行交易", "補行開市", "開始交易", "最後交易", "恢復交易")
            closed_tokens = ("市場無交易", "不交易", "休市", "放假", "補假", "停止交易")
            for row in rows:
                if not isinstance(row, (list, tuple)) or len(row) < 2:
                    continue
                day = helper["parse_date"](row[0])
                if day is None or day.year != 2026:
                    continue
                name = str(row[1] or "").strip()
                detail = str(row[2] or "").strip() if len(row) > 2 else ""
                joined = name + " " + detail
                is_open = any(token in joined for token in open_tokens)
                is_closed = any(token in joined for token in closed_tokens)
                if is_open:
                    opened.add(day)
                    closed.discard(day)
                elif is_closed:
                    closed.add(day)
                parsed.append({"date": day.isoformat(), "name": name, "description": detail,
                               "classification": "EXPLICIT_OPEN" if is_open else
                               ("EXPLICIT_CLOSED" if is_closed else "INFO_ONLY")})
            if not parsed:
                raise RuntimeError("官方日曆回應未包含 2026 年列")
            payload = p
            used_url = r.url
            used_params = params
            break
        except Exception as exc:
            last_error = f"{type(exc).__name__}: {exc}"
    if payload is None:
        raise RuntimeError(f"無法取得 2026 年 TWSE 官方開休市日曆：{last_error}")

    checked = []
    for day in FETCH_DATES:
        if day.weekday() >= 5 and day not in opened:
            raise RuntimeError(f"指定行情日期不是一般交易日，且未列為官方補行交易日：{day}")
        if day in closed:
            raise RuntimeError(f"TWSE 官方日曆明列休市，拒絕把該日當交易日：{day}")
        checked.append({"date": day.isoformat(), "weekday": day.strftime("%A"),
                        "calendar_status": "EXPLICIT_OPEN" if day in opened else "WEEKDAY_NOT_CLOSED"})
    result = {"source_url": used_url, "requested_url": TWSE_HOLIDAY_URL,
              "params": used_params, "title": payload.get("title", ""),
              "response_date": payload.get("date"), "parsed_2026_rows": parsed,
              "closed_dates": sorted(x.isoformat() for x in closed),
              "explicit_open_dates": sorted(x.isoformat() for x in opened),
              "fetch_dates_check": checked}
    log(f"[CALENDAR PASS] TWSE 官方日曆已核對 {len(checked)} 個指定日期。", logs)
    return result


def download_legacy_tpex(session, day: date, expected_rows: int | None,
                         out_dir: Path, helper: dict, logs: list) -> tuple[list[dict], dict]:
    """先走既有 v1.5.13 已使用的 TPEx 官方 type=EW 舊端點。"""
    url = "https://www.tpex.org.tw/web/stock/aftertrading/otc_quotes_no1430/stk_wn1430_result.php"
    params = {"l": "zh-tw", "d": f"{day.year - 1911:03d}/{day:%m/%d}", "se": "EW", "o": "json"}
    meta = {"trade_date": day.isoformat(), "market": "TPEX", "query_type": "EW_LEGACY_OFFICIAL",
            "url": url, "params": params, "http_status": None, "response_sha256": "",
            "raw_gzip_sha256": "", "raw_file": "", "table_title": "", "status": "ERROR",
            "validation": {}, "error": ""}
    try:
        response = session.get(url, params=params, timeout=(25, 120))
        meta["http_status"] = response.status_code
        raw = response.content
        meta["response_sha256"] = sha256_bytes(raw)
        raw_dir = out_dir / "raw" / "TPEX"
        raw_dir.mkdir(parents=True, exist_ok=True)
        raw_path = raw_dir / f"{day:%Y%m%d}_legacy.json.gz"
        compressed = gzip.compress(raw, compresslevel=6, mtime=0)
        raw_path.write_bytes(compressed)
        meta["raw_gzip_sha256"] = sha256_bytes(compressed)
        meta["raw_file"] = raw_path.relative_to(out_dir).as_posix()
        if gzip.decompress(compressed) != raw:
            raise RuntimeError("TPEx legacy raw GZIP round-trip 失敗")
        response.raise_for_status()
        payload = response.json()
        records, parse_status, parse_error, title, parse_stats = helper["parse_payload"](payload, "TPEX", day)
        meta["table_title"] = title
        meta["parse_stats"] = parse_stats
        if parse_status == "NO_DATA":
            meta["status"] = "WAITING_FOR_OFFICIAL_DATA"
            meta["error"] = f"{parse_error}；仍須嘗試另一官方端點"
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
        log(f"[TPEX LEGACY PASS] {day} rows={len(records)}; type=EW", logs)
        return records, meta
    except Exception as exc:
        meta["status"] = "ERROR"
        meta["error"] = f"{type(exc).__name__}: {exc}"
        log(f"[TPEX LEGACY ERROR] {day}: {meta['error']}", logs)
        return [], meta


def download_one_market_day(helper: dict, market: str, day: date,
                            expected_rows: int, out_dir: Path,
                            session, logs: list) -> tuple[list[dict], dict]:
    if market == "TWSE":
        return helper["download_and_validate"](session, "TWSE", day, expected_rows, out_dir, logs)

    legacy_records, legacy_meta = download_legacy_tpex(
        session, day, expected_rows, out_dir, helper, logs
    )
    if legacy_meta.get("status") == "PASS":
        return legacy_records, legacy_meta

    modern_records, modern_meta = helper["download_and_validate"](
        session, "TPEX", day, expected_rows, out_dir, logs
    )
    modern_meta["legacy_attempt"] = {
        "status": legacy_meta.get("status"),
        "http_status": legacy_meta.get("http_status"),
        "error": legacy_meta.get("error"),
        "raw_file": legacy_meta.get("raw_file"),
    }
    if modern_meta.get("status") == "PASS":
        log(f"[TPEX MODERN FALLBACK PASS] {day} rows={len(modern_records)}; type=EW", logs)
        return modern_records, modern_meta
    if (legacy_meta.get("status") == "WAITING_FOR_OFFICIAL_DATA" and
            modern_meta.get("status") == "WAITING_FOR_OFFICIAL_DATA"):
        modern_meta["status"] = "WAITING_FOR_OFFICIAL_DATA"
        modern_meta["error"] = "TPEx legacy 與 modern 官方端點均無行情列；不推定休市。"
    else:
        modern_meta["error"] = ("legacy: " + str(legacy_meta.get("error", "")) +
                                 " | modern: " + str(modern_meta.get("error", "")))
    return [], modern_meta


def records_by_code(records: list[dict]) -> dict[str, dict]:
    result = {}
    for row in records:
        code = str(row.get("instrument_code", "")).strip()
        if not code or code in result:
            raise RuntimeError(f"證券代號空白或重複：{code}")
        result[code] = row
    return result


def compare_1111_with_sealed_correction(api_records: list[dict],
                                        corrected_records: list[dict],
                                        market: str) -> dict:
    api_map = records_by_code(api_records)
    corr_map = records_by_code(corrected_records)
    if len(api_map) != len(corr_map):
        raise RuntimeError(f"{market} 2026-11-11 官方 API 與封存修正列數不同：{len(api_map)} != {len(corr_map)}")
    if set(api_map) != set(corr_map):
        missing_api = sorted(set(corr_map) - set(api_map))[:20]
        missing_corr = sorted(set(api_map) - set(corr_map))[:20]
        raise RuntimeError(f"{market} 2026-11-11 代號集合不一致；API缺少={missing_api}；修正檔缺少={missing_corr}")
    numeric_fields = ("open", "high", "low", "close", "volume", "trade_value", "transactions")
    mismatches = []
    price_change_differences = 0
    for code in sorted(api_map):
        a, c = api_map[code], corr_map[code]
        for field in numeric_fields:
            av, cv = a.get(field), c.get(field)
            if av is None and cv is None:
                continue
            if av is None or cv is None:
                mismatches.append({"code": code, "field": field, "api": av, "sealed": cv})
                continue
            if not math.isclose(float(av), float(cv), rel_tol=0, abs_tol=1e-8):
                mismatches.append({"code": code, "field": field, "api": av, "sealed": cv})
        av, cv = a.get("price_change"), c.get("price_change")
        if av is not None and cv is not None and not math.isclose(float(av), float(cv), rel_tol=0, abs_tol=1e-8):
            price_change_differences += 1
    if mismatches:
        raise RuntimeError(f"{market} 2026-11-11 API 與封存修正的 OHLCV/成交資料不一致，前20筆：{mismatches[:20]}")
    return {"status": "PASS", "rows": len(api_map), "natural_key_equal": True,
            "ohlcv_trade_fields_equal": True,
            "price_change_differences": price_change_differences,
            "selected_source": "v1.5.9 sealed correction layer"}


def load_candidates(helper: dict, logs: list) -> tuple[list[dict], str]:
    if not CANDIDATE_CSV.is_file():
        raise FileNotFoundError(f"找不到凍結候選 CSV：{CANDIDATE_CSV}")
    file_sha = sha256_file(CANDIDATE_CSV)
    if file_sha != EXPECTED_CANDIDATE_SHA256:
        raise RuntimeError(f"凍結候選 CSV SHA256 不符：{file_sha}")
    columns, rows = helper["read_csv_rows"](CANDIDATE_CSV)
    if len(rows) != EXPECTED_CANDIDATE_COUNT:
        raise RuntimeError(f"凍結候選列數不符：{len(rows)} != {EXPECTED_CANDIDATE_COUNT}")
    mapping = helper["make_mapping"](columns)
    code_col = mapping.get("instrument_code")
    market_col = mapping.get("market")
    if not code_col:
        raise RuntimeError(f"候選清單找不到股票代號欄位；現有欄位：{columns}")
    candidates = []
    seen = set()
    rank_col = next((c for c in columns if c in ("獲利可能性排名", "候選排序序號", "排名", "rank", "Rank")), None)
    name_col = mapping.get("instrument_name")
    for idx, row in enumerate(rows, 1):
        code = re.sub(r"\s+", "", str(row.get(code_col) or "")).strip()
        if not code:
            raise RuntimeError(f"候選 CSV 第 {idx} 列股票代號空白")
        market = helper["market_norm"](row.get(market_col)) if market_col else ""
        if market and market not in MARKETS:
            raise RuntimeError(f"候選 CSV 市場值未知：row={idx}, value={row.get(market_col)!r}")
        key = (market, code)
        if key in seen:
            raise RuntimeError(f"候選 CSV 出現重複自然鍵：{key}")
        seen.add(key)
        candidates.append({
            "candidate_rank": row.get(rank_col) if rank_col else idx,
            "market": market,
            "instrument_code": code,
            "instrument_name": row.get(name_col, "") if name_col else "",
            "candidate_source_row": idx,
        })
    log(f"[CANDIDATE PASS] rows={len(candidates)}; sha256={file_sha}", logs)
    return candidates, file_sha


def infer_candidate_markets(candidates: list[dict], market_day_rows: list[dict], logs: list) -> list[dict]:
    """若凍結檔沒有市場欄，僅依訊號日官方資料做唯一市場反查；不猜測。"""
    by_code = defaultdict(set)
    for row in market_day_rows:
        if row.get("trade_date") == SIGNAL_DATE.isoformat():
            by_code[str(row.get("instrument_code", "")).strip()].add(row.get("market"))
    unresolved = []
    inferred = 0
    for candidate in candidates:
        if candidate.get("market") in MARKETS:
            continue
        matches = sorted(m for m in by_code.get(candidate["instrument_code"], set()) if m in MARKETS)
        if len(matches) == 1:
            candidate["market"] = matches[0]
            candidate["market_inferred_from_signal_day_official_data"] = True
            inferred += 1
        elif len(matches) == 0:
            unresolved.append({"code": candidate["instrument_code"], "reason": "signal date has no official row"})
        else:
            unresolved.append({"code": candidate["instrument_code"], "reason": f"ambiguous markets: {matches}"})
    log(f"[CANDIDATE MARKET] inferred={inferred}; unresolved={len(unresolved)}", logs)
    return unresolved


def valid_ohlc(row: dict | None) -> bool:
    if not row:
        return False
    vals = [row.get(k) for k in ("open", "high", "low", "close")]
    if any(v is None for v in vals):
        return False
    op, hi, lo, cl = [float(v) for v in vals]
    return (min(op, hi, lo, cl) > 0 and lo <= hi and lo <= op <= hi and lo <= cl <= hi)


def net_return(gross: float, slippage: float) -> float:
    return ((1.0 + gross) * (1.0 - slippage) * (1.0 - FEE_RATE - SELL_TAX_RATE) /
            ((1.0 + slippage) * (1.0 + FEE_RATE))) - 1.0

def normalize_multi_period_csv(path: Path, helper: dict,
                               required_fields: list[str] | None = None) -> list[dict]:
    """多日／跨市場 CSV 回讀驗證；用 (date, market, code) 自然鍵，而非單獨 code。"""
    columns, rows = helper["read_csv_rows"](path)
    mapping = helper["make_mapping"](columns)
    required = list(required_fields or helper["CORE_REQUIRED_FIELDS"])
    missing = [field for field in required if mapping.get(field) is None]
    if missing:
        raise RuntimeError(f"{Path(path).name} 缺少必要欄位：{missing}")

    normalized = []
    seen = set()
    duplicates = []
    for row_number, row in enumerate(rows, 2):
        record = {}
        for field in helper["COMPARE_FIELDS"]:
            col = mapping.get(field)
            value = row.get(col) if col is not None else None
            record[field] = helper["normalize_value"](field, value)

        code = str(record.get("instrument_code") or "").strip()
        market = helper["market_norm"](record.get("market"))
        day = helper["parse_date"](record.get("trade_date"))
        if not code:
            raise RuntimeError(f"{Path(path).name} 第 {row_number} 列證券代號空白")
        if market not in MARKETS:
            raise RuntimeError(f"{Path(path).name} 第 {row_number} 列市場別無效：{market!r}")
        if day is None:
            raise RuntimeError(f"{Path(path).name} 第 {row_number} 列交易日期無效：{record.get('trade_date')!r}")
        record["market"] = market
        record["trade_date"] = day.isoformat()
        key = (record["trade_date"], market, code)
        if key in seen:
            duplicates.append(key)
        else:
            seen.add(key)
        normalized.append(record)

    if duplicates:
        raise RuntimeError(
            f"{Path(path).name} 存在重複自然鍵 (trade_date, market, instrument_code)："
            f"{duplicates[:10]}；重複數={len(duplicates)}"
        )
    if not normalized:
        raise RuntimeError(f"{Path(path).name} 沒有資料列")
    return normalized


def locate_complete_raw_cache(project_root: Path) -> Path | None:
    """尋找先前完整下載的六日×雙市場原始官方回應；只讀，不修改來源。"""
    candidates = []
    for folder in project_root.glob("H5_848官方行情補抓_*"):
        if not folder.is_dir():
            continue
        complete = True
        for day in FETCH_DATES:
            twse = folder / "raw" / "TWSE" / f"{day:%Y%m%d}.json.gz"
            tpex_legacy = folder / "raw" / "TPEX" / f"{day:%Y%m%d}_legacy.json.gz"
            tpex_modern = folder / "raw" / "TPEX" / f"{day:%Y%m%d}.json.gz"
            if not twse.is_file() or not (tpex_legacy.is_file() or tpex_modern.is_file()):
                complete = False
                break
        if complete:
            candidates.append(folder)
    if not candidates:
        return None
    return max(candidates, key=lambda p: (p.stat().st_mtime_ns, p.name))


def load_cached_calendar(cache_dir: Path | None, logs: list[str]) -> dict | None:
    if cache_dir is None:
        return None
    path = cache_dir / "TWSE_2026_official_holiday_calendar.json"
    if not path.is_file():
        return None
    obj = json.loads(path.read_text(encoding="utf-8"))
    checks = {str(x.get("date")) for x in obj.get("fetch_dates_check", [])}
    expected = {d.isoformat() for d in FETCH_DATES}
    if checks != expected:
        raise RuntimeError(f"既有官方日曆快照日期不完整：{sorted(checks)}")
    obj["reused_from_readonly_cache"] = str(path)
    log(f"[CALENDAR CACHE PASS] 重用先前已通過的 TWSE 官方日曆：{path}", logs)
    return obj


def load_cached_market_day(helper: dict, cache_dir: Path, market: str, day: date,
                           expected_rows: int, out_dir: Path, logs: list[str]) -> tuple[list[dict], dict]:
    """由先前成功下載並封存的官方原始 GZIP 重播解析及驗證，不重抓行情。"""
    if market == "TWSE":
        source_path = cache_dir / "raw" / "TWSE" / f"{day:%Y%m%d}.json.gz"
        query_type = "ALLBUT0999"
        source_url = "https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX"
        source_params = {"date": day.strftime("%Y%m%d"), "type": "ALLBUT0999", "response": "json"}
    else:
        legacy = cache_dir / "raw" / "TPEX" / f"{day:%Y%m%d}_legacy.json.gz"
        modern = cache_dir / "raw" / "TPEX" / f"{day:%Y%m%d}.json.gz"
        source_path = legacy if legacy.is_file() else modern
        if legacy.is_file():
            query_type = "EW_LEGACY_OFFICIAL"
            source_url = "https://www.tpex.org.tw/web/stock/aftertrading/otc_quotes_no1430/stk_wn1430_result.php"
            source_params = {"l": "zh-tw", "d": f"{day.year - 1911:03d}/{day:%m/%d}", "se": "EW", "o": "json"}
        else:
            query_type = "EW"
            source_url = "https://www.tpex.org.tw/www/zh-tw/afterTrading/otc"
            source_params = {"date": day.strftime("%Y/%m/%d"), "type": "EW", "response": "json"}
    if not source_path.is_file():
        raise FileNotFoundError(f"官方原始行情快照不存在：{source_path}")

    compressed = source_path.read_bytes()
    raw = gzip.decompress(compressed)
    payload = json.loads(raw.decode("utf-8-sig"))
    records, parse_status, parse_error, title, parse_stats = helper["parse_payload"](payload, market, day)
    if parse_status == "NO_DATA":
        raise RuntimeError(f"快取官方資料為空：{market} {day}：{parse_error}")
    passed, stats = helper["validate_records"](records, market, day, expected_rows)
    stats["parse_stats"] = parse_stats
    invalid_limit = max(10, int(parse_stats.get("table_rows", 0) * 0.05))
    if parse_stats.get("invalid_code_rows", 0) > invalid_limit:
        passed = False
        stats["validation_errors"].append("原始官方表格不合法代號過多")
    if not passed:
        raise RuntimeError(
            f"快取行情重新驗證失敗 {market} {day}："
            + "; ".join(stats.get("validation_errors", []))
        )

    relative = Path("raw") / market / source_path.name
    destination = out_dir / relative
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source_path, destination)
    if sha256_file(destination) != sha256_file(source_path):
        raise RuntimeError(f"原始行情快照複製 SHA 不一致：{source_path.name}")

    meta = {
        "trade_date": day.isoformat(), "market": market,
        "query_type": query_type, "url": source_url, "params": source_params, "status": "PASS",
        "source_mode": "REPLAYED_FROM_PREVIOUS_OFFICIAL_RAW_RESPONSE",
        "cache_source_file": str(source_path),
        "raw_file": relative.as_posix(),
        "raw_gzip_sha256": sha256_bytes(compressed),
        "response_sha256": sha256_bytes(raw),
        "http_status": None,
        "table_title": title,
        "parse_stats": parse_stats,
        "validation": stats,
        "validation_baseline": expected_rows,
        "error": "",
    }
    log(
        f"[RAW CACHE REPLAY PASS] {market} {day}：{len(records):,} 筆；"
        f"natural daily code unique={stats.get('unique_codes')}；GZIP SHA={meta['raw_gzip_sha256'][:16]}",
        logs,
    )
    return records, meta


def build_candidate_price_pairs(candidates: list[dict], market_day_rows: list[dict], logs: list) -> list[dict]:
    lookup = {(r["market"], r["trade_date"], r["instrument_code"]): r for r in market_day_rows}
    pairs = []
    for c in candidates:
        key_signal = (c["market"], SIGNAL_DATE.isoformat(), c["instrument_code"])
        key_entry = (c["market"], ENTRY_DATE.isoformat(), c["instrument_code"])
        key_exit = (c["market"], EXIT_DATE.isoformat(), c["instrument_code"])
        s = lookup.get(key_signal)
        e = lookup.get(key_entry)
        x = lookup.get(key_exit)
        signal_ok = valid_ohlc(s)
        entry_ok = valid_ohlc(e)
        exit_ok = valid_ohlc(x)
        entry_open = e.get("open") if e else None
        exit_close = x.get("close") if x else None
        gross = None
        net_general = None
        net_stress = None
        if (signal_ok and entry_ok and exit_ok and entry_open is not None and exit_close is not None
                and float(entry_open) > 0 and float(exit_close) > 0):
            gross = float(exit_close) / float(entry_open) - 1.0
            net_general = net_return(gross, SLIPPAGE_GENERAL)
            net_stress = net_return(gross, SLIPPAGE_STRESS)
        if not c["market"]:
            status = "MARKET_MISSING"
        elif s is None:
            status = "SIGNAL_DAY_PRICE_MISSING"
        elif e is None:
            status = "ENTRY_DAY_PRICE_MISSING"
        elif x is None:
            status = "EXIT_DAY_PRICE_MISSING"
        elif not (signal_ok and entry_ok and exit_ok):
            status = "OHLC_INVALID_OR_INCOMPLETE"
        else:
            status = "PRICE_PAIR_READY_CORP_ACTION_CHECK_PENDING"
        pairs.append({
            "candidate_rank": c["candidate_rank"],
            "market": c["market"],
            "instrument_code": c["instrument_code"],
            "instrument_name": c["instrument_name"],
            "signal_date": SIGNAL_DATE.isoformat(),
            "signal_open": s.get("open") if s else None,
            "signal_high": s.get("high") if s else None,
            "signal_low": s.get("low") if s else None,
            "signal_close": s.get("close") if s else None,
            "entry_date_t_plus_1": ENTRY_DATE.isoformat(),
            "entry_open_t_plus_1": entry_open,
            "entry_high": e.get("high") if e else None,
            "entry_low": e.get("low") if e else None,
            "entry_close": e.get("close") if e else None,
            "exit_date_t_plus_5": EXIT_DATE.isoformat(),
            "exit_open": x.get("open") if x else None,
            "exit_high": x.get("high") if x else None,
            "exit_low": x.get("low") if x else None,
            "exit_close_t_plus_5": exit_close,
            "gross_return_price_pair": gross,
            "net_return_general_price_pair": net_general,
            "net_return_stress_price_pair": net_stress,
            "signal_ohlc_valid": signal_ok,
            "entry_ohlc_valid": entry_ok,
            "exit_ohlc_valid": exit_ok,
            "signal_entry_mismatch_checked": False,
            "corporate_action_check": "PENDING_FORMAL_V231_ADAPTER",
            "price_pair_status": status,
        })
    ready = sum(r["price_pair_status"] == "PRICE_PAIR_READY_CORP_ACTION_CHECK_PENDING" for r in pairs)
    log(f"[H5 PRICE PAIR] candidates={len(pairs)}; complete price pairs={ready}; not final trade-valid", logs)
    return pairs


def make_manifest(out_dir: Path) -> dict:
    items = []
    for p in sorted(out_dir.rglob("*")):
        if not p.is_file() or p.name in {"artifact_sha256_manifest.json", "GitHub上傳結果.json", "ZIP_SHA256.txt"} or p.suffix.lower() == ".zip":
            continue
        items.append({"path": p.relative_to(out_dir).as_posix(), "bytes": p.stat().st_size,
                      "sha256": sha256_file(p)})
    obj = {"version": VERSION, "file_count": len(items), "self_excluded": True,
           "zip_excluded": True, "files": items}
    write_json(out_dir / "artifact_sha256_manifest.json", obj)
    return obj


def make_zip(out_dir: Path, manifest: dict) -> Path:
    zip_path = out_dir / f"{out_dir.name}.zip"
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
        for p in sorted(out_dir.rglob("*")):
            if not p.is_file() or p == zip_path or p.name in {"GitHub上傳結果.json", "ZIP_SHA256.txt"} or p.suffix.lower() == ".zip":
                continue
            zf.write(p, p.relative_to(out_dir).as_posix())
    with zipfile.ZipFile(zip_path, "r") as zf:
        bad = zf.testzip()
        if bad:
            raise RuntimeError(f"ZIP CRC 檢查失敗：{bad}")
        names = set(zf.namelist())
        for item in manifest["files"]:
            if item["path"] not in names:
                raise RuntimeError(f"ZIP 缺少檔案：{item['path']}")
            if sha256_bytes(zf.read(item["path"])) != item["sha256"]:
                raise RuntimeError(f"ZIP 內 SHA256 不一致：{item['path']}")
    return zip_path


def get_github_token() -> str:
    for key in ("GITHUB_TOKEN", "GITHUB_SSO_TOKEN", "GH_TOKEN"):
        value = os.environ.get(key, "").strip()
        if value:
            return value
    try:
        from google.colab import userdata
        for key in ("GITHUB_TOKEN", "GITHUB_SSO_TOKEN", "GH_TOKEN"):
            value = str(userdata.get(key) or "").strip()
            if value:
                return value
    except Exception:
        pass
    raise RuntimeError("找不到 Colab Secret：GITHUB_TOKEN（或 GITHUB_SSO_TOKEN / GH_TOKEN）。")


def github_audit_upload(out_dir: Path, run_id: str, logs: list[str]) -> dict:
    token = get_github_token()
    work = Path(tempfile.mkdtemp(prefix="sso_h5_github_"))
    askpass = work / "askpass.sh"
    askpass.write_text('#!/bin/sh\ncase "$1" in *Username*) echo x-access-token ;; *) echo "$SSO_GITHUB_TOKEN" ;; esac\n', encoding="utf-8")
    askpass.chmod(0o700)
    env = os.environ.copy()
    env.update({"GIT_ASKPASS": str(askpass), "GIT_TERMINAL_PROMPT": "0", "SSO_GITHUB_TOKEN": token})

    def git(*args, check=True):
        p = subprocess.run(["git", *args], cwd=work, env=env, text=True,
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if check and p.returncode:
            raise RuntimeError(f"git {' '.join(args[:3])} 失敗：{p.stderr[-1600:]}")
        return p.stdout.strip(), p.stderr.strip(), p.returncode

    git("init")
    git("config", "user.name", "SSO Audit Bot")
    git("config", "user.email", "sso-audit@users.noreply.github.com")
    git("remote", "add", "origin", f"https://github.com/{REPO}.git")
    git("fetch", "--depth=1", "origin", "main")
    main_before, _, _ = git("rev-parse", "FETCH_HEAD")
    if main_before != EXPECTED_MAIN_SHA:
        raise RuntimeError(f"GitHub main SHA 已變更：{main_before}；預期 {EXPECTED_MAIN_SHA}，停止上傳。")

    branch = f"audit/sso-h5-official-price-20261106-13-{run_id}"
    git("checkout", "-b", branch, "FETCH_HEAD")
    prefix = f"audit_readout/sso_h5_848_official_prices_{run_id}"
    dest = work / prefix
    dest.mkdir(parents=True, exist_ok=True)
    for src in out_dir.rglob("*"):
        if src.is_file():
            rel = src.relative_to(out_dir)
            target = dest / rel
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, target)
    git("add", prefix)
    git("commit", "-m", f"SSO H5 848 official prices 2026-11-06 to 2026-11-13 {run_id}")
    git("push", "origin", f"HEAD:refs/heads/{branch}")
    commit_sha, _, _ = git("rev-parse", "HEAD")
    git("fetch", "--depth=1", "origin", f"refs/heads/{branch}:refs/remotes/origin/{branch}")
    branch_head, _, _ = git("rev-parse", f"refs/remotes/origin/{branch}")
    if branch_head != commit_sha:
        raise RuntimeError("GitHub audit branch head 回讀 SHA 與本機 commit 不一致。")

    readback_count = 0
    for local in out_dir.rglob("*"):
        if not local.is_file():
            continue
        rel = f"{prefix}/{local.relative_to(out_dir).as_posix()}"
        p = subprocess.run(["git", "show", f"refs/remotes/origin/{branch}:{rel}"], cwd=work, env=env,
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if p.returncode != 0 or sha256_bytes(p.stdout) != sha256_file(local):
            raise RuntimeError(f"GitHub 逐檔回讀 SHA 不一致：{rel}")
        readback_count += 1

    main_after = subprocess.run(["git", "ls-remote", "origin", "refs/heads/main"], cwd=work, env=env,
                                text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True).stdout.split()[0]
    if main_after != main_before or main_after != EXPECTED_MAIN_SHA:
        raise RuntimeError(f"GitHub main 前後 SHA 不一致：{main_before} -> {main_after}")
    result = {"status": "PASS", "repository": REPO, "branch": branch,
              "commit_sha": commit_sha, "branch_head_sha": branch_head,
              "main_before": main_before, "main_after": main_after,
              "main_unchanged": True, "file_readback_count": readback_count,
              "url": f"https://github.com/{REPO}/tree/{branch}/{prefix}"}
    log(f"[GITHUB PASS] branch={branch}; files={readback_count}; main unchanged={main_after}", logs)
    return result


def run() -> dict:
    ensure_drive()
    logs: list[str] = []
    # 優先重用本次失敗前已完整抓取的原始官方行情，不再重複呼叫 12 次日行情 API。
    cache_dir = locate_complete_raw_cache(PROJECT_ROOT)
    run_id = datetime.now().astimezone().strftime("%Y%m%d_%H%M%S") + "_" + uuid.uuid4().hex[:4]
    out_dir = PROJECT_ROOT / f"H5_848官方行情補抓修復_v1.0.2_{run_id}"
    out_dir.mkdir(parents=True, exist_ok=False)
    log(f"{VERSION} 開始；輸出目錄={out_dir}", logs)
    if cache_dir is not None:
        log(f"[RAW CACHE FOUND] 將唯讀重用完整六日×雙市場官方原始資料：{cache_dir}", logs)
    else:
        log("[RAW CACHE NOT FOUND] 將重新向官方端點抓取六日×雙市場行情。", logs)

    report: dict = {
        "version": VERSION,
        "run_id": run_id,
        "status": "STARTED",
        "signal_date": SIGNAL_DATE.isoformat(),
        "entry_date_t_plus_1": ENTRY_DATE.isoformat(),
        "exit_date_t_plus_5": EXIT_DATE.isoformat(),
        "fetch_dates": [d.isoformat() for d in FETCH_DATES],
        "markets": list(MARKETS),
        "candidate_csv": str(CANDIDATE_CSV),
        "candidate_expected_sha256": EXPECTED_CANDIDATE_SHA256,
        "base_script_git_blob_sha": BASE_GIT_BLOB_SHA,
        "api_attempts": [],
        "correction_validation": {},
        "counts": {},
        "downstream_ready": False,
        "return_validity_note": "價格配對初算；尚未完成 v2.31 signal_entry_mismatch／公司行動正式核驗。",
        "raw_cache_source_dir": str(cache_dir) if cache_dir else None,
        "raw_cache_policy": "重用來源資料夾唯讀；完整快取不足時才重新向官方 API 抓取。",
    }

    helper, base_source = load_base_helpers()
    helper["mount_drive"]()
    report["base_script_url"] = base_source
    report["base_script_git_blob_sha_verified"] = True
    log("[BASE HELPERS PASS] 已載入 SHA 固定的 v1.5.10 官方行情函數，未執行舊主流程。", logs)

    candidates, candidate_sha = load_candidates(helper, logs)
    report["candidate_sha256"] = candidate_sha
    report["candidate_count"] = len(candidates)

    # 先載入既有的固定修正層與欄位規格，所有歷史檔案均唯讀。
    dirs = helper["version_dirs"]()
    baseline, baseline_source, standard_columns, overlay_columns = helper["load_baseline_and_schema"](dirs)
    report["row_count_baseline"] = baseline
    report["row_count_baseline_source"] = baseline_source
    corrections = helper["load_v159_corrections"](logs)
    report["v159_correction_source"] = {
        "source_directory": str(corrections["source_directory"]),
        "source_zip_sha256": corrections["source_zip_sha256"],
        "source_manifest_sha256": corrections["source_manifest_sha256"],
        "twse_rows": corrections["twse"]["rows"],
        "twse_file_sha256": corrections["twse"]["file_sha256"],
        "tpex_rows": corrections["tpex"]["rows"],
        "tpex_file_sha256": corrections["tpex"]["file_sha256"],
    }

    calendar = load_cached_calendar(cache_dir, logs)
    if calendar is None:
        calendar = fetch_official_calendar(helper, logs)
    write_json(out_dir / "TWSE_2026_official_holiday_calendar.json", calendar)

    session = helper["make_session"]()
    day_market_results = []
    records_by_market_day: dict[tuple[str, str], list[dict]] = {}
    failures = []
    for i, day in enumerate(FETCH_DATES, 1):
        for market in MARKETS:
            log(f"[FETCH {i}/{len(FETCH_DATES)}] {market} {day}", logs)
            if cache_dir is not None:
                records, meta = load_cached_market_day(
                    helper, cache_dir, market, day, int(baseline[market]), out_dir, logs
                )
            else:
                records, meta = download_one_market_day(
                    helper, market, day, int(baseline[market]), out_dir, session, logs
                )
            meta["validation_baseline"] = baseline[market]
            meta["validation_baseline_source"] = baseline_source[market]
            day_market_results.append(meta)
            if meta.get("status") == "PASS":
                records_by_market_day[(market, day.isoformat())] = records
            else:
                failures.append({"market": market, "trade_date": day.isoformat(),
                                 "status": meta.get("status"), "error": meta.get("error", "")})
            time.sleep(0.20)

    report["api_attempts"] = day_market_results
    report["api_failures"] = failures

    # 11/11：官方 API 必須先成功；然後以已封存 v1.5.9 修正層作為最後輸出。
    correction_checks = {}
    if not any(x["market"] == "TWSE" and x["trade_date"] == "2026-11-11" for x in failures):
        correction_checks["TWSE"] = compare_1111_with_sealed_correction(
            records_by_market_day[("TWSE", "2026-11-11")], corrections["twse"]["records"], "TWSE"
        )
        records_by_market_day[("TWSE", "2026-11-11")] = corrections["twse"]["records"]
    else:
        correction_checks["TWSE"] = {"status": "BLOCKED_API_NOT_PASS"}
    if not any(x["market"] == "TPEX" and x["trade_date"] == "2026-11-11" for x in failures):
        correction_checks["TPEX"] = compare_1111_with_sealed_correction(
            records_by_market_day[("TPEX", "2026-11-11")], corrections["tpex"]["records"], "TPEX"
        )
        records_by_market_day[("TPEX", "2026-11-11")] = corrections["tpex"]["records"]
    else:
        correction_checks["TPEX"] = {"status": "BLOCKED_API_NOT_PASS"}
    report["correction_validation"] = correction_checks
    write_json(out_dir / "20261111_official_correction_comparison.json", correction_checks)

    all_day_market_pass = len(records_by_market_day) == len(FETCH_DATES) * len(MARKETS)
    correction_pass = all(v.get("status") == "PASS" for v in correction_checks.values())

    all_records = []
    daily_summary = []
    for day in FETCH_DATES:
        for market in MARKETS:
            rows = records_by_market_day.get((market, day.isoformat()), [])
            if rows:
                all_records.extend(rows)
            daily_summary.append({"trade_date": day.isoformat(), "market": market,
                                  "rows": len(rows), "status": "PASS" if rows else "MISSING_OR_REJECTED",
                                  "unique_codes": len({r.get("instrument_code") for r in rows})})

    report["daily_summary"] = daily_summary
    report["counts"] = {
        "expected_market_days": len(FETCH_DATES) * len(MARKETS),
        "passed_market_days": len(records_by_market_day),
        "all_market_rows": len(all_records),
        "by_market": {m: sum(len(records_by_market_day.get((m, d.isoformat()), [])) for d in FETCH_DATES) for m in MARKETS},
    }
    # 重要：standard_columns 取自舊版資料庫，欄名可能是中文／舊別名；
    # 必須使用 v1.5.10 的 record_to_target 相容寫入器，不能用本檔只支援
    # 「欄名與 dict key 完全相同」的通用 write_csv，否則會寫出空白行情欄。
    helper["write_csv"](out_dir / "官方六日全市場標準化行情.csv", all_records, standard_columns)
    if all_records:
        roundtrip_path = out_dir / "官方六日全市場標準化行情.csv"
        roundtrip = normalize_multi_period_csv(
            roundtrip_path, helper, required_fields=helper["CORE_REQUIRED_FIELDS"]
        )
        expected_sha = helper["canonical_records_sha256"](all_records)
        actual_sha = helper["canonical_records_sha256"](roundtrip)
        if len(roundtrip) != len(all_records) or expected_sha != actual_sha:
            raise RuntimeError("六日標準化 CSV round-trip 列數或 canonical SHA 不一致。")
        report["standardized_csv_roundtrip"] = {"status": "PASS", "rows": len(roundtrip),
                                                 "canonical_sha256": actual_sha,
                                                 "file_sha256": sha256_file(roundtrip_path)}
    else:
        report["standardized_csv_roundtrip"] = {"status": "BLOCKED_NO_ROWS"}

    # 輸出逐股 overlay，格式沿用 v1.5.x，但放在獨立 H5 封存資料夾，不混入既有 version_dirs。
    grouped = defaultdict(list)
    for row in all_records:
        grouped[(row["market"], row["instrument_code"])].append(row)
    overlay_index = []
    for (market, code), rows in sorted(grouped.items()):
        suffix = "TWSE" if market == "TWSE" else "TPEx"
        path = out_dir / "overlay" / "官方歷史行情" / f"{code}_{suffix}.csv"
        rows = sorted(rows, key=lambda r: r["trade_date"])
        helper["write_csv"](path, rows, overlay_columns[market])
        check = normalize_multi_period_csv(
            path, helper, required_fields=helper["CORE_REQUIRED_FIELDS"]
        )
        if any(r.get("market") != market for r in check):
            raise RuntimeError(f"逐股 overlay 市場別錯誤：{path.name}")
        if helper["canonical_records_sha256"](check) != helper["canonical_records_sha256"](rows):
            raise RuntimeError(f"逐股 overlay round-trip SHA 不一致：{path.name}")
        overlay_index.append({"market": market, "instrument_code": code,
                              "rows": len(rows), "path": path.relative_to(out_dir).as_posix(),
                              "sha256": sha256_file(path)})
    write_csv(out_dir / "逐股官方行情overlay索引.csv", overlay_index,
              ["market", "instrument_code", "rows", "path", "sha256"])

    candidate_market_unresolved = []
    candidate_pairs = []
    price_pair_path = None
    if all_day_market_pass and correction_pass:
        candidate_market_unresolved = infer_candidate_markets(candidates, all_records, logs)
        report["candidate_market_inference_unresolved"] = candidate_market_unresolved
        candidate_pairs = build_candidate_price_pairs(candidates, all_records, logs)
        price_pair_path = out_dir / "848檔H5實際入出場價格配對.csv"
        pair_columns = list(candidate_pairs[0].keys()) if candidate_pairs else []
        write_csv(price_pair_path, candidate_pairs, pair_columns)
        report["candidate_price_pairs"] = {
            "file": price_pair_path.name,
            "rows": len(candidate_pairs),
            "price_pair_ready": sum(r["price_pair_status"] == "PRICE_PAIR_READY_CORP_ACTION_CHECK_PENDING" for r in candidate_pairs),
            "not_ready": sum(r["price_pair_status"] != "PRICE_PAIR_READY_CORP_ACTION_CHECK_PENDING" for r in candidate_pairs),
            "file_sha256": sha256_file(price_pair_path),
        }
    else:
        report["candidate_price_pairs"] = {"status": "NOT_BUILT_INCOMPLETE_OFFICIAL_MARKET_DAYS"}

    # 自我測試：日期序列、成本公式、自然鍵、價格有效性。
    tests = []
    def test(name, fn):
        fn(); tests.append({"test": name, "status": "PASS"}); log(f"[SELF TEST PASS] {name}", logs)
    test("H5日期序列固定", lambda: (_ for _ in ()).throw(AssertionError())
         if (SIGNAL_DATE, ENTRY_DATE, EXIT_DATE) != (date(2026, 11, 6), date(2026, 11, 9), date(2026, 11, 13)) else None)
    test("net_return成本公式有限", lambda: (_ for _ in ()).throw(AssertionError())
         if not math.isfinite(net_return(0.05, SLIPPAGE_GENERAL)) else None)
    test("官方全市場自然鍵唯一", lambda: (_ for _ in ()).throw(AssertionError())
         if len({(r["market"], r["trade_date"], r["instrument_code"]) for r in all_records}) != len(all_records) else None)
    test("候選凍結數量維持848", lambda: (_ for _ in ()).throw(AssertionError()) if len(candidates) != 848 else None)

    def test_multiday_natural_key_roundtrip():
        import tempfile
        with tempfile.TemporaryDirectory(prefix="sso_h5_natural_key_test_") as temp_dir:
            test_path = Path(temp_dir) / "multi_day.csv"
            base = {
                "instrument_code": "2330", "instrument_name": "TEST",
                "open": 100.0, "high": 101.0, "low": 99.0, "close": 100.5,
                "volume": 1000, "trade_value": 100500, "price_change": 0.5,
                "transactions": 100, "source": "UNIT_TEST",
            }
            sample = []
            for market, day in (("TWSE", "2026-11-06"), ("TWSE", "2026-11-09"),
                                ("TPEX", "2026-11-06")):
                item = dict(base); item["market"] = market; item["trade_date"] = day
                sample.append(item)
            helper["write_csv"](test_path, sample, helper["COMPARE_FIELDS"])
            actual = normalize_multi_period_csv(test_path, helper, helper["CORE_REQUIRED_FIELDS"])
            if len(actual) != 3 or helper["canonical_records_sha256"](actual) != helper["canonical_records_sha256"](sample):
                raise AssertionError("不同日期／市場相同代號應被接受且 round-trip 一致")
            sample.append(dict(sample[0]))
            helper["write_csv"](test_path, sample, helper["COMPARE_FIELDS"])
            try:
                normalize_multi_period_csv(test_path, helper, helper["CORE_REQUIRED_FIELDS"])
            except RuntimeError as exc:
                if "重複自然鍵" not in str(exc):
                    raise
            else:
                raise AssertionError("相同日期＋市場＋代號的重複自然鍵未被阻擋")

    test("多日期／雙市場自然鍵 round-trip", test_multiday_natural_key_roundtrip)

    def test_legacy_header_alias_writer_roundtrip():
        """確認官方標準化寫檔會把 canonical keys 正確映射回歷史資料庫欄名。"""
        import tempfile
        with tempfile.TemporaryDirectory(prefix="sso_h5_legacy_header_test_") as temp_dir:
            test_path = Path(temp_dir) / "legacy_headers.csv"
            legacy_columns = [
                "交易日期", "市場", "證券代號", "證券名稱", "開盤價", "最高價",
                "最低價", "收盤價", "成交股數", "成交金額", "漲跌價差", "成交筆數", "資料來源"
            ]
            sample = [{
                "trade_date": "2026-11-09", "market": "TWSE",
                "instrument_code": "2330", "instrument_name": "台積電",
                "open": 1400.0, "high": 1420.0, "low": 1390.0, "close": 1410.0,
                "volume": 1000000, "trade_value": 1410000000,
                "price_change": 10.0, "transactions": 12345, "source": "UNIT_TEST"
            }]
            # 使用 v1.5.10 writer（含 record_to_target alias mapping）。
            helper["write_csv"](test_path, sample, legacy_columns)
            actual = normalize_multi_period_csv(
                test_path, helper, required_fields=helper["CORE_REQUIRED_FIELDS"]
            )
            expected_sha = helper["canonical_records_sha256"](sample)
            actual_sha = helper["canonical_records_sha256"](actual)
            if len(actual) != 1 or expected_sha != actual_sha:
                raise AssertionError(
                    "歷史中文欄名 writer round-trip 不一致；"
                    f"rows={len(actual)} expected_sha={expected_sha} actual_sha={actual_sha}"
                )

    test("歷史欄名 alias writer round-trip", test_legacy_header_alias_writer_roundtrip)
    report["self_tests"] = tests

    price_pairs_ready = (len(candidate_pairs) == EXPECTED_CANDIDATE_COUNT and
                         all(r["price_pair_status"] == "PRICE_PAIR_READY_CORP_ACTION_CHECK_PENDING" for r in candidate_pairs))
    downstream_ready = bool(all_day_market_pass and correction_pass and price_pairs_ready and
                             not candidate_market_unresolved and len(tests) == 6)
    report["status"] = "PASS_PRICE_CAPTURE_NOT_FINAL_TRADE_AUDIT" if downstream_ready else "PARTIAL_BLOCKED"
    report["downstream_ready"] = downstream_ready
    report["integration_policy"] = {
        "old_drive_files_modified": False,
        "old_drive_files_deleted": False,
        "old_increment_files_overwritten": False,
        "full_six_day_snapshot_isolated": True,
        "raw_cache_reused_read_only": cache_dir is not None,
        "multi_date_validation_uses_natural_key": True,
        "standardized_csv_writer_uses_v1510_legacy_alias_mapping": True,
        "existing_marketdata_version_dirs_polluted": False,
        "2026_11_11_correction_layer_checked_and_applied": correction_pass,
        "official_calendar_checked": True,
        "final_model_entry_trade_valid_claimed": False,
        "reason_final_trade_valid_not_claimed": "尚未重現 v2.31 的 signal_entry_mismatch／公司行動 adapter；本檔只提供官方原始價與 H5 入出場配對價。",
    }
    report["data_sources"] = {
        "TWSE": "https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX?type=ALLBUT0999",
        "TPEX_modern": "https://www.tpex.org.tw/www/zh-tw/afterTrading/otc?type=EW",
        "TPEX_legacy": "https://www.tpex.org.tw/web/stock/aftertrading/otc_quotes_no1430/stk_wn1430_result.php?se=EW",
        "TWSE_holiday_calendar": TWSE_HOLIDAY_URL,
    }
    write_json(out_dir / "H5_848官方行情補抓報告.json", report)
    (out_dir / "README.md").write_text(
        f"# {VERSION}\n\n"
        f"狀態：{report['status']}\n\n"
        f"訊號日：{SIGNAL_DATE}；入場：{ENTRY_DATE} 開盤；出場：{EXIT_DATE} 收盤。\n\n"
        f"候選：{len(candidates)} 檔；官方市場日：{len(records_by_market_day)}/{len(FETCH_DATES)*2}；行情列：{len(all_records):,}。\n\n"
        "本資料夾是獨立 H5 行情快照，不會自動併入既有市場資料版本目錄，避免重複自然鍵。\n\n"
        "848 檔配對表提供價格層報酬初算；正式交易有效性仍須由 v2.31 公司行動／signal_entry_mismatch adapter 檢核。\n"
        "多日期檔案以 (trade_date, market, instrument_code) 自然鍵驗證，不以跨日期重複代號誤判。\n"
        "全市場標準化檔沿用 v1.5.10 record_to_target 寫入器，確保歷史中文欄名不會寫成空值。\n"
        f"原始官方回應來源：{cache_dir if cache_dir else '本次官方 API 抓取'}。\n",
        encoding="utf-8"
    )

    # 保存本次實際執行的程式碼。
    try:
        current = Path(__file__)
        if current.is_file():
            shutil.copy2(current, out_dir / "1.py")
    except Exception:
        pass

    (out_dir / "run.log").write_text("\n".join(logs) + "\n", encoding="utf-8")
    # ZIP 不在內部報告寫入自己的 SHA（避免循環雜湊）；精確 SHA 存於 ZIP 外側 sidecar。
    report["archive"] = {
        "zip": f"{out_dir.name}.zip",
        "zip_sha256_sidecar": "ZIP_SHA256.txt",
        "zip_hash_is_external_to_archive": True,
    }
    write_json(out_dir / "H5_848官方行情補抓報告.json", report)
    manifest = make_manifest(out_dir)
    zip_path = make_zip(out_dir, manifest)
    zip_sha = sha256_file(zip_path)
    (out_dir / "ZIP_SHA256.txt").write_text(
        f"{zip_sha}  {zip_path.name}\n", encoding="utf-8"
    )

    github_result = {"status": "NOT_RUN"}
    try:
        github_result = github_audit_upload(out_dir, run_id, logs)
    except Exception as exc:
        github_result = {"status": "FAILED", "error": f"{type(exc).__name__}: {exc}"}
        log(f"[GITHUB WARNING] {github_result['error']}", logs)
    write_json(out_dir / "GitHub上傳結果.json", github_result)
    (out_dir / "run.log").write_text("\n".join(logs) + "\n", encoding="utf-8")

    print("\n" + "=" * 88)
    print(f"{VERSION}：{report['status']}")
    print(f"候選：{len(candidates):,}")
    print(f"官方市場日：{len(records_by_market_day)}/{len(FETCH_DATES)*2}")
    print(f"行情列：{len(all_records):,}")
    if report.get("candidate_price_pairs", {}).get("rows"):
        print(f"848 檔價格配對：{report['candidate_price_pairs']['price_pair_ready']}/{EXPECTED_CANDIDATE_COUNT}")
        print("注意：公司行動／signal_entry_mismatch 尚未由正式 adapter 核驗。")
    print(f"Drive 輸出：{out_dir}")
    print(f"ZIP：{zip_path}  SHA256={sha256_file(zip_path)}")
    print(f"GitHub：{github_result}")
    print("=" * 88)
    return report


if __name__ == "__main__":
    RESULT = run()
