#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SSO F04 H0 Forward Shadow Validation v2.0

Frozen protocol
---------------
1) F04 = -expm1(sum(last 3 overnight log returns)); direction HIGH.
2) Rank the existing fixed SSO research universe by F04 descending; take Top 10.
3) OOS starts 2026-11-24. No backfill before OOS_START_DATE.
4) Two predeclared entry arms: 09:01 and 09:05; first 1m bar at/after target,
   with a maximum 5-minute delay; entry price proxy is that bar OPEN.
5) H0 exits: primary LAST_CONTINUOUS_BAR_CLOSE_1320_1324 (latest available
   bar start 13:20..13:24, use CLOSE); secondary AUCTION_1330_CLOSE.
6) Costs use the frozen v1 pilot assumptions: 0.1425% one-way commission,
   0.3% sell tax, and 10/20/30/50bp slippage on each side.
7) Missing entry/exit prices stay missing. A fixed 10-stock daily portfolio
   return is only calculated when all 10 stocks have valid prices.

Two-stage prospective protocol:
A) Run from 08:55 Taipei. The program preloads pinned history/state, waits until
   09:00:05, takes TSE/OTC COMMONSTOCK Snapshot Quotes, computes F04, and freezes
   Top 10 before the 09:01 entry observation.
B) Run again after 16:35 Taipei. It restores the frozen list and retrieves
   historical 1m candles only for those 10 symbols; it never recomputes ranks.
The program persists cumulative state in an append-only GitHub audit branch,
never writes Drive, and never pushes main.
"""
from __future__ import annotations

import argparse
import base64
import gzip
import hashlib
import json
import tempfile
import math
import os
import re
import shutil
import subprocess
import sys
import time
import traceback
import zipfile
from datetime import datetime, date, time as dt_time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import requests

VERSION = "SSO_F04_H0_FORWARD_SHADOW_VALIDATION_v2.0.1"
TZ = "Asia/Taipei"
OOS_START_DATE = "2026-11-24"
TOP_N = 10
ENTRY_CLOCKS = ("09:01", "09:05")
MAX_ENTRY_DELAY_MIN = 5.0
LAST_CONTINUOUS_START = "13:20"
LAST_CONTINUOUS_END = "13:24"
AUCTION_CLOCK = "13:30"
FEE_ONE_WAY = 0.001425
SELL_TAX = 0.003
SLIPPAGE_LEVELS = (0.001, 0.002, 0.003, 0.005)
MIN_DAILY_UNIVERSE_COVERAGE = 0.95
MIN_HISTORY_DAILY_COVERAGE = 0.90
MIN_F04_ELIGIBLE_COVERAGE = 0.90
MARKET_CALENDAR_VALID_UNTIL = "2027-01-31"
# Official TWSE closure dates still ahead at this protocol's start; refreshed against the TWSE calendar.
UPCOMING_MARKET_CLOSED_DATES = {"2026-12-25", "2027-01-01"}
FUGLE_BASE = "https://api.fugle.tw/marketdata/v1.0/stock/historical/candles"
FUGLE_SNAPSHOT_BASE = "https://api.fugle.tw/marketdata/v1.0/stock/snapshot/quotes"
FUGLE_DOC = "https://developer.fugle.tw/docs/data/http-api/historical/candles/"
MIN_REQUEST_INTERVAL = 1.05
MAX_RETRIES = 4
REQUEST_TIMEOUT = 45
REQUIRED_MAIN_SHA = "aaabc2433fb5df2bb68af06461178305d6bd30d1"
REPO = "CHLINX/sso"
GITHUB_REMOTE = "https://github.com/CHLINX/sso.git"
BRANCH_PREFIX = "audit/sso-fugle-f04-h0-shadow-v2_"
BASE_FILE_NAME = "相對價格因子與前瞻報酬研究資料_v2.31.csv.gz"
BASE_PATH = (
    "/content/drive/MyDrive/看股最速適/分析重跑_v1.2_20261026_155023/"
    "SSO_研究指標獨立驗證與前瞻分析_v2.31_20261116_121536/"
    + BASE_FILE_NAME
)
BASE_SHA256 = "113a4f7cd5c94d7bbd3cb78427963dde2a6c249d66a25a66fab2c408958de71c"
BASE_MAX_DATE = "2026-11-06"

BASE_USECOLS = ["日期", "市場", "股票代號", "影子開盤價", "影子最高價", "影子最低價", "影子收盤價", "價格研究區段"]
PRICE_COLS = ["影子開盤價", "影子最高價", "影子最低價", "影子收盤價"]
STATE_DIR = Path("state")
STATE_SELECTIONS = STATE_DIR / "01_凍結選股歷史.csv"
STATE_TRADES = STATE_DIR / "02_逐筆影子交易歷史.csv"
STATE_DAILY = STATE_DIR / "03_逐日績效歷史.csv"
STATE_SOURCES = STATE_DIR / "04_來源檔案雜湊歷史.csv"
REPORT_ROOT = Path(os.environ.get("SSO_REPORT_ROOT", "/content"))

# Stable CSV schemas: even a zero-observation run must persist header-only state files.
SELECTION_STATE_COLUMNS = [
    "signal_date", "signal", "rank", "market", "symbol", "factor_score",
    "factor_direction", "factor_definition", "snapshot_open_price", "snapshot_time",
    "snapshot_coverage", "frozen_at_taipei",
]
TRADE_STATE_COLUMNS = [
    "signal_date", "signal", "rank", "market", "symbol", "factor_score",
    "entry_clock", "max_entry_delay_min", "entry_bar_datetime", "entry_price_proxy",
    "entry_delay_minutes", "exit_method", "exit_bar_datetime", "exit_price_proxy",
    "exit_gap_to_1325_min", "status", "gross_return", "observed_at_taipei",
    "attempt", "price_source", "fill_status",
    "net_10bp", "net_20bp", "net_30bp", "net_50bp",
]
DAILY_STATE_COLUMNS = [
    "signal_date", "entry_clock", "exit_method", "attempt", "observed_at_taipei",
    "selected_count", "valid_count", "coverage", "complete_10_of_10",
    "valid_only_mean_gross", "valid_only_win_rate_gross",
    "valid_only_mean_net_10bp", "valid_only_win_rate_net_10bp", "complete_basket_net_10bp",
    "valid_only_mean_net_20bp", "valid_only_win_rate_net_20bp", "complete_basket_net_20bp",
    "valid_only_mean_net_30bp", "valid_only_win_rate_net_30bp", "complete_basket_net_30bp",
    "valid_only_mean_net_50bp", "valid_only_win_rate_net_50bp", "complete_basket_net_50bp",
]
SOURCE_STATE_COLUMNS = ["file", "sha256", "bytes"]
STATE_RESTORE_DIAGNOSTICS: List[Dict[str, Any]] = []


def log(msg: str) -> None:
    stamp = datetime.now(ZoneInfo(TZ)).strftime("%Y-%m-%d %H:%M:%S %Z")
    print(f"[{stamp}] {msg}", flush=True)


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(8 * 1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def write_json(obj: Any, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2, default=str), encoding="utf-8")


def write_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8-sig", lineterminator="\n")


def get_secret(name: str) -> Optional[str]:
    v = os.environ.get(name)
    if v:
        return v.strip()
    try:
        from google.colab import userdata  # type: ignore
        v = userdata.get(name)
        if v:
            return str(v).strip()
    except Exception:
        pass
    return None


def mount_drive_if_needed() -> None:
    root = Path("/content/drive/MyDrive/看股最速適")
    if root.exists():
        return
    try:
        from google.colab import drive  # type: ignore
        log("[DRIVE] 研究資料根目錄尚未掛載，嘗試掛載 Google Drive")
        drive.mount("/content/drive", force_remount=False)
    except Exception as e:
        log(f"[DRIVE] 無法自動掛載：{type(e).__name__}: {e}")


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=VERSION)
    p.add_argument("--base-data", default=BASE_PATH)
    p.add_argument("--increment-dir", default=None,
                   help="可選：官方全市場標準化增量 CSV 目錄；未提供時自動搜尋安全的 SSO 目錄")
    p.add_argument("--signal-date", default=None,
                   help="可選：明確指定當日訊號日期 YYYY-MM-DD；仍不得早於 OOS 起始日")
    p.add_argument("--phase", choices=["auto", "freeze", "close", "bootstrap"], default="auto",
                   help="auto：09:00 開盤凍結；16:35 後收盤核算；bootstrap：建立追蹤分支")
    p.add_argument("--retry-existing", action="store_true",
                   help="僅收盤核算階段可用；重抓已凍結名單的分鐘 K，追加新 attempt，不重選股")
    p.add_argument("--self-test", action="store_true")
    p.add_argument("--no-github-upload", action="store_true",
                   help="僅供自我測試；正式前瞻觀察請勿使用")
    args, _unknown = p.parse_known_args(argv)  # ignore Colab's -f kernel.json
    if args.no_github_upload and not args.self_test:
        p.error("正式前瞻執行必須上傳 GitHub audit 並驗證讀回；--no-github-upload 僅供 --self-test")
    if args.signal_date:
        try:
            d = pd.Timestamp(args.signal_date).date()
        except Exception:
            p.error("--signal-date 格式必須為 YYYY-MM-DD")
        if d < pd.Timestamp(OOS_START_DATE).date():
            p.error(f"--signal-date 不得早於凍結 OOS 起始日 {OOS_START_DATE}")
    return args


def normalize_market(value: Any) -> Optional[str]:
    s = str(value).strip().upper().replace(" ", "")
    if s in {"TWSE", "TSE", "上市", "上市市場", "1", "1.0", "T"}:
        return "TWSE"
    if s in {"TPEX", "TPEx".upper(), "OTC", "上櫃", "上櫃市場", "2", "2.0", "O"}:
        return "TPEX"
    return None


def normalize_code(value: Any) -> str:
    s = str(value).strip().upper()
    s = re.sub(r"\.0$", "", s)
    s = re.sub(r"\.(TW|TWO)$", "", s)
    return s


def parse_date_series(s: pd.Series) -> pd.Series:
    x = s.astype("string").str.strip()
    out = pd.Series(pd.NaT, index=s.index, dtype="datetime64[ns]")
    ymd = x.str.fullmatch(r"\d{8}", na=False)
    if ymd.any():
        out.loc[ymd] = pd.to_datetime(x.loc[ymd], format="%Y%m%d", errors="coerce")
    other = ~ymd
    if other.any():
        out.loc[other] = pd.to_datetime(x.loc[other], errors="coerce")
    return out.dt.normalize()


def normalize_numeric(s: pd.Series) -> pd.Series:
    x = s.astype("string").str.replace(",", "", regex=False).str.strip()
    x = x.replace({"": pd.NA, "-": pd.NA, "--": pd.NA, "N/A": pd.NA, "nan": pd.NA, "None": pd.NA})
    return pd.to_numeric(x, errors="coerce")


def first_matching_column(columns: Sequence[str], aliases: Sequence[str]) -> Optional[str]:
    lookup = {re.sub(r"[\s_\-]", "", str(c)).lower(): str(c) for c in columns}
    for a in aliases:
        k = re.sub(r"[\s_\-]", "", a).lower()
        if k in lookup:
            return lookup[k]
    return None


def locate_base_file(requested: str) -> Path:
    p = Path(requested)
    if p.is_file():
        return p
    safe_root = Path("/content/drive/MyDrive/看股最速適")
    if safe_root.exists():
        hits = list(safe_root.rglob(BASE_FILE_NAME))
        if len(hits) == 1:
            return hits[0]
        if len(hits) > 1:
            exact = [x for x in hits if str(x) == BASE_PATH]
            if len(exact) == 1:
                return exact[0]
            raise FileNotFoundError(f"找到多個 v2.31 基礎檔，無法安全選擇：{[str(x) for x in hits]}")
    raise FileNotFoundError(f"找不到固定 v2.31 基礎檔：{requested}")


def load_base(path: Path) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    actual = sha256_file(path)
    if actual != BASE_SHA256:
        raise ValueError(f"v2.31 基礎檔 SHA256 不符，停止：actual={actual}; expected={BASE_SHA256}")
    df = pd.read_csv(path, compression="infer", usecols=BASE_USECOLS,
                     dtype={"市場": "string", "股票代號": "string", "價格研究區段": "string"},
                     low_memory=False)
    df["日期"] = pd.to_datetime(df["日期"], errors="coerce").dt.normalize()
    for c in PRICE_COLS:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["市場"] = df["市場"].map(normalize_market)
    df["股票代號"] = df["股票代號"].map(normalize_code)
    df = df.dropna(subset=["日期", "市場", "股票代號", "影子開盤價", "影子收盤價"]).copy()
    df = df[(df["股票代號"] != "") & (df["影子開盤價"] > 0) & (df["影子收盤價"] > 0)].copy()
    if df.duplicated(["市場", "股票代號", "日期"]).any():
        raise ValueError("v2.31 基礎資料存在重複 (市場,股票代號,日期)，停止")
    df = df.sort_values(["市場", "股票代號", "日期"], kind="mergesort").reset_index(drop=True)
    max_date = str(df["日期"].max().date())
    if max_date != BASE_MAX_DATE:
        raise ValueError(f"v2.31 基礎資料末日異常：{max_date}; expected={BASE_MAX_DATE}")
    meta = {
        "path": str(path), "sha256": actual, "rows": int(len(df)),
        "universe_securities": int(df[["市場", "股票代號"]].drop_duplicates().shape[0]),
        "date_min": str(df["日期"].min().date()), "date_max": max_date,
        "schema": BASE_USECOLS,
    }
    return df, meta


def safe_increment_roots(custom: Optional[str]) -> List[Path]:
    if custom:
        p = Path(custom)
        if p.exists() and p.is_dir():
            return [p]
        raise FileNotFoundError(f"指定增量目錄不存在：{custom}")
    # Only search the dedicated SSO market-data continuation folder.
    # Never recursively search the whole Drive research tree.
    candidates = [
        Path("/content/SSO_Market_Data_Continuation_TWSE_TPEX_v1"),
        Path("/content/drive/MyDrive/看股最速適/SSO_Market_Data_Continuation_TWSE_TPEX_v1"),
    ]
    return [p for p in candidates if p.exists() and p.is_dir()]


def discover_increment_files(custom: Optional[str]) -> List[Path]:
    found: Dict[str, Path] = {}
    for root in safe_increment_roots(custom):
        for p in root.rglob("全市場標準化增量_*.csv*"):
            # The only Drive scan root is 看股最速適; 巡田最速適 is never searched.
            if p.is_file() and (p.suffix.lower() == ".csv" or p.name.lower().endswith(".csv.gz")):
                found[str(p.resolve())] = p
    return sorted(found.values(), key=lambda p: p.name)


def read_increment_file(path: Path, base_universe: set,
                        symbol_markets: Dict[str, set], segment_map: Dict[Tuple[str, str], str]) -> pd.DataFrame:
    raw = pd.read_csv(path, compression="infer", encoding="utf-8-sig", dtype="string", low_memory=False)
    date_col = first_matching_column(raw.columns, ["trade_date", "date", "交易日期", "日期", "資料日期"])
    code_col = first_matching_column(raw.columns, ["instrument_code", "code", "symbol", "stock_code", "股票代號", "證券代號", "代號"])
    market_col = first_matching_column(raw.columns, ["market", "market_code", "exchange", "市場", "市場別", "交易市場"])
    open_col = first_matching_column(raw.columns, ["open", "open_price", "opening_price", "開盤價", "開盤"])
    high_col = first_matching_column(raw.columns, ["high", "high_price", "最高價", "最高"])
    low_col = first_matching_column(raw.columns, ["low", "low_price", "最低價", "最低"])
    close_col = first_matching_column(raw.columns, ["close", "close_price", "closing_price", "收盤價", "收盤"])
    required = {"date": date_col, "code": code_col, "open": open_col,
                "high": high_col, "low": low_col, "close": close_col}
    missing = [k for k, v in required.items() if v is None]
    if missing:
        raise ValueError(f"增量檔欄位不足：{path.name}; missing={missing}; columns={list(raw.columns)}")

    out = pd.DataFrame(index=raw.index)
    out["日期"] = parse_date_series(raw[date_col])
    out["股票代號"] = raw[code_col].map(normalize_code)
    if market_col:
        out["市場"] = raw[market_col].map(normalize_market)
    else:
        inferred = []
        for code in out["股票代號"].tolist():
            markets = symbol_markets.get(code, set())
            inferred.append(next(iter(markets)) if len(markets) == 1 else None)
        out["市場"] = inferred
    for src, dst in [(open_col, "影子開盤價"), (high_col, "影子最高價"),
                     (low_col, "影子最低價"), (close_col, "影子收盤價")]:
        out[dst] = normalize_numeric(raw[src])
    out["價格研究區段"] = [segment_map.get((m, s), "FROZEN_V231_UNIVERSE")
                              for m, s in zip(out["市場"], out["股票代號"])]
    out = out.dropna(subset=["日期", "市場", "股票代號"]).copy()
    # Vectorized frozen-universe membership check; never introduce securities
    # outside the v2.31 baseline universe during prospective observation.
    universe_index = pd.MultiIndex.from_tuples(sorted(base_universe), names=["市場", "股票代號"])
    row_index = pd.MultiIndex.from_arrays([out["市場"], out["股票代號"]], names=["市場", "股票代號"])
    out = out.loc[row_index.isin(universe_index)].copy()
    out = out[out["日期"] > pd.Timestamp(BASE_MAX_DATE)].copy()
    # Only fully valid positive OHLC rows can participate in the frozen overnight factor.
    px = out[PRICE_COLS]
    valid = px.notna().all(axis=1) & (px > 0).all(axis=1)
    valid &= out["影子最高價"] >= out[["影子開盤價", "影子收盤價", "影子最低價"]].max(axis=1)
    valid &= out["影子最低價"] <= out[["影子開盤價", "影子收盤價", "影子最高價"]].min(axis=1)
    return out.loc[valid, BASE_USECOLS].copy()


def load_increment_data(base: pd.DataFrame, files: List[Path]) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, Any]]:
    if not files:
        return pd.DataFrame(columns=BASE_USECOLS), pd.DataFrame(columns=["file", "sha256", "bytes"]), {
            "files": 0, "rows": 0, "latest_date": None, "matching_rows": 0,
        }
    base_universe = set(zip(base["市場"], base["股票代號"]))
    symbol_markets: Dict[str, set] = {}
    segment_map: Dict[Tuple[str, str], str] = {}
    for (m, s), g in base.groupby(["市場", "股票代號"], sort=False):
        symbol_markets.setdefault(str(s), set()).add(str(m))
        seg = g["價格研究區段"].dropna()
        segment_map[(str(m), str(s))] = str(seg.iloc[-1]) if len(seg) else "FROZEN_V231_UNIVERSE"
    pieces = []
    manifests = []
    for p in files:
        h = sha256_file(p)
        manifests.append({"file": str(p), "sha256": h, "bytes": int(p.stat().st_size)})
        z = read_increment_file(p, base_universe, symbol_markets, segment_map)
        if not z.empty:
            pieces.append(z)
    if not pieces:
        return pd.DataFrame(columns=BASE_USECOLS), pd.DataFrame(manifests), {
            "files": len(files), "rows": 0, "latest_date": None, "matching_rows": 0,
        }
    x = pd.concat(pieces, ignore_index=True)
    keys = ["市場", "股票代號", "日期"]
    # Duplicates across overlapping increment files are allowed only when OHLC are identical.
    dup = x.duplicated(keys, keep=False)
    if dup.any():
        check = x.loc[dup].groupby(keys, dropna=False)[PRICE_COLS].nunique(dropna=False)
        conflict = check.gt(1).any(axis=1)
        if conflict.any():
            sample = list(conflict[conflict].index[:10])
            raise ValueError(f"增量檔同一 natural key 出現不同價格，停止：{sample}")
        x = x.drop_duplicates(keys, keep="last")
    x = x.sort_values(["市場", "股票代號", "日期"], kind="mergesort").reset_index(drop=True)
    meta = {"files": len(files), "rows": int(len(x)),
            "latest_date": str(x["日期"].max().date()) if not x.empty else None,
            "matching_rows": int(len(x)),
            "dates": sorted(str(d.date()) for d in x["日期"].drop_duplicates())}
    return x, pd.DataFrame(manifests), meta


def previous_market_sessions(signal_date: str, count: int = 3) -> List[str]:
    target = pd.Timestamp(signal_date).date()
    if target > pd.Timestamp(MARKET_CALENDAR_VALID_UNTIL).date():
        raise ValueError(f"市場日曆只驗證至 {MARKET_CALENDAR_VALID_UNTIL}；請更新 TWSE 官方開休市日後再繼續")
    closed = {pd.Timestamp(x).date() for x in UPCOMING_MARKET_CLOSED_DATES}
    found = []
    d = target - pd.Timedelta(days=1)
    while len(found) < count:
        if d.weekday() < 5 and d not in closed:
            found.append(d.strftime("%Y-%m-%d"))
        d -= pd.Timedelta(days=1)
    return found


def validate_history_sessions(history: pd.DataFrame, universe_size: int, signal_date: str) -> Dict[str, Any]:
    expected = previous_market_sessions(signal_date, 3)
    rows = []
    for d in expected:
        day = history[history["日期"] == pd.Timestamp(d)] if not history.empty else pd.DataFrame()
        n = int(day[["市場", "股票代號"]].drop_duplicates().shape[0]) if not day.empty else 0
        coverage = float(n / universe_size) if universe_size else 0.0
        rows.append({"date": d, "valid_ohlc_securities": n, "coverage": coverage,
                     "minimum_required_coverage": MIN_HISTORY_DAILY_COVERAGE,
                     "pass": bool(coverage >= MIN_HISTORY_DAILY_COVERAGE)})
    latest = str(history["日期"].max().date()) if not history.empty else None
    expected_latest = expected[0] if expected else None
    latest_match = latest == expected_latest
    return {
        "signal_date": signal_date,
        "expected_previous_market_sessions": expected,
        "history_latest_date": latest,
        "latest_date_matches_previous_session": bool(latest_match),
        "minimum_history_session_coverage": MIN_HISTORY_DAILY_COVERAGE,
        "session_checks": rows,
        "pass": bool(latest_match and all(x["pass"] for x in rows)),
        "calendar_valid_until": MARKET_CALENDAR_VALID_UNTIL,
        "calendar_source": "TWSE official market holiday schedule (2026-2027)",
    }


def daily_universe_coverage(increments: pd.DataFrame, signal_date: str,
                            universe_size: int) -> Dict[str, Any]:
    if increments.empty or universe_size <= 0:
        observed = 0
    else:
        day = increments[increments["日期"] == pd.Timestamp(signal_date)]
        observed = int(day[["市場", "股票代號"]].drop_duplicates().shape[0])
    coverage = float(observed / universe_size) if universe_size else 0.0
    return {
        "signal_date": signal_date,
        "frozen_universe_size": int(universe_size),
        "valid_ohlc_securities": observed,
        "coverage": coverage,
        "minimum_required_coverage": MIN_DAILY_UNIVERSE_COVERAGE,
        "pass": bool(coverage >= MIN_DAILY_UNIVERSE_COVERAGE),
        "note": "Coverage is calculated on valid positive OHLC rows intersected with the pinned v2.31 universe; below-threshold days are blocked from ranking.",
    }


def compute_f04(all_daily: pd.DataFrame) -> pd.DataFrame:
    x = all_daily.sort_values(["市場", "股票代號", "日期"], kind="mergesort").copy()
    keys = ["市場", "股票代號"]
    g = x.groupby(keys, sort=False, observed=True)
    x["prev_close"] = g["影子收盤價"].shift(1)
    valid = (x["影子開盤價"] > 0) & (x["prev_close"] > 0)
    x["overnight_log"] = np.where(valid, np.log(x["影子開盤價"] / x["prev_close"]), np.nan)
    # EXACTLY reproduce 6.py F04: negative 3-row accumulated overnight log-return.
    x["F04"] = -x.groupby(keys, sort=False, observed=True)["overnight_log"].transform(
        lambda s: np.expm1(s.rolling(3, min_periods=3).sum())
    )
    x["F04"] = x["F04"].replace([np.inf, -np.inf], np.nan)
    return x


def score_opening_snapshot(history: pd.DataFrame, snapshot_quotes: pd.DataFrame,
                           signal_date: str) -> pd.DataFrame:
    """Compute today's frozen F04 using only pre-entry opening snapshots + prior daily history."""
    if snapshot_quotes.empty:
        raise ValueError("開盤快照為空，不能形成當日 F04")
    current = snapshot_quotes.dropna(subset=["snapshot_open_price"]).copy()
    current = current[current["snapshot_open_price"] > 0].copy()
    current = current.rename(columns={"市場": "市場", "股票代號": "股票代號"})
    current["日期"] = pd.Timestamp(signal_date)
    current["影子開盤價"] = pd.to_numeric(current["snapshot_open_price"], errors="coerce")
    # Close is deliberately unavailable at the freeze timestamp. F04 only needs
    # current open/current previous close and the two preceding overnight returns.
    current["影子最高價"] = np.nan
    current["影子最低價"] = np.nan
    current["影子收盤價"] = np.nan
    current["價格研究區段"] = "FROZEN_V231_UNIVERSE"
    current["snapshot_time"] = current["snapshot_time"].astype(str)
    cols = BASE_USECOLS + ["snapshot_time", "snapshot_received_at_taipei", "snapshot_open_price"]
    hist = history[history["日期"] < pd.Timestamp(signal_date)].copy()
    # The current F04 rolling window uses only the current overnight return and
    # the two preceding overnight returns; three prior daily rows supply the
    # required previous closes. This keeps the 09:00 freeze computation bounded.
    hist = hist.sort_values(["市場", "股票代號", "日期"], kind="mergesort")
    hist = hist.groupby(["市場", "股票代號"], sort=False, observed=True).tail(3)
    scored = compute_f04(pd.concat([hist, current[cols]], ignore_index=True))
    scored = scored[scored["日期"] == pd.Timestamp(signal_date)].copy()
    scored = scored.dropna(subset=["F04"]).copy()
    if scored.empty:
        raise ValueError("當日開盤快照無法形成任何有效 F04（三個隔夜報酬資料不足）")
    return scored


def build_frozen_top10(scored: pd.DataFrame, signal_date: str,
                       frozen_at: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    day = scored[scored["日期"] == pd.Timestamp(signal_date)].dropna(subset=["F04"]).copy()
    day = day.sort_values(["F04", "股票代號", "市場"], ascending=[False, True, True], kind="mergesort")
    day = day.reset_index(drop=True)
    day["all_universe_rank"] = np.arange(1, len(day) + 1)
    full_rank = day.rename(columns={"市場": "market", "股票代號": "symbol", "F04": "factor_score",
                                    "影子開盤價": "snapshot_open_price"})
    full_rank["signal_date"] = pd.Timestamp(signal_date).strftime("%Y-%m-%d")
    full_rank["factor_direction"] = "HIGH"
    full_rank["factor_definition"] = "-expm1(rolling_sum(last_3_overnight_log_returns))"
    full_rank["frozen_at_taipei"] = frozen_at
    if "snapshot_time" not in full_rank.columns:
        full_rank["snapshot_time"] = ""
    full_rank["rank"] = full_rank["all_universe_rank"]
    if len(full_rank) < TOP_N:
        raise ValueError(f"當日可計算 F04 的候選只有 {len(full_rank)} 檔，未達凍結 Top{TOP_N}")
    top = full_rank.head(TOP_N).copy()
    keep = ["signal_date", "signal", "rank", "market", "symbol", "factor_score",
            "factor_direction", "factor_definition", "snapshot_open_price", "snapshot_time",
            "frozen_at_taipei"]
    top["signal"] = "F04"
    full_rank["signal"] = "F04"
    top = top[keep].reset_index(drop=True)
    return top, full_rank


def normalize_fugle_bars(payload: Dict[str, Any], symbol: str) -> pd.DataFrame:
    data = payload.get("data")
    if not isinstance(data, list):
        raise ValueError(f"Fugle data 欄位不是 list: {symbol}")
    cols = ["symbol", "datetime", "date", "time", "open", "high", "low", "close", "volume"]
    if not data:
        return pd.DataFrame(columns=cols)
    z = pd.DataFrame(data)
    required = ["date", "open", "high", "low", "close"]
    missing = [c for c in required if c not in z.columns]
    if missing:
        raise ValueError(f"Fugle 回應缺欄位 {missing}: {symbol}")
    z["datetime"] = pd.to_datetime(z["date"], errors="coerce", utc=True).dt.tz_convert(TZ).dt.tz_localize(None)
    for c in ["open", "high", "low", "close", "volume"]:
        if c not in z.columns:
            z[c] = np.nan
        z[c] = pd.to_numeric(z[c], errors="coerce")
    z = z.dropna(subset=["datetime", "open", "high", "low", "close"]).copy()
    z["symbol"] = symbol
    z["date"] = z["datetime"].dt.normalize()
    z["time"] = z["datetime"].dt.strftime("%H:%M")
    return z[cols].sort_values("datetime").reset_index(drop=True)


def normalize_snapshot_quotes(payload: Dict[str, Any], market_code: str,
                              received_at: str) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    data = payload.get("data")
    if not isinstance(data, list):
        raise ValueError(f"Fugle Snapshot Quotes 回應缺少 data list：{market_code}")
    market = "TWSE" if market_code == "TSE" else "TPEX"
    rows = []
    for item in data:
        if not isinstance(item, dict):
            continue
        symbol = normalize_code(item.get("symbol", ""))
        if not symbol:
            continue
        rows.append({
            "市場": normalize_market(item.get("market") or market),
            "股票代號": symbol,
            "snapshot_open_price": pd.to_numeric(item.get("openPrice"), errors="coerce"),
            "snapshot_previous_close": pd.to_numeric(item.get("previousClose"), errors="coerce"),
            "snapshot_last_price": pd.to_numeric(item.get("lastPrice"), errors="coerce"),
            "snapshot_last_updated": item.get("lastUpdated"),
            "snapshot_type": item.get("type"),
            "snapshot_name": item.get("name"),
            "snapshot_market_code": market_code,
            "snapshot_date": payload.get("date"),
            "snapshot_time": str(payload.get("time", "")),
            "snapshot_received_at_taipei": received_at,
        })
    cols = ["市場", "股票代號", "snapshot_open_price", "snapshot_previous_close",
            "snapshot_last_price", "snapshot_last_updated", "snapshot_type", "snapshot_name",
            "snapshot_market_code", "snapshot_date", "snapshot_time", "snapshot_received_at_taipei"]
    frame = pd.DataFrame(rows, columns=cols)
    raw_time = payload.get("time")
    snapshot_stamp = None
    try:
        if isinstance(raw_time, (int, float)):
            ts = pd.to_datetime(raw_time, unit="ms", utc=True).tz_convert(TZ).tz_localize(None)
        else:
            raw_s = str(raw_time or "").strip()
            if raw_s.isdigit() and len(raw_s) >= 10:
                unit = "ms" if len(raw_s) >= 13 else "s"
                ts = pd.to_datetime(int(raw_s), unit=unit, utc=True).tz_convert(TZ).tz_localize(None)
            elif "T" in raw_s:
                ts = pd.to_datetime(raw_s, errors="raise", utc=True).tz_convert(TZ).tz_localize(None)
            else:
                ts = pd.Timestamp(f"{payload.get('date')} {raw_s}")
        snapshot_stamp = str(ts)
    except Exception:
        snapshot_stamp = None
    meta = {
        "market_code": market_code,
        "status": "OK" if len(frame) else "EMPTY_DATA",
        "snapshot_date": payload.get("date"),
        "snapshot_time": str(raw_time or ""),
        "snapshot_timestamp": snapshot_stamp,
        "market": payload.get("market"),
        "raw_rows": len(data),
        "normalized_rows": len(frame),
        "received_at_taipei": received_at,
    }
    return frame, meta


class FugleClient:
    def __init__(self, api_key: str):
        self.session = requests.Session()
        self.session.headers.update({"X-API-KEY": api_key, "Accept": "application/json"})
        self.last_call = 0.0

    def snapshot_quotes(self, market_code: str) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        url = f"{FUGLE_SNAPSHOT_BASE}/{market_code}"
        params = {"type": "COMMONSTOCK"}
        received = datetime.now(ZoneInfo(TZ)).isoformat()
        try:
            r = self.session.get(url, params=params, timeout=REQUEST_TIMEOUT)
        except requests.RequestException as e:
            return pd.DataFrame(), {"market_code": market_code, "status": "REQUEST_EXCEPTION", "detail": str(e)[:300]}
        if r.status_code in (401, 403):
            return pd.DataFrame(), {
                "market_code": market_code,
                "status": "SNAPSHOT_PLAN_PERMISSION_REQUIRED" if r.status_code == 403 else "AUTH_FAILED",
                "http_status": r.status_code,
                "detail": "Snapshot Quotes requires Fugle Developer plan or above; do not fall back to after-close ranking.",
            }
        if r.status_code != 200:
            return pd.DataFrame(), {"market_code": market_code, "status": f"HTTP_{r.status_code}",
                                    "http_status": r.status_code, "detail": r.text[:300]}
        try:
            payload = r.json()
            received = datetime.now(ZoneInfo(TZ)).isoformat()
            return normalize_snapshot_quotes(payload, market_code, received)
        except Exception as e:
            return pd.DataFrame(), {"market_code": market_code, "status": "BAD_RESPONSE", "detail": str(e)[:300]}

    def fetch_day(self, symbol: str, signal_date: str) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        url = f"{FUGLE_BASE}/{symbol}"
        params = {"timeframe": "1", "from": signal_date, "to": signal_date,
                  "fields": "open,high,low,close,volume", "sort": "asc"}
        for attempt in range(1, MAX_RETRIES + 1):
            wait = MIN_REQUEST_INTERVAL - (time.monotonic() - self.last_call)
            if wait > 0:
                time.sleep(wait)
            try:
                r = self.session.get(url, params=params, timeout=REQUEST_TIMEOUT)
                self.last_call = time.monotonic()
            except requests.RequestException as e:
                if attempt == MAX_RETRIES:
                    return pd.DataFrame(), {"symbol": symbol, "status": "REQUEST_EXCEPTION", "detail": str(e)[:300]}
                time.sleep(min(2 ** attempt, 20))
                continue
            if r.status_code == 200:
                try:
                    payload = r.json()
                    bars = normalize_fugle_bars(payload, symbol)
                    return bars, {"symbol": symbol, "status": "OK" if not bars.empty else "EMPTY_200",
                                  "http_status": 200, "bar_count": int(len(bars)),
                                  "exchange": payload.get("exchange"), "market": payload.get("market")}
                except Exception as e:
                    return pd.DataFrame(), {"symbol": symbol, "status": "BAD_RESPONSE", "http_status": 200,
                                            "detail": str(e)[:300]}
            if r.status_code == 404:
                return pd.DataFrame(), {"symbol": symbol, "status": "NO_DATA_404", "http_status": 404,
                                        "detail": r.text[:200]}
            if r.status_code in (401, 403):
                raise RuntimeError(f"Fugle API 認證／方案權限錯誤 HTTP {r.status_code}；確認 FUGLE_API 或 FUGLE_API_KEY")
            if r.status_code == 429:
                retry_after = r.headers.get("Retry-After")
                try:
                    delay = max(60.0, float(retry_after)) if retry_after else 65.0
                except Exception:
                    delay = 65.0
                log(f"[FUGLE RATE LIMIT] {symbol} HTTP 429，等待 {delay:.0f} 秒")
                time.sleep(delay)
                continue
            if r.status_code >= 500 and attempt < MAX_RETRIES:
                time.sleep(min(2 ** attempt, 20))
                continue
            return pd.DataFrame(), {"symbol": symbol, "status": f"HTTP_{r.status_code}",
                                    "http_status": r.status_code, "detail": r.text[:300]}
        return pd.DataFrame(), {"symbol": symbol, "status": "RETRIES_EXHAUSTED"}


def choose_entry(daybars: pd.DataFrame, signal_date: str, clock: str) -> Tuple[Optional[float], Optional[str], Optional[float], str]:
    if daybars.empty:
        return None, None, None, "NO_BARS"
    target = pd.Timestamp(f"{signal_date} {clock}")
    end = target + pd.Timedelta(minutes=MAX_ENTRY_DELAY_MIN)
    z = daybars[(daybars["datetime"] >= target) & (daybars["datetime"] <= end)].sort_values("datetime")
    if z.empty:
        return None, None, None, "NO_ENTRY_BAR_WITHIN_5M"
    row = z.iloc[0]
    delay = (row["datetime"] - target).total_seconds() / 60.0
    return float(row["open"]), row["datetime"].strftime("%Y-%m-%d %H:%M:%S"), float(delay), "OK"


def choose_last_continuous(daybars: pd.DataFrame) -> Tuple[Optional[float], Optional[str], Optional[float], str]:
    if daybars.empty:
        return None, None, None, "NO_BARS"
    z = daybars[(daybars["time"] >= LAST_CONTINUOUS_START) & (daybars["time"] <= LAST_CONTINUOUS_END)].sort_values("datetime")
    if z.empty:
        return None, None, None, "NO_LAST_CONTINUOUS_BAR_1320_1324"
    row = z.iloc[-1]
    lag = (pd.Timestamp(f"{row['date'].date()} 13:25:00") - row["datetime"]).total_seconds() / 60.0
    return float(row["close"]), row["datetime"].strftime("%Y-%m-%d %H:%M:%S"), float(lag), "OK"


def choose_auction(daybars: pd.DataFrame) -> Tuple[Optional[float], Optional[str], str]:
    if daybars.empty:
        return None, None, "NO_BARS"
    z = daybars[daybars["time"] == AUCTION_CLOCK].sort_values("datetime")
    if z.empty:
        return None, None, "NO_1330_AUCTION_BAR"
    row = z.iloc[-1]
    return float(row["close"]), row["datetime"].strftime("%Y-%m-%d %H:%M:%S"), "OK"


def net_return(gross: float, slip: float) -> float:
    # Same frozen cost formula as pilot v1.0.1: two-sided slippage, buy/sell fee,
    # and Taiwan stock sell-side transaction tax.
    return ((1.0 + gross) * ((1.0 - slip) / (1.0 + slip)) *
            ((1.0 - FEE_ONE_WAY) * (1.0 - SELL_TAX) / (1.0 + FEE_ONE_WAY)) - 1.0)


def build_trade_rows(selection: pd.DataFrame, bars: pd.DataFrame, request_log: pd.DataFrame,
                     observed_at: str, attempt: int) -> pd.DataFrame:
    req_map = {str(r.symbol): str(r.status) for r in request_log.itertuples(index=False)} if not request_log.empty else {}
    by_symbol = {str(s): g.sort_values("datetime") for s, g in bars.groupby("symbol", sort=False)} if not bars.empty else {}
    rows = []
    for p in selection.itertuples(index=False):
        sym = str(p.symbol)
        sb = by_symbol.get(sym, pd.DataFrame())
        day = sb[sb["date"] == pd.Timestamp(p.signal_date)] if not sb.empty else pd.DataFrame()
        for clock in ENTRY_CLOCKS:
            entry_px, entry_dt, delay, entry_status = choose_entry(day, p.signal_date, clock)
            last_px, last_dt, exit_gap, last_status = choose_last_continuous(day)
            auction_px, auction_dt, auction_status = choose_auction(day)
            exits = [
                ("LAST_CONTINUOUS_BAR_CLOSE_1320_1324", last_px, last_dt, exit_gap, last_status),
                ("AUCTION_1330_CLOSE", auction_px, auction_dt, None, auction_status),
            ]
            for method, exit_px, exit_dt, exit_gap, exit_status in exits:
                if req_map.get(sym, "") not in {"OK"}:
                    status = f"FUGLE_{req_map.get(sym, 'NO_REQUEST_LOG')}"
                elif entry_status != "OK":
                    status = entry_status
                elif exit_status != "OK":
                    status = exit_status
                else:
                    status = "OK"
                gross = float(exit_px / entry_px - 1.0) if status == "OK" and entry_px and exit_px else np.nan
                row = {
                    "signal_date": p.signal_date, "signal": "F04", "rank": int(p.rank),
                    "market": p.market, "symbol": sym, "factor_score": float(p.factor_score),
                    "entry_clock": clock, "max_entry_delay_min": MAX_ENTRY_DELAY_MIN,
                    "entry_bar_datetime": entry_dt, "entry_price_proxy": entry_px,
                    "entry_delay_minutes": delay, "exit_method": method,
                    "exit_bar_datetime": exit_dt, "exit_price_proxy": exit_px,
                    "exit_gap_to_1325_min": exit_gap, "status": status,
                    "gross_return": gross, "observed_at_taipei": observed_at,
                    "attempt": attempt, "price_source": "FUGLE_HISTORICAL_1M_PROXY",
                    "fill_status": "NOT_A_CONFIRMED_FILL",
                }
                for bps, slip in zip((10, 20, 30, 50), SLIPPAGE_LEVELS):
                    row[f"net_{bps}bp"] = net_return(gross, slip) if status == "OK" else np.nan
                rows.append(row)
    return pd.DataFrame(rows)


def summarize_daily(trades: pd.DataFrame) -> pd.DataFrame:
    if trades.empty:
        return pd.DataFrame()
    out = []
    for (d, clock, method), g in trades.groupby(["signal_date", "entry_clock", "exit_method"], sort=True):
        ok = g[g["status"] == "OK"].copy()
        n_all = TOP_N
        rec = {"signal_date": d, "entry_clock": clock, "exit_method": method,
               "attempt": int(pd.to_numeric(g["attempt"], errors="coerce").max()) if "attempt" in g.columns else 1,
               "observed_at_taipei": str(g["observed_at_taipei"].max()) if "observed_at_taipei" in g.columns else None,
               "selected_count": int(len(g)), "valid_count": int(len(ok)),
               "coverage": float(len(ok) / n_all),
               "complete_10_of_10": bool(len(ok) == n_all),
               "valid_only_mean_gross": float(ok["gross_return"].mean()) if len(ok) else np.nan,
               "valid_only_win_rate_gross": float((ok["gross_return"] > 0).mean()) if len(ok) else np.nan}
        for bps in (10, 20, 30, 50):
            col = f"net_{bps}bp"
            rec[f"valid_only_mean_{col}"] = float(ok[col].mean()) if len(ok) else np.nan
            rec[f"valid_only_win_rate_{col}"] = float((ok[col] > 0).mean()) if len(ok) else np.nan
            # Fixed equal-weight 10-stock portfolio is only computable with 10/10 prices.
            rec[f"complete_basket_{col}"] = float(ok[col].mean()) if len(ok) == n_all else np.nan
        out.append(rec)
    return pd.DataFrame(out)


def summarize_cumulative(daily: pd.DataFrame) -> pd.DataFrame:
    methods = ["LAST_CONTINUOUS_BAR_CLOSE_1320_1324", "AUCTION_1330_CLOSE"]
    if daily.empty:
        return pd.DataFrame([{
            "entry_clock": c, "exit_method": m, "signal_days": 0,
            "complete_days": 0, "complete_day_coverage": np.nan,
            "mean_complete_daily_net_10bp": np.nan,
            "median_complete_daily_net_10bp": np.nan,
            "positive_complete_days_10bp": 0,
            "complete_day_win_rate_10bp": np.nan,
            "mean_daily_valid_only_net_10bp": np.nan,
            "review_threshold_signal_days": 30,
            "review_threshold_met": False,
        } for c in ENTRY_CLOCKS for m in methods])

    d = daily.copy()
    for col in ["signal_date", "entry_clock", "exit_method"]:
        if col not in d.columns:
            raise ValueError(f"累積逐日狀態缺少欄位：{col}")
    if "attempt" not in d.columns:
        d["attempt"] = 1
    d["attempt"] = pd.to_numeric(d["attempt"], errors="coerce").fillna(1)
    # Retries are retained in raw state, but cumulative statistics use only the
    # latest attempt for each signal-date / clock / exit-method arm.
    d = d.sort_values(["signal_date", "entry_clock", "exit_method", "attempt", "observed_at_taipei"]
                      if "observed_at_taipei" in d.columns
                      else ["signal_date", "entry_clock", "exit_method", "attempt"], kind="mergesort")
    d = d.drop_duplicates(["signal_date", "entry_clock", "exit_method"], keep="last")
    if "complete_10_of_10" in d.columns:
        d["_complete"] = d["complete_10_of_10"].astype(str).str.strip().str.lower().isin({"true", "1", "yes"})
    else:
        d["_complete"] = False
    if "complete_basket_net_10bp" not in d.columns:
        d["complete_basket_net_10bp"] = np.nan
    if "valid_only_mean_net_10bp" not in d.columns:
        d["valid_only_mean_net_10bp"] = np.nan

    out=[]
    for clock in ENTRY_CLOCKS:
        for method in methods:
            g = d[(d["entry_clock"] == clock) & (d["exit_method"] == method)].copy()
            if g.empty:
                out.append({
                    "entry_clock": clock, "exit_method": method, "signal_days": 0,
                    "complete_days": 0, "complete_day_coverage": np.nan,
                    "mean_complete_daily_net_10bp": np.nan,
                    "median_complete_daily_net_10bp": np.nan,
                    "positive_complete_days_10bp": 0,
                    "complete_day_win_rate_10bp": np.nan,
                    "mean_daily_valid_only_net_10bp": np.nan,
                    "review_threshold_signal_days": 30, "review_threshold_met": False,
                })
                continue
            complete = g[g["_complete"]].copy()
            vals = pd.to_numeric(complete["complete_basket_net_10bp"], errors="coerce").dropna()
            days = int(g["signal_date"].nunique())
            complete_days = int(complete["signal_date"].nunique())
            valid_daily = pd.to_numeric(g["valid_only_mean_net_10bp"], errors="coerce").dropna()
            out.append({
                "entry_clock": clock, "exit_method": method,
                "signal_days": days, "complete_days": complete_days,
                "complete_day_coverage": float(complete_days / days) if days else np.nan,
                "mean_complete_daily_net_10bp": float(vals.mean()) if len(vals) else np.nan,
                "median_complete_daily_net_10bp": float(vals.median()) if len(vals) else np.nan,
                "positive_complete_days_10bp": int((vals > 0).sum()),
                "complete_day_win_rate_10bp": float((vals > 0).mean()) if len(vals) else np.nan,
                "mean_daily_valid_only_net_10bp": float(valid_daily.mean()) if len(valid_daily) else np.nan,
                "review_threshold_signal_days": 30,
                "review_threshold_met": bool(days >= 30),
            })
    return pd.DataFrame(out)


def load_state_csv(
    path: Path,
    expected_cols: Sequence[str],
    *,
    allow_empty_file: bool = False,
) -> pd.DataFrame:
    """Load one cumulative-state CSV without mistaking corrupt required state for no data.

    The v2.0 bootstrap/waiting run could write zero-byte trades/daily files because
    those in-memory DataFrames had no columns. Those two observation tables are
    explicitly allowed to recover as schema-correct empty tables. A zero-byte
    selection/source file remains a hard error because silently dropping it could
    erase a previously frozen Top10 or its lineage.
    """
    cols = list(expected_cols)
    if not path.is_file():
        STATE_RESTORE_DIAGNOSTICS.append({
            "file": path.name, "status": "MISSING_FILE_AS_EMPTY", "bytes": None,
            "allow_empty_file": bool(allow_empty_file),
        })
        log(f"[STATE RESTORE NOTE] {path.name} 不存在；以 0 列資料的 schema 空表載入")
        return pd.DataFrame(columns=cols)

    raw = path.read_bytes()
    # BOM-only, whitespace-only and zero-byte files are all empty.
    try:
        content_text = raw.decode("utf-8-sig")
    except UnicodeDecodeError as e:
        raise ValueError(f"GitHub 累積狀態不是有效 UTF-8 CSV：{path}; {e}") from e

    if not content_text.strip():
        if not allow_empty_file:
            raise ValueError(
                f"GitHub 關鍵累積狀態檔為空，停止以免遺失既有資料：{path}; bytes={len(raw)}"
            )
        STATE_RESTORE_DIAGNOSTICS.append({
            "file": path.name, "status": "EMPTY_FILE_RECOVERED_AS_ZERO_ROWS",
            "bytes": len(raw), "allow_empty_file": True,
            "note": "舊版等待階段可能將無欄位空 DataFrame 輸出為零位元組；本版以固定 schema 空表復原。",
        })
        log(f"[STATE EMPTY RECOVERED] {path.name}; bytes={len(raw)}; rows=0; schema_columns={len(cols)}")
        return pd.DataFrame(columns=cols)

    try:
        df = pd.read_csv(path, encoding="utf-8-sig", low_memory=False)
    except pd.errors.EmptyDataError as e:
        # Defensive fallback for parser-recognized empty input. The raw-text test
        # above handles ordinary blank files; any unexpected empty parse is gated.
        if not allow_empty_file:
            raise ValueError(f"GitHub 關鍵累積狀態 CSV 無欄位，停止：{path}") from e
        STATE_RESTORE_DIAGNOSTICS.append({
            "file": path.name, "status": "EMPTY_DATA_ERROR_RECOVERED_AS_ZERO_ROWS",
            "bytes": len(raw), "allow_empty_file": True,
        })
        log(f"[STATE EMPTY RECOVERED] {path.name}; pandas EmptyDataError; rows=0")
        return pd.DataFrame(columns=cols)

    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"GitHub 累積狀態欄位不符：{path}; missing={missing}; actual={list(df.columns)}")
    STATE_RESTORE_DIAGNOSTICS.append({
        "file": path.name, "status": "LOADED", "bytes": len(raw),
        "rows": int(len(df)), "columns": list(df.columns),
    })
    return df


def compare_selection_existing(old: pd.DataFrame, new: pd.DataFrame) -> None:
    d = str(new["signal_date"].iloc[0])
    prior = old[old["signal_date"].astype(str) == d].copy()
    if prior.empty:
        return
    cols = ["signal_date", "rank", "market", "symbol", "factor_score"]
    a = prior[cols].sort_values("rank").reset_index(drop=True)
    b = new[cols].sort_values("rank").reset_index(drop=True)
    if len(a) != len(b) or a[["signal_date", "rank", "market", "symbol"]].to_dict("records") != b[["signal_date", "rank", "market", "symbol"]].to_dict("records"):
        raise ValueError(f"凍結選股衝突：{d} 已存在的 Top10 與本次輸入不同；拒絕覆寫")
    if not np.allclose(pd.to_numeric(a["factor_score"], errors="coerce"),
                       pd.to_numeric(b["factor_score"], errors="coerce"), rtol=0, atol=1e-12, equal_nan=True):
        raise ValueError(f"凍結因子分數衝突：{d}；拒絕覆寫")


def verify_existing_trade_rows(old: pd.DataFrame, new: pd.DataFrame) -> None:
    if old.empty or new.empty:
        return
    keys = ["signal_date", "symbol", "entry_clock", "exit_method", "attempt"]
    old_keys = set(tuple(str(r[c]) for c in keys) for r in old.to_dict("records"))
    new = new[~new.apply(lambda r: tuple(str(r[c]) for c in keys) in old_keys, axis=1)].copy()
    # The caller concatenates only genuinely new observations.
    return new


def get_github_token() -> Optional[str]:
    return get_secret("GITHUB_SSO_TOKEN") or get_secret("GITHUB_TOKEN")


def git_run(cwd: Path, args: List[str], auth: Optional[str] = None, check: bool = True) -> subprocess.CompletedProcess:
    cmd = ["git"]
    if auth:
        cmd += ["-c", f"http.extraHeader=AUTHORIZATION: basic {auth}"]
    cmd += args
    return subprocess.run(cmd, cwd=str(cwd), check=check, stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE, text=False)


def upload_cumulative_audit(report_dir: Path, zip_path: Path, run_id: str,
                            no_upload: bool = False) -> Dict[str, Any]:
    if no_upload:
        return {"status": "SKIPPED_BY_FLAG"}
    token = get_github_token()
    if not token:
        raise RuntimeError("找不到 GitHub token：Colab Secrets GITHUB_SSO_TOKEN 或 GITHUB_TOKEN")
    auth = base64.b64encode(f"x-access-token:{token}".encode()).decode()
    work = Path("/content") / f".sso_f04_h0_git_{run_id}"
    if work.exists():
        shutil.rmtree(work)
    work.mkdir(parents=True)
    git_run(work, ["init"])
    git_run(work, ["config", "user.name", "SSO Audit Bot"])
    git_run(work, ["config", "user.email", "sso-audit@users.noreply.github.com"])
    git_run(work, ["remote", "add", "origin", GITHUB_REMOTE])

    main_ls = git_run(work, ["ls-remote", "origin", "refs/heads/main"], auth)
    main_before = main_ls.stdout.decode().strip().split()[0] if main_ls.stdout.strip() else ""
    if main_before != REQUIRED_MAIN_SHA:
        raise RuntimeError(f"保護性停止：main SHA 已變更 actual={main_before}; expected={REQUIRED_MAIN_SHA}")

    branches_cp = git_run(work, ["ls-remote", "--heads", "origin", f"refs/heads/{BRANCH_PREFIX}*"], auth)
    branch_lines = [x.split() for x in branches_cp.stdout.decode().splitlines() if x.strip()]
    previous = sorted([x[1].replace("refs/heads/", "") for x in branch_lines])[-1] if branch_lines else None
    if previous:
        base_ref = f"refs/heads/{previous}"
        git_run(work, ["fetch", "--depth=1", "origin", f"+{base_ref}:refs/remotes/origin/{previous}"], auth)
        parent_ref = f"refs/remotes/origin/{previous}"
        base_branch = previous
    else:
        git_run(work, ["fetch", "--depth=1", "origin", "+refs/heads/main:refs/remotes/origin/main"], auth)
        parent_ref = "refs/remotes/origin/main"
        base_branch = "main"
    branch = BRANCH_PREFIX + run_id
    git_run(work, ["checkout", "-b", branch, parent_ref])

    # Carry forward the cumulative state files already tracked by the previous audit branch.
    dest = work / "audit_readout" / "sso_fugle_f04_h0_forward_shadow_v2"
    dest.mkdir(parents=True, exist_ok=True)
    report_dest = dest / "runs" / run_id
    if report_dest.exists():
        raise RuntimeError(f"本次 run_id 已存在於 audit branch，避免覆寫：{report_dest}")
    report_dest.mkdir(parents=True, exist_ok=True)
    for p in report_dir.iterdir():
        if p.is_file():
            shutil.copy2(p, report_dest / p.name)
    shutil.copy2(zip_path, report_dest / zip_path.name)

    # Save cumulative state in the stable folder; overwrite with the merged append-only state.
    state_src = report_dir / "state"
    state_dst = dest / "state"
    state_dst.mkdir(parents=True, exist_ok=True)
    for p in state_src.iterdir():
        if p.is_file():
            shutil.copy2(p, state_dst / p.name)

    git_run(work, ["add", "-A"])
    git_run(work, ["commit", "-m", f"SSO F04 H0 forward shadow validation {run_id}"])
    commit_sha = git_run(work, ["rev-parse", "HEAD"]).stdout.decode().strip()
    git_run(work, ["push", "origin", f"HEAD:refs/heads/{branch}"], auth)
    git_run(work, ["fetch", "origin", f"+refs/heads/{branch}:refs/remotes/origin/{branch}"], auth)

    checked = [p for p in report_dest.iterdir() if p.is_file()]
    checked.extend([p for p in state_dst.iterdir() if p.is_file()])
    readback = 0
    for local in checked:
        rel = local.relative_to(work).as_posix()
        blob = git_run(work, ["cat-file", "blob", f"refs/remotes/origin/{branch}:{rel}"]).stdout
        if sha256_bytes(blob) != sha256_file(local):
            raise RuntimeError(f"GitHub readback SHA mismatch: {rel}")
        readback += 1
    main_after_cp = git_run(work, ["ls-remote", "origin", "refs/heads/main"], auth)
    main_after = main_after_cp.stdout.decode().strip().split()[0] if main_after_cp.stdout.strip() else ""
    if main_after != REQUIRED_MAIN_SHA:
        raise RuntimeError(f"保護性檢查失敗：main SHA after={main_after}")
    shutil.rmtree(work, ignore_errors=True)
    return {"status": "PASS", "branch": branch, "base_branch": base_branch,
            "commit": commit_sha, "readback": readback,
            "main_before": main_before, "main_after": main_after,
            "url": f"https://github.com/{REPO}/tree/{branch}/audit_readout/sso_fugle_f04_h0_forward_shadow_v2"}


def test_state_csv_recovery() -> None:
    """Verify only designated no-observation state tables accept empty files."""
    with tempfile.TemporaryDirectory(prefix="sso_f04_state_test_") as td:
        root = Path(td)
        zero = root / "zero.csv"
        zero.write_bytes(b"")
        recovered = load_state_csv(zero, ["signal_date", "symbol"], allow_empty_file=True)
        assert recovered.empty and list(recovered.columns) == ["signal_date", "symbol"]

        bom_blank = root / "bom_blank.csv"
        bom_blank.write_bytes(b"\xef\xbb\xbf  \n")
        recovered2 = load_state_csv(bom_blank, ["a", "b"], allow_empty_file=True)
        assert recovered2.empty and list(recovered2.columns) == ["a", "b"]

        header_only = root / "header_only.csv"
        header_only.write_text("signal_date,symbol\n", encoding="utf-8")
        loaded = load_state_csv(header_only, ["signal_date", "symbol"], allow_empty_file=False)
        assert loaded.empty and list(loaded.columns) == ["signal_date", "symbol"]

        canonical = root / "canonical_empty_state.csv"
        write_csv(pd.DataFrame(columns=TRADE_STATE_COLUMNS), canonical)
        assert canonical.stat().st_size > 0, "empty state writer must persist a header"
        canonical_loaded = load_state_csv(canonical, TRADE_STATE_COLUMNS, allow_empty_file=False)
        assert canonical_loaded.empty and list(canonical_loaded.columns) == TRADE_STATE_COLUMNS

        strict_zero = root / "strict_zero.csv"
        strict_zero.write_bytes(b"")
        blocked = False
        try:
            load_state_csv(strict_zero, ["signal_date"], allow_empty_file=False)
        except ValueError:
            blocked = True
        assert blocked, "critical empty state must remain blocked"

        bad_header = root / "bad_header.csv"
        bad_header.write_text("wrong_column\n1\n", encoding="utf-8")
        blocked = False
        try:
            load_state_csv(bad_header, ["signal_date"], allow_empty_file=True)
        except ValueError:
            blocked = True
        assert blocked, "non-empty malformed state must remain blocked"
    log("[SELF TEST] PASS | zero-byte observation-state recovery + header-only CSV + strict critical-state and malformed-header gates")


def make_self_test() -> None:
    # Exercise the real two-stage flow without network calls.
    STATE_RESTORE_DIAGNOSTICS.clear()
    test_state_csv_recovery()
    assert previous_market_sessions("2026-11-24", 3) == ["2026-11-23", "2026-11-20", "2026-11-19"]
    assert previous_market_sessions("2026-12-28", 3) == ["2026-12-24", "2026-12-23", "2026-12-22"]
    assert previous_market_sessions("2027-01-04", 3) == ["2026-12-31", "2026-12-30", "2026-12-29"]
    test_snapshot, test_meta = normalize_snapshot_quotes({
        "date": "2026-11-11", "time": "090020", "market": "TSE",
        "data": [{"type": "EQUITY", "symbol": "2330", "openPrice": 600.0,
                  "lastPrice": 601.0, "lastUpdated": 1794358820000000}],
    }, "TSE", "2026-11-11T09:00:22+08:00")
    assert len(test_snapshot) == 1 and test_meta["snapshot_timestamp"] == "2026-11-11 09:00:20"
    hist_dates = pd.bdate_range("2026-11-02", periods=7)
    signal_date = "2026-11-11"
    rows = []
    for i, d in enumerate(hist_dates):
        for j in range(15):
            close = 30.0 + j * 0.4 + i * 0.05
            open_px = close * (1.0 + ((j % 5) - 2) * 0.001 + (i % 3) * 0.0003)
            rows.append({
                "日期": d, "市場": "TWSE", "股票代號": f"{j+1000:04d}",
                "影子開盤價": open_px,
                "影子最高價": max(open_px, close) * 1.003,
                "影子最低價": min(open_px, close) * 0.997,
                "影子收盤價": close, "價格研究區段": "TEST",
            })
    history = pd.DataFrame(rows)
    hist_check = validate_history_sessions(history, 15, signal_date)
    assert hist_check["pass"] and hist_check["expected_previous_market_sessions"] == ["2026-11-10", "2026-11-09", "2026-11-06"]
    quotes = pd.DataFrame([{
        "市場": "TWSE", "股票代號": f"{j+1000:04d}",
        "snapshot_open_price": 31.0 + j * 0.4,
        "snapshot_time": "09:00:20", "snapshot_received_at_taipei": "2026-11-11T09:00:22+08:00",
    } for j in range(15)])
    scored = score_opening_snapshot(history, quotes, signal_date)
    top, full = build_frozen_top10(scored, signal_date, "2026-11-11T09:00:22+08:00")
    assert len(top) == 10 and len(full) == 15
    assert top.iloc[0]["factor_score"] >= top.iloc[-1]["factor_score"]
    # Formula must equal the negative exponential of the latest 3 overnight log returns.
    sym = str(top.iloc[0]["symbol"])
    one = scored[scored["股票代號"] == sym].iloc[0]
    h = history[history["股票代號"] == sym].sort_values("日期").copy()
    prev2 = np.log(h["影子開盤價"].iloc[-2] / h["影子收盤價"].iloc[-3])
    prev1 = np.log(h["影子開盤價"].iloc[-1] / h["影子收盤價"].iloc[-2])
    cur = np.log(float(quotes.loc[quotes["股票代號"] == sym, "snapshot_open_price"].iloc[0]) / h["影子收盤價"].iloc[-1])
    expected = -np.expm1(prev2 + prev1 + cur)
    assert np.isclose(float(one["F04"]), expected, atol=1e-12)

    bars = []
    for p in top.itertuples(index=False):
        for tm, px in [("09:00", 100.0), ("09:01", 100.5), ("09:05", 101.0),
                       ("13:23", 102.0), ("13:24", 102.5), ("13:30", 103.0)]:
            dt = pd.Timestamp(f"{signal_date} {tm}")
            bars.append({"symbol": p.symbol, "datetime": dt, "date": dt.normalize(), "time": tm,
                         "open": px, "high": px + 0.2, "low": px - 0.2,
                         "close": px + 0.1, "volume": 100})
    bars = pd.DataFrame(bars)
    ep, edt, delay, es = choose_entry(bars[bars.symbol == top.iloc[0].symbol], signal_date, "09:01")
    xp, xdt, gap, xs = choose_last_continuous(bars[bars.symbol == top.iloc[0].symbol])
    assert es == "OK" and ep == 100.5 and delay == 0
    assert xs == "OK" and xp == 102.6 and gap == 1.0
    assert net_return(0.01, 0.001) > 0

    req = pd.DataFrame([{"symbol": sym, "status": "OK"} for sym in top.symbol])
    tr = build_trade_rows(top, bars, req, "2026-11-11T16:40:00+08:00", 1)
    dy = summarize_daily(tr)
    assert len(tr) == 40 and len(dy) == 4 and dy["complete_10_of_10"].all()
    # No last-continuous bar for one selected security must produce 9/10, not a fabricated basket return.
    b2 = bars[~((bars.symbol == top.iloc[0].symbol) & (bars.time.isin(["13:20", "13:21", "13:22", "13:23", "13:24"])))].copy()
    tr2 = build_trade_rows(top, b2, req, "2026-11-11T16:40:00+08:00", 1)
    dy2 = summarize_daily(tr2)
    row = dy2[(dy2.entry_clock == "09:01") & (dy2.exit_method == "LAST_CONTINUOUS_BAR_CLOSE_1320_1324")].iloc[0]
    assert row.valid_count == 9 and not row.complete_10_of_10 and pd.isna(row.complete_basket_net_10bp)
    auction_row = dy2[(dy2.entry_clock == "09:01") & (dy2.exit_method == "AUCTION_1330_CLOSE")].iloc[0]
    assert auction_row.valid_count == 10 and auction_row.complete_10_of_10

    retry = pd.concat([dy, dy.assign(attempt=2, complete_10_of_10=False,
                                    complete_basket_net_10bp=np.nan,
                                    valid_only_mean_net_10bp=-0.01)], ignore_index=True)
    cum = summarize_cumulative(retry)
    c = cum[(cum.entry_clock == "09:01") & (cum.exit_method == "LAST_CONTINUOUS_BAR_CLOSE_1320_1324")].iloc[0]
    assert c.signal_days == 1 and c.complete_days == 0 and not c.review_threshold_met
    log("[SELF TEST] PASS | real-time snapshot F04 + frozen Top10 + entry-before-bar timing + H0 exits + 10/10 basket gate + retry dedup")


def main(argv: Optional[List[str]] = None) -> Dict[str, Any]:
    args = parse_args(argv)
    if args.self_test:
        make_self_test()
        return {"status": "SELF_TEST_PASS"}

    run_dt = datetime.now(ZoneInfo(TZ))
    run_id = run_dt.strftime("%Y%m%d_%H%M%S")
    today = run_dt.date()
    signal_date = pd.Timestamp(args.signal_date).strftime("%Y-%m-%d") if args.signal_date else today.strftime("%Y-%m-%d")
    if pd.Timestamp(signal_date).date() != today:
        raise ValueError(f"前瞻程序只允許台北當日訊號日；target={signal_date}, today={today}")

    now_time = run_dt.time().replace(tzinfo=None)
    freeze_prepare_start = dt_time(8, 30, 0)
    freeze_start = dt_time(9, 0, 0)
    freeze_snapshot_end = dt_time(9, 0, 55)
    freeze_completed_end = dt_time(9, 0, 58)
    close_start = dt_time(16, 35)
    if args.phase == "bootstrap":
        phase = "bootstrap"
    elif args.phase == "freeze":
        phase = "freeze"
    elif args.phase == "close":
        phase = "close"
    elif today < pd.Timestamp(OOS_START_DATE).date():
        phase = "bootstrap"
    elif freeze_prepare_start <= now_time <= freeze_completed_end:
        phase = "freeze"
    elif now_time >= close_start:
        phase = "close"
    else:
        phase = "waiting"

    if phase == "freeze" and not (freeze_prepare_start <= now_time <= freeze_completed_end):
        raise RuntimeError(f"開盤凍結程序請於 08:30:00–09:00:58 啟動，讓程式先載入資料，再於 09:00 開盤後抓快照；現在 {now_time}")
    if phase == "close" and now_time < close_start:
        raise RuntimeError(f"收盤核算階段必須在 16:35 後執行；現在 {now_time}")
    if phase in {"freeze", "close"} and today < pd.Timestamp(OOS_START_DATE).date():
        raise ValueError(f"前瞻觀察不得早於 OOS 起始日 {OOS_START_DATE}；目前日期 {today}")
    if args.retry_existing and phase != "close":
        raise ValueError("--retry-existing 只允許收盤核算階段使用；不允許重算或覆寫開盤凍結名單")

    log("=" * 96)
    log(f"{VERSION}｜開始")
    log(f"[PHASE] {phase.upper()} | signal_date={signal_date} | OOS_START={OOS_START_DATE}")
    log("[FROZEN] F04 HIGH / Top10 / H0 / Entry=09:01,09:05 / Exit=last continuous 13:20-13:24 CLOSE + 13:30 auction CLOSE")
    log("[PROSPECTIVE RULE] Top10 must be frozen from 09:00 market snapshots before the 09:01 entry; close phase may not recompute rankings")
    log("[SAFETY] Drive input read-only; output only /content; never push main; missing prices remain NA")

    mount_drive_if_needed()
    base_path = locate_base_file(args.base_data)
    base, base_meta = load_base(base_path)
    log(f"[BASE] PASS rows={len(base):,}; universe={base_meta['universe_securities']:,}; max_date={base_meta['date_max']}; SHA={base_meta['sha256']}")

    increment_files = discover_increment_files(args.increment_dir)
    log(f"[INCREMENT FILES] found={len(increment_files)}")
    increments, increment_manifest, increment_meta = load_increment_data(base, increment_files)
    if not increments.empty:
        log(f"[HISTORY] rows={len(increments):,}; dates={increments['日期'].nunique()}; latest={increment_meta['latest_date']}")
    else:
        log("[HISTORY] 無 v2.31 之後的官方日線增量；如不滿足 lookback freshness，會阻擋開盤凍結")
    history = pd.concat([base, increments], ignore_index=True) if not increments.empty else base.copy()
    history = history[history["日期"] < pd.Timestamp(signal_date)].copy()
    history = history.drop_duplicates(["市場", "股票代號", "日期"], keep="last")
    history_max = str(history["日期"].max().date()) if not history.empty else None
    try:
        history_validation = validate_history_sessions(history, base_meta["universe_securities"], signal_date)
    except Exception as e:
        history_validation = {"signal_date": signal_date, "pass": False, "error": str(e),
                              "history_latest_date": history_max}

    REPORT_ROOT.mkdir(parents=True, exist_ok=True)
    out_dir = REPORT_ROOT / f"SSO_F04_H0_Forward_Shadow_v2_{run_id}"
    out_dir.mkdir(parents=True, exist_ok=False)
    (out_dir / "state").mkdir()

    STATE_RESTORE_DIAGNOSTICS.clear()
    state_old_selections = pd.DataFrame(columns=SELECTION_STATE_COLUMNS)
    state_old_trades = pd.DataFrame(columns=TRADE_STATE_COLUMNS)
    state_old_daily = pd.DataFrame(columns=DAILY_STATE_COLUMNS)
    state_old_sources = pd.DataFrame(columns=SOURCE_STATE_COLUMNS)
    previous_state_branch = None
    token = get_github_token() if not args.no_github_upload else None
    if token:
        auth = base64.b64encode(f"x-access-token:{token}".encode()).decode()
        tmp = Path("/content") / f".sso_f04_h0_restore_{run_id}"
        if tmp.exists():
            shutil.rmtree(tmp)
        tmp.mkdir(parents=True)
        try:
            git_run(tmp, ["init"])
            git_run(tmp, ["remote", "add", "origin", GITHUB_REMOTE])
            ls = git_run(tmp, ["ls-remote", "--heads", "origin", f"refs/heads/{BRANCH_PREFIX}*"], auth)
            lines = [x.split() for x in ls.stdout.decode().splitlines() if x.strip()]
            previous = sorted([x[1].replace("refs/heads/", "") for x in lines])[-1] if lines else None
            if previous:
                previous_state_branch = previous
                git_run(tmp, ["fetch", "--depth=1", "origin", f"+refs/heads/{previous}:refs/remotes/origin/{previous}"], auth)
                git_run(tmp, ["checkout", "--detach", f"refs/remotes/origin/{previous}"])
                st = tmp / "audit_readout" / "sso_fugle_f04_h0_forward_shadow_v2" / "state"
                if st.exists():
                    state_old_selections = load_state_csv(st / STATE_SELECTIONS.name, SELECTION_STATE_COLUMNS)
                    state_old_trades = load_state_csv(
                        st / STATE_TRADES.name, TRADE_STATE_COLUMNS, allow_empty_file=True
                    )
                    state_old_daily = load_state_csv(
                        st / STATE_DAILY.name, DAILY_STATE_COLUMNS, allow_empty_file=True
                    )
                    state_old_sources = load_state_csv(st / STATE_SOURCES.name, SOURCE_STATE_COLUMNS)
                log(f"[STATE RESTORE] PASS branch={previous}; frozen_days={state_old_selections.get('signal_date', pd.Series(dtype=str)).nunique()}; trade_observations={len(state_old_trades)}")
            else:
                log("[STATE RESTORE] 尚無 v2 前瞻狀態，建立新累積分支")
        finally:
            shutil.rmtree(tmp, ignore_errors=True)
    elif not args.no_github_upload:
        raise RuntimeError("缺少 GitHub Token，無法延續累積狀態；請設定 GITHUB_SSO_TOKEN 或 GITHUB_TOKEN")

    selection_today = pd.DataFrame(columns=state_old_selections.columns)
    full_rank_today = pd.DataFrame()
    snapshot_today = pd.DataFrame()
    trades_today = pd.DataFrame()
    requests_today = pd.DataFrame(columns=["symbol", "status", "http_status", "bar_count", "exchange", "market", "detail"])
    bars_today = pd.DataFrame(columns=["symbol", "datetime", "date", "time", "open", "high", "low", "close", "volume"])
    daily_today = pd.DataFrame()
    coverage_meta: Dict[str, Any] = {
        "signal_date": signal_date, "frozen_universe_size": base_meta["universe_securities"],
        "valid_open_price_securities": 0, "coverage": 0.0,
        "minimum_required_coverage": MIN_DAILY_UNIVERSE_COVERAGE, "pass": False,
    }
    snapshot_logs = []
    attempt = 1
    status = "WAITING"
    api_key = get_secret("FUGLE_API") or get_secret("FUGLE_API_KEY")

    # Phase 0: bootstrap only. No pre-OOS candidates or trades are generated.
    if phase == "bootstrap":
        status = "WAITING_FOR_OOS_START"
        log(f"[WAIT] OOS 起始日 {OOS_START_DATE} 尚未到達；本次只建立規則與狀態分支，不建立訊號")

    # Phase 1: freeze Top10 before 09:01 using two market-wide snapshots.
    elif phase == "freeze":
        existing_dates = set(state_old_selections.get("signal_date", pd.Series(dtype=str)).astype(str))
        if signal_date in existing_dates:
            status = "ALREADY_FROZEN"
            log(f"[SKIP] {signal_date} 已凍結 Top10；不重新呼叫快照、不重新排名")
        elif not history_validation.get("pass", False):
            status = "BLOCKED_INCOMPLETE_OR_STALE_DAILY_HISTORY"
            log(f"[BLOCK] 開盤訊號所需前三個交易日官方日線不完整：{json.dumps(history_validation, ensure_ascii=False)}；不呼叫快照")
        elif not api_key:
            raise RuntimeError("缺少 Fugle API Key；Colab Secrets 必須設定 FUGLE_API 或 FUGLE_API_KEY")
        else:
            current_clock = datetime.now(ZoneInfo(TZ)).time().replace(tzinfo=None)
            snapshot_start_time = dt_time(9, 0, 5)
            if current_clock < snapshot_start_time:
                wait_seconds = (datetime.combine(today, snapshot_start_time, tzinfo=ZoneInfo(TZ)) - datetime.now(ZoneInfo(TZ))).total_seconds()
                log(f"[PRELOAD PASS] 歷史資料與 GitHub 狀態已載入；等待至 09:00:05 開始全市場快照（約 {max(0, wait_seconds):.1f} 秒）")
                if wait_seconds > 0:
                    time.sleep(wait_seconds)
            if datetime.now(ZoneInfo(TZ)).time().replace(tzinfo=None) > freeze_snapshot_end:
                status = "MISSED_OPENING_SNAPSHOT_WINDOW"
                log("[BLOCK] 已超過 09:00:55 快照窗口；不得事後回填當日開盤快照")
            else:
                client = FugleClient(api_key)
                quote_frames = []
                for market_code in ("TSE", "OTC"):
                    q, meta = client.snapshot_quotes(market_code)
                    snapshot_logs.append(meta)
                    log(f"[SNAPSHOT] {market_code} status={meta.get('status')} rows={meta.get('normalized_rows', 0)} time={meta.get('snapshot_time')}")
                    if not q.empty:
                        quote_frames.append(q)
                snapshot_today = pd.concat(quote_frames, ignore_index=True) if quote_frames else pd.DataFrame()
                if any(x.get("status") == "SNAPSHOT_PLAN_PERMISSION_REQUIRED" for x in snapshot_logs):
                    status = "BLOCKED_SNAPSHOT_DEVELOPER_PLAN_REQUIRED"
                    log("[BLOCK] Fugle Snapshot Quotes 權限不足（官方文件要求 Developer 以上）；不以收盤日線回補，避免前視偏誤")
                elif any(x.get("status") != "OK" for x in snapshot_logs):
                    status = "BLOCKED_SNAPSHOT_REQUEST_FAILURE"
                    log("[BLOCK] 至少一個市場快照查詢失敗；不產生部分市場 Top10")
                else:
                    # Both exchanges must represent the same requested trading date.
                    dates_ok = all(str(x.get("snapshot_date", ""))[:10] == signal_date for x in snapshot_logs)
                    snapshot_finish_dt = datetime.now(ZoneInfo(TZ))
                    snapshot_finish_ok = snapshot_finish_dt.time().replace(tzinfo=None) <= freeze_snapshot_end
                    parsed_stamps = [pd.Timestamp(x["snapshot_timestamp"]) for x in snapshot_logs if x.get("snapshot_timestamp")]
                    times_parseable = len(parsed_stamps) == 2
                    snapshot_times_window_ok = bool(times_parseable and all(
                        pd.Timestamp(f"{signal_date} 09:00:00") <= ts <= pd.Timestamp(f"{signal_date} 09:00:55")
                        for ts in parsed_stamps))
                    snapshot_times_consistent = bool(times_parseable and (max(parsed_stamps) - min(parsed_stamps)).total_seconds() <= 10)
                    universe = set(zip(base["市場"], base["股票代號"]))
                    snapshot_today = snapshot_today.drop_duplicates(["市場", "股票代號"], keep="last")
                    row_index = pd.MultiIndex.from_arrays([snapshot_today["市場"], snapshot_today["股票代號"]])
                    universe_index = pd.MultiIndex.from_tuples(sorted(universe))
                    snapshot_today = snapshot_today.loc[row_index.isin(universe_index)].copy()
                    valid_open = snapshot_today.dropna(subset=["snapshot_open_price"])
                    valid_open = valid_open[valid_open["snapshot_open_price"] > 0]
                    coverage = len(valid_open[["市場", "股票代號"]].drop_duplicates()) / base_meta["universe_securities"]
                    coverage_meta = {
                        "signal_date": signal_date,
                        "frozen_universe_size": int(base_meta["universe_securities"]),
                        "valid_open_price_securities": int(valid_open[["市場", "股票代號"]].drop_duplicates().shape[0]),
                        "coverage": float(coverage),
                        "minimum_required_coverage": MIN_DAILY_UNIVERSE_COVERAGE,
                        "pass": bool(coverage >= MIN_DAILY_UNIVERSE_COVERAGE),
                        "snapshot_requests": snapshot_logs,
                        "snapshot_download_completed_at_taipei": snapshot_finish_dt.isoformat(),
                        "snapshot_date_matches_signal_date": bool(dates_ok),
                        "snapshot_download_completed_before_090055": bool(snapshot_finish_ok),
                        "snapshot_timestamps_parseable": bool(times_parseable),
                        "snapshot_times_within_freeze_window": bool(snapshot_times_window_ok),
                        "snapshot_markets_within_10_seconds": bool(snapshot_times_consistent),
                        "snapshot_timestamps": [str(x) for x in parsed_stamps],
                    }
                    if not dates_ok:
                        status = "BLOCKED_SNAPSHOT_DATE_MISMATCH"
                    elif not snapshot_finish_ok:
                        status = "BLOCKED_SNAPSHOT_DOWNLOAD_FINISHED_AFTER_090055"
                    elif not times_parseable or not snapshot_times_window_ok or not snapshot_times_consistent:
                        status = "BLOCKED_STALE_OR_INCONSISTENT_SNAPSHOT_TIME"
                    elif not coverage_meta["pass"]:
                        status = "BLOCKED_LOW_OPEN_SNAPSHOT_COVERAGE"
                        log(f"[BLOCK] 開盤快照有效覆蓋率 {coverage:.2%} < {MIN_DAILY_UNIVERSE_COVERAGE:.0%}；不進行 Top10 排名")
                    else:
                        scored_today = score_opening_snapshot(history, snapshot_today, signal_date)
                        eligible_f04 = int(scored_today[["市場", "股票代號"]].drop_duplicates().shape[0])
                        eligible_f04_coverage = float(eligible_f04 / base_meta["universe_securities"])
                        coverage_meta["eligible_f04_securities"] = eligible_f04
                        coverage_meta["eligible_f04_coverage"] = eligible_f04_coverage
                        coverage_meta["minimum_f04_eligible_coverage"] = MIN_F04_ELIGIBLE_COVERAGE
                        if eligible_f04_coverage < MIN_F04_ELIGIBLE_COVERAGE:
                            status = "BLOCKED_LOW_F04_HISTORY_INTERSECTION_COVERAGE"
                            log(f"[BLOCK] 有效 F04 歷史交集覆蓋率 {eligible_f04_coverage:.2%} < {MIN_F04_ELIGIBLE_COVERAGE:.0%}；不以縮小後的股票池排名")
                        else:
                            selection_today, full_rank_today = build_frozen_top10(scored_today, signal_date, "PENDING_FREEZE_TIMESTAMP")
                            freeze_done = datetime.now(ZoneInfo(TZ))
                            freeze_done_ok = freeze_done.time().replace(tzinfo=None) <= freeze_completed_end
                            coverage_meta["ranking_completed_at_taipei"] = freeze_done.isoformat()
                            coverage_meta["freeze_completed_before_090058"] = bool(freeze_done_ok)
                            if not freeze_done_ok:
                                status = "BLOCKED_RANKING_COMPLETED_AFTER_090058"
                                selection_today = pd.DataFrame(columns=state_old_selections.columns)
                                log("[BLOCK] F04 排名未能在 09:00:58 前完成；不保存成正式凍結名單")
                            else:
                                selection_today["frozen_at_taipei"] = freeze_done.isoformat()
                                full_rank_today["frozen_at_taipei"] = freeze_done.isoformat()
                                selection_today["snapshot_coverage"] = float(coverage)
                                selection_today["eligible_f04_coverage"] = eligible_f04_coverage
                                compare_selection_existing(state_old_selections, selection_today)
                                status = "TOP10_FROZEN_BEFORE_ENTRY"
                                log(f"[FREEZE PASS] top10={len(selection_today)}; eligible_F04={len(full_rank_today)}; snapshot_coverage={coverage:.2%}; f04_coverage={eligible_f04_coverage:.2%}; frozen_at={freeze_done.isoformat()}")


    # Phase 2: after close, load the frozen list; never recompute or rerank it.
    elif phase == "close":
        frozen = state_old_selections[state_old_selections.get("signal_date", pd.Series(dtype=str)).astype(str) == signal_date].copy()
        rank_values = set(pd.to_numeric(frozen.get("rank", pd.Series(dtype=float)), errors="coerce").dropna().astype(int).tolist())
        if frozen.empty:
            status = "WAITING_FOR_PREOPEN_FREEZE"
            log("[BLOCK] 找不到當日 09:00 前凍結名單；拒絕使用收盤後資料補做 Top10")
        elif len(frozen) != TOP_N or rank_values != set(range(1, TOP_N + 1)):
            status = "BLOCKED_INVALID_FROZEN_TOP10_STATE"
            log(f"[BLOCK] 凍結名單筆數或排名異常：rows={len(frozen)}; ranks={sorted(rank_values)}；不下載分鐘 K")
        else:
            selection_today = frozen.sort_values("rank").reset_index(drop=True)
            saved_cov = float(pd.to_numeric(selection_today.get("snapshot_coverage", pd.Series([np.nan])), errors="coerce").dropna().iloc[0]) if selection_today.get("snapshot_coverage", pd.Series(dtype=float)).notna().any() else np.nan
            coverage_meta = {
                "signal_date": signal_date,
                "frozen_universe_size": int(base_meta["universe_securities"]),
                "selected_top10": int(len(selection_today)),
                "snapshot_coverage": saved_cov,
                "minimum_required_coverage": MIN_DAILY_UNIVERSE_COVERAGE,
                "pass": bool(pd.notna(saved_cov) and saved_cov >= MIN_DAILY_UNIVERSE_COVERAGE),
                "source": "Restored from the pre-entry frozen Top10 state; not recalculated at close.",
            }
            existing_trade = state_old_trades[state_old_trades.get("signal_date", pd.Series(dtype=str)).astype(str) == signal_date].copy() if not state_old_trades.empty else pd.DataFrame()
            if not existing_trade.empty and not args.retry_existing:
                status = "ALREADY_CLOSED"
                log(f"[SKIP] {signal_date} 已存在收盤核算紀錄；不重抓行情。需重試才加 --retry-existing")
            else:
                if not api_key:
                    raise RuntimeError("缺少 Fugle API Key；Colab Secrets 必須設定 FUGLE_API 或 FUGLE_API_KEY")
                attempt = int(pd.to_numeric(existing_trade.get("attempt", pd.Series(dtype=float)), errors="coerce").max()) + 1 if not existing_trade.empty else 1
                client = FugleClient(api_key)
                chunks, req = [], []
                symbols = selection_today["symbol"].astype(str).drop_duplicates().tolist()
                log(f"[FUGLE HISTORICAL] 收盤核算已凍結 Top10；取得 {len(symbols)} 檔 {signal_date} 單日分鐘 K")
                for i, sym in enumerate(symbols, 1):
                    b, meta = client.fetch_day(sym, signal_date)
                    chunks.append(b)
                    req.append(meta)
                    log(f"[FUGLE] {i}/{len(symbols)} {sym} status={meta.get('status')} bars={meta.get('bar_count', 0)}")
                bars_today = pd.concat(chunks, ignore_index=True) if chunks else bars_today.copy()
                requests_today = pd.DataFrame(req)
                if not bars_today.empty:
                    dup = int(bars_today.duplicated(["symbol", "datetime"]).sum())
                    invalid = int(((bars_today["high"] < bars_today[["open", "close", "low"]].max(axis=1)) |
                                   (bars_today["low"] > bars_today[["open", "close", "high"]].min(axis=1)) |
                                   (bars_today[["open", "high", "low", "close"]] <= 0).any(axis=1)).sum())
                    if dup or invalid:
                        raise ValueError(f"分鐘 K 品質檢查失敗：duplicate={dup}; invalid_OHLC={invalid}")
                trades_today = build_trade_rows(selection_today, bars_today, requests_today, run_dt.isoformat(), attempt)
                if trades_today.empty and len(trades_today.columns) == 0:
                    trades_today = pd.DataFrame(columns=TRADE_STATE_COLUMNS)
                daily_today = summarize_daily(trades_today)
                if daily_today.empty and len(daily_today.columns) == 0:
                    daily_today = pd.DataFrame(columns=DAILY_STATE_COLUMNS)
                status = "CLOSE_EVALUATED" if not trades_today.empty else "CLOSE_EVALUATED_NO_TRADES"

    else:
        status = "WAITING_FOR_PREOPEN_OR_CLOSE_WINDOW"
        log("[WAIT] 目前不在 09:00 開盤凍結窗口或 16:35 後收盤核算窗口；本次不抓行情、不建立當日排名")

    # Append immutable frozen selections and observation attempts to state.
    if not selection_today.empty and phase == "freeze" and status == "TOP10_FROZEN_BEFORE_ENTRY":
        compare_selection_existing(state_old_selections, selection_today)
        state_old_selections = pd.concat([state_old_selections, selection_today], ignore_index=True)
    if trades_today.empty and len(trades_today.columns) == 0:
        trades_today = pd.DataFrame(columns=TRADE_STATE_COLUMNS)
    if daily_today.empty and len(daily_today.columns) == 0:
        daily_today = pd.DataFrame(columns=DAILY_STATE_COLUMNS)
    if not trades_today.empty:
        state_old_trades = pd.concat([state_old_trades, trades_today], ignore_index=True)
    if not daily_today.empty:
        state_old_daily = pd.concat([state_old_daily, daily_today], ignore_index=True)
        if "attempt" not in state_old_daily.columns:
            state_old_daily["attempt"] = 1
        state_old_daily["attempt"] = pd.to_numeric(state_old_daily["attempt"], errors="coerce").fillna(1)
        sort_cols = ["signal_date", "entry_clock", "exit_method", "attempt"]
        if "observed_at_taipei" in state_old_daily.columns:
            sort_cols.append("observed_at_taipei")
        state_old_daily = state_old_daily.sort_values(sort_cols, kind="mergesort").drop_duplicates(
            ["signal_date", "entry_clock", "exit_method", "attempt"], keep="last").reset_index(drop=True)
    if not increment_manifest.empty:
        state_old_sources = pd.concat([state_old_sources, increment_manifest], ignore_index=True).drop_duplicates("sha256", keep="last")

    cum_daily = summarize_cumulative(state_old_daily) if not state_old_daily.empty else summarize_cumulative(pd.DataFrame())
    write_csv(selection_today, out_dir / "01_當日凍結Top10.csv")
    write_csv(trades_today, out_dir / "02_當日逐筆影子交易.csv")
    write_csv(daily_today, out_dir / "03_當日績效摘要.csv")
    write_csv(cum_daily, out_dir / "04_累積前瞻摘要.csv")
    write_csv(requests_today, out_dir / "05_Fugle歷史分鐘請求紀錄.csv")
    if not bars_today.empty:
        bars_today.to_csv(out_dir / "06_Fugle單日一分K.csv.gz", index=False, encoding="utf-8-sig", compression="gzip", lineterminator="\n")
    else:
        pd.DataFrame(columns=["symbol", "datetime", "date", "time", "open", "high", "low", "close", "volume"]).to_csv(
            out_dir / "06_Fugle單日一分K.csv.gz", index=False, encoding="utf-8-sig", compression="gzip")
    write_csv(increment_manifest, out_dir / "07_官方歷史來源雜湊.csv")
    write_json(coverage_meta, out_dir / "08_當日開盤快照覆蓋率.json")
    write_json(history_validation, out_dir / "09_前三個交易日日線連續性.json")
    write_csv(full_rank_today, out_dir / "10_全市場F04排序.csv")
    write_csv(snapshot_today, out_dir / "11_Fugle開盤全市場快照.csv")
    if snapshot_logs:
        write_csv(pd.DataFrame(snapshot_logs), out_dir / "12_Fugle開盤快照請求紀錄.csv")

    write_csv(state_old_selections, out_dir / "state" / STATE_SELECTIONS.name)
    write_csv(state_old_trades, out_dir / "state" / STATE_TRADES.name)
    write_csv(state_old_daily, out_dir / "state" / STATE_DAILY.name)
    write_csv(state_old_sources, out_dir / "state" / STATE_SOURCES.name)

    rules = {
        "version": VERSION, "run_id": run_id, "run_at_taipei": run_dt.isoformat(),
        "phase": phase, "status": status, "oos_start_date": OOS_START_DATE,
        "target_signal_date": signal_date,
        "factor": "F04", "factor_direction": "HIGH",
        "formula": "-expm1(rolling_sum(last_3_overnight_log_returns))",
        "factor_source_equivalence": "Same rolling 3 overnight-log formula and HIGH ranking as 6.py; today's open comes from pre-entry Fugle Snapshot Quotes, not end-of-day daily files.",
        "top_n": TOP_N, "tie_break": "factor_score DESC, symbol ASC, market ASC",
        "snapshot": {"markets": ["TSE", "OTC"], "type": "COMMONSTOCK", "freeze_window_taipei": "09:00:00-09:00:55",
                     "fields_used": ["symbol", "openPrice"], "snapshot_quotes": snapshot_logs,
                     "minimum_universe_open_price_coverage": MIN_DAILY_UNIVERSE_COVERAGE,
            "minimum_f04_eligible_history_intersection_coverage": MIN_F04_ELIGIBLE_COVERAGE},
        "entry_clocks": list(ENTRY_CLOCKS),
        "entry_selection": "first 1m bar at/after target time, max delay 5 minutes; OPEN is a price proxy",
        "exit_primary": "latest 1m bar starting 13:20..13:24; use CLOSE",
        "exit_secondary": "13:30 closing-auction 1m candle CLOSE",
        "horizon": "H0 same signal date only",
        "fee_one_way": FEE_ONE_WAY, "sell_tax": SELL_TAX,
        "slippage_each_side": list(SLIPPAGE_LEVELS), "slippage_bps_each_side": [10, 20, 30, 50],
        "missing_data": "NA; no zero imputation; a fixed equal-weight basket metric requires 10/10 valid prices",
        "no_retraining": True, "no_reranking_after_freeze": True, "no_live_orders": True,
        "history_source": {"base": base_meta, "increment_files": increment_meta.get("files"),
                           "increment_rows": increment_meta.get("rows"), "history_max_before_signal": history_max,
                           "three_previous_sessions_validation": history_validation},
        "fugle_snapshot_doc": "https://developer.fugle.tw/docs/data/http-api/snapshot/quotes/",
        "fugle_snapshot_plan_note": "Fugle docs state Snapshot Quotes is Developer plan or above; if access denied the script blocks and does not use after-close data to reconstruct the ranking.",
        "fugle_historical_candles_doc": FUGLE_DOC,
        "fugle_historical_update_note": "Historical candles complete by 16:30 after each trading day; close stage is gated to 16:35 or later.",
        "main_sha_required": REQUIRED_MAIN_SHA,
        "cumulative_state_restore": {
            "previous_branch": previous_state_branch,
            "diagnostics": STATE_RESTORE_DIAGNOSTICS,
            "empty_file_policy": "Only trades and daily observation state files may recover zero-byte inputs as schema-correct zero-row tables; frozen selections and source lineage remain strict.",
        },
        "limitations": [
            "Opening snapshot may omit securities without a valid openPrice; daily ranking is blocked below the 95% frozen-universe coverage gate.",
            "The selected list is timestamped locally before the 09:01 entry observation; GitHub upload follows the freeze, so the commit time is not claimed as a pre-entry trusted timestamp.",
            "Minute K OPEN/CLOSE values are price proxies, not guaranteed fills; no bid/ask queue, partial fills or market impact.",
            "Both entry clocks and both exit methods are predeclared arms; do not select the best retrospectively.",
            "This is a shadow test only, not a trading recommendation.",
        ],
    }
    write_json(rules, out_dir / "00_凍結規則與來源.json")
    write_json({
        "schema_version": "2.0.1",
        "files": {
            STATE_SELECTIONS.name: SELECTION_STATE_COLUMNS,
            STATE_TRADES.name: TRADE_STATE_COLUMNS,
            STATE_DAILY.name: DAILY_STATE_COLUMNS,
            STATE_SOURCES.name: SOURCE_STATE_COLUMNS,
        },
        "empty_state_policy": "Header-only CSV is the canonical representation of zero observations. Zero-byte recovery is allowed only for trades and daily observations from legacy v2.0 runs.",
    }, out_dir / "state_schema.json")

    readme = (
        f"# {VERSION}\n\n- Phase: `{phase}`\n- Status: `{status}`\n- Target date: `{signal_date}`\n"
        f"- OOS start: `{OOS_START_DATE}`\n- Run once from 08:30:00 (recommended 08:45-08:50): the program preloads history/state, waits until 09:00:05, and freezes F04 HIGH Top10 from TSE/OTC COMMONSTOCK snapshots before 09:01.\n"
        "- Run again at/after 16:35: the program loads the already-frozen list and computes H0 execution proxies.\n"
        "- The 16:35+ close phase only loads the already-frozen list and never recomputes the ranking.\n"
        "- Snapshot open-price coverage and valid F04 history-intersection coverage each must be at least 90-95% before ranking.\n"
        "- Missing prices remain NA; the 10-stock basket metric requires 10/10 valid prices.\n"
        "- FUGLE Snapshot Quotes requires Developer plan or above; lack of access blocks the prospective run.\n"
        "- Minute K prices are proxies, not confirmed fills.\n"
    )
    (out_dir / "README.md").write_text(readme, encoding="utf-8")
    core_result = {
        "status": status, "phase": phase, "run_id": run_id, "signal_date": signal_date,
        "frozen_top10": int(len(selection_today)), "full_rank_rows": int(len(full_rank_today)),
        "trade_observation_rows": int(len(trades_today)), "minute_rows": int(len(bars_today)),
        "cumulative_signal_days": int(state_old_selections["signal_date"].nunique()) if not state_old_selections.empty else 0,
        "minimum_review_signal_days": 30, "snapshot_coverage": coverage_meta,
        "history_max_before_signal": history_max, "history_validation": history_validation,
        "state_restore": {"previous_branch": previous_state_branch, "diagnostics": STATE_RESTORE_DIAGNOSTICS},
        "note": "A freeze-stage run precedes the entry observation; a close-stage run cannot recreate or alter the frozen list.",
    }
    write_json(core_result, out_dir / "RESULT.json")

    files = [q for q in out_dir.rglob("*") if q.is_file()]
    manifest = {q.relative_to(out_dir).as_posix(): {"sha256": sha256_file(q), "bytes": q.stat().st_size} for q in files}
    write_json({"run_id": run_id, "files": manifest}, out_dir / "artifact_sha256_manifest.json")
    zip_path = REPORT_ROOT / (out_dir.name + ".zip")
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
        for q in out_dir.rglob("*"):
            if q.is_file():
                zf.write(q, q.relative_to(out_dir.parent))
    zip_sha = sha256_file(zip_path)
    log(f"[ZIP PASS] {zip_path}; bytes={zip_path.stat().st_size:,}; sha256={zip_sha}")

    github = {"status": "SKIPPED_BY_FLAG"} if args.no_github_upload else upload_cumulative_audit(out_dir, zip_path, run_id)
    log(f"[GITHUB] {json.dumps(github, ensure_ascii=False)}")
    result = {**core_result, "report_dir": str(out_dir), "zip_path": str(zip_path),
              "zip_sha256": zip_sha, "github": github}
    write_json(result, out_dir / "RESULT_GITHUB.json")
    log(f"[RESULT] {json.dumps(result, ensure_ascii=False)}")
    return result


if __name__ == "__main__":
    try:
        RESULT=main()
    except Exception as e:
        log(f"[FATAL] {type(e).__name__}: {e}")
        traceback.print_exc()
        raise