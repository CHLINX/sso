#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SSO Fugle 1-minute execution-price pilot v1.0.1

Purpose
-------
Use Fugle historical 1-minute candles as a NON-OFFICIAL independent price
validation layer for frozen F02/F04/50-50 overnight-pair signals.
The program:
  1) reads the existing v2.31 shadow-daily source (read-only), verifies SHA256;
  2) recomputes the frozen HIGH-direction F02/F04 rankings for a small recent
     sample of signal dates, keeping 10 subsequent market sessions for exits;
  3) downloads 1-minute candles for only the securities selected by those
     daily Top10 lists (one API request per symbol; <= 1 year per request);
  4) measures first available minute-bar OPEN at/after 09:01 and 09:05, then
     uses the explicit 13:30 closing-auction bar as exit proxy for H=0/1/3/5/10;
  5) validates minute-bar integrity and compares minute OHLC with the source
     shadow-daily OHLC; outputs to /content only;
  6) packages a ZIP and uploads the audit folder to a fresh GitHub audit branch,
     verifying remote readback and the protected main SHA before/after.

No Drive source is modified. No trading decision is made. Minute-bar OPEN is a
price proxy, not a guaranteed fill; queue, spread, depth, partial fills and
market impact are not available from OHLCV bars.
"""
from __future__ import annotations

import argparse
import base64
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
import time
import traceback
import zipfile
from datetime import datetime, timedelta, time as dt_time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import requests

VERSION = "SSO_FUGLE_1M_EXECUTION_PRICE_PILOT_v1.0.1"
SOURCE_PATH = (
    "/content/drive/MyDrive/看股最速適/分析重跑_v1.2_20261026_155023/"
    "SSO_研究指標獨立驗證與前瞻分析_v2.31_20261116_121536/"
    "相對價格因子與前瞻報酬研究資料_v2.31.csv.gz"
)
SOURCE_SHA256 = "113a4f7cd5c94d7bbd3cb78427963dde2a6c249d66a25a66fab2c408958de71c"
BASELINE_SHA256 = "a781d4979c94ceb022a3c6ef72d1b044fd1bc2a6edb1927d33dabd80e2478ffb"
PROTECTED_MAIN_SHA = "aaabc2433fb5df2bb68af06461178305d6bd30d1"
REPO = "CHLINX/sso"
GITHUB_REMOTE = "https://github.com/CHLINX/sso.git"
FUGLE_BASE = "https://api.fugle.tw/marketdata/v1.0/stock/historical/candles"
FUGLE_DOC = "https://developer.fugle.tw/docs/data/http-api/historical/candles/"
FEE_ONE_WAY = 0.001425
SELL_TAX = 0.003
SLIPPAGE_LEVELS = (0.001, 0.002, 0.003, 0.005)
TOP_N = 10
SIGNAL_DAYS = 20
EXIT_HORIZONS = (0, 1, 3, 5, 10)
ENTRY_CLOCKS = ("09:01", "09:05")
TZ = "Asia/Taipei"
MIN_REQUEST_INTERVAL = 1.05  # <= 57 requests/minute, below Basic plan 60/min.
MAX_RETRIES = 4
REQUEST_TIMEOUT = 45


def log(message: str) -> None:
    stamp = datetime.now(ZoneInfo(TZ)).strftime("%Y-%m-%d %H:%M:%S %Z")
    print(f"[{stamp}] {message}", flush=True)


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(8 * 1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def write_json(obj: Any, path: Path) -> None:
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2, default=str), encoding="utf-8")


def write_csv(df: pd.DataFrame, path: Path) -> None:
    df.to_csv(path, index=False, encoding="utf-8-sig", lineterminator="\n")


def get_secret(name: str) -> Optional[str]:
    value = os.environ.get(name)
    if value:
        return value.strip()
    try:
        from google.colab import userdata  # type: ignore
        value = userdata.get(name)
        if value:
            return str(value).strip()
    except Exception:
        pass
    return None


def require_api_key() -> str:
    # Accept both common Colab Secret names. The user's existing name
    # FUGLE_API is preferred, with FUGLE_API_KEY retained for compatibility.
    for secret_name in ("FUGLE_API", "FUGLE_API_KEY"):
        key = get_secret(secret_name)
        if key:
            log(f"[FUGLE SECRET] PASS | name={secret_name} | value hidden")
            return key
    raise RuntimeError(
        "找不到 Fugle API Key。程式會依序讀取 Colab Secrets："
        "FUGLE_API、FUGLE_API_KEY。請確認其中一個名稱存在，"
        "填入自行申請的 Fugle API Key，並開啟此 Notebook 的存取權；"
        "不要把 API Key 貼在對話或程式碼中。官方申請入口："
        "https://developer.fugle.tw/ 。完成後重新執行 6.py。"
    )


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=VERSION)
    p.add_argument("--self-test", action="store_true")
    p.add_argument("--signal-days", type=int, default=SIGNAL_DAYS)
    p.add_argument("--top-n", type=int, default=TOP_N)
    p.add_argument("--data-path", default=SOURCE_PATH)
    p.add_argument("--no-github-upload", action="store_true")
    args, _unknown = p.parse_known_args(argv)  # ignore Colab's -f kernel.json
    if args.signal_days < 5 or args.signal_days > 60:
        p.error("--signal-days must be between 5 and 60")
    if args.top_n < 1 or args.top_n > 50:
        p.error("--top-n must be between 1 and 50")
    return args


def load_daily_source(path: Path) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    if not path.exists():
        raise FileNotFoundError(f"找不到指定來源檔：{path}")
    actual_sha = sha256_file(path)
    if actual_sha != SOURCE_SHA256:
        raise ValueError(f"來源 SHA256 不符：actual={actual_sha}; expected={SOURCE_SHA256}")
    log(f"[SOURCE SHA256] PASS {actual_sha}")
    usecols = ["日期", "市場", "股票代號", "影子開盤價", "影子最高價", "影子最低價", "影子收盤價", "價格研究區段"]
    dtype = {"市場": "string", "股票代號": "string", "價格研究區段": "string"}
    df = pd.read_csv(path, compression="gzip", usecols=usecols, dtype=dtype, low_memory=False)
    df["日期"] = pd.to_datetime(df["日期"], errors="coerce").dt.normalize()
    for c in ["影子開盤價", "影子最高價", "影子最低價", "影子收盤價"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["市場"] = df["市場"].astype("string").str.strip()
    df["股票代號"] = df["股票代號"].astype("string").str.strip().str.replace(r"\.0$", "", regex=True)
    df = df.dropna(subset=["日期", "市場", "股票代號", "影子開盤價", "影子收盤價"]).copy()
    df = df[(df["股票代號"] != "") & (df["影子開盤價"] > 0) & (df["影子收盤價"] > 0)].copy()
    # A market + symbol + date natural key is required; don't silently choose among duplicates.
    dup = df.duplicated(["市場", "股票代號", "日期"], keep=False)
    if dup.any():
        sample = df.loc[dup, ["市場", "股票代號", "日期"]].head(10).to_dict("records")
        raise ValueError(f"來源存在重複 natural key，停止以免錯配：{sample}")
    df = df.sort_values(["市場", "股票代號", "日期"], kind="mergesort").reset_index(drop=True)
    meta = {
        "path": str(path), "sha256": actual_sha, "rows": int(len(df)),
        "securities": int(df[["市場", "股票代號"]].drop_duplicates().shape[0]),
        "date_min": str(df["日期"].min().date()), "date_max": str(df["日期"].max().date()),
        "schema": {"date": "日期", "market": "市場", "symbol": "股票代號",
                   "open": "影子開盤價", "high": "影子最高價", "low": "影子最低價",
                   "close": "影子收盤價", "segment": "價格研究區段"}
    }
    if meta["date_max"] != "2026-11-06":
        raise ValueError(f"來源資料末日與基準不符：{meta['date_max']}")
    return df, meta


def compute_frozen_scores(df: pd.DataFrame) -> pd.DataFrame:
    """F02=-simple overnight gap; F04=-3-session cumulative overnight return."""
    x = df.copy()
    keys = ["市場", "股票代號"]
    g = x.groupby(keys, sort=False, observed=True)
    x["prev_close"] = g["影子收盤價"].shift(1)
    valid = (x["影子開盤價"] > 0) & (x["prev_close"] > 0)
    x["overnight_log"] = np.where(valid, np.log(x["影子開盤價"] / x["prev_close"]), np.nan)
    x["F02"] = np.where(valid, -(x["影子開盤價"] / x["prev_close"] - 1.0), np.nan)
    # Rolling three consecutive rows in each security's available daily series.
    x["F04"] = -x.groupby(keys, sort=False, observed=True)["overnight_log"].transform(
        lambda s: np.expm1(s.rolling(3, min_periods=3).sum())
    )
    # F04 is already the negative accumulated overnight simple return.
    x["F04"] = x["F04"].replace([np.inf, -np.inf], np.nan)
    x["F02"] = x["F02"].replace([np.inf, -np.inf], np.nan)
    return x


def select_top_signals(scored: pd.DataFrame, signal_dates: List[pd.Timestamp], top_n: int) -> pd.DataFrame:
    wanted = set(pd.Timestamp(d).normalize() for d in signal_dates)
    x = scored[scored["日期"].isin(wanted)].copy()
    out: List[pd.DataFrame] = []
    for d, day in x.groupby("日期", sort=True):
        a = day.dropna(subset=["F02"]).copy()
        if not a.empty:
            a["score"] = a["F02"]
            a = a.sort_values(["score", "股票代號", "市場"], ascending=[False, True, True], kind="mergesort").head(top_n)
            a["signal"] = "F02"
            a["rank"] = np.arange(1, len(a) + 1)
            out.append(a)
        b = day.dropna(subset=["F04"]).copy()
        if not b.empty:
            b["score"] = b["F04"]
            b = b.sort_values(["score", "股票代號", "市場"], ascending=[False, True, True], kind="mergesort").head(top_n)
            b["signal"] = "F04"
            b["rank"] = np.arange(1, len(b) + 1)
            out.append(b)
        c = day.dropna(subset=["F02", "F04"]).copy()
        if not c.empty:
            # Both factors have frozen HIGH direction. 50/50 average of daily percentile ranks.
            # pct=True with ascending=True maps the largest HIGH-direction score to 1.0.
            c["rank_F02_pct"] = c["F02"].rank(method="average", ascending=True, pct=True)
            c["rank_F04_pct"] = c["F04"].rank(method="average", ascending=True, pct=True)
            c["score"] = (c["rank_F02_pct"] + c["rank_F04_pct"]) / 2.0
            c = c.sort_values(["score", "股票代號", "市場"], ascending=[False, True, True], kind="mergesort").head(top_n)
            c["signal"] = "OVERNIGHT_PAIR"
            c["rank"] = np.arange(1, len(c) + 1)
            out.append(c)
    if not out:
        raise ValueError("選股樣本為空；請檢查來源日期與因子有效值")
    z = pd.concat(out, ignore_index=True)
    z = z.rename(columns={"日期": "signal_date", "市場": "market", "股票代號": "symbol",
                          "影子開盤價": "shadow_open", "影子最高價": "shadow_high",
                          "影子最低價": "shadow_low", "影子收盤價": "shadow_close",
                          "價格研究區段": "price_segment"})
    keep = ["signal_date", "signal", "rank", "score", "market", "symbol", "F02", "F04",
            "shadow_open", "shadow_high", "shadow_low", "shadow_close", "price_segment"]
    return z[keep].sort_values(["signal_date", "signal", "rank"]).reset_index(drop=True)


def normalize_fugle_data(payload: Dict[str, Any], symbol: str) -> pd.DataFrame:
    rows = payload.get("data")
    if not isinstance(rows, list):
        raise ValueError(f"Fugle 回應缺少 data list: {symbol}")
    if not rows:
        return pd.DataFrame(columns=["symbol", "datetime", "date", "time", "open", "high", "low", "close", "volume"])
    b = pd.DataFrame(rows)
    required = ["date", "open", "high", "low", "close"]
    missing = [c for c in required if c not in b.columns]
    if missing:
        raise ValueError(f"Fugle 回應欄位缺少 {missing}: {symbol}")
    b["datetime"] = pd.to_datetime(b["date"], errors="coerce", utc=True).dt.tz_convert(TZ).dt.tz_localize(None)
    for c in ["open", "high", "low", "close", "volume"]:
        if c not in b.columns:
            b[c] = np.nan
        b[c] = pd.to_numeric(b[c], errors="coerce")
    b = b.dropna(subset=["datetime", "open", "high", "low", "close"]).copy()
    b["symbol"] = symbol
    b["date"] = b["datetime"].dt.normalize()
    b["time"] = b["datetime"].dt.strftime("%H:%M")
    cols = ["symbol", "datetime", "date", "time", "open", "high", "low", "close", "volume"]
    b = b[cols].sort_values("datetime")
    # Keep repeated timestamps here so the quality report can detect them.
    return b.reset_index(drop=True)


class FugleClient:
    def __init__(self, api_key: str):
        self.session = requests.Session()
        self.session.headers.update({"X-API-KEY": api_key, "Accept": "application/json"})
        self.last_call = 0.0

    def candles(self, symbol: str, start_date: str, end_date: str) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        url = f"{FUGLE_BASE}/{symbol}"
        params = {"timeframe": "1", "from": start_date, "to": end_date,
                  "fields": "open,high,low,close,volume", "sort": "asc"}
        for attempt in range(1, MAX_RETRIES + 1):
            wait = MIN_REQUEST_INTERVAL - (time.monotonic() - self.last_call)
            if wait > 0:
                time.sleep(wait)
            try:
                response = self.session.get(url, params=params, timeout=REQUEST_TIMEOUT)
                self.last_call = time.monotonic()
            except requests.RequestException as e:
                if attempt == MAX_RETRIES:
                    return pd.DataFrame(), {"symbol": symbol, "status": "REQUEST_EXCEPTION", "detail": str(e)[:500]}
                time.sleep(min(2 ** attempt, 30))
                continue
            if response.status_code == 200:
                try:
                    payload = response.json()
                    bars = normalize_fugle_data(payload, symbol)
                    return bars, {"symbol": symbol, "status": "OK" if len(bars) else "EMPTY_200",
                                  "http_status": 200, "bars": int(len(bars)),
                                  "exchange": payload.get("exchange"), "market": payload.get("market"),
                                  "timeframe": payload.get("timeframe")}
                except Exception as e:
                    return pd.DataFrame(), {"symbol": symbol, "status": "BAD_RESPONSE", "http_status": 200,
                                            "detail": str(e)[:500]}
            if response.status_code == 404:
                return pd.DataFrame(), {"symbol": symbol, "status": "NO_DATA_404", "http_status": 404,
                                        "detail": response.text[:300]}
            if response.status_code in (401, 403):
                raise RuntimeError(f"Fugle API 認證或權限失敗 HTTP {response.status_code}；請確認 FUGLE_API/FUGLE_API_KEY 與歷史行情權限。")
            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After")
                try:
                    delay = max(60.0, float(retry_after)) if retry_after else 65.0
                except Exception:
                    delay = 65.0
                log(f"[RATE LIMIT] {symbol} HTTP 429；等待 {delay:.0f}s 後重試")
                time.sleep(delay)
                continue
            if response.status_code >= 500 and attempt < MAX_RETRIES:
                time.sleep(min(2 ** attempt, 30))
                continue
            return pd.DataFrame(), {"symbol": symbol, "status": f"HTTP_{response.status_code}",
                                    "http_status": response.status_code, "detail": response.text[:500]}
        return pd.DataFrame(), {"symbol": symbol, "status": "RETRIES_EXHAUSTED"}


def expected_exit_date(signal_date: pd.Timestamp, horizon: int, market_dates: List[pd.Timestamp]) -> Optional[pd.Timestamp]:
    lookup = {pd.Timestamp(d).normalize(): i for i, d in enumerate(market_dates)}
    idx = lookup.get(pd.Timestamp(signal_date).normalize())
    if idx is None or idx + horizon >= len(market_dates):
        return None
    return pd.Timestamp(market_dates[idx + horizon]).normalize()


def select_entry_bar(daybars: pd.DataFrame, clock: str) -> Tuple[Optional[float], Optional[str], Optional[float]]:
    if daybars.empty:
        return None, None, None
    threshold = pd.Timestamp(daybars["date"].iloc[0]) + pd.Timedelta(hours=int(clock[:2]), minutes=int(clock[3:]))
    latest_allowed = threshold + pd.Timedelta(minutes=10)
    cand = daybars[(daybars["datetime"] >= threshold) & (daybars["datetime"] <= latest_allowed)].sort_values("datetime")
    if cand.empty:
        return None, None, None
    row = cand.iloc[0]
    delay = (row["datetime"] - threshold).total_seconds() / 60.0
    return float(row["open"]), row["datetime"].strftime("%Y-%m-%d %H:%M"), float(delay)


def get_close_auction(daybars: pd.DataFrame) -> Tuple[Optional[float], Optional[str], str]:
    if daybars.empty:
        return None, None, "NO_BARS"
    close_bar = daybars[daybars["time"] == "13:30"]
    if close_bar.empty:
        return None, None, "NO_1330_AUCTION_BAR"
    row = close_bar.sort_values("datetime").iloc[-1]
    return float(row["close"]), row["datetime"].strftime("%Y-%m-%d %H:%M"), "CLOSE_AUCTION_1330"


def validate_minute_bars(bars: pd.DataFrame) -> pd.DataFrame:
    if bars.empty:
        return pd.DataFrame(columns=["symbol", "date", "bars", "duplicate_datetime", "invalid_ohlc",
                                     "has_0900", "has_1330", "minute_high", "minute_low", "minute_close"])
    out = []
    for (symbol, date), g in bars.groupby(["symbol", "date"], sort=True):
        g = g.sort_values("datetime")
        dup = int(g["datetime"].duplicated().sum())
        invalid = int(((g["high"] < g[["open", "close", "low"]].max(axis=1)) |
                       (g["low"] > g[["open", "close", "high"]].min(axis=1)) |
                       (g[["open", "high", "low", "close"]] <= 0).any(axis=1)).sum())
        exit_px, _, exit_status = get_close_auction(g)
        out.append({"symbol": symbol, "date": str(pd.Timestamp(date).date()), "bars": int(len(g)),
                    "duplicate_datetime": dup, "invalid_ohlc": invalid,
                    "has_0900": bool((g["time"] == "09:00").any()),
                    "has_1330": bool((g["time"] == "13:30").any()),
                    "minute_high": float(g["high"].max()), "minute_low": float(g["low"].min()),
                    "minute_close": exit_px, "close_status": exit_status})
    return pd.DataFrame(out)


def compare_shadow_daily(selections: pd.DataFrame, bars: pd.DataFrame) -> pd.DataFrame:
    if bars.empty or selections.empty:
        return pd.DataFrame()
    daily = []
    for (symbol, date), g in bars.groupby(["symbol", "date"], sort=True):
        g = g.sort_values("datetime")
        day_open = g.iloc[0]["open"]
        day_high = g["high"].max()
        day_low = g["low"].min()
        day_close, _, status = get_close_auction(g)
        daily.append({"symbol": symbol, "date": pd.Timestamp(date), "minute_open": day_open,
                      "minute_high": day_high, "minute_low": day_low, "minute_close": day_close,
                      "close_status": status})
    d = pd.DataFrame(daily)
    sel = selections[["signal_date", "signal", "market", "symbol", "shadow_open", "shadow_high", "shadow_low", "shadow_close"]].drop_duplicates(["signal_date", "signal", "symbol"])
    x = sel.merge(d, left_on=["signal_date", "symbol"], right_on=["date", "symbol"], how="left")
    for a, b in [("open", "shadow_open"), ("high", "shadow_high"), ("low", "shadow_low"), ("close", "shadow_close")]:
        x[f"{a}_abs_diff"] = (x[f"minute_{a}"] - x[b]).abs()
        x[f"{a}_pct_diff"] = x[f"{a}_abs_diff"] / x[b].abs()
        x[f"{a}_match_1pct"] = x[f"{a}_pct_diff"] <= 0.01
    return x.drop(columns=["date"], errors="ignore")


def evaluate_selected_trades(selections: pd.DataFrame, bars: pd.DataFrame, market_dates: List[pd.Timestamp]) -> pd.DataFrame:
    if selections.empty:
        return pd.DataFrame()
    by_symbol = {s: g.sort_values("datetime") for s, g in bars.groupby("symbol", sort=False)} if not bars.empty else {}
    rows = []
    for p in selections.itertuples(index=False):
        sym = str(p.symbol)
        symbars = by_symbol.get(sym, pd.DataFrame())
        sig_date = pd.Timestamp(p.signal_date).normalize()
        sig_day = symbars[symbars["date"] == sig_date] if not symbars.empty else pd.DataFrame()
        for clock in ENTRY_CLOCKS:
            entry_px, entry_dt, entry_delay = select_entry_bar(sig_day, clock)
            for h in EXIT_HORIZONS:
                exit_date = expected_exit_date(sig_date, h, market_dates)
                exit_bars = symbars[symbars["date"] == exit_date] if (not symbars.empty and exit_date is not None) else pd.DataFrame()
                exit_px, exit_dt, exit_status = get_close_auction(exit_bars)
                if entry_px is None:
                    status = "NO_ENTRY_BAR_AFTER_TIME"
                elif exit_date is None:
                    status = "NO_FORWARD_MARKET_DATE"
                elif exit_px is None:
                    status = exit_status
                else:
                    status = "OK"
                gross = (exit_px / entry_px - 1.0) if status == "OK" else np.nan
                rec = {"signal_date": str(sig_date.date()), "signal": p.signal, "rank": int(p.rank),
                       "symbol": sym, "market": p.market, "factor_score": float(p.score),
                       "entry_clock": clock, "entry_bar_datetime": entry_dt, "entry_price_proxy": entry_px,
                       "entry_delay_minutes": entry_delay, "horizon": h,
                       "exit_date": str(exit_date.date()) if exit_date is not None else None,
                       "exit_bar_datetime": exit_dt, "exit_price_proxy": exit_px,
                       "exit_status": exit_status, "status": status, "gross_return": gross}
                for slip in SLIPPAGE_LEVELS:
                    bps = int(round(slip * 10000))
                    net = ((1.0 + gross) * ((1.0 - slip) / (1.0 + slip)) *
                           ((1.0 - FEE_ONE_WAY) * (1.0 - SELL_TAX) / (1.0 + FEE_ONE_WAY)) - 1.0) if status == "OK" else np.nan
                    rec[f"net_{bps}bp"] = net
                rows.append(rec)
    return pd.DataFrame(rows)


def summarize_trades(trades: pd.DataFrame) -> pd.DataFrame:
    if trades.empty:
        return pd.DataFrame()
    rows = []
    for (signal, clock, horizon), g in trades.groupby(["signal", "entry_clock", "horizon"], sort=True):
        ok = g[g["status"] == "OK"].copy()
        r = {"signal": signal, "entry_clock": clock, "horizon": int(horizon),
             "selected_rows": int(len(g)), "valid_trades": int(len(ok)),
             "coverage": float(len(ok) / len(g)) if len(g) else np.nan,
             "signal_dates": int(g["signal_date"].nunique()),
             "mean_gross": float(ok["gross_return"].mean()) if len(ok) else np.nan,
             "median_gross": float(ok["gross_return"].median()) if len(ok) else np.nan,
             "gross_positive_ratio": float((ok["gross_return"] > 0).mean()) if len(ok) else np.nan}
        for bps in [10, 20, 30, 50]:
            col = f"net_{bps}bp"
            r[f"mean_{col}"] = float(ok[col].mean()) if len(ok) else np.nan
            r[f"positive_ratio_{col}"] = float((ok[col] > 0).mean()) if len(ok) else np.nan
        rows.append(r)
    return pd.DataFrame(rows)


def get_github_token() -> Optional[str]:
    return get_secret("GITHUB_SSO_TOKEN") or get_secret("GITHUB_TOKEN")


def git_run(repo_dir: Path, args: List[str], auth_header: Optional[str] = None,
            check: bool = True, capture: bool = True) -> subprocess.CompletedProcess:
    cmd = ["git"]
    if auth_header:
        cmd += ["-c", f"http.extraHeader=AUTHORIZATION: basic {auth_header}"]
    cmd += args
    return subprocess.run(cmd, cwd=str(repo_dir), check=check,
                          stdout=subprocess.PIPE if capture else None,
                          stderr=subprocess.PIPE if capture else None)


def upload_audit(report_dir: Path, zip_path: Path, run_id: str) -> Dict[str, Any]:
    token = get_github_token()
    if not token:
        raise RuntimeError("找不到 GitHub token（GITHUB_SSO_TOKEN/GITHUB_TOKEN），無法上傳 audit branch。")
    auth = base64.b64encode(f"x-access-token:{token}".encode()).decode()
    work = Path("/content") / f".sso_fugle_git_{run_id}"
    if work.exists():
        shutil.rmtree(work)
    work.mkdir(parents=True)
    git_run(work, ["init"])
    git_run(work, ["config", "user.name", "SSO Audit Bot"])
    git_run(work, ["config", "user.email", "sso-audit@users.noreply.github.com"])
    git_run(work, ["remote", "add", "origin", GITHUB_REMOTE])
    branch = f"audit/sso-fugle-1m-pilot-v1_{run_id}"
    ls = git_run(work, ["ls-remote", "origin", "refs/heads/main"], auth)
    main_sha = ls.stdout.decode().strip().split()[0] if ls.stdout.strip() else ""
    if main_sha != PROTECTED_MAIN_SHA:
        raise RuntimeError(f"保護性停止：GitHub main SHA 改變。actual={main_sha}; expected={PROTECTED_MAIN_SHA}")
    git_run(work, ["fetch", "--depth=1", "origin", "+refs/heads/main:refs/remotes/origin/main"], auth)
    git_run(work, ["checkout", "-b", branch, "refs/remotes/origin/main"])
    dest = work / "audit_readout" / f"sso_fugle_1m_execution_pilot_v1_{run_id}"
    (dest / "report").mkdir(parents=True)
    for p in report_dir.iterdir():
        if p.is_file():
            shutil.copy2(p, dest / "report" / p.name)
    shutil.copy2(zip_path, dest / zip_path.name)
    git_run(work, ["add", "-A"])
    git_run(work, ["commit", "-m", f"SSO Fugle 1m execution price pilot {run_id}"])
    commit_sha = git_run(work, ["rev-parse", "HEAD"]).stdout.decode().strip()
    git_run(work, ["push", "origin", f"HEAD:refs/heads/{branch}"], auth)
    # Explicitly fetch the new branch into a tracking ref, then hash every report artifact.
    git_run(work, ["fetch", "origin", f"+refs/heads/{branch}:refs/remotes/origin/{branch}"], auth)
    tracked = [p for p in (dest / "report").iterdir() if p.is_file()]
    tracked.append(dest / zip_path.name)
    readback = 0
    for local in tracked:
        rel = local.relative_to(work).as_posix()
        blob = git_run(work, ["cat-file", "blob", f"refs/remotes/origin/{branch}:{rel}"]).stdout
        if hashlib.sha256(blob).hexdigest() != sha256_file(local):
            raise RuntimeError(f"GitHub readback SHA mismatch: {rel}")
        readback += 1
    ls2 = git_run(work, ["ls-remote", "origin", "refs/heads/main"], auth)
    main_after = ls2.stdout.decode().strip().split()[0] if ls2.stdout.strip() else ""
    if main_after != PROTECTED_MAIN_SHA:
        raise RuntimeError(f"保護性檢查失敗：main SHA after upload={main_after}")
    shutil.rmtree(work, ignore_errors=True)
    return {"status": "PASS", "branch": branch, "commit": commit_sha,
            "readback": readback, "main_before": main_sha, "main_after": main_after,
            "url": f"https://github.com/{REPO}/tree/{branch}/audit_readout/sso_fugle_1m_execution_pilot_v1_{run_id}"}


def make_self_test() -> None:
    dates = pd.bdate_range("2026-01-05", periods=35)
    rows = []
    for j in range(35):
        for i in range(30):
            base = 20 + i * 0.3
            close_prev = base * (1 + 0.0008 * (j % 3))
            op = close_prev * (1 + ((i % 7) - 3) * 0.001)
            cl = op * (1 + ((i % 5) - 2) * 0.001)
            rows.append({"日期": dates[j], "市場": "TWSE", "股票代號": f"{i+1000:04d}",
                         "影子開盤價": op, "影子最高價": max(op, cl) * 1.002,
                         "影子最低價": min(op, cl) * 0.998, "影子收盤價": cl,
                         "價格研究區段": "TEST"})
    raw = pd.DataFrame(rows).sort_values(["市場", "股票代號", "日期"])
    scored = compute_frozen_scores(raw)
    one = scored[scored["股票代號"] == "1000"].sort_values("日期").reset_index(drop=True)
    log_values = one["overnight_log"].iloc[1:4].to_numpy(dtype=float)
    expected_f04 = -np.expm1(log_values.sum())
    assert np.isclose(one.loc[3, "F04"], expected_f04, rtol=0, atol=1e-12)
    chosen_dates = list(dates[-15:-10])
    picked = select_top_signals(scored, chosen_dates, 10)
    assert set(picked["signal"].unique()) == {"F02", "F04", "OVERNIGHT_PAIR"}
    assert picked.groupby(["signal_date", "signal"]).size().eq(10).all()
    pair_day = scored[scored["日期"] == chosen_dates[-1]].dropna(subset=["F02", "F04"]).copy()
    pair_day["r2"] = pair_day["F02"].rank(method="average", ascending=True, pct=True)
    pair_day["r4"] = pair_day["F04"].rank(method="average", ascending=True, pct=True)
    pair_day["pair"] = (pair_day["r2"] + pair_day["r4"]) / 2
    best = pair_day.sort_values("pair", ascending=False).iloc[0]
    assert best["pair"] == pair_day["pair"].max()
    test_payload = {"data": [
        {"date": "2026-01-05T09:00:00.000+08:00", "open": 10, "high": 10.1, "low": 9.9, "close": 10, "volume": 1},
        {"date": "2026-01-05T09:01:00.000+08:00", "open": 10.1, "high": 10.2, "low": 10, "close": 10.15, "volume": 2},
        {"date": "2026-01-05T09:05:00.000+08:00", "open": 10.2, "high": 10.3, "low": 10.1, "close": 10.25, "volume": 2},
        {"date": "2026-01-05T13:30:00.000+08:00", "open": 10.4, "high": 10.4, "low": 10.4, "close": 10.4, "volume": 5},
    ]}
    bars = normalize_fugle_data(test_payload, "1000")
    e1, t1, d1 = select_entry_bar(bars, "09:01")
    e5, t5, d5 = select_entry_bar(bars, "09:05")
    assert e1 == 10.1 and t1.endswith("09:01") and d1 == 0
    assert e5 == 10.2 and t5.endswith("09:05") and d5 == 0
    px, t, status = get_close_auction(bars)
    assert px == 10.4 and status == "CLOSE_AUCTION_1330"
    q = validate_minute_bars(bars)
    assert len(q) == 1 and int(q.iloc[0]["bars"]) == 4
    # Full miniature evaluation: signal-date entry, H0 close-auction exit.
    picked_one = picked.iloc[[0]].copy()
    picked_one["signal_date"] = pd.Timestamp("2026-01-05")
    picked_one["market"] = "TWSE"
    picked_one["symbol"] = "1000"
    trade_rows = evaluate_selected_trades(picked_one, bars, [pd.Timestamp("2026-01-05")])
    assert len(trade_rows) == len(ENTRY_CLOCKS) * len(EXIT_HORIZONS)
    h0 = trade_rows[(trade_rows["entry_clock"] == "09:01") & (trade_rows["horizon"] == 0)].iloc[0]
    assert h0["status"] == "OK" and abs(float(h0["gross_return"]) - (10.4 / 10.1 - 1)) < 1e-12
    sm = summarize_trades(trade_rows)
    assert len(sm) == len(ENTRY_CLOCKS) * len(EXIT_HORIZONS)
    log(f"[SELF TEST] PASS; selected={len(picked)}; signals={picked['signal'].nunique()}; entry_09:01={e1}; entry_09:05={e5}; exit_13:30={px}; trade_eval=PASS")


def main(argv: Optional[List[str]] = None) -> Dict[str, Any]:
    args = parse_args(argv)
    if args.self_test:
        make_self_test()
        return {"self_test": "PASS"}

    api_key = require_api_key()
    github_token = get_github_token()
    if not github_token and not args.no_github_upload:
        raise RuntimeError("缺少 GITHUB_SSO_TOKEN/GITHUB_TOKEN；請確認 Colab Secrets，或明確以 --no-github-upload 執行。")
    path = Path(args.data_path)
    if not path.exists():
        raise FileNotFoundError(f"來源資料不存在：{path}")

    run_id = datetime.now(ZoneInfo(TZ)).strftime("%Y%m%d_%H%M%S")
    out = Path("/content") / f"SSO_Fugle_1m_Execution_Pilot_v1_{run_id}"
    out.mkdir(parents=True, exist_ok=False)
    log("=" * 96)
    log(f"{VERSION}｜開始")
    log("[SAFETY] Drive source read-only；只讀 v2.31；輸出只寫 /content；Fugle 分鐘資料非官方獨立驗證層")
    try:
        if not path.exists() and str(path).startswith("/content/drive/"):
            try:
                from google.colab import drive  # type: ignore
                log("[DRIVE] Google Drive 尚未掛載，嘗試掛載唯讀來源路徑")
                drive.mount("/content/drive")
            except Exception as mount_exc:
                log(f"[DRIVE MOUNT] {type(mount_exc).__name__}: {mount_exc}")
        daily, source_meta = load_daily_source(path)
        log(f"[SOURCE] rows={len(daily):,}; securities={source_meta['securities']}; dates={source_meta['date_min']}..{source_meta['date_max']}")
        all_dates = sorted(pd.Timestamp(x).normalize() for x in daily["日期"].drop_duplicates().tolist())
        if len(all_dates) <= args.signal_days + max(EXIT_HORIZONS):
            raise ValueError("可用市場交易日不足以建立樣本與 H10 後續退出日期")
        signal_dates = all_dates[-(args.signal_days + max(EXIT_HORIZONS)):-max(EXIT_HORIZONS)]
        # Include the last date needed for H10 exits; this is the latest source date.
        sample_start = signal_dates[0]
        sample_end = all_dates[-1]
        log(f"[SAMPLE] signal_days={len(signal_dates)}; {signal_dates[0].date()}..{signal_dates[-1].date()}; exit_data_end={sample_end.date()}; H10 forward sessions preserved")
        scored = compute_frozen_scores(daily)
        selections = select_top_signals(scored, signal_dates, args.top_n)
        log(f"[SELECTION] rows={len(selections):,}; dates={selections['signal_date'].nunique()}; unique_symbols={selections['symbol'].nunique()}; top_n={args.top_n}; directions=F02/F04 HIGH; pair=50/50 percentile-rank")
        write_csv(selections, out / "01_凍結訊號TopN樣本.csv")
        symbol_market_pairs = selections[["symbol", "market"]].drop_duplicates()
        market_counts = symbol_market_pairs.groupby("symbol")["market"].nunique()
        ambiguous = market_counts[market_counts > 1]
        if len(ambiguous):
            raise ValueError(
                "同一股票代號在來源中對應多個市場；Fugle historical candles 路徑只以 symbol 查詢，"
                f"為避免跨市場錯配，停止執行。ambiguous={ambiguous.index.tolist()[:20]}"
            )
        symbols = sorted(selections["symbol"].dropna().astype(str).unique().tolist())
        start_s, end_s = str(sample_start.date()), str(sample_end.date())
        # Single symbol query; the range is much less than one year.
        client = FugleClient(api_key)
        all_bars, request_logs = [], []
        log(f"[FUGLE] endpoint=historical/candles; timeframe=1; symbols={len(symbols)}; range={start_s}..{end_s}; planned_limit~57 requests/min")
        for ix, symbol in enumerate(symbols, 1):
            bars_one, req_meta = client.candles(symbol, start_s, end_s)
            request_logs.append(req_meta)
            if not bars_one.empty:
                all_bars.append(bars_one)
            if ix % 25 == 0 or ix == len(symbols):
                log(f"[FUGLE] progress={ix}/{len(symbols)}; successful_symbols={sum(1 for q in request_logs if q.get('status')=='OK')}; bars={sum(len(z) for z in all_bars):,}")
        bars = pd.concat(all_bars, ignore_index=True) if all_bars else pd.DataFrame(columns=["symbol", "datetime", "date", "time", "open", "high", "low", "close", "volume"])
        bars = bars.sort_values(["symbol", "datetime"]).reset_index(drop=True) if not bars.empty else bars
        log(f"[FUGLE RESULT] requests={len(request_logs)}; ok={sum(q.get('status')=='OK' for q in request_logs)}; no_data_404={sum(q.get('status')=='NO_DATA_404' for q in request_logs)}; other_fail={sum(q.get('status') not in ('OK','NO_DATA_404') for q in request_logs)}; minute_rows={len(bars):,}")
        write_csv(pd.DataFrame(request_logs), out / "02_Fugle請求紀錄.csv")
        write_csv(bars, out / "03_Fugle一分K資料.csv.gz")
        quality = validate_minute_bars(bars)
        write_csv(quality, out / "04_分鐘K品質檢查.csv")
        comparison = compare_shadow_daily(selections, bars)
        write_csv(comparison, out / "05_分鐘與影子日線對照.csv")
        trades = evaluate_selected_trades(selections, bars, all_dates)
        summary = summarize_trades(trades)
        write_csv(trades, out / "06_09時後進場逐筆績效.csv")
        write_csv(summary, out / "07_09時後進場績效摘要.csv")
        failures = pd.DataFrame([x for x in request_logs if x.get("status") != "OK"])
        write_csv(failures, out / "08_未取得行情或請求異常.csv")

        data_quality_summary = {
            "minute_rows": int(len(bars)), "symbols_requested": len(symbols),
            "symbols_ok": int(sum(q.get("status") == "OK" for q in request_logs)),
            "symbols_404": int(sum(q.get("status") == "NO_DATA_404" for q in request_logs)),
            "symbols_other_failed": int(sum(q.get("status") not in ("OK", "NO_DATA_404") for q in request_logs)),
            "symbol_days": int(len(quality)),
            "symbol_days_with_1330": int(quality["has_1330"].sum()) if len(quality) else 0,
            "symbol_days_with_0900": int(quality["has_0900"].sum()) if len(quality) else 0,
            "duplicate_minute_timestamps": int(quality["duplicate_datetime"].sum()) if len(quality) else 0,
            "invalid_ohlc_rows": int(quality["invalid_ohlc"].sum()) if len(quality) else 0,
            "comparison_rows": int(len(comparison)),
            "open_match_1pct_ratio": float(comparison["open_match_1pct"].mean()) if len(comparison) else None,
            "high_match_1pct_ratio": float(comparison["high_match_1pct"].mean()) if len(comparison) else None,
            "low_match_1pct_ratio": float(comparison["low_match_1pct"].mean()) if len(comparison) else None,
            "close_match_1pct_ratio": float(comparison["close_match_1pct"].mean()) if len(comparison) else None,
            "trade_rows": int(len(trades)), "valid_trade_rows": int((trades["status"] == "OK").sum()) if len(trades) else 0
        }
        params = {
            "version": VERSION, "runtime_taipei": datetime.now(ZoneInfo(TZ)).isoformat(),
            "source": source_meta, "baseline_daily_ic_sha256": BASELINE_SHA256,
            "provider": "Fugle / 富果行情 REST API (non-official independent validation layer)",
            "provider_docs": FUGLE_DOC, "endpoint": f"{FUGLE_BASE}/{{symbol}}",
            "request": {"timeframe": "1", "from": start_s, "to": end_s,
                        "fields": "open,high,low,close,volume", "sort": "asc",
                        "date_range_less_than_one_year": True, "planned_rate_per_minute": 57},
            "signal": {"F02": "-(current shadow open / previous available shadow close - 1)",
                       "F04": "-expm1(sum of current and prior two available-session log(open / previous close))",
                       "directions": {"F02": "HIGH", "F04": "HIGH"},
                       "overnight_pair": "50/50 mean of daily cross-sectional percentile ranks; both factors HIGH",
                       "selection": f"Top {args.top_n} by frozen score per signal per date; no refit or weight optimization"},
            "entry_exit": {"entry": "first 1-minute candle whose timestamp is >= 09:01 or >= 09:05 (within 10 minutes); use that candle OPEN as price proxy",
                           "exit": "13:30 separate closing-auction candle CLOSE on signal date + H market sessions",
                           "horizons": list(EXIT_HORIZONS),
                           "minute_bar_timestamp": "bar start time; 09:00 bar covers 09:00:00-09:00:59",
                           "execution_caveat": "OHLCV candle open/close is not a guaranteed order fill; no bid/ask queue, spread, depth, partial fills or impact"},
            "cost": {"fee_one_way": FEE_ONE_WAY, "sell_tax": SELL_TAX,
                     "one_way_slippage_sensitivity": list(SLIPPAGE_LEVELS),
                     "formula": "(1+gross)*((1-slip)/(1+slip))*((1-fee)*(1-tax)/(1+fee))-1",
                     "not_included": ["minimum commission", "bid-ask spread beyond slippage assumption", "queue position", "partial fills", "market impact"]},
            "sample": {"signal_dates": [str(d.date()) for d in signal_dates],
                       "signal_start": start_s, "signal_end": str(signal_dates[-1].date()),
                       "minute_data_end": end_s, "signal_days": len(signal_dates),
                       "top_n_per_signal_day": args.top_n, "unique_symbols": len(symbols)},
            "data_quality_summary": data_quality_summary,
            "status": "PILOT_DIAGNOSTIC_ONLY_NOT_TRADING_APPROVAL"
        }
        write_json(params, out / "00_方法來源與執行限制.json")
        readme = f"""# {VERSION}\n\n- Fugle is a non-official independent minute-price validation layer; the SSO source data remains read-only.\n- Sample signal dates: {start_s} through {signal_dates[-1].date()} ({len(signal_dates)} sessions); minute data downloaded through {end_s} to preserve H10 exits.\n- Selection: F02 HIGH, F04 HIGH, OVERNIGHT_PAIR = 50/50 mean cross-sectional percentile ranks; daily Top{args.top_n}.\n- Entry: first 1-minute bar at or after 09:01 / 09:05, using bar OPEN as a proxy. Exit: explicit 13:30 closing-auction bar CLOSE after H=0/1/3/5/10 further market sessions.\n- OHLCV bar prices do not guarantee actual execution. No spread/depth/queue/partial fill/market-impact simulation.\n- Historical 1-minute API is limited to 60 historical requests per minute on the published plans; this run throttles below that.\n- This is a short sample pilot, not a full OOS validation or trading approval.\n\n## Key counts\n\n{json.dumps(data_quality_summary, ensure_ascii=False, indent=2)}\n"""
        (out / "README.md").write_text(readme, encoding="utf-8")
        shutil.copy2(Path(__file__), out / "6.py")

        manifest = {}
        for f in sorted(out.iterdir()):
            if f.is_file() and f.name != "artifact_sha256_manifest.json":
                manifest[f.name] = {"bytes": f.stat().st_size, "sha256": sha256_file(f)}
        write_json({"files": manifest, "source_sha256": SOURCE_SHA256}, out / "artifact_sha256_manifest.json")

        zip_path = Path("/content") / f"SSO_Fugle_1m_Execution_Pilot_v1_{run_id}.zip"
        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
            for f in sorted(out.iterdir()):
                if f.is_file():
                    zf.write(f, arcname=f"{out.name}/{f.name}")
        zip_sha = sha256_file(zip_path)
        log(f"[ZIP PASS] {zip_path}; bytes={zip_path.stat().st_size:,}; sha256={zip_sha}")

        upload_result = {"status": "SKIPPED"}
        if not args.no_github_upload:
            upload_result = upload_audit(out, zip_path, run_id)
            log(f"[GITHUB] {json.dumps(upload_result, ensure_ascii=False)}")
        result = {"status": "SUCCESS", "report_dir": str(out), "zip_path": str(zip_path),
                  "zip_sha256": zip_sha, "quality": data_quality_summary, "github": upload_result}
        write_json(result, out / "FINAL_RESULT.json")
        log(f"[{VERSION}] 完成；report={out}; zip={zip_path}")
        return result
    except Exception as exc:
        log(f"[FATAL] {type(exc).__name__}: {exc}")
        (out / "ERROR.txt").write_text(traceback.format_exc(), encoding="utf-8")
        log(f"[TRACEBACK] {out / 'ERROR.txt'}")
        raise


if __name__ == "__main__":
    RESULT = main()
