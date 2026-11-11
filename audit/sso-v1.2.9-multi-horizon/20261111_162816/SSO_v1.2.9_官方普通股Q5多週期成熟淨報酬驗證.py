# -*- coding: utf-8 -*-
"""
SSO v1.2.9｜官方普通股 Q5 多週期成熟淨報酬驗證

承接 v1.2.8：
- 原凍結 Q5 與官方普通股重新排名 Q5，兩種口徑分開。
- 沿用 v1.2.6 特徵重建、v1.2.7 凍結訊號及 v1.2.2 成本公式。
- 官方行情截至 2026-11-10。
- 只讀取既有 Drive 與 GitHub 封存資料，不呼叫官方 API。
- 一次建立共用價格索引，計算 H=1、3、5、10。
- H=10 必須與 v1.2.8 的 747 筆成熟樣本逐筆一致，否則停止。
- 跨日期重複入選與跨週期表現分開統計。
- 不依短期樣本硬選 10 檔股票。
- 所有輸出放在新的唯一資料夾。
- GitHub 僅建立獨立 audit branch，逐檔核對 SHA。
- GitHub main SHA 前後必須一致。
"""

from __future__ import annotations

import os
import sys
import re
import io
import json
import uuid
import hashlib
import zipfile
import tempfile
import subprocess
import importlib.util
import urllib.request

from pathlib import Path
from datetime import datetime
from collections import defaultdict
from urllib.parse import quote
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd


# =============================================================================
# 01. 固定來源及版本
# =============================================================================

VERSION = "SSO_v1.2.9_官方普通股Q5多週期成熟淨報酬驗證"

ROOT = Path("/content/drive/MyDrive/看股最速適")

TAIPEI = ZoneInfo("Asia/Taipei")

REPO_OWNER = "CHLINX"
REPO_NAME = "sso"
REPO_URL = "https://github.com/CHLINX/sso.git"
DEFAULT_BRANCH = "main"

# 已封存 v1.2.8 的 GitHub 來源
V128_BRANCH = (
    "audit/sso-v1.2.8-q5-h10-official-ordinary-"
    "20261111_113853-9b36"
)

V128_SCRIPT_PATH = (
    "audit/sso-v1.2.8-q5-h10-official-ordinary/"
    "20261111_113853/"
    "SSO_v1.2.8_官方普通股Q5與H10成熟樣本實際淨報酬驗證.py"
)

V128_SCRIPT_BLOB_SHA1 = (
    "66d6735b39a136a2cc786a3bfa9ec0c9a54fce13"
)

# v1.2.8 原始 ZIP，作為 H=10 相容性基準
V128_ZIP = (
    ROOT
    / "看股最速適_v1_2_8_官方普通股Q5與H10成熟樣本驗證_"
      "20261111_113853.zip"
)

V128_ZIP_SHA256 = (
    "35b9af048b99545d27bbe6a224a903b1a8ddf9fed27f5074aff53b4aab7cda92"
)

# 本次研究週期
HORIZONS = (1, 3, 5, 10)

EXPECTED_V126_QA = 126
EXPECTED_V127_FIRST_NINE_Q5 = 3446
EXPECTED_ORDINARY_KEYS = 1951

EXPECTED_H10_MATURED = 747

# v1.2.2 原始成本，完全不變
BASE_SLIP = 0.001
BASE_FEE = 0.001425
BASE_TAX = 0.003

STRESS_SLIP = 0.002
STRESS_FEE = 0.001425
STRESS_TAX = 0.003


# =============================================================================
# 02. 基礎工具
# =============================================================================

def log(message):
    now = datetime.now(TAIPEI).strftime("%H:%M:%S")
    print(f"[{now}] {message}", flush=True)


def run_stamp():
    return datetime.now(TAIPEI).strftime("%Y%m%d_%H%M%S")


def sha256_file(path):
    digest = hashlib.sha256()

    with open(path, "rb") as f:
        for block in iter(
            lambda: f.read(8 * 1024 * 1024),
            b"",
        ):
            digest.update(block)

    return digest.hexdigest()


def git_blob_sha1(data):
    header = f"blob {len(data)}\0".encode("utf-8")
    return hashlib.sha1(header + data).hexdigest()


def csv_write(frame, path):
    frame.to_csv(
        path,
        index=False,
        encoding="utf-8-sig",
        na_rep="",
    )


def json_safe(value):
    if value is None:
        return None

    if value is pd.NA:
        return None

    if isinstance(value, dict):
        out = {}

        for key, item in value.items():
            if isinstance(key, tuple):
                safe_key = "|".join(map(str, key))
            else:
                safe_key = str(key)

            if safe_key in out:
                raise ValueError(
                    f"JSON 鍵值轉換碰撞：{safe_key}"
                )

            out[safe_key] = json_safe(item)

        return out

    if isinstance(value, (list, tuple)):
        return [json_safe(x) for x in value]

    if isinstance(value, (set, frozenset)):
        return [json_safe(x) for x in sorted(value, key=str)]

    if isinstance(value, (pd.Timestamp, datetime)):
        return value.isoformat()

    if isinstance(value, Path):
        return str(value)

    if isinstance(value, np.generic):
        return json_safe(value.item())

    if isinstance(value, float) and not np.isfinite(value):
        return None

    return value


def json_write(data, path):
    Path(path).write_text(
        json.dumps(
            json_safe(data),
            ensure_ascii=False,
            indent=2,
            allow_nan=False,
        ),
        encoding="utf-8-sig",
    )


def find_member_by_basename(names, basename):
    matches = [
        x for x in names
        if Path(x).name == basename
    ]

    if len(matches) != 1:
        raise RuntimeError(
            f"ZIP 成員不唯一：{basename}；找到 {len(matches)} 個。"
        )

    return matches[0]


def net_return_v122(gross, slip, fee, tax):
    return (
        (1.0 + gross)
        * (1.0 - slip)
        * (1.0 - fee - tax)
        / ((1.0 + slip) * (1.0 + fee))
        - 1.0
    )


def mean_or_nan(values):
    x = pd.to_numeric(
        pd.Series(values),
        errors="coerce",
    ).dropna()

    return float(x.mean()) if len(x) else np.nan


def median_or_nan(values):
    x = pd.to_numeric(
        pd.Series(values),
        errors="coerce",
    ).dropna()

    return float(x.median()) if len(x) else np.nan


def wilson_lower(wins, n, z=1.959963984540054):
    """
    95% Wilson 勝率區間下界。
    小樣本仍會很寬，不代表樣本彼此獨立。
    """
    n = int(n)
    wins = int(wins)

    if n <= 0:
        return np.nan

    p = wins / n
    z2 = z * z

    denominator = 1.0 + z2 / n

    center = p + z2 / (2.0 * n)

    margin = z * np.sqrt(
        (p * (1.0 - p) / n)
        + (z2 / (4.0 * n * n))
    )

    return float(
        (center - margin) / denominator
    )


# =============================================================================
# 03. 載入並驗證已封存的 v1.2.8 程式
# =============================================================================

def load_v128_module():
    url = (
        "https://raw.githubusercontent.com/"
        f"{REPO_OWNER}/{REPO_NAME}/"
        f"{V128_BRANCH}/"
        f"{quote(V128_SCRIPT_PATH, safe='/')}"
    )

    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": "SSO-v1.2.9-multihorizon-audit"
        },
    )

    with urllib.request.urlopen(
        request,
        timeout=120,
    ) as response:
        source_bytes = response.read()

    actual_sha = git_blob_sha1(source_bytes)

    if actual_sha != V128_SCRIPT_BLOB_SHA1:
        raise RuntimeError(
            "v1.2.8 程式 Git blob SHA1 不符。"
            f"\n預期：{V128_SCRIPT_BLOB_SHA1}"
            f"\n實際：{actual_sha}"
        )

    temp_script = (
        Path(tempfile.gettempdir())
        / f"SSO_v128_dependency_{uuid.uuid4().hex[:8]}.py"
    )

    temp_script.write_bytes(source_bytes)

    spec = importlib.util.spec_from_file_location(
        "sso_v128_frozen_dependency",
        str(temp_script),
    )

    if spec is None or spec.loader is None:
        raise RuntimeError("無法載入 v1.2.8 固定程式。")

    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)

    required = [
        "load_v127_module",
        "load_overlay",
        "append_new_overlay",
        "load_official_ordinary",
        "build_official_ordinary_q5",
        "build_official_daily_summary",
        "compare_q5_membership",
        "find_member_by_basename",
        "sha256_file",
        "run_git",
    ]

    missing = [
        name for name in required
        if not hasattr(module, name)
    ]

    if missing:
        raise RuntimeError(
            "v1.2.8 固定程式缺少必要函式：" + repr(missing)
        )

    log("[PASS] v1.2.8 程式 SHA1 核驗完成。")

    return module


# =============================================================================
# 04. 多週期共用價格索引及實際淨報酬
# =============================================================================

def build_multi_horizon_returns(
    signal_sets,
    official,
    events,
    v126,
    v127,
    horizons=HORIZONS,
):
    """
    signal_sets:
        [
          (detail_dataframe, signal_label),
          ...
        ]

    共用原則：
    1. 全市場交易日曆只建立一次。
    2. 先建立所有訊號真正需要的進出場日期集合。
    3. 只讀取這些日期的候選股票 OHLC。
    4. 所有週期共用同一個 bar_map。
    5. H=10 計算結果必須與 v1.2.8 舊版逐筆相同。
    """

    log("[PRICE 1/4] 建立一次全市場交易日曆。")

    calendar = v127.build_global_calendar(official)

    calendar = [
        pd.Timestamp(x).normalize()
        for x in calendar
    ]

    if not calendar:
        raise RuntimeError("全市場交易日曆為空。")

    calendar_index = {
        date: i
        for i, date in enumerate(calendar)
    }

    latest_source_date = calendar[-1]

    log(
        f"[PASS] 全市場交易日曆：{len(calendar):,} 日；"
        f"最新 {latest_source_date.date()}。"
    )

    # -------------------------------------------------------------
    # 建立任務清單
    # -------------------------------------------------------------

    tasks = []
    all_codes = set()
    required_dates = set()

    log("[PRICE 2/4] 建立兩種 Q5 口徑及四種週期的配對任務。")

    for detail, signal_label in signal_sets:
        signals = detail.loc[
            detail["是否Q5訊號"]
        ].copy()

        if signals.empty:
            continue

        if signals.duplicated(
            ["訊號日期", "股票代號"]
        ).any():
            raise RuntimeError(
                f"{signal_label} 日期－代號訊號鍵重複。"
            )

        for signal in signals.to_dict("records"):
            code = str(signal["股票代號"])

            signal_date = pd.Timestamp(
                signal["訊號日期"]
            ).normalize()

            if signal_date not in calendar_index:
                raise RuntimeError(
                    f"訊號日期不在全市場日曆：{signal_date.date()}"
                )

            signal_index = calendar_index[signal_date]

            all_codes.add(code)

            for horizon in horizons:
                entry_index = signal_index + 1
                exit_index = signal_index + int(horizon)

                entry_date = (
                    calendar[entry_index]
                    if entry_index < len(calendar)
                    else None
                )

                exit_date = (
                    calendar[exit_index]
                    if exit_index < len(calendar)
                    else None
                )

                if entry_date is not None:
                    required_dates.add(entry_date)

                if exit_date is not None:
                    required_dates.add(exit_date)

                tasks.append({
                    "signal": signal,
                    "label": signal_label,
                    "code": code,
                    "signal_date": signal_date,
                    "horizon": int(horizon),
                    "entry_date": entry_date,
                    "exit_date": exit_date,
                })

    if not tasks:
        raise RuntimeError("沒有任何 Q5 訊號任務。")

    log(
        f"[PASS] 任務數 {len(tasks):,}；"
        f"候選代號 {len(all_codes):,}；"
        f"必要價格日期 {len(required_dates):,}。"
    )

    # -------------------------------------------------------------
    # 只取必要日期，不掃描全部歷史價格
    # -------------------------------------------------------------

    date_set = set(required_dates)

    recent = official.loc[
        official["date"].isin(date_set)
        & official["code"].astype(str).isin(all_codes)
    ].copy()

    if recent.empty:
        raise RuntimeError("必要前瞻日期沒有任何候選股票行情。")

    log(
        f"[PRICE 3/4] 必要價格列數：{len(recent):,}；"
        "開始建立共用 OHLC 索引。"
    )

    bar_map = {}

    for (code, date), group in recent.groupby(
        ["code", "date"],
        sort=False,
        observed=True,
    ):
        valid_mask = v126.valid_ohlc(group)
        valid = group.loc[valid_mask].copy()

        key = (
            str(code),
            pd.Timestamp(date).normalize(),
        )

        if valid.empty:
            bar_map[key] = {
                "status": "INVALID_OHLC",
                "row": None,
            }

        elif len(valid) > 1:
            bar_map[key] = {
                "status": "AMBIGUOUS_CODE_DATE",
                "row": None,
            }

        else:
            bar_map[key] = {
                "status": "VALID_OHLC",
                "row": valid.iloc[0],
            }

    del recent

    log(
        f"[PASS] 共用價格索引建立：{len(bar_map):,} 組代號－日期。"
    )

    # -------------------------------------------------------------
    # 價格區段事件索引
    # -------------------------------------------------------------

    event_map = defaultdict(list)

    if events is not None and not events.empty:
        event_frame = events.copy()

        if {"日期", "股票代號"}.issubset(event_frame.columns):
            event_frame["_date"] = pd.to_datetime(
                event_frame["日期"],
                errors="coerce",
            ).dt.normalize()

            event_frame = event_frame.loc[
                event_frame["_date"].notna()
            ].copy()

            for row in event_frame.to_dict("records"):
                code = str(row.get("股票代號", ""))

                event_map[code].append({
                    "date": pd.Timestamp(row["_date"]),
                    "type": str(row.get("事件類型", "")),
                })

    for code in event_map:
        event_map[code].sort(
            key=lambda x: x["date"]
        )

    # -------------------------------------------------------------
    # 逐筆計算
    # -------------------------------------------------------------

    output = []

    for i, task in enumerate(tasks, start=1):
        signal = task["signal"]
        code = task["code"]
        label = task["label"]
        signal_date = task["signal_date"]
        horizon = task["horizon"]

        entry_date = task["entry_date"]
        exit_date = task["exit_date"]

        # 和 v1.2.8 保持同樣的事件觀察區間
        if exit_date is None:
            event_end = latest_source_date
        else:
            event_end = min(
                exit_date,
                latest_source_date,
            )

        event_hits = [
            event
            for event in event_map.get(code, [])
            if signal_date < event["date"] <= event_end
        ]

        # 進場資料
        if entry_date is None:
            entry_status = "DATE_NOT_YET_AVAILABLE"
            entry_market = ""
            entry_open = np.nan

        else:
            entry_info = bar_map.get(
                (
                    code,
                    pd.Timestamp(entry_date).normalize(),
                ),
                {
                    "status": "STOCK_BAR_NOT_FOUND",
                    "row": None,
                },
            )

            entry_status = entry_info["status"]

            if entry_status == "VALID_OHLC":
                entry_market = str(
                    entry_info["row"]["market"]
                )
                entry_open = float(
                    entry_info["row"]["open"]
                )
            else:
                entry_market = ""
                entry_open = np.nan

        # 出場資料
        if exit_date is None:
            exit_status = "DATE_NOT_YET_AVAILABLE"
            exit_market = ""
            exit_close = np.nan

        else:
            exit_info = bar_map.get(
                (
                    code,
                    pd.Timestamp(exit_date).normalize(),
                ),
                {
                    "status": "STOCK_BAR_NOT_FOUND",
                    "row": None,
                },
            )

            exit_status = exit_info["status"]

            if exit_status == "VALID_OHLC":
                exit_market = str(
                    exit_info["row"]["market"]
                )
                exit_close = float(
                    exit_info["row"]["close"]
                )
            else:
                exit_market = ""
                exit_close = np.nan

        gross = np.nan
        net_base = np.nan
        net_stress = np.nan

        # 保留原先價格事件、未成熟及 OHLC 例外處理順序
        if event_hits:
            overall_status = (
                f"PRICE_SEGMENT_EVENT_DURING_H{horizon}_REVIEW"
            )

        elif exit_date is None:
            overall_status = (
                f"WAITING_FOR_H{horizon}_EXIT_DATA"
            )

        elif entry_status != "VALID_OHLC":
            overall_status = "ENTRY_DATA_REQUIRES_REVIEW"

        elif exit_status == "AMBIGUOUS_CODE_DATE":
            overall_status = "EXIT_CODE_DATE_AMBIGUOUS"

        elif exit_status != "VALID_OHLC":
            overall_status = "EXIT_STOCK_BAR_REQUIRES_REVIEW"

        else:
            gross = exit_close / entry_open - 1.0

            net_base = net_return_v122(
                gross,
                BASE_SLIP,
                BASE_FEE,
                BASE_TAX,
            )

            net_stress = net_return_v122(
                gross,
                STRESS_SLIP,
                STRESS_FEE,
                STRESS_TAX,
            )

            overall_status = "MATURED_RETURN_COMPUTED"

        output.append({
            "訊號口徑": label,
            "訊號日期": str(signal_date.date()),
            "股票代號": code,
            "股票名稱": str(signal["股票名稱"]),
            "訊號當日市場": str(signal["訊號當日市場"]),
            "range_pos20": float(signal["range_pos20"]),
            "橫斷面百分位排名": float(
                signal["橫斷面百分位排名"]
            ),
            "H": horizon,

            "進場規則": "訊號日後第1個全市場交易日開盤",
            "出場規則": (
                f"訊號日後第{horizon}個全市場交易日收盤"
            ),

            "進場日期": (
                str(entry_date.date())
                if entry_date is not None else ""
            ),
            "進場日市場": entry_market,
            "進場日資料狀態": entry_status,
            "進場開盤價": entry_open,

            "出場日期": (
                str(exit_date.date())
                if exit_date is not None else ""
            ),
            "出場日市場": exit_market,
            "出場日資料狀態": exit_status,
            "出場收盤價": exit_close,

            "毛報酬": gross,
            "基準成本淨報酬": net_base,
            "壓力成本淨報酬": net_stress,

            "基準成本是否獲利": (
                bool(net_base > 0)
                if pd.notna(net_base) else None
            ),

            "壓力成本是否獲利": (
                bool(net_stress > 0)
                if pd.notna(net_stress) else None
            ),

            "價格區段事件": "；".join(
                f"{e['date'].date()}:{e['type']}"
                for e in event_hits
            ),

            "來源最後交易日": str(
                latest_source_date.date()
            ),

            "觀察狀態": overall_status,
        })

        if i % 10000 == 0:
            log(f"[PRICE] 已完成 {i:,}/{len(tasks):,} 筆配對。")

    result = pd.DataFrame(output)

    result.sort_values(
        ["訊號口徑", "H", "訊號日期", "股票代號"],
        kind="mergesort",
        inplace=True,
    )

    result.reset_index(drop=True, inplace=True)

    log(
        f"[PASS] 多週期配對完成：{len(result):,} 筆。"
    )

    return result


# =============================================================================
# 05. 整體週期績效摘要
# =============================================================================

def summarize_by_horizon(schedule):
    rows = []

    for (scope, horizon), group in schedule.groupby(
        ["訊號口徑", "H"],
        sort=True,
        observed=True,
    ):
        matured = group.loc[
            group["觀察狀態"].eq("MATURED_RETURN_COMPUTED")
        ].copy()

        base = pd.to_numeric(
            matured["基準成本淨報酬"],
            errors="coerce",
        ).dropna()

        stress = pd.to_numeric(
            matured["壓力成本淨報酬"],
            errors="coerce",
        ).dropna()

        gross = pd.to_numeric(
            matured["毛報酬"],
            errors="coerce",
        ).dropna()

        base_wins = int((base > 0).sum())
        stress_wins = int((stress > 0).sum())

        status_counts = group["觀察狀態"].value_counts()

        rows.append({
            "訊號口徑": scope,
            "H": int(horizon),

            "訊號總筆數": int(len(group)),
            "訊號交易日數": int(group["訊號日期"].nunique()),

            "已到期排程筆數": int(
                group["出場日期"].astype(str).str.len().gt(0).sum()
            ),

            "已成熟且完成報酬筆數": int(len(matured)),
            "成熟訊號交易日數": int(
                matured["訊號日期"].nunique()
            ) if len(matured) else 0,

            "待未來出場資料筆數": int(
                status_counts.get(
                    f"WAITING_FOR_H{horizon}_EXIT_DATA",
                    0,
                )
            ),

            "進場資料待核對筆數": int(
                status_counts.get(
                    "ENTRY_DATA_REQUIRES_REVIEW",
                    0,
                )
            ),

            "出場資料待核對筆數": int(
                status_counts.get(
                    "EXIT_STOCK_BAR_REQUIRES_REVIEW",
                    0,
                )
            ),

            "價格區段事件待核對筆數": int(
                status_counts.get(
                    f"PRICE_SEGMENT_EVENT_DURING_H{horizon}_REVIEW",
                    0,
                )
            ),

            "毛報酬平均": mean_or_nan(gross),
            "毛報酬中位數": median_or_nan(gross),

            "基準成本平均淨報酬": mean_or_nan(base),
            "基準成本中位數淨報酬": median_or_nan(base),
            "基準成本獲利筆數": base_wins,
            "基準成本勝率": (
                base_wins / len(base) if len(base) else np.nan
            ),
            "基準成本勝率95%Wilson下界": wilson_lower(
                base_wins,
                len(base),
            ),

            "壓力成本平均淨報酬": mean_or_nan(stress),
            "壓力成本中位數淨報酬": median_or_nan(stress),
            "壓力成本獲利筆數": stress_wins,
            "壓力成本勝率": (
                stress_wins / len(stress)
                if len(stress) else np.nan
            ),
            "壓力成本勝率95%Wilson下界": wilson_lower(
                stress_wins,
                len(stress),
            ),

            "基準成本最差單筆": (
                float(base.min()) if len(base) else np.nan
            ),
            "基準成本最佳單筆": (
                float(base.max()) if len(base) else np.nan
            ),
        })

    return pd.DataFrame(rows)


# =============================================================================
# 06. 每個訊號日的週期成熟情形
# =============================================================================

def summarize_by_signal_date(schedule):
    rows = []

    for (scope, horizon, signal_date), group in schedule.groupby(
        ["訊號口徑", "H", "訊號日期"],
        sort=True,
        observed=True,
    ):
        matured = group.loc[
            group["觀察狀態"].eq("MATURED_RETURN_COMPUTED")
        ]

        rows.append({
            "訊號口徑": scope,
            "H": int(horizon),
            "訊號日期": str(signal_date),

            "Q5訊號數": int(len(group)),
            "已到期排程數": int(
                group["出場日期"].astype(str).str.len().gt(0).sum()
            ),
            "完成淨報酬數": int(len(matured)),

            "待未來出場資料數": int(
                group["觀察狀態"]
                .eq(f"WAITING_FOR_H{horizon}_EXIT_DATA")
                .sum()
            ),

            "進場待核對數": int(
                group["觀察狀態"]
                .eq("ENTRY_DATA_REQUIRES_REVIEW")
                .sum()
            ),

            "出場待核對數": int(
                group["觀察狀態"]
                .eq("EXIT_STOCK_BAR_REQUIRES_REVIEW")
                .sum()
            ),

            "基準成本平均淨報酬": mean_or_nan(
                matured["基準成本淨報酬"]
            ),

            "基準成本勝率": (
                float(
                    pd.to_numeric(
                        matured["基準成本淨報酬"],
                        errors="coerce",
                    ).gt(0).mean()
                )
                if len(matured) else np.nan
            ),

            "壓力成本平均淨報酬": mean_or_nan(
                matured["壓力成本淨報酬"]
            ),

            "壓力成本勝率": (
                float(
                    pd.to_numeric(
                        matured["壓力成本淨報酬"],
                        errors="coerce",
                    ).gt(0).mean()
                )
                if len(matured) else np.nan
            ),
        })

    return pd.DataFrame(rows)


# =============================================================================
# 07. 個股 × 週期彙總
# =============================================================================

def summarize_stock_horizon(matured):
    if matured.empty:
        return pd.DataFrame()

    rows = []

    for (scope, horizon, code, name), group in matured.groupby(
        ["訊號口徑", "H", "股票代號", "股票名稱"],
        sort=True,
        observed=True,
    ):
        base = pd.to_numeric(
            group["基準成本淨報酬"],
            errors="coerce",
        ).dropna()

        stress = pd.to_numeric(
            group["壓力成本淨報酬"],
            errors="coerce",
        ).dropna()

        base_wins = int((base > 0).sum())
        stress_wins = int((stress > 0).sum())

        signal_days = int(
            group["訊號日期"].nunique()
        )

        base_mean = mean_or_nan(base)
        base_median = median_or_nan(base)
        base_win_rate = (
            base_wins / len(base)
            if len(base) else np.nan
        )

        stress_mean = mean_or_nan(stress)
        stress_median = median_or_nan(stress)
        stress_win_rate = (
            stress_wins / len(stress)
            if len(stress) else np.nan
        )

        rows.append({
            "訊號口徑": scope,
            "H": int(horizon),
            "股票代號": str(code),
            "股票名稱": str(name),

            "訊號市場分布": "；".join(
                sorted(
                    set(
                        group["訊號當日市場"]
                        .astype(str)
                        .tolist()
                    )
                )
            ),

            "成熟訊號筆數": int(len(group)),
            "獨立訊號日期數": signal_days,

            "首次成熟訊號日期": group["訊號日期"].min(),
            "最近成熟訊號日期": group["訊號日期"].max(),

            "基準成本獲利筆數": base_wins,
            "基準成本平均淨報酬": base_mean,
            "基準成本中位數淨報酬": base_median,
            "基準成本勝率": base_win_rate,
            "基準成本勝率95%Wilson下界": wilson_lower(
                base_wins,
                len(base),
            ),

            "壓力成本獲利筆數": stress_wins,
            "壓力成本平均淨報酬": stress_mean,
            "壓力成本中位數淨報酬": stress_median,
            "壓力成本勝率": stress_win_rate,
            "壓力成本勝率95%Wilson下界": wilson_lower(
                stress_wins,
                len(stress),
            ),

            "基準成本最差單筆": (
                float(base.min()) if len(base) else np.nan
            ),
            "基準成本最佳單筆": (
                float(base.max()) if len(base) else np.nan
            ),

            "壓力成本最差單筆": (
                float(stress.min()) if len(stress) else np.nan
            ),
            "壓力成本最佳單筆": (
                float(stress.max()) if len(stress) else np.nan
            ),

            "是否跨不同訊號日重複入選": signal_days >= 2,

            "是否呈現正平均及正中位數": (
                bool(
                    pd.notna(base_mean)
                    and pd.notna(base_median)
                    and base_mean > 0
                    and base_median > 0
                )
            ),

            "是否基準成本勝率過半": (
                bool(base_win_rate > 0.5)
                if pd.notna(base_win_rate) else False
            ),

            "是否壓力成本仍為正平均": (
                bool(stress_mean > 0)
                if pd.notna(stress_mean) else False
            ),
        })

    return pd.DataFrame(rows)


# =============================================================================
# 08. 跨週期個股觀察表
# =============================================================================

def build_cross_horizon_stock_table(stock_summary):
    if stock_summary.empty:
        return pd.DataFrame()

    metrics = [
        "成熟訊號筆數",
        "獨立訊號日期數",
        "基準成本平均淨報酬",
        "基準成本中位數淨報酬",
        "基準成本勝率",
        "基準成本勝率95%Wilson下界",
        "壓力成本平均淨報酬",
        "壓力成本中位數淨報酬",
        "壓力成本勝率",
        "壓力成本最差單筆",
    ]

    index_cols = [
        "訊號口徑",
        "股票代號",
        "股票名稱",
    ]

    pivot = stock_summary.pivot(
        index=index_cols,
        columns="H",
        values=metrics,
    )

    pivot.columns = [
        f"H{int(h)}_{metric}"
        for metric, h in pivot.columns
    ]

    pivot = pivot.reset_index()

    rows = []

    for _, row in pivot.iterrows():
        scope = row["訊號口徑"]
        code = row["股票代號"]

        source = stock_summary.loc[
            stock_summary["訊號口徑"].eq(scope)
            & stock_summary["股票代號"].eq(code)
        ].copy()

        positive_average_horizons = 0
        positive_median_horizons = 0
        positive_stress_average_horizons = 0
        repeated_horizons = 0
        valid_horizons = 0

        for _, item in source.iterrows():
            valid_horizons += 1

            if (
                pd.notna(item["基準成本平均淨報酬"])
                and item["基準成本平均淨報酬"] > 0
            ):
                positive_average_horizons += 1

            if (
                pd.notna(item["基準成本中位數淨報酬"])
                and item["基準成本中位數淨報酬"] > 0
            ):
                positive_median_horizons += 1

            if (
                pd.notna(item["壓力成本平均淨報酬"])
                and item["壓力成本平均淨報酬"] > 0
            ):
                positive_stress_average_horizons += 1

            if item["獨立訊號日期數"] >= 2:
                repeated_horizons += 1

        rows.append({
            **row.to_dict(),
            "有成熟報酬週期數": valid_horizons,
            "基準成本平均為正的週期數": positive_average_horizons,
            "基準成本中位數為正的週期數": positive_median_horizons,
            "壓力成本平均為正的週期數": positive_stress_average_horizons,
            "跨不同訊號日重複入選週期數": repeated_horizons,

            "至少兩個週期平均與中位數均為正": (
                positive_average_horizons >= 2
                and positive_median_horizons >= 2
            ),

            "至少兩個週期且均有不同訊號日重複入選": (
                repeated_horizons >= 2
            ),

            "注意": (
                "跨週期報酬可能重疊；此表為觀察彙總，"
                "不是獨立樣本投資組合績效，也不是推薦排名。"
            ),
        })

    return pd.DataFrame(rows)


# =============================================================================
# 09. H=10 與 v1.2.8 原始成果逐筆相容性核對
# =============================================================================

def verify_h10_against_v128(new_schedule):
    if not V128_ZIP.is_file():
        raise FileNotFoundError(
            f"找不到 v1.2.8 舊版 ZIP：{V128_ZIP}"
        )

    actual_sha = sha256_file(V128_ZIP)

    if actual_sha != V128_ZIP_SHA256:
        raise RuntimeError(
            "v1.2.8 ZIP SHA256 不符。"
            f"\n預期：{V128_ZIP_SHA256}"
            f"\n實際：{actual_sha}"
        )

    with zipfile.ZipFile(V128_ZIP, "r") as zf:
        member = find_member_by_basename(
            zf.namelist(),
            "12_H10已成熟逐筆實際淨報酬.csv",
        )

        old = pd.read_csv(
            zf.open(member),
            dtype="string",
            encoding="utf-8-sig",
            low_memory=False,
        )

    new = new_schedule.loc[
        new_schedule["H"].eq(10)
        & new_schedule["觀察狀態"].eq("MATURED_RETURN_COMPUTED")
    ].copy()

    if len(old) != EXPECTED_H10_MATURED:
        raise RuntimeError(
            "v1.2.8 舊版 H=10 成熟筆數與封存基準不符。"
        )

    if len(new) != EXPECTED_H10_MATURED:
        raise RuntimeError(
            "v1.2.9 新版 H=10 成熟筆數與 v1.2.8 不同。"
            f"\n預期：{EXPECTED_H10_MATURED}"
            f"\n實際：{len(new)}"
        )

    keys = [
        "訊號口徑",
        "訊號日期",
        "股票代號",
        "H",
    ]

    for frame in (old, new):
        for col in keys:
            frame[col] = frame[col].astype(str).str.strip()

    if old.duplicated(keys).any():
        raise RuntimeError("v1.2.8 H=10 舊版鍵重複。")

    if new.duplicated(keys).any():
        raise RuntimeError("v1.2.9 H=10 新版鍵重複。")

    compare_cols = [
        "毛報酬",
        "基準成本淨報酬",
        "壓力成本淨報酬",
    ]

    old_cols = keys + compare_cols
    new_cols = keys + compare_cols

    merged = old[old_cols].merge(
        new[new_cols],
        on=keys,
        how="outer",
        suffixes=("_v128", "_v129"),
        indicator=True,
    )

    if len(merged) != EXPECTED_H10_MATURED:
        raise RuntimeError(
            "H=10 新舊版合併筆數不同。"
        )

    if not merged["_merge"].eq("both").all():
        raise RuntimeError(
            "H=10 新舊版訊號鍵集合不一致。"
        )

    max_diffs = {}

    for col in compare_cols:
        a = pd.to_numeric(
            merged[f"{col}_v128"],
            errors="coerce",
        )

        b = pd.to_numeric(
            merged[f"{col}_v129"],
            errors="coerce",
        )

        diff = (a - b).abs()
        max_diff = float(diff.max()) if len(diff) else 0.0

        max_diffs[col] = max_diff

        if not np.allclose(
            a.to_numpy(dtype=float),
            b.to_numpy(dtype=float),
            rtol=0,
            atol=1e-12,
            equal_nan=True,
        ):
            raise RuntimeError(
                f"H=10 舊新版報酬不一致：{col}；"
                f"最大差異 {max_diff}"
            )

    result = pd.DataFrame([
        {
            "核對項目": "v1.2.8 舊版 ZIP SHA256",
            "預期": V128_ZIP_SHA256,
            "實際": actual_sha,
            "核對結果": "PASS",
        },
        {
            "核對項目": "H10 成熟筆數",
            "預期": EXPECTED_H10_MATURED,
            "實際": len(new),
            "核對結果": "PASS",
        },
        {
            "核對項目": "H10 訊號鍵集合",
            "預期": "新舊版完全相同",
            "實際": "新舊版完全相同",
            "核對結果": "PASS",
        },
    ])

    for col, diff in max_diffs.items():
        result.loc[len(result)] = {
            "核對項目": f"{col}最大絕對差異",
            "預期": "<= 1e-12",
            "實際": diff,
            "核對結果": "PASS",
        }

    log("[PASS] H=10 與 v1.2.8 747 筆舊版結果逐筆一致。")

    return result


# =============================================================================
# 10. 保存本次完整程式
# =============================================================================

def archive_current_program(out_dir):
    target = (
        out_dir
        / "SSO_v1.2.9_官方普通股Q5多週期成熟淨報酬驗證.py"
    )

    try:
        from IPython import get_ipython

        shell = get_ipython()

        if shell is not None:
            history = shell.history_manager.input_hist_raw

            for source in reversed(history):
                if (
                    isinstance(source, str)
                    and "SSO v1.2.9｜官方普通股 Q5 多週期成熟淨報酬驗證"
                    in source
                    and "def build_multi_horizon_returns(" in source
                    and "def main():" in source
                    and len(source) > 25000
                ):
                    target.write_text(
                        source,
                        encoding="utf-8",
                    )

                    log("[PASS] 已封存 v1.2.9 完整程式。")
                    return target

    except Exception as exc:
        log(f"[WARN] 程式封存失敗：{exc}")

    log("[WARN] 無法從 Colab history 封存程式原文。")

    return None


# =============================================================================
# 11. GitHub 上傳
# =============================================================================

def run_git(args, cwd, env, input_bytes=None):
    process = subprocess.run(
        ["git", *args],
        cwd=str(cwd) if cwd else None,
        env=env,
        input=input_bytes,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )

    if process.returncode != 0:
        stderr = process.stderr.decode(
            "utf-8",
            "replace",
        )[-6000:]

        raise RuntimeError(
            f"git {' '.join(args[:6])} 失敗；"
            f"returncode={process.returncode}\n{stderr}"
        )

    return process.stdout.decode(
        "utf-8",
        "replace",
    ).strip()


def upload_github_v129(out_dir, zip_path, run_id, token, report):
    upload_files = [
        path
        for path in sorted(out_dir.iterdir())
        if path.is_file()
    ] + [zip_path]

    too_large = [
        (path.name, path.stat().st_size)
        for path in upload_files
        if path.stat().st_size >= 100 * 1024 * 1024
    ]

    if too_large:
        raise RuntimeError(
            "單檔超過 GitHub 100MB 上限：" + repr(too_large)
        )

    branch = (
        "audit/sso-v1.2.9-multi-horizon-"
        f"{run_id}-{uuid.uuid4().hex[:4]}"
    )

    artifact_dir = (
        f"audit/sso-v1.2.9-multi-horizon/{run_id}"
    )

    readme_lines = [
        "# SSO v1.2.9｜官方普通股 Q5 多週期成熟淨報酬驗證",
        "",
        f"- 執行時間：`{report['執行時間台北']}`",
        f"- 官方普通股：`{report['官方普通股市場代號鍵數']:,}`",
        f"- v1.2.7 原九日 Q5：`{report['v127原九日Q5訊號數']:,}`",
        f"- v1.2.8 官方普通股 Q5：`{report['v128官方普通股Q5訊號數']:,}`",
        f"- 多週期：`{HORIZONS}`",
        f"- 全部配對筆數：`{report['全部多週期配對筆數']:,}`",
        f"- 完成淨報酬筆數：`{report['完成淨報酬筆數']:,}`",
        f"- H=10 與 v1.2.8 相容性：`PASS`",
        "- Q5 定義及成本公式未修改。",
        "- 不重新訓練，不呼叫官方行情 API。",
        "- Google Drive 原始來源唯讀。",
        "- GitHub 只新增 audit branch，不更新 main。",
        "",
        "## 檔案",
    ]

    for path in upload_files:
        readme_lines.append(
            f"- `{path.name}` — "
            f"{path.stat().st_size:,} bytes — "
            f"SHA256 `{sha256_file(path)}`"
        )

    readme_bytes = (
        "\n".join(readme_lines) + "\n"
    ).encode("utf-8")

    with tempfile.TemporaryDirectory(
        prefix="sso_v129_gitpush_"
    ) as temp_dir:

        temp = Path(temp_dir)
        bare = temp / "repo.git"
        askpass = temp / "askpass.sh"

        askpass.write_text(
            "#!/bin/sh\n"
            'case "$1" in\n'
            "  *Username*|*username*) echo x-access-token ;;\n"
            '  *Password*|*password*) printf "%s\\n" "$GITHUB_TOKEN" ;;\n'
            "  *) echo ;;\n"
            "esac\n",
            encoding="utf-8",
        )

        askpass.chmod(0o700)

        env = os.environ.copy()

        env.update({
            "GITHUB_TOKEN": token,
            "GIT_ASKPASS": str(askpass),
            "GIT_TERMINAL_PROMPT": "0",
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_AUTHOR_NAME": "SSO Audit Uploader",
            "GIT_AUTHOR_EMAIL": (
                "sso-audit-uploader@users.noreply.github.com"
            ),
            "GIT_COMMITTER_NAME": "SSO Audit Uploader",
            "GIT_COMMITTER_EMAIL": (
                "sso-audit-uploader@users.noreply.github.com"
            ),
        })

        log("[GITHUB 1/7] 建立暫存 repository。")

        run_git(
            ["init", "--bare", str(bare)],
            None,
            env,
        )

        run_git(
            ["remote", "add", "origin", REPO_URL],
            bare,
            env,
        )

        log("[GITHUB 2/7] 取得 main 基準 SHA。")

        run_git(
            [
                "-c",
                "protocol.version=2",
                "fetch",
                "--depth=1",
                "--filter=blob:none",
                "origin",
                f"refs/heads/{DEFAULT_BRANCH}",
            ],
            bare,
            env,
        )

        main_before = run_git(
            ["rev-parse", "FETCH_HEAD^{commit}"],
            bare,
            env,
        )

        run_git(
            ["read-tree", main_before],
            bare,
            env,
        )

        log(f"[PASS] main before：{main_before}")

        items = [
            (
                path,
                f"{artifact_dir}/{path.name}",
                None,
            )
            for path in upload_files
        ]

        items.append((
            None,
            f"{artifact_dir}/README_GitHub上傳索引.md",
            readme_bytes,
        ))

        local_blobs = {}

        log("[GITHUB 3/7] 寫入 v1.2.9 成果。")

        for idx, (local_path, remote_path, inline_bytes) in enumerate(
            items,
            start=1,
        ):
            if local_path is not None:
                blob = run_git(
                    ["hash-object", "-w", str(local_path)],
                    bare,
                    env,
                )

                name = local_path.name
                size = local_path.stat().st_size

            else:
                blob = run_git(
                    ["hash-object", "-w", "--stdin"],
                    bare,
                    env,
                    inline_bytes,
                )

                name = "README_GitHub上傳索引.md"
                size = len(inline_bytes)

            run_git(
                [
                    "update-index",
                    "--add",
                    "--cacheinfo",
                    "100644",
                    blob,
                    remote_path,
                ],
                bare,
                env,
            )

            local_blobs[remote_path] = blob

            log(
                f"[ADD {idx}/{len(items)}] "
                f"{name} | {size:,} bytes"
            )

        log("[GITHUB 4/7] 建立獨立 audit commit。")

        tree_sha = run_git(
            ["write-tree"],
            bare,
            env,
        )

        commit_sha = run_git(
            [
                "-c",
                "user.name=SSO Audit Uploader",
                "-c",
                "user.email=sso-audit-uploader@users.noreply.github.com",
                "commit-tree",
                tree_sha,
                "-p",
                main_before,
                "-m",
                f"Add SSO v1.2.9 multi-horizon audit ({run_id})",
            ],
            bare,
            env,
        )

        log("[GITHUB 5/7] 推送 audit branch。")

        run_git(
            [
                "push",
                "origin",
                f"{commit_sha}:refs/heads/{branch}",
            ],
            bare,
            env,
        )

        remote_branch = run_git(
            [
                "ls-remote",
                "origin",
                f"refs/heads/{branch}",
            ],
            bare,
            env,
        ).split()

        if not remote_branch or remote_branch[0] != commit_sha:
            raise RuntimeError(
                "GitHub audit branch SHA read-back 失敗。"
            )

        log("[GITHUB 6/7] 逐檔 Git blob SHA read-back。")

        run_git(
            [
                "fetch",
                "--depth=1",
                "origin",
                f"refs/heads/{branch}",
            ],
            bare,
            env,
        )

        for remote_path, expected_blob in local_blobs.items():
            actual_blob = run_git(
                [
                    "rev-parse",
                    f"FETCH_HEAD:{remote_path}",
                ],
                bare,
                env,
            )

            if actual_blob != expected_blob:
                raise RuntimeError(
                    f"GitHub 檔案 SHA 不符：{remote_path}"
                )

        log(
            f"[PASS] 已核對 {len(local_blobs):,} 個 Git blob。"
        )

        log("[GITHUB 7/7] 檢查 main SHA 前後一致。")

        remote_main = run_git(
            [
                "ls-remote",
                "origin",
                f"refs/heads/{DEFAULT_BRANCH}",
            ],
            bare,
            env,
        ).split()

        main_after = (
            remote_main[0]
            if remote_main else ""
        )

        if main_after != main_before:
            raise RuntimeError(
                "GitHub main SHA 前後不同："
                f"{main_before} -> {main_after}"
            )

    return {
        "branch": branch,
        "commit": commit_sha,
        "main_before": main_before,
        "main_after": main_after,
        "uploaded_files": len(items),

        "folder_url": (
            f"https://github.com/{REPO_OWNER}/{REPO_NAME}"
            f"/tree/{branch}/{artifact_dir}"
        ),

        "zip_url": (
            f"https://github.com/{REPO_OWNER}/{REPO_NAME}"
            f"/blob/{branch}/{artifact_dir}/{quote(zip_path.name)}"
        ),

        "commit_url": (
            f"https://github.com/{REPO_OWNER}/{REPO_NAME}"
            f"/commit/{commit_sha}"
        ),
    }


# =============================================================================
# 12. 主流程
# =============================================================================

def main():
    print("=" * 104)
    log(VERSION)
    log(f"研究週期：{HORIZONS}")
    log("沿用 v1.2.8 Q5 定義、v1.2.2 成本公式及官方普通股分類。")
    log("H=10 必須與 v1.2.8 舊版 747 筆逐筆相同。")
    log("共用價格索引，不為每個 H 重掃全部歷史行情。")
    print("=" * 104)

    if not ROOT.is_dir():
        try:
            from google.colab import drive
            drive.mount(
                "/content/drive",
                force_remount=False,
            )
        except Exception as exc:
            raise RuntimeError(
                f"Google Drive 掛載失敗：{exc}"
            )

    if not ROOT.is_dir():
        raise FileNotFoundError(
            f"找不到 SSO 根目錄：{ROOT}"
        )

    # 先確認舊版 ZIP，避免計算完成才發現無法做 H10 相容性驗證
    if not V128_ZIP.is_file():
        raise FileNotFoundError(
            f"找不到 v1.2.8 舊版 ZIP：{V128_ZIP}"
        )

    if sha256_file(V128_ZIP) != V128_ZIP_SHA256:
        raise RuntimeError(
            "v1.2.8 舊版 ZIP SHA256 不符，停止執行。"
        )

    # 載入固定版本程式
    log("[01] 載入 v1.2.8 已封存程式。")

    v128 = load_v128_module()

    # 載入凍結 v1.2.7
    log("[02] 載入 v1.2.7 固定模組。")

    v127 = v128.load_v127_module()

    # v1.2.8 部分函式會引用全域 v127
    v128.v127 = v127

    # 載入 v1.2.6
    log("[03] 載入 v1.2.6 固定計算模組。")

    v126, dependency_path = v127.load_v126_module()
    v126.self_test()

    original_summary = (
        v127.load_v126_reference_summary()
    )

    # 載入原始行情來源
    log("[04] 載入 v1.2.7 已使用的行情來源。")

    sources = v127.load_and_validate_sources(v126)

    # 讀取 v1.5.1 行情增量
    log("[05] 讀取 v1.5.1 官方增量。")

    new_rows, overlay_info, overlay_report = (
        v128.load_overlay(v126)
    )

    sources = v128.append_new_overlay(
        sources,
        new_rows,
    )

    # 官方 ISIN 普通股分類
    log("[06] 讀取官方 ISIN 普通股清單。")

    (
        ordinary_frame,
        ordinary_codes,
        ordinary_name_map,
        ordinary_market_map,
        ordinary_info,
    ) = v128.load_official_ordinary()

    if len(ordinary_frame) != EXPECTED_ORDINARY_KEYS:
        raise RuntimeError(
            "官方普通股清單數量不符。"
        )

    # 只重建一次特徵
    log("[07] 沿用 v1.2.6 函式重建 11 日特徵。")

    rebuilt = v127.rebuild_v126_feature_rows(
        v126,
        sources,
    )

    # v1.2.7 原凍結 Q5
    log("[08] 重建 v1.2.7 原凍結 Q5。")

    legacy_detail, legacy_daily_detail_summary = (
        v127.build_q5_detail(
            rebuilt["extra"]["feature_rows"],
            rebuilt["universe"],
        )
    )

    qa = v127.verify_against_v126(
        original_summary,
        rebuilt["daily_q5"],
        legacy_detail,
        legacy_daily_detail_summary,
    )

    qa_failures = int(
        qa["核對結果"].ne("PASS").sum()
    )

    if len(qa) != EXPECTED_V126_QA or qa_failures:
        raise RuntimeError(
            "v1.2.6 原始摘要核對未通過。"
        )

    legacy_q5 = legacy_detail.loc[
        legacy_detail["是否Q5訊號"]
    ].copy()

    legacy_first_nine = legacy_q5.loc[
        legacy_q5["訊號日期"].le("2026-11-06")
    ]

    legacy_first_nine_n = int(
        len(legacy_first_nine)
    )

    if legacy_first_nine_n != EXPECTED_V127_FIRST_NINE_Q5:
        raise RuntimeError(
            "v1.2.7 原九日 Q5 訊號數不符。"
        )

    log(
        f"[PASS] v1.2.7 原九日 Q5："
        f"{legacy_first_nine_n:,} 筆。"
    )

    # v1.2.8 官方普通股重新排名 Q5
    log("[09] 以官方普通股重新計算 Q5。")

    official_q5_result = (
        v128.build_official_ordinary_q5(
            rebuilt,
            ordinary_codes,
            ordinary_name_map,
            ordinary_market_map,
            v127,
        )
    )

    official_detail = official_q5_result["detail"]
    official_q5 = official_q5_result["q5"]
    official_latest = official_q5_result["latest"]
    official_latest_date = official_q5_result["latest_date"]
    official_frequency = official_q5_result["frequency"]

    if official_latest_date != "2026-11-10":
        raise RuntimeError(
            "官方普通股 Q5 最新日期不是 2026-11-10。"
        )

    # 舊凍結 Q5 只過濾官方普通股，不變更原排名
    legacy_ordinary_detail = legacy_detail.loc[
        legacy_detail["股票代號"]
        .astype(str)
        .isin(ordinary_codes)
    ].copy()

    legacy_ordinary_q5 = legacy_ordinary_detail.loc[
        legacy_ordinary_detail["是否Q5訊號"]
    ].copy()

    q5_diff = v128.compare_q5_membership(
        legacy_detail,
        official_detail,
        ordinary_codes,
    )

    # -------------------------------------------------------------
    # 兩種 Q5 口徑，共用一份價格索引，計算四個週期
    # -------------------------------------------------------------

    log("[10] 開始共用價格索引與多週期配對。")

    signal_sets = [
        (
            legacy_ordinary_detail,
            "v1.2.7原凍結Q5_官方普通股過濾",
        ),
        (
            official_detail,
            "v1.2.8官方普通股重新排名Q5",
        ),
    ]

    schedule = build_multi_horizon_returns(
        signal_sets=signal_sets,
        official=sources["official"],
        events=rebuilt["extra"]["events"],
        v126=v126,
        v127=v127,
        horizons=HORIZONS,
    )

    matured = schedule.loc[
        schedule["觀察狀態"].eq("MATURED_RETURN_COMPUTED")
    ].copy()

    # -------------------------------------------------------------
    # H10 舊版逐筆核對
    # -------------------------------------------------------------

    log("[11] H=10 與 v1.2.8 舊版 747 筆逐筆核對。")

    h10_qa = verify_h10_against_v128(
        schedule
    )

    # -------------------------------------------------------------
    # 週期摘要及個股摘要
    # -------------------------------------------------------------

    log("[12] 建立四週期整體績效摘要。")

    horizon_summary = summarize_by_horizon(
        schedule
    )

    daily_horizon_summary = summarize_by_signal_date(
        schedule
    )

    stock_horizon_summary = summarize_stock_horizon(
        matured
    )

    repeated_stock_summary = (
        stock_horizon_summary.loc[
            stock_horizon_summary["獨立訊號日期數"].ge(2)
        ].copy()
        if not stock_horizon_summary.empty
        else pd.DataFrame()
    )

    cross_horizon_summary = (
        build_cross_horizon_stock_table(
            stock_horizon_summary
        )
    )

    # 官方普通股每日摘要
    official_daily = (
        v128.build_official_daily_summary(
            official_q5_result["daily_summary"],
            official_q5_result["feature_rows"],
            sources["official"],
            ordinary_codes,
            v126,
        )
    )

    # 舊版與新版每日 Q5 數量對照
    daily_legacy = (
        legacy_q5.groupby("訊號日期")
        .size()
        .rename("v127近似普通股Q5數")
        .reset_index()
    )

    daily_official = (
        official_q5.groupby("訊號日期")
        .size()
        .rename("v128官方普通股Q5數")
        .reset_index()
    )

    daily_compare = daily_legacy.merge(
        daily_official,
        on="訊號日期",
        how="outer",
    ).sort_values("訊號日期")

    daily_compare["v128減v127訊號數"] = (
        daily_compare["v128官方普通股Q5數"].fillna(0)
        - daily_compare["v127近似普通股Q5數"].fillna(0)
    )

    warmup = v127.build_warmup_exceptions(
        rebuilt["stock_report"]
    )

    # -------------------------------------------------------------
    # 週期成熟日期資訊
    # -------------------------------------------------------------

    maturity_dates = {}

    for horizon in HORIZONS:
        selected = matured.loc[
            matured["H"].eq(horizon)
        ]

        maturity_dates[str(horizon)] = sorted(
            selected["訊號日期"]
            .astype(str)
            .unique()
            .tolist()
        )

    status_distribution = {
        str(k): int(v)
        for k, v in schedule["觀察狀態"]
        .value_counts(dropna=False)
        .items()
    }

    # -------------------------------------------------------------
    # 整理來源清單
    # -------------------------------------------------------------

    source_manifest = sources["source_manifest"].copy()

    extra_manifest = pd.DataFrame([
        {
            "來源": "v1.5.1官方全市場增量",
            "路徑": str(
                v128.V151_ZIP
            ),
            "SHA256": overlay_info["zip_sha256"],
            "檔案大小bytes": v128.V151_ZIP.stat().st_size,
            "讀取列數": int(len(new_rows)),
            "起日": "2026-11-09",
            "迄日": "2026-11-10",
        },
        {
            "來源": "v1.4.1官方ISIN普通股分類",
            "路徑": str(v128.V141_ZIP),
            "SHA256": ordinary_info["zip_sha256"],
            "檔案大小bytes": v128.V141_ZIP.stat().st_size,
            "讀取列數": int(len(ordinary_frame)),
            "起日": "2026-11-10",
            "迄日": "2026-11-10",
        },
        {
            "來源": "v1.2.8已封存完整程式",
            "路徑": V128_SCRIPT_PATH,
            "SHA256": V128_SCRIPT_BLOB_SHA1,
            "檔案大小bytes": None,
            "讀取列數": None,
            "起日": None,
            "迄日": None,
        },
    ])

    source_manifest = pd.concat(
        [source_manifest, extra_manifest],
        ignore_index=True,
    )

    # -------------------------------------------------------------
    # 報告
    # -------------------------------------------------------------

    base = pd.to_numeric(
        matured["基準成本淨報酬"],
        errors="coerce",
    ).dropna()

    stress = pd.to_numeric(
        matured["壓力成本淨報酬"],
        errors="coerce",
    ).dropna()

    report = {
        "版本": VERSION,
        "執行時間台北": datetime.now(TAIPEI).isoformat(),

        "v1.2.8基準程式": {
            "branch": V128_BRANCH,
            "script_path": V128_SCRIPT_PATH,
            "script_git_blob_sha1": V128_SCRIPT_BLOB_SHA1,
            "zip_sha256": V128_ZIP_SHA256,
        },

        "v1.2.7原九日Q5訊號數": legacy_first_nine_n,
        "v1.2.6原始摘要核對項目": int(len(qa)),
        "v1.2.6原始摘要核對失敗數": qa_failures,

        "官方普通股市場代號鍵數": int(len(ordinary_frame)),
        "官方普通股唯一代號數": int(len(ordinary_codes)),

        "v1.2.7原凍結Q5延伸訊號數": int(len(legacy_q5)),
        "v1.2.7原凍結Q5官方普通股訊號數": int(
            len(legacy_ordinary_q5)
        ),

        "v1.2.8官方普通股Q5訊號數": int(
            len(official_q5)
        ),

        "v1.2.8最新Q5日期": official_latest_date,
        "v1.2.8最新日Q5訊號數": int(
            len(official_latest)
        ),

        "研究週期": list(HORIZONS),

        "全部多週期配對筆數": int(len(schedule)),
        "完成淨報酬筆數": int(len(matured)),

        "基準成本獲利筆數": int(
            matured["基準成本淨報酬"].gt(0).sum()
        ),
        "基準成本勝率": (
            float(matured["基準成本淨報酬"].gt(0).mean())
            if len(matured) else None
        ),

        "壓力成本獲利筆數": int(
            matured["壓力成本淨報酬"].gt(0).sum()
        ),
        "壓力成本勝率": (
            float(matured["壓力成本淨報酬"].gt(0).mean())
            if len(matured) else None
        ),

        "各週期已成熟訊號日期": maturity_dates,
        "各週期整體摘要": horizon_summary.to_dict("records"),

        "狀態分布": status_distribution,

        "H10與v128舊版逐筆相容性": {
            "result": "PASS",
            "matured_rows": EXPECTED_H10_MATURED,
            "max_abs_return_differences": {
                str(row["核對項目"]): row["實際"]
                for row in h10_qa.to_dict("records")
                if "最大絕對差異" in str(row["核對項目"])
            },
        },

        "成本設定": {
            "基準": {
                "slippage": BASE_SLIP,
                "fee": BASE_FEE,
                "tax": BASE_TAX,
            },
            "壓力": {
                "slippage": STRESS_SLIP,
                "fee": STRESS_FEE,
                "tax": STRESS_TAX,
            },
        },

        "重要限制": [
            "H=1、3、5、10 是不同持有期，不代表互相獨立的觀測。",
            "同一訊號日期的多檔股票受到共同市場環境影響。",
            "同一訊號的不同週期報酬可能重疊。",
            "跨日期重複入選數與獨立訊號日期數分開計算。",
            "H=10 只有已到期的訊號才納入報酬。",
            "未成熟訊號不計算或推估報酬。",
            "Wilson 區間不會消除訊號間的相關性。",
            "本版不依短期觀察結果推薦或硬湊十檔股票。",
        ],

        "明確未執行": [
            "未修改 v1.2.7 凍結訊號",
            "未改變 Q5 門檻",
            "未重新訓練模型",
            "未呼叫 TWSE 或 TPEx API",
            "未修改正式行情資料庫",
            "未修改既有 v1.2.8 成果",
            "未把未成熟樣本算入已實現報酬",
        ],
    }

    # -------------------------------------------------------------
    # 建立唯一輸出資料夾
    # -------------------------------------------------------------

    run_id = run_stamp()

    out_dir = (
        ROOT
        / (
            "看股最速適_v1_2_9_多週期成熟淨報酬驗證_"
            + run_id
        )
    )

    out_dir.mkdir(
        parents=True,
        exist_ok=False,
    )

    log(f"[OUTPUT] {out_dir}")

    # 保存程式
    archived_script = archive_current_program(
        out_dir
    )

    # -------------------------------------------------------------
    # CSV 輸出
    # -------------------------------------------------------------

    csv_write(
        source_manifest,
        out_dir / "00_來源檔案與版本核驗.csv",
    )

    csv_write(
        qa,
        out_dir / "01_v126原始九日逐欄重現核對.csv",
    )

    csv_write(
        legacy_detail,
        out_dir / "02_v127原凍結Q5訊號明細.csv",
    )

    csv_write(
        legacy_ordinary_q5,
        out_dir / "03_v127原凍結Q5官方普通股訊號.csv",
    )

    csv_write(
        official_detail,
        out_dir / "04_v128官方普通股Q5特徵與訊號明細.csv",
    )

    csv_write(
        official_daily,
        out_dir / "05_v128官方普通股每日Q5摘要.csv",
    )

    csv_write(
        official_frequency,
        out_dir / "06_v128官方普通股Q5出現頻率.csv",
    )

    csv_write(
        official_latest,
        out_dir / "07_v128最新交易日官方普通股Q5名單.csv",
    )

    csv_write(
        q5_diff,
        out_dir / "08_v127與v128Q5名單差異.csv",
    )

    csv_write(
        daily_compare,
        out_dir / "09_每日Q5訊號數對照.csv",
    )

    csv_write(
        schedule,
        out_dir / "10_兩種Q5口徑四週期完整觀察排程.csv",
    )

    csv_write(
        matured,
        out_dir / "11_H1H3H5H10已成熟逐筆淨報酬.csv",
    )

    csv_write(
        horizon_summary,
        out_dir / "12_各週期整體績效摘要.csv",
    )

    csv_write(
        daily_horizon_summary,
        out_dir / "13_各訊號日週期成熟概況.csv",
    )

    csv_write(
        stock_horizon_summary,
        out_dir / "14_個股各週期成熟報酬彙總.csv",
    )

    csv_write(
        repeated_stock_summary,
        out_dir / "15_跨不同訊號日重複入選個股.csv",
    )

    csv_write(
        cross_horizon_summary,
        out_dir / "16_個股跨週期表現總表.csv",
    )

    csv_write(
        h10_qa,
        out_dir / "17_H10與v128舊版逐筆相容性核對.csv",
    )

    csv_write(
        warmup,
        out_dir / "18_價格銜接與暖機例外.csv",
    )

    # -------------------------------------------------------------
    # 執行摘要
    # -------------------------------------------------------------

    lines = [
        "=" * 104,
        VERSION,
        "=" * 104,
        f"執行時間：{report['執行時間台北']}",
        "",
        "一、舊版相容性",
        f"v1.2.7 原九日 Q5：{legacy_first_nine_n:,}",
        f"v1.2.6 原始摘要核對：{len(qa):,} 項",
        f"v1.2.6 核對失敗：{qa_failures:,}",
        "H=10 與 v1.2.8 舊版 747 筆逐筆核對：PASS",
        "",
        "二、證券範圍",
        f"官方普通股市場－代號鍵：{len(ordinary_frame):,}",
        f"官方普通股唯一代號：{len(ordinary_codes):,}",
        "",
        "三、Q5 訊號",
        f"v1.2.7 原凍結 Q5 官方普通股訊號：{len(legacy_ordinary_q5):,}",
        f"v1.2.8 官方普通股重排名 Q5：{len(official_q5):,}",
        f"v1.2.8 最新 Q5 日期：{official_latest_date}",
        f"v1.2.8 最新日 Q5：{len(official_latest):,}",
        "",
        "四、多週期已成熟結果",
        f"全部配對筆數：{len(schedule):,}",
        f"已成熟且完成淨報酬：{len(matured):,}",
        f"基準成本正報酬：{int(matured['基準成本淨報酬'].gt(0).sum()):,}",
        f"壓力成本正報酬：{int(matured['壓力成本淨報酬'].gt(0).sum()):,}",
        "",
        "五、各週期摘要",
    ]

    for row in horizon_summary.to_dict("records"):
        lines.append(
            f"{row['訊號口徑']} | H={row['H']} | "
            f"訊號 {row['訊號總筆數']:,} | "
            f"成熟 {row['已成熟且完成報酬筆數']:,} | "
            f"成熟日期 {row['成熟訊號交易日數']} | "
            f"基準平均 {row['基準成本平均淨報酬']:.4%} | "
            f"勝率 {row['基準成本勝率']:.2%}"
            if pd.notna(row["基準成本平均淨報酬"])
            and pd.notna(row["基準成本勝率"])
            else
            f"{row['訊號口徑']} | H={row['H']} | "
            f"訊號 {row['訊號總筆數']:,} | 尚無成熟報酬"
        )

    lines += [
        "",
        "六、個股重複入選",
        f"個股－週期彙總列數：{len(stock_horizon_summary):,}",
        f"至少跨兩個不同訊號日重複入選的個股－週期列數：{len(repeated_stock_summary):,}",
        f"跨週期個股總表列數：{len(cross_horizon_summary):,}",
        "",
        "七、研究限制",
        "不同持有週期可能重疊，不等於獨立樣本。",
        "同一訊號日多檔股票不是彼此完全獨立的市場觀測。",
        "短期正報酬不等於可重複獲利，也不構成推薦。",
        "未成熟訊號不計算報酬。",
        "不為達成十檔目標而放寬證據門檻。",
        "",
        "八、安全",
        "Drive 原始來源唯讀。",
        "未呼叫官方 API。",
        "未修改 v1.2.8 舊成果及正式行情資料。",
        "GitHub 僅新增獨立 audit branch。",
        "GitHub main SHA 前後必須一致。",
        "=" * 104,
    ]

    (out_dir / "19_執行摘要.txt").write_text(
        "\n".join(lines) + "\n",
        encoding="utf-8-sig",
    )

    # -------------------------------------------------------------
    # 報告 JSON
    # -------------------------------------------------------------

    json_write(
        report,
        out_dir / "20_v129多週期成熟淨報酬驗證結論.json",
    )

    # README
    readme_lines = [
        "# SSO v1.2.9｜多週期成熟淨報酬驗證",
        "",
        f"- 執行時間：`{report['執行時間台北']}`",
        f"- 官方普通股市場－代號鍵：`{len(ordinary_frame):,}`",
        f"- v1.2.7 原九日 Q5：`{legacy_first_nine_n:,}`",
        f"- v1.2.8 官方普通股 Q5：`{len(official_q5):,}`",
        f"- 週期：`{HORIZONS}`",
        f"- 全部配對：`{len(schedule):,}`",
        f"- 完成淨報酬：`{len(matured):,}`",
        "- H=10 與 v1.2.8 舊版 747 筆逐筆核對通過。",
        "- Q5 定義及成本公式沿用，不重新訓練。",
        "- 未成熟訊號不計算報酬。",
        "- 不把重複週期當成獨立樣本。",
        "- 不硬湊十檔股票。",
        "",
        "## 輸出檔案",
    ]

    for path in sorted(out_dir.iterdir()):
        if path.is_file():
            readme_lines.append(
                f"- `{path.name}` — "
                f"{path.stat().st_size:,} bytes — "
                f"SHA256 `{sha256_file(path)}`"
            )

    (out_dir / "README_稽核說明.md").write_text(
        "\n".join(readme_lines) + "\n",
        encoding="utf-8",
    )

    # -------------------------------------------------------------
    # ZIP
    # -------------------------------------------------------------

    zip_path = (
        ROOT
        / f"{out_dir.name}.zip"
    )

    if zip_path.exists():
        raise FileExistsError(
            f"預期 ZIP 已存在，停止避免覆寫：{zip_path}"
        )

    log("[OUTPUT] 建立 ZIP。")

    with zipfile.ZipFile(
        zip_path,
        "w",
        compression=zipfile.ZIP_DEFLATED,
        compresslevel=6,
    ) as archive:
        for path in sorted(out_dir.iterdir()):
            if path.is_file():
                archive.write(
                    path,
                    arcname=path.name,
                )

    with zipfile.ZipFile(zip_path, "r") as archive:
        bad = archive.testzip()

    if bad:
        raise RuntimeError(
            f"ZIP CRC 驗證失敗：{bad}"
        )

    zip_sha = sha256_file(zip_path)

    log(
        f"[PASS] ZIP CRC；SHA256={zip_sha}"
    )

    report["output"] = {
        "directory": str(out_dir),
        "zip": str(zip_path),
        "zip_sha256": zip_sha,
        "zip_bytes": zip_path.stat().st_size,
    }

    # 更新最終 JSON，並重新封裝 ZIP
    json_write(
        report,
        out_dir / "20_v129多週期成熟淨報酬驗證結論.json",
    )

    with zipfile.ZipFile(
        zip_path,
        "w",
        compression=zipfile.ZIP_DEFLATED,
        compresslevel=6,
    ) as archive:
        for path in sorted(out_dir.iterdir()):
            if path.is_file():
                archive.write(
                    path,
                    arcname=path.name,
                )

    with zipfile.ZipFile(zip_path, "r") as archive:
        bad = archive.testzip()

    if bad:
        raise RuntimeError(
            f"最終 ZIP CRC 驗證失敗：{bad}"
        )

    zip_sha = sha256_file(zip_path)

    # -------------------------------------------------------------
    # GitHub
    # -------------------------------------------------------------

    log("[GITHUB] 準備上傳 v1.2.9。")

    github_token = v127.find_github_token()

    if not github_token:
        raise RuntimeError(
            "找不到 GITHUB_TOKEN；成果已保存在 Drive，尚未上傳 GitHub。"
        )

    github_result = upload_github_v129(
        out_dir=out_dir,
        zip_path=zip_path,
        run_id=run_id,
        token=github_token,
        report=report,
    )

    # 上傳核驗 JSON 放在資料夾之外，不改動本次 ZIP
    verification_path = (
        ROOT
        / f"{out_dir.name}_GitHub上傳核驗.json"
    )

    json_write(
        {
            "版本": VERSION,
            "run_id": run_id,
            "output_dir": str(out_dir),
            "zip_path": str(zip_path),
            "zip_sha256": zip_sha,
            "github": github_result,
            "驗證": {
                "H10與v128舊版逐筆一致": True,
                "github_blob_readback": True,
                "main_sha_unchanged": (
                    github_result["main_before"]
                    == github_result["main_after"]
                ),
            },
        },
        verification_path,
    )

    # -------------------------------------------------------------
    # 最終輸出
    # -------------------------------------------------------------

    print("\n" + "=" * 104)
    print("[PASS] SSO v1.2.9 多週期成熟淨報酬驗證完成")
    print("=" * 104)

    print(f"輸出資料夾：{out_dir}")
    print(f"ZIP：{zip_path}")
    print(f"ZIP SHA256：{zip_sha}")

    print("")
    print(f"v1.2.7 原九日 Q5：{legacy_first_nine_n:,}")
    print(f"官方普通股：{len(ordinary_frame):,}")
    print(f"v1.2.8 官方普通股 Q5：{len(official_q5):,}")

    print("")
    print(f"多週期配對總筆數：{len(schedule):,}")
    print(f"成熟且完成淨報酬：{len(matured):,}")
    print(f"個股－週期彙總：{len(stock_horizon_summary):,}")
    print(f"重複入選觀察：{len(repeated_stock_summary):,}")
    print(f"跨週期個股表：{len(cross_horizon_summary):,}")

    print("")
    print("各週期摘要：")
    print(
        horizon_summary.to_string(
            index=False,
            max_rows=50,
        )
    )

    print("")
    print(f"GitHub branch：{github_result['branch']}")
    print(f"GitHub commit：{github_result['commit']}")
    print(f"資料夾：{github_result['folder_url']}")
    print(f"ZIP：{github_result['zip_url']}")
    print(f"Commit URL：{github_result['commit_url']}")

    print("")
    print(f"main before：{github_result['main_before']}")
    print(f"main after：{github_result['main_after']}")

    print("")
    print(f"上傳核驗：{verification_path}")
    print("[PASS] H=10 與 v1.2.8 747 筆舊版逐筆一致。")
    print("[PASS] ZIP CRC。")
    print("[PASS] GitHub 每檔 Git blob SHA read-back。")
    print("[PASS] GitHub main SHA 前後一致。")
    print("[PASS] 未修改正式資料及 v1.2.8 舊成果。")
    print("=" * 104)

    return {
        "output_dir": str(out_dir),
        "zip_path": str(zip_path),
        "zip_sha256": zip_sha,
        "github": github_result,
        "horizon_summary": horizon_summary,
        "matured": matured,
    }


# =============================================================================
# 13. 執行
# =============================================================================

RESULT = main()