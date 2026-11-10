#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SSO v1.2.7｜全市場 Q5 訊號凍結與 H=10 觀察排程

本版目的
1. 嚴格沿用 v1.2.6 的來源、價格銜接、暖機及 range_pos20 計算。
2. 從 v1.2.6 已計算的逐日 feature rows，重建每日全市場 Q5 名單。
3. 與 v1.2.6 GitHub 已封存的 9 日摘要逐日核對，未通過就停止。
4. 輸出每日 Q5 訊號、最新日期名單、個股出現頻率。
5. 建立訊號日 +1 全市場交易日進場、訊號日 +10 全市場交易日出場的觀察排程。
6. 不計算尚未成熟樣本的報酬、不推薦股票、不挑選或硬湊 10 檔。

安全限制
- Google Drive 原始來源唯讀。
- 不呼叫 TWSE／TPEx 官方 API。
- 不修改、刪除、移動或覆寫任何既有資料。
- 輸出只建立在新的唯一資料夾。
- GitHub 只建立獨立 audit branch，commit parent 直接指向 main；
  不對 main 執行 push，並檢查 main 前後 SHA 相同。

執行方式
- 可用原本 Colab Python 上傳執行器 v2 執行。
- 若以 Colab cell 執行，請將整份程式貼入同一個 cell。
"""

from __future__ import annotations

import os
import re
import io
import sys
import json
import uuid
import shutil
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
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd


# =============================================================================
# 01. 固定設定
# =============================================================================

VERSION = "SSO_v1.2.7_全市場Q5訊號凍結與H10觀察排程"
TAIPEI = ZoneInfo("Asia/Taipei")

ROOT = Path("/content/drive/MyDrive/看股最速適")

# v1.2.6 已完成成果
V126_RUN_STAMP = "20261110_125920"
V126_FOLDER_NAME = (
    "看股最速適_v1_2_6_全市場Q5橫斷面準備度稽核_"
    + V126_RUN_STAMP
)
V126_OUTPUT_DIR = ROOT / V126_FOLDER_NAME

# v1.2.6 GitHub 已驗證的獨立 audit branch
V126_BRANCH = (
    "audit/sso-v1.2.6-fullmarket-q5-readiness-"
    "20261110_125920-gitpush-d721"
)
V126_ARTIFACT_DIR = (
    "audit/sso-v1.2.6-fullmarket-q5-readiness/20261110_125920"
)

V126_SCRIPT_NAME = "SSO_v1.2.6_全市場Q5橫斷面準備度稽核.py"
V126_SUMMARY_NAME = "05_每日Q5橫斷面準備度摘要.csv"

V126_SCRIPT_BLOB_SHA1 = (
    "5d0f5053766cea24ef97e45fd81b8c0bc7bea268"
)
V126_SUMMARY_BLOB_SHA1 = (
    "7b7a9088d78ff59a33462e3fe98fe3f0d02035fb"
)

REPO_OWNER = "CHLINX"
REPO_NAME = "sso"
REPO_URL = "https://github.com/CHLINX/sso.git"
DEFAULT_BRANCH = "main"

# v1.2.6 原始摘要中的核對欄位
QA_INTEGER_FIELDS = [
    "TWSE有效普通股OHLC數",
    "TPEx有效普通股OHLC數",
    "當日有效普通股數",
    "可計算range_pos20數",
    "TWSE可計算特徵數",
    "TPEx可計算特徵數",
    "range_pos20不同值數",
    "Q5百分位排名列數",
]

QA_FLOAT_FIELDS = [
    "特徵覆蓋率",
    "Q80原始特徵分位數",
]

QA_TEXT_FIELDS = [
    "Q5橫斷面狀態",
]

QA_BOOL_FIELDS = [
    "雙市場OHLC覆蓋初步通過",
    "雙市場特徵樣本均存在",
]

Q5_PERCENTILE_CUTOFF = 0.80
HORIZON = 10


# =============================================================================
# 02. 一般工具
# =============================================================================

def log(message: str) -> None:
    now = datetime.now(TAIPEI).strftime("%H:%M:%S")
    print(f"[{now}] {message}", flush=True)


def run_stamp() -> str:
    return datetime.now(TAIPEI).strftime("%Y%m%d_%H%M%S")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def git_blob_sha1(data: bytes) -> str:
    header = f"blob {len(data)}\0".encode("utf-8")
    return hashlib.sha1(header + data).hexdigest()


def fmt_bytes(size: int) -> str:
    if size >= 1024 ** 3:
        return f"{size / 1024 ** 3:.3f} GB"
    if size >= 1024 ** 2:
        return f"{size / 1024 ** 2:.2f} MB"
    return f"{size / 1024:.1f} KB"


def csv_write(frame: pd.DataFrame, path: Path) -> None:
    frame.to_csv(
        path,
        index=False,
        encoding="utf-8-sig",
        na_rep="",
    )


def json_write(data: Dict[str, Any], path: Path) -> None:
    path.write_text(
        json.dumps(
            data,
            ensure_ascii=False,
            indent=2,
            default=str,
        ),
        encoding="utf-8",
    )


def download_bytes(url: str, timeout: int = 90) -> bytes:
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": "SSO-v1.2.7-Audit/1.0",
            "Accept": "*/*",
        },
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return response.read()


def download_pinned_github_file(
    path: str,
    expected_blob_sha1: str,
) -> bytes:
    """
    從固定 v1.2.6 audit branch 下載檔案。
    下載後先驗證 Git blob SHA1，避免誤讀其他版本。
    """
    raw_url = (
        "https://raw.githubusercontent.com/"
        f"{REPO_OWNER}/{REPO_NAME}/{V126_BRANCH}/"
        + quote(path, safe="/")
    )

    data = download_bytes(raw_url)
    actual = git_blob_sha1(data)

    if actual != expected_blob_sha1:
        raise RuntimeError(
            "GitHub 歷史檔案 SHA 驗證失敗："
            f"\n檔案：{path}"
            f"\n預期：{expected_blob_sha1}"
            f"\n實際：{actual}"
        )

    return data


# =============================================================================
# 03. 取得並載入 v1.2.6 原始程式
# =============================================================================

def load_v126_module():
    """
    優先使用已存在的 v1.2.6 成果資料夾內程式。
    若該檔不存在，才從固定 GitHub audit branch 下載。
    兩者均須通過 Git blob SHA1 驗證。
    """
    local_script = V126_OUTPUT_DIR / V126_SCRIPT_NAME

    if local_script.is_file():
        local_data = local_script.read_bytes()
        local_sha = git_blob_sha1(local_data)

        if local_sha == V126_SCRIPT_BLOB_SHA1:
            log("[PASS] 使用 Drive 已封存的 v1.2.6 程式。")
            script_bytes = local_data
        else:
            log("[WARN] Drive v1.2.6 程式 SHA 不符，改由 GitHub 固定版本取得。")
            script_bytes = download_pinned_github_file(
                f"{V126_ARTIFACT_DIR}/{V126_SCRIPT_NAME}",
                V126_SCRIPT_BLOB_SHA1,
            )
    else:
        log("[INFO] Drive 未找到 v1.2.6 程式，從 GitHub 固定 audit branch 取得。")
        script_bytes = download_pinned_github_file(
            f"{V126_ARTIFACT_DIR}/{V126_SCRIPT_NAME}",
            V126_SCRIPT_BLOB_SHA1,
        )

    temp_script = (
        Path(tempfile.gettempdir())
        / f"SSO_v126_dependency_{uuid.uuid4().hex[:8]}.py"
    )
    temp_script.write_bytes(script_bytes)

    spec = importlib.util.spec_from_file_location(
        "sso_v126_pinned_dependency",
        str(temp_script),
    )
    if spec is None or spec.loader is None:
        raise RuntimeError("無法建立 v1.2.6 模組載入器。")

    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)

    if not hasattr(module, "run_forward_readiness"):
        raise RuntimeError("載入的 v1.2.6 程式缺少 run_forward_readiness。")

    if not hasattr(module, "build_scale_bridge"):
        raise RuntimeError("載入的 v1.2.6 程式缺少 build_scale_bridge。")

    log("[PASS] v1.2.6 固定版本程式載入完成。")
    return module, temp_script


def load_v126_reference_summary() -> pd.DataFrame:
    """
    讀取 v1.2.6 原始每日 Q5 摘要。
    本程式將它當作重現核對基準，不會修改該檔案。
    """
    local_csv = V126_OUTPUT_DIR / V126_SUMMARY_NAME

    if local_csv.is_file():
        data = local_csv.read_bytes()
        actual = git_blob_sha1(data)

        if actual == V126_SUMMARY_BLOB_SHA1:
            log("[PASS] 使用 Drive 已封存的 v1.2.6 每日摘要。")
            return pd.read_csv(
                io.BytesIO(data),
                encoding="utf-8-sig",
            )

        log("[WARN] Drive 摘要 SHA 不符，改由 GitHub 固定版本取得。")

    data = download_pinned_github_file(
        f"{V126_ARTIFACT_DIR}/{V126_SUMMARY_NAME}",
        V126_SUMMARY_BLOB_SHA1,
    )

    log("[PASS] v1.2.6 GitHub 原始每日摘要 SHA 驗證完成。")

    return pd.read_csv(
        io.BytesIO(data),
        encoding="utf-8-sig",
    )


# =============================================================================
# 04. Drive 掛載及 GitHub Token
# =============================================================================

def ensure_drive() -> None:
    if ROOT.is_dir():
        return

    log("[INFO] 嘗試掛載 Google Drive……")

    try:
        from google.colab import drive
        drive.mount("/content/drive", force_remount=False)
    except Exception as exc:
        raise RuntimeError(f"Google Drive 掛載失敗：{exc}")

    if not ROOT.is_dir():
        raise FileNotFoundError(f"找不到 SSO 根目錄：{ROOT}")


def find_github_token() -> str:
    token = (
        os.environ.get("GITHUB_TOKEN", "").strip()
        or os.environ.get("GH_TOKEN", "").strip()
    )

    if not token:
        try:
            from google.colab import userdata
            token = (userdata.get("GITHUB_TOKEN") or "").strip()
        except Exception:
            token = ""

    if not token:
        raise RuntimeError(
            "找不到 GITHUB_TOKEN。"
            "請確認既有 Colab Secret 已設定；不要把 Token 貼到程式碼或對話。"
        )

    return token


# =============================================================================
# 05. 使用 v1.2.6 相同規則讀取來源
# =============================================================================

def load_and_validate_sources(module):
    """
    依照 v1.2.6 main_audit 的來源檢核順序重新讀取。
    不修改任何來源。
    """
    required = [
        module.V469_PATH,
        module.A_PATH,
        module.DELTA_PATH,
        module.STATE_PATH,
    ]

    missing = [str(path) for path in required if not path.is_file()]
    if missing:
        raise FileNotFoundError(
            "缺少 v1.2.6 必要來源：\n" + "\n".join(missing)
        )

    log("[01/08] 驗證 v4.69 SHA256……")
    v469_sha = sha256_file(module.V469_PATH)

    if v469_sha != module.V469_SHA256:
        raise RuntimeError(
            "v4.69 SHA256 不符："
            f"\n預期：{module.V469_SHA256}"
            f"\n實際：{v469_sha}"
        )

    log("[PASS] v4.69 SHA256 相符。")

    log("[02/08] 核對 v4.17 與 append-only delta 中繼資料……")

    state = json.loads(
        module.STATE_PATH.read_text(encoding="utf-8-sig")
    )

    if state.get("storage_mode") != "BASE_A_PLUS_APPEND_ONLY_DELTA":
        raise RuntimeError("行情資料庫不是 A + append-only delta 結構。")

    if (
        Path(state.get("original_source_database", "")).resolve()
        != module.A_PATH.resolve()
    ):
        raise RuntimeError("state 記錄的 v4.17 來源路徑不符。")

    if state.get("original_source_sha256") != module.A_SHA256_EXPECTED:
        raise RuntimeError("state 記錄的 v4.17 SHA256 不符。")

    a_stat = module.A_PATH.stat()

    if (
        "base_source_size_bytes" in state
        and a_stat.st_size != int(state["base_source_size_bytes"])
    ):
        raise RuntimeError("v4.17 檔案大小與 state 不符。")

    if (
        "base_source_mtime_ns" in state
        and a_stat.st_mtime_ns != int(state["base_source_mtime_ns"])
    ):
        raise RuntimeError("v4.17 修改時間與 state 不符。")

    delta_stat = module.DELTA_PATH.stat()

    if (
        "delta_size_bytes" in state
        and delta_stat.st_size != int(state["delta_size_bytes"])
    ):
        raise RuntimeError("delta 檔案大小與 state 不符。")

    log("[03/08] 讀取 v4.69、v4.17 及 delta……")

    vmap = module.source_mapping(module.V469_PATH, "v469")
    amap = module.source_mapping(module.A_PATH, "official")
    dmap = module.source_mapping(module.DELTA_PATH, "official")

    v469 = module.read_recent(
        module.V469_PATH,
        vmap,
        module.HISTORY_SCAN_START,
        module.V469_END,
        "v4.69全市場",
    )

    official_base = module.read_recent(
        module.A_PATH,
        amap,
        module.HISTORY_SCAN_START,
        None,
        "v4.17全市場",
    )

    delta = module.load_delta(
        module.DELTA_PATH,
        dmap,
    )

    if v469.empty or official_base.empty or delta.empty:
        raise RuntimeError(
            "來源讀取結果異常："
            f"v4.69={len(v469):,}、"
            f"v4.17={len(official_base):,}、"
            f"delta={len(delta):,}"
        )

    # 研究日軸沿用 v1.2.6：排除星期六補行日。
    v469 = v469.loc[v469["date"].dt.weekday.lt(5)].copy()

    official = pd.concat(
        [official_base, delta],
        ignore_index=True,
    )

    official = official.loc[
        official["date"].dt.weekday.lt(5)
    ].copy()

    if v469["date"].max() != module.V469_END:
        raise RuntimeError(
            "v4.69 最新日期錯誤："
            f"{v469['date'].max()}，"
            f"預期 {module.V469_END.date()}"
        )

    if official.duplicated(["market", "code", "date"]).any():
        duplicate_n = int(
            official.duplicated(["market", "code", "date"]).sum()
        )
        raise RuntimeError(
            f"v4.17 + delta 市場自然鍵重複 {duplicate_n:,} 列。"
        )

    delta_rows_expected = state.get("delta_rows")
    if (
        delta_rows_expected is not None
        and int(delta_rows_expected) != len(delta)
    ):
        raise RuntimeError(
            "delta 列數與 state 不符："
            f"{len(delta):,}/{delta_rows_expected:,}"
        )

    delta_latest = (
        delta.groupby("market")["date"]
        .max()
        .dropna()
        .to_dict()
    )

    state_latest = state.get("last_date_by_market", {})

    for market, latest in delta_latest.items():
        expected = (
            state_latest.get(market)
            or state_latest.get("TPEX" if market == "TPEx" else "TWSE")
        )

        if expected:
            expected_date = pd.to_datetime(expected).normalize()

            if pd.Timestamp(latest).normalize() != expected_date:
                raise RuntimeError(
                    f"{market} delta 最新日期與 state 不符："
                    f"{latest}/{expected}"
                )

    log(
        "[PASS] 來源讀取完成："
        f"v4.69={len(v469):,} 列；"
        f"v4.17={len(official_base):,} 列；"
        f"delta={len(delta):,} 列。"
    )

    source_manifest = pd.DataFrame([
        {
            "來源": "v4.69歷史影子行情",
            "路徑": str(module.V469_PATH),
            "SHA256": v469_sha,
            "檔案大小bytes": module.V469_PATH.stat().st_size,
            "讀取列數": len(v469),
            "起日": str(v469["date"].min().date()),
            "迄日": str(v469["date"].max().date()),
        },
        {
            "來源": "v4.17官方行情主檔",
            "路徑": str(module.A_PATH),
            "SHA256": state.get("original_source_sha256"),
            "檔案大小bytes": a_stat.st_size,
            "讀取列數": len(official_base),
            "起日": str(official_base["date"].min().date()),
            "迄日": str(official_base["date"].max().date()),
        },
        {
            "來源": "官方行情append-only delta",
            "路徑": str(module.DELTA_PATH),
            "SHA256": sha256_file(module.DELTA_PATH),
            "檔案大小bytes": delta_stat.st_size,
            "讀取列數": len(delta),
            "起日": str(delta["date"].min().date()),
            "迄日": str(delta["date"].max().date()),
        },
    ])

    return {
        "state": state,
        "v469": v469,
        "official_base": official_base,
        "delta": delta,
        "official": official,
        "source_manifest": source_manifest,
        "v469_sha256": v469_sha,
    }


# =============================================================================
# 06. 依 v1.2.6 原函式重建全市場特徵
# =============================================================================

def rebuild_v126_feature_rows(module, sources):
    """
    關鍵原則：
    不在 v1.2.7 重新發明特徵計算。
    直接呼叫 v1.2.6 的：
      build_universe
      build_historical_segments
      build_scale_bridge
      run_forward_readiness
    """
    v469 = sources["v469"]
    official = sources["official"]

    log("[04/08] 重建 v1.2.6 近似普通股範圍……")

    universe, exclusions = module.build_universe(
        v469,
        official,
    )

    universe_codes = set(
        universe["股票代號"].astype(str)
    )

    v469_common = v469.loc[
        v469["code"].astype(str).isin(universe_codes)
    ].copy()

    official_hist = official.loc[
        official["date"].le(module.V469_END)
        & official["code"].astype(str).isin(universe_codes)
    ].copy()

    future = official.loc[
        official["date"].ge(module.FORWARD_START)
        & official["code"].astype(str).isin(universe_codes)
    ].copy()

    log(
        f"[PASS] 近似普通股={len(universe):,} 檔；"
        f"排除代號={len(exclusions):,}。"
    )

    log("[05/08] 重建 v1.2.6 歷史區段及價格尺度橋接……")

    segments, segment_report, hist_ambiguous = (
        module.build_historical_segments(
            v469_common,
            universe_codes,
        )
    )

    ratio_report, bridge, all_ambiguous = (
        module.build_scale_bridge(
            segments,
            official_hist,
            hist_ambiguous,
        )
    )

    # 與 v1.2.6 main_audit 完全相同：
    # 將缺標籤或無有效歷史 OHLC 的具體狀態補回 bridge。
    for row in segment_report.to_dict("records"):
        code = str(row["股票代號"])

        if code not in bridge:
            bridge[code] = {
                "status": str(
                    row.get("歷史橋接狀態") or "NO_HISTORICAL_BASE"
                ),
                "factor": None,
                "matched_rows": 0,
            }

    log("[06/08] 呼叫 v1.2.6 原始前瞻暖機與特徵函式……")

    stock_report, daily_coverage, daily_q5, extra = (
        module.run_forward_readiness(
            universe,
            segments,
            bridge,
            future,
        )
    )

    if daily_q5.empty:
        raise RuntimeError("v1.2.6 前瞻結果沒有任何交易日摘要。")

    if not extra.get("feature_rows"):
        raise RuntimeError("v1.2.6 沒有回傳可重建的 feature rows。")

    log(
        "[PASS] v1.2.6 前瞻特徵重建完成："
        f"{len(daily_q5):,} 個交易日；"
        f"{sum(len(v) for v in extra['feature_rows'].values()):,} 筆特徵。"
    )

    return {
        "universe": universe,
        "exclusions": exclusions,
        "segments": segments,
        "segment_report": segment_report,
        "ratio_report": ratio_report,
        "bridge": bridge,
        "all_ambiguous": all_ambiguous,
        "stock_report": stock_report,
        "daily_coverage": daily_coverage,
        "daily_q5": daily_q5,
        "extra": extra,
        "official_hist": official_hist,
        "future": future,
    }


# =============================================================================
# 07. 重建逐日 Q5 名單
# =============================================================================

def build_q5_detail(
    feature_rows_by_date: Dict,
    universe: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Q5 定義沿用 v1.2.6：
      pct_rank = range_pos20.rank(method='average', pct=True)
      Q5 = pct_rank >= 0.80

    對每個交易日獨立排名。
    不跨日期合併計算 percentile。
    """
    name_map = (
        universe.set_index("股票代號")["股票名稱"]
        .fillna("")
        .astype(str)
        .to_dict()
    )

    market_map = (
        universe.set_index("股票代號")["現行市場"]
        .fillna("")
        .astype(str)
        .to_dict()
    )

    detail_rows = []
    daily_rebuilt_rows = []

    for raw_date in sorted(feature_rows_by_date.keys()):
        date = pd.Timestamp(raw_date).normalize()
        rows = feature_rows_by_date[raw_date]

        if not rows:
            daily_rebuilt_rows.append({
                "日期": str(date.date()),
                "可計算range_pos20數": 0,
                "Q5百分位排名列數": 0,
                "range_pos20不同值數": 0,
                "Q80原始特徵分位數": np.nan,
            })
            continue

        frame = pd.DataFrame(rows).copy()

        if frame.empty:
            continue

        frame["code"] = frame["code"].astype(str)
        frame["range_pos20"] = pd.to_numeric(
            frame["range_pos20"],
            errors="coerce",
        )

        frame = frame.loc[
            frame["range_pos20"].notna()
            & np.isfinite(frame["range_pos20"])
        ].copy()

        if frame.empty:
            continue

        # 必須保持與 v1.2.6 相同的 rank 方法。
        frame["橫斷面百分位排名"] = (
            frame["range_pos20"]
            .rank(method="average", pct=True)
        )

        frame["Q5訊號"] = (
            frame["橫斷面百分位排名"]
            >= Q5_PERCENTILE_CUTOFF
        )

        q80 = float(frame["range_pos20"].quantile(0.80))
        feature_n = int(len(frame))
        q5_n = int(frame["Q5訊號"].sum())
        distinct_n = int(frame["range_pos20"].nunique(dropna=True))

        daily_rebuilt_rows.append({
            "日期": str(date.date()),
            "可計算range_pos20數": feature_n,
            "Q5百分位排名列數": q5_n,
            "range_pos20不同值數": distinct_n,
            "Q80原始特徵分位數": q80,
        })

        for row in frame.itertuples(index=False):
            code = str(row.code)
            value = float(row.range_pos20)
            pct_rank = float(row.橫斷面百分位排名)

            detail_rows.append({
                "訊號日期": str(date.date()),
                "股票代號": code,
                "股票名稱": name_map.get(code, ""),
                "訊號當日市場": str(row.market),
                "range_pos20": value,
                "橫斷面百分位排名": pct_rank,
                "當日橫斷面樣本數": feature_n,
                "當日Q80原始特徵門檻": q80,
                "Q5門檻": Q5_PERCENTILE_CUTOFF,
                "是否Q5訊號": bool(pct_rank >= Q5_PERCENTILE_CUTOFF),
            })

    detail = pd.DataFrame(detail_rows)

    if detail.empty:
        raise RuntimeError("Q5 明細為空，停止產生訊號名單。")

    detail = detail.sort_values(
        [
            "訊號日期",
            "橫斷面百分位排名",
            "range_pos20",
            "股票代號",
        ],
        ascending=[True, False, False, True],
        kind="mergesort",
    ).reset_index(drop=True)

    daily_rebuilt = pd.DataFrame(daily_rebuilt_rows)

    return detail, daily_rebuilt


# =============================================================================
# 08. v1.2.6 九日摘要逐日核對
# =============================================================================

def is_close_value(a: Any, b: Any, tol: float = 1e-12) -> bool:
    a_na = pd.isna(a)
    b_na = pd.isna(b)

    if a_na and b_na:
        return True

    if a_na != b_na:
        return False

    try:
        return bool(
            np.isclose(
                float(a),
                float(b),
                atol=tol,
                rtol=0,
                equal_nan=True,
            )
        )
    except Exception:
        return str(a) == str(b)


def is_same_bool(a: Any, b: Any) -> bool:
    def convert(x):
        if isinstance(x, (bool, np.bool_)):
            return bool(x)
        if pd.isna(x):
            return None
        value = str(x).strip().lower()
        if value in ("true", "1", "yes", "是"):
            return True
        if value in ("false", "0", "no", "否"):
            return False
        return None

    return convert(a) == convert(b)


def verify_against_v126(
    original: pd.DataFrame,
    daily_q5: pd.DataFrame,
    detail: pd.DataFrame,
    rebuilt_detail_summary: pd.DataFrame,
) -> pd.DataFrame:
    """
    以 v1.2.6 原始保存的 9 個日期為基準。
    若未來新增交易日，只核對原始 9 日，不要求新日期出現在舊摘要中。
    """
    required_original = {"日期"} | set(
        QA_INTEGER_FIELDS + QA_FLOAT_FIELDS + QA_TEXT_FIELDS + QA_BOOL_FIELDS
    )

    missing = sorted(required_original - set(original.columns))
    if missing:
        raise RuntimeError(
            "v1.2.6 原始摘要缺少核對欄位：" + ", ".join(missing)
        )

    original = original.copy()
    daily_q5 = daily_q5.copy()
    rebuilt_detail_summary = rebuilt_detail_summary.copy()

    original["日期"] = original["日期"].astype(str)
    daily_q5["日期"] = daily_q5["日期"].astype(str)
    rebuilt_detail_summary["日期"] = rebuilt_detail_summary["日期"].astype(str)

    original_map = original.set_index("日期").to_dict("index")
    current_map = daily_q5.set_index("日期").to_dict("index")
    detail_map = (
        rebuilt_detail_summary
        .set_index("日期")
        .to_dict("index")
    )

    results = []

    q5_detail_count = (
        detail.loc[detail["是否Q5訊號"]]
        .groupby("訊號日期")
        .size()
        .to_dict()
    )

    for date in original["日期"].tolist():
        if date not in current_map:
            results.append({
                "日期": date,
                "核對欄位": "日期存在",
                "v1.2.6原始值": "存在",
                "v1.2.7重建值": "缺少",
                "核對結果": "FAIL",
            })
            continue

        old = original_map[date]
        new = current_map[date]

        for field in QA_INTEGER_FIELDS:
            old_value = pd.to_numeric(old[field], errors="coerce")
            new_value = pd.to_numeric(new.get(field), errors="coerce")

            passed = (
                pd.notna(old_value)
                and pd.notna(new_value)
                and int(old_value) == int(new_value)
            )

            results.append({
                "日期": date,
                "核對欄位": field,
                "v1.2.6原始值": old_value,
                "v1.2.7重建值": new_value,
                "核對結果": "PASS" if passed else "FAIL",
            })

        for field in QA_FLOAT_FIELDS:
            old_value = pd.to_numeric(old[field], errors="coerce")
            new_value = pd.to_numeric(new.get(field), errors="coerce")

            passed = is_close_value(old_value, new_value)

            results.append({
                "日期": date,
                "核對欄位": field,
                "v1.2.6原始值": old_value,
                "v1.2.7重建值": new_value,
                "核對結果": "PASS" if passed else "FAIL",
            })

        for field in QA_TEXT_FIELDS:
            old_value = str(old[field])
            new_value = str(new.get(field, ""))

            passed = old_value == new_value

            results.append({
                "日期": date,
                "核對欄位": field,
                "v1.2.6原始值": old_value,
                "v1.2.7重建值": new_value,
                "核對結果": "PASS" if passed else "FAIL",
            })

        for field in QA_BOOL_FIELDS:
            old_value = old[field]
            new_value = new.get(field)

            passed = is_same_bool(old_value, new_value)

            results.append({
                "日期": date,
                "核對欄位": field,
                "v1.2.6原始值": old_value,
                "v1.2.7重建值": new_value,
                "核對結果": "PASS" if passed else "FAIL",
            })

        # 再檢查逐筆訊號重建數與 v1.2.6 摘要 Q5 數量一致。
        detail_day = detail_map.get(date, {})
        detail_n = int(detail_day.get("Q5百分位排名列數", 0))
        listed_n = int(q5_detail_count.get(date, 0))
        original_q5_n = int(old["Q5百分位排名列數"])

        passed = (
            detail_n == original_q5_n
            and listed_n == original_q5_n
        )

        results.append({
            "日期": date,
            "核對欄位": "逐筆Q5名單筆數",
            "v1.2.6原始值": original_q5_n,
            "v1.2.7重建值": listed_n,
            "核對結果": "PASS" if passed else "FAIL",
        })

    qa = pd.DataFrame(results)

    failures = qa.loc[qa["核對結果"].ne("PASS")]

    if len(failures):
        log("[FAIL] v1.2.6 原始摘要重現核對未通過。")
        log(f"[FAIL] 不一致項目：{len(failures):,}。")
        print(failures.head(100).to_string(index=False), flush=True)

        raise RuntimeError(
            "v1.2.6 九日摘要重現核對失敗；"
            "已停止正式 Q5 訊號輸出，不會建立正式成果資料夾。"
        )

    log(
        "[PASS] v1.2.6 原始九日摘要逐欄重現核對通過："
        f"{len(qa):,} 項。"
    )

    return qa


# =============================================================================
# 09. 最新日 Q5 名單與個股出現頻率
# =============================================================================

def build_latest_q5(detail: pd.DataFrame) -> Tuple[pd.DataFrame, str]:
    signal = detail.loc[detail["是否Q5訊號"]].copy()

    if signal.empty:
        raise RuntimeError("沒有任何 Q5 訊號，不能產生最新日名單。")

    latest_date = signal["訊號日期"].max()

    latest = signal.loc[
        signal["訊號日期"].eq(latest_date)
    ].copy()

    latest = latest.sort_values(
        [
            "橫斷面百分位排名",
            "range_pos20",
            "股票代號",
        ],
        ascending=[False, False, True],
        kind="mergesort",
    ).reset_index(drop=True)

    latest.insert(
        0,
        "當日Q5名單序位",
        np.arange(1, len(latest) + 1),
    )

    return latest, latest_date


def build_q5_frequency(detail: pd.DataFrame) -> pd.DataFrame:
    signal = detail.loc[detail["是否Q5訊號"]].copy()

    total_dates = int(detail["訊號日期"].nunique())

    if signal.empty:
        return pd.DataFrame()

    result = (
        signal.groupby(
            ["股票代號", "股票名稱"],
            dropna=False,
        )
        .agg(
            Q5出現次數=("訊號日期", "nunique"),
            首次Q5日期=("訊號日期", "min"),
            最近Q5日期=("訊號日期", "max"),
            出現市場=("訊號當日市場", lambda x: "；".join(sorted(set(map(str, x))))),
        )
        .reset_index()
    )

    result["觀察交易日數"] = total_dates
    result["Q5出現比例"] = (
        result["Q5出現次數"] / total_dates
        if total_dates
        else np.nan
    )

    result = result.sort_values(
        ["Q5出現次數", "最近Q5日期", "股票代號"],
        ascending=[False, False, True],
        kind="mergesort",
    ).reset_index(drop=True)

    result.insert(
        0,
        "出現頻率序位",
        np.arange(1, len(result) + 1),
    )

    result["注意"] = (
        "僅表示觀察期間Q5訊號出現頻率，不代表獲利能力或推薦排名"
    )

    return result


# =============================================================================
# 10. H=10 全市場交易日觀察排程
# =============================================================================

def build_global_calendar(official: pd.DataFrame) -> List[pd.Timestamp]:
    """
    使用官方行情來源中實際出現的全市場日期建立交易日軸。
    不使用 pandas BDay、不自行推算國定假日，也不補造未出現在來源的日期。
    星期六補行日沿用 v1.2.6 研究規則排除。
    """
    dates = (
        official.loc[
            official["date"].notna()
            & official["date"].dt.weekday.lt(5),
            "date",
        ]
        .drop_duplicates()
        .sort_values()
    )

    result = [
        pd.Timestamp(x).normalize()
        for x in dates.tolist()
    ]

    if not result:
        raise RuntimeError("官方行情來源無法建立全市場交易日軸。")

    return result


def stock_bar_status(
    official: pd.DataFrame,
    module,
    code: str,
    date: Optional[pd.Timestamp],
) -> Tuple[str, str]:
    """
    只檢查該代號該日是否有唯一且有效的 OHLC。
    不取價、不計算報酬。
    """
    if date is None:
        return "DATE_NOT_YET_AVAILABLE", ""

    selected = official.loc[
        official["code"].astype(str).eq(str(code))
        & official["date"].eq(pd.Timestamp(date))
    ].copy()

    if selected.empty:
        return "STOCK_BAR_NOT_FOUND", ""

    if len(selected) > 1:
        return "AMBIGUOUS_CODE_DATE", ""

    market = str(selected.iloc[0]["market"])

    valid = module.valid_ohlc(selected)

    if not bool(valid.iloc[0]):
        return "INVALID_OHLC", market

    return "VALID_OHLC", market


def build_h10_schedule(
    detail: pd.DataFrame,
    official: pd.DataFrame,
    module,
) -> pd.DataFrame:
    """
    每一筆 Q5 訊號分別建立：
      entry_date = signal_date + 1 個全市場交易日
      exit_date  = signal_date + 10 個全市場交易日

    日期只取自實際來源交易日軸。
    尚未出現在來源的未來交易日不預測、不填造。
    本版不計算任何報酬。
    """
    signals = detail.loc[detail["是否Q5訊號"]].copy()

    if signals.empty:
        return pd.DataFrame()

    calendar = build_global_calendar(official)
    calendar_index = {
        pd.Timestamp(date): idx
        for idx, date in enumerate(calendar)
    }

    source_latest = calendar[-1]
    rows = []

    for signal in signals.itertuples(index=False):
        code = str(signal.股票代號)
        signal_date = pd.Timestamp(signal.訊號日期).normalize()

        if signal_date not in calendar_index:
            raise RuntimeError(
                f"訊號日期不在官方全市場交易日軸：{signal_date.date()}"
            )

        idx = calendar_index[signal_date]

        entry_date = (
            calendar[idx + 1]
            if idx + 1 < len(calendar)
            else None
        )

        exit_date = (
            calendar[idx + HORIZON]
            if idx + HORIZON < len(calendar)
            else None
        )

        known_after = int(
            sum(
                1
                for date in calendar
                if signal_date < date <= source_latest
            )
        )

        observed_after = min(HORIZON, known_after)
        remaining = max(0, HORIZON - observed_after)

        if entry_date is None:
            entry_status = "WAITING_FOR_ENTRY_DATE"
            entry_market = ""
        else:
            entry_status, entry_market = stock_bar_status(
                official,
                module,
                code,
                entry_date,
            )

        if exit_date is None:
            exit_status = "WAITING_FOR_H10_EXIT_DATA"
            exit_market = ""
        else:
            exit_status, exit_market = stock_bar_status(
                official,
                module,
                code,
                exit_date,
            )

        if exit_date is None:
            overall_status = "WAITING_FOR_H10_EXIT_DATA"
        elif entry_status != "VALID_OHLC":
            overall_status = "ENTRY_DATA_REQUIRES_REVIEW"
        elif exit_status == "AMBIGUOUS_CODE_DATE":
            overall_status = "EXIT_CODE_DATE_AMBIGUOUS"
        elif exit_status != "VALID_OHLC":
            overall_status = "EXIT_STOCK_BAR_REQUIRES_REVIEW"
        else:
            overall_status = "DATES_AND_OHLC_READY_NOT_RETURNED"

        rows.append({
            "訊號日期": str(signal_date.date()),
            "股票代號": code,
            "股票名稱": str(signal.股票名稱),
            "訊號當日市場": str(signal.訊號當日市場),
            "range_pos20": float(signal.range_pos20),
            "橫斷面百分位排名": float(signal.橫斷面百分位排名),
            "H": HORIZON,
            "預定進場規則": "訊號日後第1個全市場交易日開盤",
            "預定出場規則": "訊號日後第10個全市場交易日收盤",
            "進場日期": (
                str(entry_date.date()) if entry_date is not None else ""
            ),
            "進場日資料狀態": entry_status,
            "進場日市場": entry_market,
            "出場日期": (
                str(exit_date.date()) if exit_date is not None else ""
            ),
            "出場日資料狀態": exit_status,
            "出場日市場": exit_market,
            "截至來源最後日已觀察後續交易日數": known_after,
            "H10已完成觀察交易日數": observed_after,
            "距H10尚需交易日數": remaining,
            "來源最後交易日": str(source_latest.date()),
            "觀察狀態": overall_status,
            "淨報酬": np.nan,
            "毛報酬": np.nan,
            "本版不計算報酬": True,
        })

    result = pd.DataFrame(rows)

    result = result.sort_values(
        ["訊號日期", "股票代號"],
        kind="mergesort",
    ).reset_index(drop=True)

    return result


# =============================================================================
# 11. 暖機／價格銜接例外名單
# =============================================================================

def build_warmup_exceptions(
    stock_report: pd.DataFrame,
) -> pd.DataFrame:
    if stock_report.empty:
        return stock_report.copy()

    result = stock_report.copy()

    bridge_col = "歷史橋接狀態"
    warm_col = "目前區段尚需有效K棒數"

    bridge_not_ready = (
        result[bridge_col].astype(str).ne("BRIDGE_READY")
        if bridge_col in result.columns
        else pd.Series(True, index=result.index)
    )

    warm_not_ready = (
        pd.to_numeric(result[warm_col], errors="coerce").fillna(20).gt(0)
        if warm_col in result.columns
        else pd.Series(True, index=result.index)
    )

    result = result.loc[
        bridge_not_ready | warm_not_ready
    ].copy()

    result["本次不納入Q5特徵的原因"] = np.where(
        result.get(bridge_col, "").astype(str).ne("BRIDGE_READY"),
        "歷史價格橋接未達可用條件或需重新暖機",
        "目前價格區段有效K棒尚未達20根",
    )

    return result.reset_index(drop=True)


# =============================================================================
# 12. 執行報告
# =============================================================================

def create_reports(
    out_dir: Path,
    sources: Dict[str, Any],
    rebuilt: Dict[str, Any],
    qa: pd.DataFrame,
    detail: pd.DataFrame,
    latest: pd.DataFrame,
    latest_date: str,
    frequency: pd.DataFrame,
    h10: pd.DataFrame,
    warmup: pd.DataFrame,
    daily_detail_summary: pd.DataFrame,
    module,
    run_id: str,
) -> Dict[str, Any]:

    source_latest = (
        sources["official"]["date"].max()
    )

    daily_q5 = rebuilt["daily_q5"].copy()

    # v1.2.6 daily_q5 已包含雙市場覆蓋等原始欄位。
    daily_summary = daily_q5.merge(
        daily_detail_summary,
        on="日期",
        how="left",
        suffixes=("", "_逐筆重建"),
    )

    signal = detail.loc[detail["是否Q5訊號"]].copy()

    status_counts = (
        h10["觀察狀態"].value_counts(dropna=False).to_dict()
        if len(h10)
        else {}
    )

    report = {
        "版本": VERSION,
        "執行時間台北": datetime.now(TAIPEI).isoformat(),
        "run_id": run_id,
        "v1.2.6程式GitBlobSHA1": V126_SCRIPT_BLOB_SHA1,
        "v1.2.6原始摘要GitBlobSHA1": V126_SUMMARY_BLOB_SHA1,
        "v4.69_SHA256": sources["v469_sha256"],
        "v4.69最後日期": str(module.V469_END.date()),
        "官方行情來源最後日期": str(pd.Timestamp(source_latest).date()),
        "近似全市場普通股數": int(len(rebuilt["universe"])),
        "前瞻交易日數": int(len(daily_q5)),
        "有特徵值的交易日數": int(detail["訊號日期"].nunique()),
        "每日特徵明細筆數": int(len(detail)),
        "Q5訊號總筆數": int(len(signal)),
        "最新訊號日期": latest_date,
        "最新日期Q5訊號數": int(len(latest)),
        "Q5定義": (
            "逐交易日 range_pos20.rank(method='average', pct=True) >= 0.80"
        ),
        "range_pos20定義": (
            "(當日收盤價-最近20根有效K棒最低價)"
            "/(最近20根有效K棒最高價-最近20根有效K棒最低價)"
        ),
        "H10規則": (
            "訊號日後第1個全市場交易日開盤進場；"
            "訊號日後第10個全市場交易日收盤觀察出場。"
        ),
        "H10排程筆數": int(len(h10)),
        "H10觀察狀態分布": {
            str(k): int(v)
            for k, v in status_counts.items()
        },
        "v1.2.6九日原始摘要核對": (
            "PASS" if qa["核對結果"].eq("PASS").all() else "FAIL"
        ),
        "v1.2.6摘要核對項目數": int(len(qa)),
        "v1.2.6摘要核對失敗數": int(
            qa["核對結果"].ne("PASS").sum()
        ),
        "暖機與價格銜接例外股票數": int(len(warmup)),
        "明確未執行": [
            "未重新訓練模型",
            "未修改v2.48.1或其他凍結公式",
            "未加入成交量因子",
            "未計算毛報酬或淨報酬",
            "未進行H10績效回測",
            "未做多重檢定",
            "未按出現頻率挑選股票",
            "未推薦股票",
            "未硬湊10檔股票",
        ],
        "研究限制": [
            "普通股範圍沿用v1.2.6近似規則，不等於逐日官方證券類別完整認證。",
            "Q5只表示當日range_pos20橫斷面排名位於前20%，不代表未來獲利。",
            "Q5出現頻率只作為觀察欄位，不是預測分數。",
            "H10日期只使用官方行情來源實際存在的全市場交易日。",
            "來源尚未出現的未來交易日不推算、不補造。",
            "本版不計算尚未完成觀察樣本的報酬。",
            "公司行動、跨市場身分及價格區段仍須延續v1.2.6規則核對。",
        ],
        "安全限制": [
            "Google Drive原始來源唯讀",
            "不呼叫官方API",
            "不修改、刪除、移動或覆寫既有檔案",
            "只建立新的唯一成果資料夾",
            "GitHub只建立獨立audit branch",
            "不更新main",
        ],
    }

    # 來源清冊
    csv_write(
        sources["source_manifest"],
        out_dir / "00_來源檔案與版本驗證.csv",
    )

    # 摘要逐欄核對
    csv_write(
        qa,
        out_dir / "01_v126每日摘要逐日重現核對.csv",
    )

    # 每日全部可計算 feature rows，另列 Q5 True/False
    csv_write(
        detail,
        out_dir / "02_每日全市場Q5訊號明細.csv",
    )

    # 最新日 Q5 名單
    csv_write(
        latest,
        out_dir / "03_最新交易日Q5名單.csv",
    )

    # 個股頻率，不作獲利排名
    csv_write(
        frequency,
        out_dir / "04_個股Q5出現頻率.csv",
    )

    # H10 排程
    csv_write(
        h10,
        out_dir / "05_H10觀察排程.csv",
    )

    # 暖機例外
    csv_write(
        warmup,
        out_dir / "06_價格銜接與暖機例外.csv",
    )

    # 每日摘要
    csv_write(
        daily_summary,
        out_dir / "07_每日Q5橫斷面摘要.csv",
    )

    # 近似普通股清單及排除表
    csv_write(
        rebuilt["universe"],
        out_dir / "08_近似全市場普通股範圍.csv",
    )

    csv_write(
        rebuilt["exclusions"],
        out_dir / "09_普通股範圍排除清單.csv",
    )

    # JSON 結論
    json_write(
        report,
        out_dir / "10_v127訊號凍結與H10觀察結論.json",
    )

    latest_q5_counts = (
        latest["是否Q5訊號"].sum()
        if len(latest)
        else 0
    )

    pending_h10 = (
        int(h10["觀察狀態"].eq("WAITING_FOR_H10_EXIT_DATA").sum())
        if len(h10)
        else 0
    )

    summary_lines = [
        "=" * 100,
        "SSO v1.2.7｜全市場 Q5 訊號凍結與 H=10 觀察排程",
        "=" * 100,
        f"執行時間：{report['執行時間台北']}",
        f"v4.69 SHA256：{sources['v469_sha256']}",
        f"官方行情來源最後日期：{report['官方行情來源最後日期']}",
        f"近似全市場普通股：{len(rebuilt['universe']):,} 檔",
        "",
        "一、v1.2.6 原始結果重現",
        f"核對項目：{len(qa):,}",
        f"核對失敗：{int(qa['核對結果'].ne('PASS').sum()):,}",
        "核對結果：PASS",
        "",
        "二、Q5 訊號凍結",
        f"可計算特徵明細：{len(detail):,} 筆",
        f"Q5 訊號總筆數：{len(signal):,}",
        f"有特徵值交易日：{detail['訊號日期'].nunique():,} 日",
        f"最新訊號日期：{latest_date}",
        f"最新日期 Q5 訊號：{int(latest_q5_counts):,} 檔",
        "",
        "三、H=10 觀察排程",
        f"排程筆數：{len(h10):,}",
        f"尚待 H10 出場資料：{pending_h10:,} 筆",
        f"來源最後交易日：{report['官方行情來源最後日期']}",
        "",
        "四、解讀限制",
        "Q5 僅代表當日 range_pos20 橫斷面排名前20%。",
        "Q5 出現頻率不等於獲利機率，也不是選股分數。",
        "H=10 尚未成熟者不計算報酬，不補造未來交易日。",
        "本版不推薦股票、不回測、不重訓、不硬湊10檔。",
        "",
        "五、安全",
        "Drive原始來源唯讀。",
        "GitHub只新增獨立audit branch，不更新main。",
        "=" * 100,
    ]

    (out_dir / "11_執行摘要.txt").write_text(
        "\n".join(summary_lines) + "\n",
        encoding="utf-8-sig",
    )

    return report


# =============================================================================
# 13. ZIP
# =============================================================================

def create_zip(out_dir: Path, run_id: str) -> Path:
    zip_path = out_dir.with_suffix(".zip")

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
            raise RuntimeError(f"ZIP CRC 驗證失敗：{bad}")

    log(
        f"[PASS] ZIP 建立及 CRC 驗證完成："
        f"{zip_path.name} | {fmt_bytes(zip_path.stat().st_size)}"
    )

    return zip_path


# =============================================================================
# 14. GitHub 獨立 audit branch 上傳
# =============================================================================

def run_git(
    args: List[str],
    cwd: Optional[Path],
    env: Dict[str, str],
    input_bytes: Optional[bytes] = None,
) -> str:
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


def upload_github(
    out_dir: Path,
    zip_path: Path,
    report: Dict[str, Any],
    run_id: str,
) -> Dict[str, Any]:
    token = find_github_token()

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
            "存在單檔達 GitHub 100MB 限制："
            + repr(too_large)
        )

    branch = (
        "audit/sso-v1.2.7-q5-signal-freeze-"
        f"{run_id}-gitpush-{uuid.uuid4().hex[:4]}"
    )

    artifact_dir = (
        f"audit/sso-v1.2.7-q5-signal-freeze/{run_id}"
    )

    readme_lines = [
        "# SSO v1.2.7｜全市場 Q5 訊號凍結與 H=10 觀察排程",
        "",
        f"- 執行時間：`{report['執行時間台北']}`",
        f"- 近似普通股：`{report['近似全市場普通股數']}`",
        f"- Q5 訊號總筆數：`{report['Q5訊號總筆數']}`",
        f"- 最新訊號日期：`{report['最新訊號日期']}`",
        f"- 最新日期 Q5：`{report['最新日期Q5訊號數']}`",
        f"- H10 排程筆數：`{report['H10排程筆數']}`",
        "- v1.2.6 原始九日摘要核對：PASS",
        "- 本版不計算淨報酬、不進行回測、不重訓、不推薦股票。",
        "- Google Drive 原始來源唯讀。",
        "- GitHub 僅新增獨立 audit branch，不更新 main。",
        "",
        "## 本次上傳檔案",
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
        prefix="sso_v127_gitpush_"
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

        log("[GITHUB 01/06] 初始化暫存 bare repository……")

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

        log("[GITHUB 02/06] 取得 main 基準 commit……")

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

        main_sha = run_git(
            ["rev-parse", "FETCH_HEAD^{commit}"],
            bare,
            env,
        )

        run_git(
            ["read-tree", main_sha],
            bare,
            env,
        )

        log(f"[PASS] main 基準 SHA：{main_sha}")

        log("[GITHUB 03/06] 寫入本次報告、程式及 ZIP……")

        items = [
            (path, f"{artifact_dir}/{path.name}", None)
            for path in upload_files
        ]

        items.append((
            None,
            f"{artifact_dir}/README_GitHub上傳索引.md",
            readme_bytes,
        ))

        for index, (local_path, remote_path, inline_bytes) in enumerate(
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
                size = len(inline_bytes or b"")

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

            log(
                f"[ADD {index}/{len(items)}] "
                f"{name} | {fmt_bytes(size)}"
            )

        log("[GITHUB 04/06] 建立以 main 為 parent 的 audit commit……")

        tree = run_git(
            ["write-tree"],
            bare,
            env,
        )

        commit = run_git(
            [
                "-c",
                "user.name=SSO Audit Uploader",
                "-c",
                "user.email=sso-audit-uploader@users.noreply.github.com",
                "commit-tree",
                tree,
                "-p",
                main_sha,
                "-m",
                f"Add SSO v1.2.7 Q5 signal freeze audit ({run_id})",
            ],
            bare,
            env,
        )

        log("[GITHUB 05/06] 推送獨立 audit branch……")

        run_git(
            [
                "push",
                "origin",
                f"{commit}:refs/heads/{branch}",
            ],
            bare,
            env,
        )

        log("[GITHUB 06/06] 驗證 audit branch 與 main……")

        remote_branch = run_git(
            [
                "ls-remote",
                "origin",
                f"refs/heads/{branch}",
            ],
            bare,
            env,
        ).split()

        if not remote_branch or remote_branch[0] != commit:
            raise RuntimeError(
                "GitHub audit branch SHA 驗證失敗。"
            )

        remote_main = run_git(
            [
                "ls-remote",
                "origin",
                f"refs/heads/{DEFAULT_BRANCH}",
            ],
            bare,
            env,
        ).split()

        main_after = remote_main[0] if remote_main else ""

        if main_after != main_sha:
            raise RuntimeError(
                "GitHub main SHA 前後不同："
                f"{main_sha} -> {main_after}。"
                "本程式沒有對 main 執行 push，請人工檢查是否有其他並行更新。"
            )

        env["GITHUB_TOKEN"] = ""

    return {
        "branch": branch,
        "commit": commit,
        "main_before": main_sha,
        "main_after": main_after,
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
            f"/commit/{commit}"
        ),
        "uploaded_files": len(items),
    }


# =============================================================================
# 15. 主流程
# =============================================================================

def main() -> int:
    print("=" * 104, flush=True)
    log(VERSION)
    log("本版只凍結 Q5 訊號並建立 H10 觀察排程。")
    log("不計算報酬、不回測、不重訓、不推薦股票、不修改 main。")

    ensure_drive()

    # 在大量讀取前先確認 GitHub token 可取得。
    find_github_token()

    # 載入並驗證固定版本 v1.2.6。
    module, dependency_path = load_v126_module()
    module.self_test()

    original_summary = load_v126_reference_summary()

    # 讀取來源並重建 v1.2.6 原始計算。
    sources = load_and_validate_sources(module)
    rebuilt = rebuild_v126_feature_rows(module, sources)

    # 逐日重建 Q5 明細。
    log("[07/08] 重建逐日 Q5 橫斷面訊號……")

    detail, rebuilt_detail_summary = build_q5_detail(
        rebuilt["extra"]["feature_rows"],
        rebuilt["universe"],
    )

    # 與 v1.2.6 原始九日結果逐欄核對。
    qa = verify_against_v126(
        original_summary,
        rebuilt["daily_q5"],
        detail,
        rebuilt_detail_summary,
    )

    # 核對通過才建立正式 Q5 名單。
    latest, latest_date = build_latest_q5(detail)
    frequency = build_q5_frequency(detail)

    # 建立 H10 觀察排程。
    log("[08/08] 建立 H=10 觀察排程……")

    h10 = build_h10_schedule(
        detail,
        sources["official"],
        module,
    )

    # 暖機／橋接例外。
    warmup = build_warmup_exceptions(
        rebuilt["stock_report"],
    )

    # 唯一輸出資料夾，不覆寫舊資料夾。
    run_id = run_stamp()

    out_dir = (
        ROOT
        / f"看股最速適_v1_2_7_Q5訊號凍結與H10觀察排程_{run_id}"
    )

    out_dir.mkdir(
        parents=True,
        exist_ok=False,
    )

    # 複製目前執行程式至成果資料夾。
    # 如果是從 .py 上傳執行，會將完整 .py 一併封存。
    script_path = None

    try:
        possible_script = Path(__file__).resolve()

        if possible_script.is_file():
            script_path = out_dir / (
                "SSO_v1.2.7_全市場Q5訊號凍結與H10觀察排程.py"
            )
            shutil.copy2(
                possible_script,
                script_path,
            )
            log("[PASS] 已將本次完整 Python 程式納入成果資料夾。")
    except Exception as exc:
        log(
            "[WARN] 無法自動複製執行程式；"
            f"其餘報告仍會產生。原因：{exc}"
        )

    report = create_reports(
        out_dir=out_dir,
        sources=sources,
        rebuilt=rebuilt,
        qa=qa,
        detail=detail,
        latest=latest,
        latest_date=latest_date,
        frequency=frequency,
        h10=h10,
        warmup=warmup,
        daily_detail_summary=rebuilt_detail_summary,
        module=module,
        run_id=run_id,
    )

    # README 檔案清單及 SHA256。
    readme_lines = [
        "# SSO v1.2.7｜全市場 Q5 訊號凍結與 H=10 觀察排程",
        "",
        f"- 執行時間：`{report['執行時間台北']}`",
        f"- Q5 訊號總筆數：`{report['Q5訊號總筆數']}`",
        f"- 最新訊號日期：`{report['最新訊號日期']}`",
        f"- 最新日期 Q5 數量：`{report['最新日期Q5訊號數']}`",
        f"- H10 排程：`{report['H10排程筆數']}`",
        "- v1.2.6 原始九日摘要逐欄重現：PASS",
        "- 不計算毛報酬／淨報酬，不進行回測或模型訓練。",
        "- 不以 Q5 出現頻率作為獲利排名或推薦依據。",
        "- Google Drive 原始來源唯讀。",
        "- GitHub 僅建立獨立 audit branch，不更新 main。",
        "",
        "## 檔案清單",
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

    # ZIP 包含所有報告、README 及完整程式。
    zip_path = create_zip(
        out_dir,
        run_id,
    )

    # GitHub 獨立 audit branch。
    log("[GITHUB] 上傳本次完整成果……")

    upload = upload_github(
        out_dir,
        zip_path,
        report,
        run_id,
    )

    # 最終輸出。
    print("\n" + "=" * 104, flush=True)
    print("[PASS] SSO v1.2.7 執行完成", flush=True)
    print("=" * 104, flush=True)

    print(f"版本：{VERSION}", flush=True)
    print(f"執行資料夾：{out_dir}", flush=True)
    print(f"ZIP：{zip_path}", flush=True)

    print("", flush=True)
    print("一、v1.2.6 原始摘要重現核對", flush=True)
    print(
        f"核對項目：{len(qa):,}；"
        f"失敗：{int(qa['核對結果'].ne('PASS').sum()):,}",
        flush=True,
    )

    print("", flush=True)
    print("二、Q5 訊號", flush=True)
    print(f"逐日特徵明細：{len(detail):,} 筆", flush=True)
    print(
        f"Q5 訊號總筆數：{report['Q5訊號總筆數']:,}",
        flush=True,
    )
    print(f"最新訊號日期：{latest_date}", flush=True)
    print(
        f"最新日期 Q5 數量：{len(latest):,}",
        flush=True,
    )

    print("", flush=True)
    print("三、H=10 觀察排程", flush=True)
    print(f"排程筆數：{len(h10):,}", flush=True)

    if len(h10):
        print(
            h10["觀察狀態"].value_counts(dropna=False).to_string(),
            flush=True,
        )

    print("", flush=True)
    print("四、GitHub", flush=True)
    print(f"Repository：{REPO_OWNER}/{REPO_NAME}", flush=True)
    print(f"Audit branch：{upload['branch']}", flush=True)
    print(f"Commit：{upload['commit']}", flush=True)
    print(f"上傳檔案數：{upload['uploaded_files']}", flush=True)
    print(f"GitHub 資料夾：{upload['folder_url']}", flush=True)
    print(f"GitHub ZIP：{upload['zip_url']}", flush=True)
    print(f"GitHub Commit：{upload['commit_url']}", flush=True)
    print(f"main SHA before：{upload['main_before']}", flush=True)
    print(f"main SHA after：{upload['main_after']}", flush=True)

    print("", flush=True)
    print("[PASS] main 未更新。", flush=True)
    print("[PASS] Drive 原始來源唯讀。", flush=True)
    print("[PASS] 本版未計算任何未成熟樣本報酬。", flush=True)
    print("=" * 104, flush=True)

    # 暫存下載的 v1.2.6 dependency 留在 /tmp；
    # 不在 Drive 建立或改寫任何來源檔。
    return 0


if __name__ == "__main__":
    raise SystemExit(main())