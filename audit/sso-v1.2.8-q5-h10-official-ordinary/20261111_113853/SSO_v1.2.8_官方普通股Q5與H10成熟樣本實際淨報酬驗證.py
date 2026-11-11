# -*- coding: utf-8 -*-
"""
SSO v1.2.8｜官方普通股 Q5 與 H=10 成熟樣本實際淨報酬驗證

承接：
1. SSO v1.2.7 凍結的 Q5 訊號定義及 v1.2.6 特徵重建函式。
2. v1.5.1 官方全市場行情增量，僅補入 2026-11-09、2026-11-10。
3. v1.4.1 官方 ISIN CFI=ES 普通股分類名單。

本版分開輸出兩種訊號口徑：
A. v1.2.7 原凍結 Q5：保留原排名，只用官方普通股名單過濾。
B. v1.2.8 官方普通股 Q5：只以官方確認普通股重新計算每日橫斷面排名。

H=10：
訊號日後第 1 個全市場交易日開盤進場；
訊號日後第 10 個全市場交易日收盤出場。

淨報酬：
完全沿用 v1.2.2 的既有成本公式及參數。

安全：
- Google Drive 原始來源唯讀。
- 不呼叫 TWSE／TPEx API。
- 不修改、刪除、移動、覆寫正式資料。
- 不重新訓練模型。
- 不修改 v1.2.7 凍結訊號。
- 不將未成熟樣本當成績效。
- GitHub 只建立獨立 audit branch，不更新 main。
- 自動驗證 GitHub main SHA 前後一致。
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

import numpy as np
import pandas as pd


# =============================================================================
# 01. 固定版本及來源
# =============================================================================

VERSION = "SSO_v1.2.8_官方普通股Q5與H10成熟樣本實際淨報酬驗證"

ROOT = Path("/content/drive/MyDrive/看股最速適")
STATE_DIR = ROOT / "SSO_Market_Data_Continuation_TWSE_TPEX_v1"

TAIPEI = ZoneInfo("Asia/Taipei")

REPO_OWNER = "CHLINX"
REPO_NAME = "sso"
REPO_URL = "https://github.com/CHLINX/sso.git"
DEFAULT_BRANCH = "main"

# v1.2.7 凍結版本
V127_BRANCH = (
    "audit/sso-v1.2.7-q5-signal-freeze-"
    "20261110_133121-gitpush-e605"
)

V127_SCRIPT_PATH = (
    "audit/sso-v1.2.7-q5-signal-freeze/"
    "20261110_133121/"
    "SSO_v1.2.7_全市場Q5訊號凍結與H10觀察排程.py"
)

V127_SCRIPT_BLOB_SHA1 = (
    "46b725cf85f0d2cb15509d46de31de53705a970f"
)

# 官方全市場增量 ZIP
V151_ZIP = (
    STATE_DIR
    / "v1.5.1_官方全市場行情增量銜接至20261110_"
      "20261111_013339_f42c9.zip"
)

V151_SHA256 = (
    "527ed90bd333162d169a7c0df03630ee0b132eddf4c6f57b583e60f72925bc43"
)

# 官方 ISIN 普通股分類 ZIP
V141_ZIP = (
    STATE_DIR
    / "v1.4.1_官方ISIN商品種類核驗與普通股行情輸出_"
      "20261110_20261110_230801.zip"
)

V141_SHA256 = (
    "b5785140fd80552f4c8bd6fbeeba2df65fa4b15252ef687f058545edc4fd916d"
)

# 僅新增這兩個交易日，不重新讀取或補抓舊日期
NEW_DATES = {
    pd.Timestamp("2026-11-09"),
    pd.Timestamp("2026-11-10"),
}

LATEST_EXPECTED_DATE = pd.Timestamp("2026-11-10")

# v1.4.2／v1.5.1 已保存的 11/9、11/10 雙市場原始列數
EXPECTED_DAILY_MARKET_ROWS = {
    ("2026-11-09", "TWSE"): 1389,
    ("2026-11-09", "TPEx"): 1020,
    ("2026-11-10", "TWSE"): 1389,
    ("2026-11-10", "TPEx"): 1020,
}

EXPECTED_OFFICIAL_ORDINARY_KEYS = 1951
EXPECTED_V127_Q5_FIRST_NINE_DAYS = 3446
EXPECTED_V126_QA_ITEMS = 126

HORIZON = 10
Q5_CUTOFF = 0.80

# v1.2.2 原始基準成本
BASE_SLIP = 0.001
BASE_FEE = 0.001425
BASE_TAX = 0.003

# v1.2.2 原始壓力成本
STRESS_SLIP = 0.002
STRESS_FEE = 0.001425
STRESS_TAX = 0.003


# =============================================================================
# 02. 通用工具
# =============================================================================

def log(message):
    now = datetime.now(TAIPEI).strftime("%H:%M:%S")
    print(f"[{now}] {message}", flush=True)


def run_stamp():
    return datetime.now(TAIPEI).strftime("%Y%m%d_%H%M%S")


def sha256_file(path):
    digest = hashlib.sha256()

    with open(path, "rb") as f:
        for block in iter(lambda: f.read(8 * 1024 * 1024), b""):
            digest.update(block)

    return digest.hexdigest()


def sha256_bytes(data):
    return hashlib.sha256(data).hexdigest()


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


def json_write(data, path):
    path.write_text(
        json.dumps(
            data,
            ensure_ascii=False,
            indent=2,
            default=str,
        ),
        encoding="utf-8-sig",
    )


def fmt_bytes(size):
    size = int(size)

    if size >= 1024 ** 3:
        return f"{size / 1024**3:.3f} GB"

    if size >= 1024 ** 2:
        return f"{size / 1024**2:.2f} MB"

    return f"{size / 1024:.1f} KB"


def read_zip_member_csv(zf, member):
    raw = zf.read(member)

    frame = pd.read_csv(
        io.BytesIO(raw),
        dtype="string",
        encoding="utf-8-sig",
        low_memory=False,
    )

    return frame, raw


def find_member_by_basename(names, basename):
    matches = [
        name
        for name in names
        if Path(name).name == basename
    ]

    if len(matches) != 1:
        raise RuntimeError(
            f"ZIP 成員數量錯誤：{basename}；"
            f"找到 {len(matches)} 個。"
        )

    return matches[0]


def archive_current_program(out_dir):
    """
    若從 .py 執行，保存原程式。
    若從 Colab 儲存格執行，嘗試從 IPython history 保存完整儲存格。
    """

    target = out_dir / (
        "SSO_v1.2.8_官方普通股Q5與H10成熟樣本實際淨報酬驗證.py"
    )

    try:
        current_file = globals().get("__file__")

        if current_file:
            source = Path(current_file)

            if source.is_file():
                shutil.copy2(source, target)
                log("[PASS] 已封存執行程式。")
                return str(target)

    except Exception as exc:
        log(f"[WARN] 無法由 __file__ 封存程式：{exc}")

    try:
        from IPython import get_ipython

        shell = get_ipython()

        if shell is not None:
            history = shell.history_manager.input_hist_raw

            for candidate in reversed(history):
                if (
                    isinstance(candidate, str)
                    and "SSO_v1.2.8_官方普通股Q5" in candidate
                    and "def main():" in candidate
                    and len(candidate) > 10000
                ):
                    target.write_text(candidate, encoding="utf-8")
                    log("[PASS] 已從 Colab 執行紀錄封存完整程式。")
                    return str(target)

    except Exception as exc:
        log(f"[WARN] 無法由 Colab history 封存程式：{exc}")

    log("[INFO] 本次程式完整原文保留於目前對話，未另建程式檔。")
    return None


# =============================================================================
# 03. 動態載入 v1.2.7 凍結程式
# =============================================================================

def load_v127_module():
    url = (
        "https://raw.githubusercontent.com/"
        f"{REPO_OWNER}/{REPO_NAME}/"
        f"{V127_BRANCH}/"
        f"{quote(V127_SCRIPT_PATH, safe='/')}"
    )

    request = urllib.request.Request(
        url,
        headers={"User-Agent": "SSO-v1.2.8-audit"},
    )

    with urllib.request.urlopen(request, timeout=120) as response:
        source_bytes = response.read()

    actual_sha = git_blob_sha1(source_bytes)

    if actual_sha != V127_SCRIPT_BLOB_SHA1:
        raise RuntimeError(
            "v1.2.7 程式 Git blob SHA1 不符。"
            f"\n預期：{V127_SCRIPT_BLOB_SHA1}"
            f"\n實際：{actual_sha}"
        )

    temp_script = (
        Path(tempfile.gettempdir())
        / f"SSO_v127_dependency_{uuid.uuid4().hex[:8]}.py"
    )

    temp_script.write_bytes(source_bytes)

    spec = importlib.util.spec_from_file_location(
        "sso_v127_frozen_dependency",
        str(temp_script),
    )

    if spec is None or spec.loader is None:
        raise RuntimeError("無法載入 v1.2.7 固定版本程式。")

    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)

    required = [
        "load_v126_module",
        "load_v126_reference_summary",
        "load_and_validate_sources",
        "rebuild_v126_feature_rows",
        "build_q5_detail",
        "verify_against_v126",
        "build_h10_schedule",
        "build_global_calendar",
    ]

    missing = [
        name
        for name in required
        if not hasattr(module, name)
    ]

    if missing:
        raise RuntimeError(
            "v1.2.7 固定版本缺少必要函式：" + repr(missing)
        )

    log("[PASS] v1.2.7 固定程式 SHA1 驗證完成。")

    return module


# =============================================================================
# 04. 讀取 11/9、11/10 官方全市場增量
# =============================================================================

def find_overlay_member(zf):
    names = zf.namelist()

    standardized = [
        name
        for name in names
        if Path(name).name
        == "全市場標準化增量_20261029_20261110.csv"
    ]

    if len(standardized) == 1:
        return standardized[0]

    # 若標準化主 CSV 不在 ZIP，使用每檔 overlay 與索引重建。
    index_matches = [
        name
        for name in names
        if Path(name).name == "個股增量覆蓋檔索引.csv"
    ]

    if len(index_matches) != 1:
        raise RuntimeError(
            "找不到標準化增量 CSV，且沒有唯一的個股增量索引。"
        )

    return None


def load_overlay(v126):
    if not V151_ZIP.is_file():
        raise FileNotFoundError(f"找不到 v1.5.1 ZIP：{V151_ZIP}")

    actual_zip_sha = sha256_file(V151_ZIP)

    if actual_zip_sha != V151_SHA256:
        raise RuntimeError(
            "v1.5.1 ZIP SHA256 不符。"
            f"\n預期：{V151_SHA256}"
            f"\n實際：{actual_zip_sha}"
        )

    log("[PASS] v1.5.1 ZIP SHA256 相符。")

    with zipfile.ZipFile(V151_ZIP, "r") as zf:
        names = zf.namelist()

        report_member = find_member_by_basename(
            names,
            "v1.5.1_官方全市場行情增量銜接報告.json",
        )

        report_raw = zf.read(report_member)
        report = json.loads(report_raw.decode("utf-8-sig"))

        if report.get("mode") != "APPEND_ONLY_OVERLAY":
            raise RuntimeError("v1.5.1 不是 append-only overlay。")

        if report.get("append", {}).get("max_date") != "2026-11-10":
            raise RuntimeError("v1.5.1 官方行情最新日期不是 2026-11-10。")

        if report.get("append", {}).get("date_market_pairs") != 18:
            raise RuntimeError("v1.5.1 18 個交易日－市場組合檢核不符。")

        member = find_overlay_member(zf)

        if member is not None:
            raw_frame, member_bytes = read_zip_member_csv(zf, member)
        else:
            index_member = find_member_by_basename(
                names,
                "個股增量覆蓋檔索引.csv",
            )

            index_frame, _ = read_zip_member_csv(zf, index_member)

            if "overlay_file" not in index_frame.columns:
                raise RuntimeError("個股增量索引缺少 overlay_file 欄位。")

            frames = []

            for overlay_file in index_frame["overlay_file"].dropna().astype(str):
                candidates = [
                    "overlay/" + overlay_file.lstrip("/"),
                    overlay_file.lstrip("/"),
                ]

                selected = next(
                    (x for x in candidates if x in names),
                    None,
                )

                if selected is None:
                    raise RuntimeError(
                        "找不到 overlay 成員：" + overlay_file
                    )

                frame, _ = read_zip_member_csv(zf, selected)
                frames.append(frame)

            if not frames:
                raise RuntimeError("沒有讀到任何個股 overlay 檔案。")

            raw_frame = pd.concat(frames, ignore_index=True)
            member = "由個股 overlay 索引合併"
            member_bytes = b""

    log(
        f"[INFO] v1.5.1 原始標準化增量列數：{len(raw_frame):,}"
    )

    log(
        "[INFO] v1.5.1 標準化欄位："
        + repr(list(raw_frame.columns))
    )

    # 欄位名稱沿用既有 v1.2.6 / v1.2.7 欄位解析工具
    find_col = v126.find_col

    date_col = find_col(
        raw_frame.columns,
        [
            "trade_date", "date", "日期", "交易日期",
            "交易日", "資料日期",
        ],
    )

    market_col = find_col(
        raw_frame.columns,
        [
            "market", "market_type", "市場",
            "市場別", "市場類別", "交易市場",
        ],
    )

    code_col = find_col(
        raw_frame.columns,
        [
            "instrument_code", "完整證券代號",
            "股票代號", "證券代號", "股票代碼",
            "證券代碼", "symbol", "code",
        ],
    )

    name_col = find_col(
        raw_frame.columns,
        [
            "instrument_name", "official_name",
            "官方名稱", "股票名稱", "證券名稱",
            "名稱", "公司名稱", "name",
        ],
        required=False,
    )

    open_col = find_col(
        raw_frame.columns,
        [
            "open", "開盤價", "開盤",
            "raw_開盤價", "raw_開盤",
        ],
    )

    high_col = find_col(
        raw_frame.columns,
        [
            "high", "最高價", "最高",
            "raw_最高價", "raw_最高",
        ],
    )

    low_col = find_col(
        raw_frame.columns,
        [
            "low", "最低價", "最低",
            "raw_最低價", "raw_最低",
        ],
    )

    close_col = find_col(
        raw_frame.columns,
        [
            "close", "收盤價", "收盤",
            "raw_收盤價", "raw_收盤",
        ],
    )

    valid_col = find_col(
        raw_frame.columns,
        [
            "ohlc_valid_source", "ohlc_valid",
            "OHLC有效", "OHLC是否有效",
        ],
        required=False,
    )

    note_col = find_col(
        raw_frame.columns,
        [
            "official_note", "source_note",
            "官方備註", "備註", "附註",
        ],
        required=False,
    )

    new = pd.DataFrame()

    new["date"] = v126.parse_date_series(raw_frame[date_col])
    new["market"] = v126.normalize_market_series(raw_frame[market_col])
    new["code"] = v126.clean_code_series(raw_frame[code_col])

    if name_col is None:
        new["name"] = ""
    else:
        new["name"] = (
            raw_frame[name_col]
            .astype("string")
            .fillna("")
            .str.strip()
        )

    for source_col, target_col in [
        (open_col, "open"),
        (high_col, "high"),
        (low_col, "low"),
        (close_col, "close"),
    ]:
        new[target_col] = v126.numeric_series(raw_frame[source_col])

    new["segment"] = ""

    if valid_col is None:
        new["ohlc_valid_source"] = ""
    else:
        new["ohlc_valid_source"] = (
            raw_frame[valid_col]
            .astype("string")
            .fillna("")
            .astype(str)
        )

    if note_col is None:
        new["official_note"] = ""
    else:
        new["official_note"] = (
            raw_frame[note_col]
            .astype("string")
            .fillna("")
            .astype(str)
        )

    if new["date"].isna().any():
        raise RuntimeError("v1.5.1 增量存在無法解析的日期。")

    if new["market"].isna().any():
        bad = raw_frame.loc[new["market"].isna(), market_col].head(20).tolist()
        raise RuntimeError("v1.5.1 市場欄位解析失敗：" + repr(bad))

    if new["code"].isna().any() or new["code"].astype(str).str.strip().eq("").any():
        raise RuntimeError("v1.5.1 存在空白證券代號。")

    new = new.loc[new["date"].isin(NEW_DATES)].copy()

    if new.empty:
        raise RuntimeError("v1.5.1 未取得 11/9、11/10 增量。")

    new = new.loc[
        new["market"].isin(["TWSE", "TPEx"])
    ].copy()

    duplicate_n = int(
        new.duplicated(["market", "code", "date"]).sum()
    )

    if duplicate_n:
        raise RuntimeError(
            f"11/9、11/10 官方增量自然鍵重複 {duplicate_n:,} 列。"
        )

    actual_counts = {
        (str(pd.Timestamp(date).date()), str(market)): int(count)
        for (date, market), count in
        new.groupby(["date", "market"], observed=True).size().items()
    }

    if actual_counts != EXPECTED_DAILY_MARKET_ROWS:
        raise RuntimeError(
            "11/9、11/10 官方雙市場逐日列數與已保存來源不符。"
            f"\n預期：{EXPECTED_DAILY_MARKET_ROWS}"
            f"\n實際：{actual_counts}"
        )

    new.sort_values(
        ["date", "market", "code"],
        kind="mergesort",
        inplace=True,
    )

    log(
        f"[PASS] 11/9、11/10 官方增量：{len(new):,} 列；"
        "4 個日期－市場組合全部通過。"
    )

    source_info = {
        "zip_path": str(V151_ZIP),
        "zip_sha256": actual_zip_sha,
        "member": member,
        "member_sha256": sha256_bytes(member_bytes) if member_bytes else "",
        "member_rows_before_date_filter": int(len(raw_frame)),
        "new_rows_20261109_20261110": int(len(new)),
        "daily_market_rows": actual_counts,
    }

    return new, source_info, report


# =============================================================================
# 05. 讀取官方確認普通股清單
# =============================================================================

def load_official_ordinary():
    if not V141_ZIP.is_file():
        raise FileNotFoundError(f"找不到 v1.4.1 ZIP：{V141_ZIP}")

    actual_sha = sha256_file(V141_ZIP)

    if actual_sha != V141_SHA256:
        raise RuntimeError(
            "v1.4.1 ZIP SHA256 不符。"
            f"\n預期：{V141_SHA256}"
            f"\n實際：{actual_sha}"
        )

    with zipfile.ZipFile(V141_ZIP, "r") as zf:
        names = zf.namelist()

        member = find_member_by_basename(
            names,
            "已確認普通股行情.csv",
        )

        frame, raw_bytes = read_zip_member_csv(zf, member)

    required = [
        "market",
        "trade_date",
        "instrument_code",
        "instrument_name",
        "security_category",
        "classification_code",
        "isin_cfi_code",
        "ordinary_stock_confirmed",
    ]

    missing = [
        name
        for name in required
        if name not in frame.columns
    ]

    if missing:
        raise RuntimeError(
            "v1.4.1 官方普通股清單缺少欄位：" + repr(missing)
        )

    frame["trade_date"] = pd.to_datetime(
        frame["trade_date"],
        errors="coerce",
    ).dt.normalize()

    frame["market"] = frame["market"].astype("string").str.strip()
    frame["instrument_code"] = (
        frame["instrument_code"]
        .astype("string")
        .str.strip()
    )

    true_mask = (
        frame["ordinary_stock_confirmed"]
        .astype("string")
        .str.strip()
        .str.lower()
        .isin(["true", "1", "yes", "y"])
    )

    cfi_mask = (
        frame["isin_cfi_code"]
        .astype("string")
        .str.strip()
        .str.startswith("ES", na=False)
    )

    category_mask = (
        frame["security_category"]
        .astype("string")
        .str.strip()
        .eq("普通股")
    )

    classification_mask = (
        frame["classification_code"]
        .astype("string")
        .str.strip()
        .eq("OFFICIAL_ISIN_CFI_ES")
    )

    target = frame.loc[
        true_mask
        & cfi_mask
        & category_mask
        & classification_mask
        & frame["trade_date"].eq(LATEST_EXPECTED_DATE)
    ].copy()

    if len(target) != EXPECTED_OFFICIAL_ORDINARY_KEYS:
        raise RuntimeError(
            "官方普通股清單列數不符。"
            f"\n預期：{EXPECTED_OFFICIAL_ORDINARY_KEYS:,}"
            f"\n實際：{len(target):,}"
        )

    if target.duplicated(["market", "instrument_code"]).any():
        raise RuntimeError("官方普通股清單市場－代號鍵重複。")

    codes = set(target["instrument_code"].astype(str))

    if len(codes) == 0:
        raise RuntimeError("官方普通股代號集合為空。")

    name_map = (
        target.sort_values("trade_date")
        .groupby("instrument_code", sort=False)["instrument_name"]
        .last()
        .astype(str)
        .to_dict()
    )

    market_map = (
        target.sort_values("trade_date")
        .groupby("instrument_code", sort=False)["market"]
        .last()
        .astype(str)
        .to_dict()
    )

    info = {
        "zip_path": str(V141_ZIP),
        "zip_sha256": actual_sha,
        "member": member,
        "member_sha256": sha256_bytes(raw_bytes),
        "official_ordinary_market_code_keys": int(len(target)),
        "official_ordinary_unique_codes": int(len(codes)),
        "classification": "ISIN CFI 前兩碼 ES",
        "target_date": "2026-11-10",
    }

    log(
        f"[PASS] 2026/11/10 官方 ISIN 普通股："
        f"{len(target):,} 組市場－代號鍵；"
        f"{len(codes):,} 個代號。"
    )

    return target, codes, name_map, market_map, info


# =============================================================================
# 06. 將 11/9、11/10 追加到記憶體中的官方行情
# =============================================================================

def append_new_overlay(sources, new_rows):
    official = sources["official"].copy()

    old_max = pd.Timestamp(official["date"].max()).normalize()

    if old_max != pd.Timestamp("2026-11-06"):
        raise RuntimeError(
            "v1.2.7 原始資料最新日不是 2026-11-06。"
            f"實際：{old_max.date()}"
        )

    old_keys = set(
        zip(
            official["market"].astype(str),
            official["code"].astype(str),
            pd.to_datetime(official["date"]).dt.normalize(),
        )
    )

    new_keys = list(
        zip(
            new_rows["market"].astype(str),
            new_rows["code"].astype(str),
            pd.to_datetime(new_rows["date"]).dt.normalize(),
        )
    )

    overlap = sum(key in old_keys for key in new_keys)

    if overlap:
        raise RuntimeError(
            f"新增量與 v1.2.7 原始資料重疊 {overlap:,} 列；"
            "停止合併，不去重、不覆蓋。"
        )

    combined = pd.concat(
        [official, new_rows],
        ignore_index=True,
    )

    duplicate_n = int(
        combined.duplicated(["market", "code", "date"]).sum()
    )

    if duplicate_n:
        raise RuntimeError(
            f"合併後官方行情自然鍵重複 {duplicate_n:,} 列。"
        )

    combined.sort_values(
        ["date", "code", "market"],
        kind="mergesort",
        inplace=True,
    )

    sources["official"] = combined.reset_index(drop=True)

    if pd.Timestamp(combined["date"].max()).normalize() != LATEST_EXPECTED_DATE:
        raise RuntimeError("合併後官方行情最新日不是 2026-11-10。")

    log(
        f"[PASS] 官方行情在記憶體中 append-only 延伸："
        f"{old_max.date()} → {LATEST_EXPECTED_DATE.date()}。"
    )

    return sources


# =============================================================================
# 07. 官方普通股 Q5 名單重建
# =============================================================================

def build_official_ordinary_q5(
    rebuilt,
    ordinary_codes,
    ordinary_name_map,
    ordinary_market_map,
    v127,
):
    universe = rebuilt["universe"].copy()

    universe_codes = set(
        universe["股票代號"].astype(str)
    )

    missing_codes = ordinary_codes - universe_codes

    if missing_codes:
        raise RuntimeError(
            "官方確認普通股未完整出現在 v1.2.7 特徵 universe。"
            f"\n缺少數：{len(missing_codes):,}"
            f"\n前 30 個：{sorted(missing_codes)[:30]}"
        )

    official_universe = universe.loc[
        universe["股票代號"].astype(str).isin(ordinary_codes)
    ].copy()

    official_universe["股票名稱"] = (
        official_universe["股票代號"]
        .astype(str)
        .map(ordinary_name_map)
        .fillna(official_universe["股票名稱"])
    )

    official_universe["現行市場"] = (
        official_universe["股票代號"]
        .astype(str)
        .map(ordinary_market_map)
        .fillna(official_universe["現行市場"])
    )

    official_universe["證券範圍註記"] = (
        "2026-11-10 官方 ISIN CFI=ES 普通股"
    )

    ordinary_features = {}

    for date, rows in rebuilt["extra"]["feature_rows"].items():
        filtered = [
            row
            for row in rows
            if str(row.get("code", "")) in ordinary_codes
        ]

        ordinary_features[date] = filtered

    detail, daily_summary = v127.build_q5_detail(
        ordinary_features,
        official_universe,
    )

    if detail.empty:
        raise RuntimeError("官方普通股 Q5 特徵明細為空。")

    if not detail["股票代號"].astype(str).isin(ordinary_codes).all():
        raise RuntimeError("官方普通股 Q5 明細混入非官方普通股代號。")

    detail = detail.sort_values(
        ["訊號日期", "橫斷面百分位排名", "股票代號"],
        ascending=[True, False, True],
        kind="mergesort",
    ).reset_index(drop=True)

    q5 = detail.loc[detail["是否Q5訊號"]].copy()

    latest, latest_date = v127.build_latest_q5(detail)
    frequency = v127.build_q5_frequency(detail)

    return {
        "universe": official_universe,
        "feature_rows": ordinary_features,
        "detail": detail,
        "daily_summary": daily_summary,
        "q5": q5,
        "latest": latest,
        "latest_date": latest_date,
        "frequency": frequency,
    }


# =============================================================================
# 08. v1.2.2 完全相同的淨報酬公式
# =============================================================================

def net_return_v122(gross, slip, fee, tax):
    return (
        (1.0 + gross)
        * (1.0 - slip)
        * (1.0 - fee - tax)
        / ((1.0 + slip) * (1.0 + fee))
        - 1.0
    )


# =============================================================================
# 09. H=10 實際成交資料配對與淨報酬
# =============================================================================

def build_h10_returns(
    detail,
    official,
    events,
    v126,
    signal_label,
):
    signals = detail.loc[detail["是否Q5訊號"]].copy()

    if signals.empty:
        return pd.DataFrame()

    calendar = v127.build_global_calendar(official)

    calendar_index = {
        pd.Timestamp(date).normalize(): i
        for i, date in enumerate(calendar)
    }

    latest_source_date = calendar[-1]

    signal_codes = set(
        signals["股票代號"].astype(str)
    )

    recent = official.loc[
        official["date"].isin(calendar)
        & official["code"].astype(str).isin(signal_codes)
    ].copy()

    # 逐筆建立 code + date 價格狀態。
    # 同代號同日跨市場多筆有效 OHLC 不自動選擇。
    bar_map = {}

    for (code, date), group in recent.groupby(
        ["code", "date"],
        sort=False,
        observed=True,
    ):
        valid = group.loc[v126.valid_ohlc(group)].copy()

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

    # v1.2.7 前瞻價格區段事件。
    event_map = defaultdict(list)

    if events is not None and not events.empty:
        event_frame = events.copy()

        if {"日期", "股票代號"}.issubset(event_frame.columns):
            event_frame["_date"] = pd.to_datetime(
                event_frame["日期"],
                errors="coerce",
            ).dt.normalize()

            for row in event_frame.to_dict("records"):
                date = row.get("_date")

                if pd.isna(date):
                    continue

                code = str(row.get("股票代號", ""))

                event_map[code].append({
                    "date": pd.Timestamp(date),
                    "type": str(row.get("事件類型", "")),
                })

    output = []

    for signal in signals.to_dict("records"):
        code = str(signal["股票代號"])

        signal_date = pd.Timestamp(
            signal["訊號日期"]
        ).normalize()

        if signal_date not in calendar_index:
            raise RuntimeError(
                f"訊號日不在官方全市場交易日軸：{signal_date.date()}"
            )

        signal_idx = calendar_index[signal_date]

        entry_idx = signal_idx + 1
        exit_idx = signal_idx + HORIZON

        entry_date = (
            calendar[entry_idx]
            if entry_idx < len(calendar)
            else None
        )

        exit_date = (
            calendar[exit_idx]
            if exit_idx < len(calendar)
            else None
        )

        if exit_date is None:
            event_end = latest_source_date
        else:
            event_end = min(exit_date, latest_source_date)

        event_hits = [
            event
            for event in event_map.get(code, [])
            if signal_date < event["date"] <= event_end
        ]

        if entry_date is None:
            entry_status = "DATE_NOT_YET_AVAILABLE"
            entry_market = ""
            entry_open = np.nan

        else:
            entry_info = bar_map.get(
                (code, pd.Timestamp(entry_date).normalize()),
                {"status": "STOCK_BAR_NOT_FOUND", "row": None},
            )

            entry_status = entry_info["status"]

            if entry_status == "VALID_OHLC":
                entry_market = str(entry_info["row"]["market"])
                entry_open = float(entry_info["row"]["open"])
            else:
                entry_market = ""
                entry_open = np.nan

        if exit_date is None:
            exit_status = "DATE_NOT_YET_AVAILABLE"
            exit_market = ""
            exit_close = np.nan

        else:
            exit_info = bar_map.get(
                (code, pd.Timestamp(exit_date).normalize()),
                {"status": "STOCK_BAR_NOT_FOUND", "row": None},
            )

            exit_status = exit_info["status"]

            if exit_status == "VALID_OHLC":
                exit_market = str(exit_info["row"]["market"])
                exit_close = float(exit_info["row"]["close"])
            else:
                exit_market = ""
                exit_close = np.nan

        gross = np.nan
        net_base = np.nan
        net_stress = np.nan

        if event_hits:
            overall_status = "PRICE_SEGMENT_EVENT_DURING_H10_REVIEW"

        elif exit_date is None:
            overall_status = "WAITING_FOR_H10_EXIT_DATA"

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
            "訊號口徑": signal_label,
            "訊號日期": str(signal_date.date()),
            "股票代號": code,
            "股票名稱": str(signal["股票名稱"]),
            "訊號當日市場": str(signal["訊號當日市場"]),
            "range_pos20": float(signal["range_pos20"]),
            "橫斷面百分位排名": float(signal["橫斷面百分位排名"]),
            "H": HORIZON,
            "進場規則": "訊號日後第1個全市場交易日開盤",
            "出場規則": "訊號日後第10個全市場交易日收盤",
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
            "來源最後交易日": str(latest_source_date.date()),
            "觀察狀態": overall_status,
        })

    result = pd.DataFrame(output)

    result.sort_values(
        ["訊號日期", "股票代號"],
        kind="mergesort",
        inplace=True,
    )

    result.reset_index(drop=True, inplace=True)

    return result


# =============================================================================
# 10. 成熟 H=10 樣本彙總
# =============================================================================

def summarize_matured_h10(schedule):
    if schedule.empty:
        return pd.DataFrame()

    matured = schedule.loc[
        schedule["觀察狀態"].eq("MATURED_RETURN_COMPUTED")
    ].copy()

    if matured.empty:
        return pd.DataFrame(columns=[
            "訊號口徑",
            "訊號市場",
            "股票代號",
            "股票名稱",
            "成熟訊號數",
            "成熟訊號交易日數",
            "基準成本平均淨報酬",
            "基準成本中位數淨報酬",
            "基準成本勝率",
            "壓力成本平均淨報酬",
            "壓力成本中位數淨報酬",
            "壓力成本勝率",
            "壓力成本最大單筆虧損",
        ])

    result = matured.groupby(
        ["訊號口徑", "訊號當日市場", "股票代號", "股票名稱"],
        observed=True,
    ).agg(
        成熟訊號數=("訊號日期", "size"),
        成熟訊號交易日數=("訊號日期", "nunique"),
        基準成本平均淨報酬=("基準成本淨報酬", "mean"),
        基準成本中位數淨報酬=("基準成本淨報酬", "median"),
        基準成本勝率=("基準成本淨報酬", lambda x: float((x > 0).mean())),
        壓力成本平均淨報酬=("壓力成本淨報酬", "mean"),
        壓力成本中位數淨報酬=("壓力成本淨報酬", "median"),
        壓力成本勝率=("壓力成本淨報酬", lambda x: float((x > 0).mean())),
        壓力成本最大單筆虧損=("壓力成本淨報酬", "min"),
    ).reset_index()

    # 不依這一批樣本排序或挑選股票。
    return result.sort_values(
        ["訊號口徑", "股票代號"],
        kind="mergesort",
    ).reset_index(drop=True)


# =============================================================================
# 11. Q5 口徑差異比較
# =============================================================================

def compare_q5_membership(legacy_detail, official_detail, ordinary_codes):
    legacy_q5 = legacy_detail.loc[
        legacy_detail["是否Q5訊號"]
        & legacy_detail["股票代號"].astype(str).isin(ordinary_codes)
    ].copy()

    official_q5 = official_detail.loc[
        official_detail["是否Q5訊號"]
    ].copy()

    old = legacy_q5[
        ["訊號日期", "股票代號", "股票名稱", "訊號當日市場"]
    ].rename(columns={
        "股票名稱": "v127股票名稱",
        "訊號當日市場": "v127市場",
    })

    new = official_q5[
        ["訊號日期", "股票代號", "股票名稱", "訊號當日市場"]
    ].rename(columns={
        "股票名稱": "v128股票名稱",
        "訊號當日市場": "v128市場",
    })

    if old.duplicated(["訊號日期", "股票代號"]).any():
        raise RuntimeError("v1.2.7 Q5 名單日期－代號鍵重複。")

    if new.duplicated(["訊號日期", "股票代號"]).any():
        raise RuntimeError("v1.2.8 官方普通股 Q5 名單日期－代號鍵重複。")

    diff = old.merge(
        new,
        on=["訊號日期", "股票代號"],
        how="outer",
        indicator=True,
    )

    diff["Q5名單差異"] = diff["_merge"].map({
        "both": "兩版本均為Q5",
        "left_only": "僅v1.2.7原凍結Q5",
        "right_only": "僅v1.2.8官方普通股Q5",
    })

    diff.drop(columns=["_merge"], inplace=True)

    return diff.sort_values(
        ["訊號日期", "股票代號"],
        kind="mergesort",
    ).reset_index(drop=True)


# =============================================================================
# 12. 官方普通股每日摘要
# =============================================================================

def build_official_daily_summary(
    official_detail,
    ordinary_features,
    official,
    ordinary_codes,
    v126,
):
    summary = official_detail.copy()

    valid = official.loc[
        official["code"].astype(str).isin(ordinary_codes)
        & official["date"].isin(
            pd.to_datetime(summary["日期"], errors="coerce")
        )
    ].copy()

    valid = valid.loc[v126.valid_ohlc(valid)].copy()

    active_rows = []

    for (date, market), group in valid.groupby(
        ["date", "market"],
        observed=True,
    ):
        active_rows.append({
            "日期": str(pd.Timestamp(date).date()),
            "市場": str(market),
            "有效普通股OHLC列數": int(len(group)),
            "有效普通股代號數": int(group["code"].nunique()),
        })

    active = pd.DataFrame(active_rows)

    if not active.empty:
        pivot = active.pivot_table(
            index="日期",
            columns="市場",
            values="有效普通股OHLC列數",
            aggfunc="sum",
            fill_value=0,
        ).reset_index()

        pivot.columns.name = None

        rename = {
            "TWSE": "TWSE有效普通股OHLC數",
            "TPEx": "TPEx有效普通股OHLC數",
        }

        pivot.rename(columns=rename, inplace=True)

        summary = summary.merge(
            pivot,
            on="日期",
            how="left",
        )

    for col in [
        "TWSE有效普通股OHLC數",
        "TPEx有效普通股OHLC數",
    ]:
        if col not in summary.columns:
            summary[col] = 0

        summary[col] = summary[col].fillna(0).astype(int)

    tw_feature = []
    ot_feature = []

    for date in pd.to_datetime(summary["日期"]):
        rows = ordinary_features.get(pd.Timestamp(date).normalize(), [])

        tw_feature.append(
            sum(1 for row in rows if str(row.get("market")) == "TWSE")
        )

        ot_feature.append(
            sum(1 for row in rows if str(row.get("market")) == "TPEx")
        )

    summary["TWSE可計算特徵數"] = tw_feature
    summary["TPEx可計算特徵數"] = ot_feature
    summary["官方普通股代號範圍數"] = len(ordinary_codes)

    summary["雙市場均有有效OHLC"] = (
        summary["TWSE有效普通股OHLC數"].gt(0)
        & summary["TPEx有效普通股OHLC數"].gt(0)
    )

    summary["官方普通股特徵覆蓋率"] = (
        summary["可計算range_pos20數"]
        / summary["官方普通股代號範圍數"]
    )

    return summary


# =============================================================================
# 13. GitHub 一次上傳及 SHA read-back
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


def upload_github(out_dir, zip_path, report, run_id, token):
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
        "audit/sso-v1.2.8-q5-h10-official-ordinary-"
        f"{run_id}-{uuid.uuid4().hex[:4]}"
    )

    artifact_dir = (
        f"audit/sso-v1.2.8-q5-h10-official-ordinary/{run_id}"
    )

    readme_lines = [
        "# SSO v1.2.8｜官方普通股 Q5 與 H=10 成熟樣本驗證",
        "",
        f"- 執行時間：`{report['執行時間台北']}`",
        f"- 官方普通股市場－代號鍵：`{report['官方普通股市場代號鍵數']}`",
        f"- 官方普通股 Q5 最新日期：`{report['v128最新Q5日期']}`",
        f"- 官方普通股 Q5 最新日訊號數：`{report['v128最新日Q5訊號數']}`",
        f"- 官方普通股 H10 已成熟且完成淨報酬：`{report['v128_H10已成熟淨報酬筆數']}`",
        f"- v1.2.7 原九日 Q5 訊號重現：`{report['v127原九日Q5訊號數']}`",
        "- v1.2.6 原始 126 項逐日摘要核對：PASS",
        "- Q5 門檻維持 0.80，未重訓、未修改 v1.2.7 凍結規則。",
        "- 只新增 2026-11-09、2026-11-10 官方行情。",
        "- Google Drive 原始來源唯讀。",
        "- GitHub 只新增 audit branch，不更新 main。",
        "",
        "## 上傳檔案",
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
        prefix="sso_v128_gitpush_"
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

        log("[GITHUB 1/7] 建立暫存 bare repository。")

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
            (path, f"{artifact_dir}/{path.name}", None)
            for path in upload_files
        ]

        items.append((
            None,
            f"{artifact_dir}/README_GitHub上傳索引.md",
            readme_bytes,
        ))

        local_blobs = {}

        log("[GITHUB 3/7] 寫入本次分析、報告及 ZIP。")

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
                f"{name} | {fmt_bytes(size)}"
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
                f"Add SSO v1.2.8 official ordinary Q5 H10 audit ({run_id})",
            ],
            bare,
            env,
        )

        log("[GITHUB 5/7] 推送獨立 audit branch。")

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
            raise RuntimeError("GitHub audit branch SHA read-back 失敗。")

        log("[GITHUB 6/7] 下載 audit branch 並逐檔核對 Git blob SHA。")

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
                ["rev-parse", f"FETCH_HEAD:{remote_path}"],
                bare,
                env,
            )

            if actual_blob != expected_blob:
                raise RuntimeError(
                    f"GitHub 檔案 read-back SHA 不符：{remote_path}"
                )

        log(
            f"[PASS] 已逐檔核對 {len(local_blobs):,} 個 Git blob。"
        )

        log("[GITHUB 7/7] 確認 main SHA 前後一致。")

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
# 14. 主流程
# =============================================================================

def main():
    print("=" * 104, flush=True)
    log(VERSION)
    log("沿用 v1.2.7 Q5 訊號邏輯，追加官方 11/9、11/10 行情。")
    log("不重新抓行情、不重訓、不修改 v1.2.7 凍結結果。")
    log("基準與壓力成本沿用 v1.2.2 原始公式。")
    print("=" * 104, flush=True)

    if not ROOT.is_dir():
        try:
            from google.colab import drive
            drive.mount("/content/drive", force_remount=False)
        except Exception as exc:
            raise RuntimeError(f"Google Drive 掛載失敗：{exc}")

    if not ROOT.is_dir():
        raise FileNotFoundError(f"找不到 SSO 根目錄：{ROOT}")

    # 先確認 GitHub token，避免完成大量運算後才發現無法上傳。
    v127 = load_v127_module()
    github_token = v127.find_github_token()

    # 載入 v1.2.6 原始特徵、價格銜接及暖機程式。
    log("[01] 載入 v1.2.6 固定計算模組。")

    v126, dependency_path = v127.load_v126_module()
    v126.self_test()

    # v1.2.6 九日原始摘要是舊版 Q5 逐欄核對基準。
    original_summary = v127.load_v126_reference_summary()

    # 讀取並驗證既有 v4.69、v4.17、舊 append-only delta。
    log("[02] 載入 v1.2.7 已使用的官方行情來源。")

    sources = v127.load_and_validate_sources(v126)

    # 只從 v1.5.1 ZIP 讀取 11/9、11/10。
    log("[03] 讀取 v1.5.1 官方增量 ZIP。")

    new_rows, overlay_info, overlay_report = load_overlay(v126)

    # 對新資料做 append-only 記憶體合併；不寫回來源檔。
    sources = append_new_overlay(
        sources,
        new_rows,
    )

    # 載入 v1.4.1 官方 ISIN CFI=ES 普通股名單。
    log("[04] 讀取 2026/11/10 官方 ISIN 普通股分類。")

    (
        ordinary_frame,
        ordinary_codes,
        ordinary_name_map,
        ordinary_market_map,
        ordinary_info,
    ) = load_official_ordinary()

    # 按 v1.2.6 原函式重建歷史價格銜接、前瞻暖機及 range_pos20。
    log("[05] 沿用 v1.2.6 原函式重建 11/10 前瞻特徵。")

    rebuilt = v127.rebuild_v126_feature_rows(
        v126,
        sources,
    )

    # 先完整重建 v1.2.7 原始近似普通股 Q5。
    # 這是 v1.2.7 凍結規則的相容性核對，不先套用官方普通股篩選。
    log("[06] 重建 v1.2.7 原凍結 Q5 訊號。")

    legacy_detail, legacy_daily_detail_summary = v127.build_q5_detail(
        rebuilt["extra"]["feature_rows"],
        rebuilt["universe"],
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

    if qa_failures != 0 or len(qa) != EXPECTED_V126_QA_ITEMS:
        raise RuntimeError(
            "v1.2.7 原九日結果未完整重現，停止輸出。"
        )

    legacy_q5 = legacy_detail.loc[
        legacy_detail["是否Q5訊號"]
    ].copy()

    legacy_first_nine = legacy_q5.loc[
        legacy_q5["訊號日期"].le("2026-11-06")
    ]

    legacy_first_nine_n = int(len(legacy_first_nine))

    if legacy_first_nine_n != EXPECTED_V127_Q5_FIRST_NINE_DAYS:
        raise RuntimeError(
            "v1.2.7 原九日 Q5 訊號總數不符。"
            f"\n預期：{EXPECTED_V127_Q5_FIRST_NINE_DAYS:,}"
            f"\n實際：{legacy_first_nine_n:,}"
        )

    log(
        f"[PASS] v1.2.7 原九日 Q5 訊號重現："
        f"{legacy_first_nine_n:,} 筆。"
    )

    # 官方普通股名單必須完整落在 v1.2.7 特徵 universe。
    log("[07] 以官方普通股名單重算每日橫斷面 Q5。")

    official_q5_result = build_official_ordinary_q5(
        rebuilt,
        ordinary_codes,
        ordinary_name_map,
        ordinary_market_map,
        v127,
    )

    official_detail = official_q5_result["detail"]
    official_q5 = official_q5_result["q5"]
    official_latest = official_q5_result["latest"]
    official_latest_date = official_q5_result["latest_date"]
    official_frequency = official_q5_result["frequency"]

    if official_latest_date != "2026-11-10":
        raise RuntimeError(
            "官方普通股 Q5 最新訊號日不是 2026-11-10。"
        )

    # v1.2.7 原凍結 Q5 僅過濾官方普通股，保留原排名。
    legacy_ordinary_detail = legacy_detail.loc[
        legacy_detail["股票代號"].astype(str).isin(ordinary_codes)
    ].copy()

    legacy_ordinary_q5 = legacy_ordinary_detail.loc[
        legacy_ordinary_detail["是否Q5訊號"]
    ].copy()

    # 比較原凍結 Q5 及官方普通股重排名後 Q5 的名單差異。
    q5_diff = compare_q5_membership(
        legacy_detail,
        official_detail,
        ordinary_codes,
    )

    # 建立 v1.2.7 原凍結訊號的普通股 H10 實際報酬。
    log("[08] 計算 v1.2.7 原凍結 Q5 的 H=10 成熟報酬。")

    legacy_h10 = build_h10_returns(
        legacy_ordinary_detail,
        sources["official"],
        rebuilt["extra"]["events"],
        v126,
        "v1.2.7原凍結Q5_官方普通股過濾",
    )

    # 建立 v1.2.8 官方普通股重新排名 Q5 的 H10 實際報酬。
    log("[09] 計算 v1.2.8 官方普通股 Q5 的 H=10 成熟報酬。")

    official_h10 = build_h10_returns(
        official_detail,
        sources["official"],
        rebuilt["extra"]["events"],
        v126,
        "v1.2.8官方普通股重新排名Q5",
    )

    combined_h10 = pd.concat(
        [legacy_h10, official_h10],
        ignore_index=True,
    )

    matured = combined_h10.loc[
        combined_h10["觀察狀態"].eq("MATURED_RETURN_COMPUTED")
    ].copy()

    matured_summary = summarize_matured_h10(
        combined_h10
    )

    # 官方普通股逐日 Q5 摘要。
    official_daily = build_official_daily_summary(
        official_q5_result["daily_summary"],
        official_q5_result["feature_rows"],
        sources["official"],
        ordinary_codes,
        v126,
    )

    # 暖機及價格區段事件報告。
    warmup = v127.build_warmup_exceptions(
        rebuilt["stock_report"]
    )

    # 官方普通股當日最新 Q5 名單。
    latest_q5 = official_latest.copy()

    # 舊 Q5 與官方 Q5 每日訊號數對照。
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

    # 來源檔案版本清單。
    source_manifest = sources["source_manifest"].copy()

    extra_manifest = pd.DataFrame([
        {
            "來源": "v1.5.1官方全市場增量（僅新增11/9及11/10）",
            "路徑": str(V151_ZIP),
            "SHA256": overlay_info["zip_sha256"],
            "檔案大小bytes": V151_ZIP.stat().st_size,
            "讀取列數": len(new_rows),
            "起日": "2026-11-09",
            "迄日": "2026-11-10",
        },
        {
            "來源": "v1.4.1官方ISIN CFI=ES普通股清單",
            "路徑": str(V141_ZIP) + "::" + ordinary_info["member"],
            "SHA256": ordinary_info["member_sha256"],
            "檔案大小bytes": V141_ZIP.stat().st_size,
            "讀取列數": ordinary_info["official_ordinary_market_code_keys"],
            "起日": "2026-11-10",
            "迄日": "2026-11-10",
        },
    ])

    source_manifest = pd.concat(
        [source_manifest, extra_manifest],
        ignore_index=True,
    )

    # 統計成熟 H10 的狀態。
    legacy_matured = legacy_h10.loc[
        legacy_h10["觀察狀態"].eq("MATURED_RETURN_COMPUTED")
    ]

    official_matured = official_h10.loc[
        official_h10["觀察狀態"].eq("MATURED_RETURN_COMPUTED")
    ]

    matured_signal_dates = sorted(
        matured["訊號日期"].dropna().astype(str).unique().tolist()
    )

    report = {
        "版本": VERSION,
        "執行時間台北": datetime.now(TAIPEI).isoformat(),
        "v1.2.7凍結分支": V127_BRANCH,
        "v1.2.7程式GitBlobSHA1": V127_SCRIPT_BLOB_SHA1,

        "v1.2.7原九日Q5訊號數": legacy_first_nine_n,
        "v1.2.6原始摘要核對項目": int(len(qa)),
        "v1.2.6原始摘要核對失敗數": qa_failures,

        "v1.5.1行情增量": overlay_info,
        "v1.4.1普通股分類": ordinary_info,

        "官方普通股市場代號鍵數": int(len(ordinary_frame)),
        "官方普通股唯一代號數": int(len(ordinary_codes)),

        "v1.2.7原凍結Q5總訊號數_延伸至20261110": int(
            legacy_q5.shape[0]
        ),

        "v1.2.7原凍結Q5_官方普通股過濾後訊號數": int(
            legacy_ordinary_q5.shape[0]
        ),

        "v1.2.8官方普通股可計算特徵數": int(
            official_detail.shape[0]
        ),

        "v1.2.8官方普通股Q5總訊號數": int(
            official_q5.shape[0]
        ),

        "v1.2.8最新Q5日期": official_latest_date,
        "v1.2.8最新日Q5訊號數": int(len(official_latest)),

        "H": HORIZON,

        "買進規則": (
            "訊號日後第1個全市場交易日開盤"
        ),

        "賣出規則": (
            "訊號日後第10個全市場交易日收盤"
        ),

        "淨報酬公式": (
            "((1+gross)*(1-slip)*(1-fee-tax)"
            "/((1+slip)*(1+fee)))-1"
        ),

        "基準成本": {
            "slippage": BASE_SLIP,
            "fee": BASE_FEE,
            "tax": BASE_TAX,
        },

        "壓力成本": {
            "slippage": STRESS_SLIP,
            "fee": STRESS_FEE,
            "tax": STRESS_TAX,
        },

        "v127_H10已成熟淨報酬筆數": int(
            len(legacy_matured)
        ),

        "v128_H10已成熟淨報酬筆數": int(
            len(official_matured)
        ),

        "合併H10已成熟淨報酬筆數": int(
            len(matured)
        ),

        "成熟訊號日期": matured_signal_dates,

        "基準成本正報酬筆數": int(
            matured["基準成本淨報酬"].gt(0).sum()
        ) if len(matured) else 0,

        "壓力成本正報酬筆數": int(
            matured["壓力成本淨報酬"].gt(0).sum()
        ) if len(matured) else 0,

        "H10狀態分布": {
            str(k): int(v)
            for k, v in combined_h10["觀察狀態"]
            .value_counts(dropna=False)
            .items()
        },

        "明確未執行": [
            "未重新訓練模型",
            "未修改 v1.2.7 Q5 門檻",
            "未改寫 v1.2.7 原凍結結果",
            "未重新抓取官方行情",
            "未計算尚未成熟的 H10 報酬",
            "未將未成熟訊號當成獲利樣本",
            "未依單日報酬挑選股票",
            "未硬湊 10 檔股票",
        ],

        "研究限制": [
            "v1.2.7 原凍結訊號與 v1.2.8 官方普通股重新排名訊號分開保存。",
            "官方普通股名單採 2026-11-10 ISIN CFI=ES 分類。",
            "官方普通股分類是目前證券範圍，不代表已完成全部歷史日期的證券類別回溯認證。",
            "H10 樣本可能重疊，不等同資金配置後投資組合績效。",
            "價格區段事件發生於訊號日後至出場日者，不自動計算跨區段報酬。",
            "本版不依成熟樣本數不足的個股建立推薦或排序。",
        ],

        "安全限制": [
            "Google Drive 原始來源唯讀",
            "不呼叫 TWSE／TPEx 官方 API",
            "不修改正式行情資料庫",
            "不修改、不刪除、不移動既有檔案",
            "GitHub 僅建立獨立 audit branch",
            "GitHub main SHA 前後必須一致",
        ],
    }

    # 檢查首批成熟樣本的日期範圍。
    if len(matured):
        if not all(
            pd.Timestamp(date) == pd.Timestamp("2026-10-27")
            for date in matured["訊號日期"].unique()
        ):
            log(
                "[INFO] 已成熟樣本不只包含 10/27，"
                "將依各筆訊號日逐筆列出。"
            )

    # =============================================================================
    # 15. 建立全新輸出資料夾
    # =============================================================================

    run_id = run_stamp()

    out_dir = (
        ROOT
        / f"看股最速適_v1_2_8_官方普通股Q5與H10成熟樣本驗證_{run_id}"
    )

    out_dir.mkdir(
        parents=True,
        exist_ok=False,
    )

    log(f"[OUTPUT] {out_dir}")

    # 如果能取得目前完整程式，納入 ZIP。
    archive_current_program(out_dir)

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
        out_dir / "02_v127原凍結Q5訊號延伸至1110.csv",
    )

    csv_write(
        legacy_ordinary_q5,
        out_dir / "03_v127原凍結Q5官方普通股過濾名單.csv",
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
        out_dir / "08_v127與v128官方普通股Q5名單差異.csv",
    )

    csv_write(
        daily_compare,
        out_dir / "09_v127與v128每日Q5數量對照.csv",
    )

    csv_write(
        legacy_h10,
        out_dir / "10_v127原凍結Q5官方普通股H10觀察與淨報酬.csv",
    )

    csv_write(
        official_h10,
        out_dir / "11_v128官方普通股Q5_H10觀察與淨報酬.csv",
    )

    csv_write(
        matured,
        out_dir / "12_H10已成熟逐筆實際淨報酬.csv",
    )

    csv_write(
        matured_summary,
        out_dir / "13_H10已成熟個股彙總.csv",
    )

    csv_write(
        warmup,
        out_dir / "14_價格銜接與暖機例外.csv",
    )

    json_write(
        report,
        out_dir / "15_v128官方普通股Q5與H10驗證結論.json",
    )

    # 執行摘要
    summary_lines = [
        "=" * 100,
        VERSION,
        "=" * 100,
        f"執行時間：{report['執行時間台北']}",
        "",
        "一、版本與來源核對",
        f"v1.2.7 原九日 Q5 訊號：{legacy_first_nine_n:,}",
        f"v1.2.6 原始核對項目：{len(qa):,}",
        f"v1.2.6 核對失敗：{qa_failures:,}",
        f"官方增量新增列數：{len(new_rows):,}",
        "官方行情最新日期：2026-11-10",
        "",
        "二、證券範圍",
        f"官方 ISIN CFI=ES 普通股市場－代號鍵：{len(ordinary_frame):,}",
        f"官方普通股唯一代號：{len(ordinary_codes):,}",
        "",
        "三、Q5 訊號",
        f"v1.2.7 原凍結 Q5 延伸訊號：{len(legacy_q5):,}",
        f"v1.2.7 原凍結 Q5 過濾官方普通股：{len(legacy_ordinary_q5):,}",
        f"v1.2.8 官方普通股 Q5 訊號：{len(official_q5):,}",
        f"v1.2.8 最新 Q5 日期：{official_latest_date}",
        f"v1.2.8 最新日 Q5 數量：{len(official_latest):,}",
        "",
        "四、H=10 成熟樣本",
        f"v1.2.7 原凍結 Q5 已成熟報酬：{len(legacy_matured):,}",
        f"v1.2.8 官方普通股 Q5 已成熟報酬：{len(official_matured):,}",
        f"合計已成熟且完成淨報酬：{len(matured):,}",
        f"基準成本正報酬筆數：{report['基準成本正報酬筆數']:,}",
        f"壓力成本正報酬筆數：{report['壓力成本正報酬筆數']:,}",
        "",
        "五、解讀限制",
        "尚未完成 H=10 的訊號只列入觀察排程，不計算或推估報酬。",
        "已成熟樣本數不足以建立新的個股推薦或排名。",
        "Q5 仍是每日 range_pos20 橫斷面排名前 20%，不是獲利保證。",
        "本版未重訓、未修改 Q5 門檻，也未改寫 v1.2.7 原始結果。",
        "",
        "六、安全",
        "Drive 原始資料唯讀。",
        "只建立新的唯一輸出資料夾及 ZIP。",
        "GitHub 只新增獨立 audit branch，不更新 main。",
        "=" * 100,
    ]

    (out_dir / "16_執行摘要.txt").write_text(
        "\n".join(summary_lines) + "\n",
        encoding="utf-8-sig",
    )

    # README
    readme_lines = [
        "# SSO v1.2.8｜官方普通股 Q5 與 H=10 成熟樣本驗證",
        "",
        f"- 執行時間：`{report['執行時間台北']}`",
        f"- 官方普通股鍵數：`{len(ordinary_frame)}`",
        f"- v1.2.7 原九日 Q5：`{legacy_first_nine_n}`",
        f"- v1.2.6 九日原始摘要核對：`{len(qa)} 項，0 失敗`",
        f"- v1.2.8 最新 Q5 日期：`{official_latest_date}`",
        f"- v1.2.8 最新日 Q5 訊號：`{len(official_latest)}`",
        f"- H10 已成熟且完成淨報酬：`{len(matured)}`",
        "- 保留 v1.2.7 原凍結 Q5 與官方普通股重新排名兩種口徑。",
        "- 不重新訓練、不修改 Q5 門檻、不呼叫官方 API。",
        "- 未成熟訊號不計算報酬。",
        "- Drive 原始來源唯讀；GitHub 僅新增獨立 audit branch。",
        "",
        "## 檔案",
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

    # ZIP 位於輸出資料夾外，避免 ZIP 包含自身。
    zip_path = ROOT / f"{out_dir.name}.zip"

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

    report["output"] = {
        "output_dir": str(out_dir),
        "zip_path": str(zip_path),
        "zip_sha256": sha256_file(zip_path),
        "zip_bytes": zip_path.stat().st_size,
    }

    # 將 output 檔案雜湊資訊重新寫回結論 JSON。
    json_write(
        report,
        out_dir / "15_v128官方普通股Q5與H10驗證結論.json",
    )

    # README 重新更新一次，加入結論 JSON 最終 SHA。
    readme_lines = [
        "# SSO v1.2.8｜官方普通股 Q5 與 H=10 成熟樣本驗證",
        "",
        f"- 執行時間：`{report['執行時間台北']}`",
        f"- 官方普通股鍵數：`{len(ordinary_frame)}`",
        f"- v1.2.7 原九日 Q5：`{legacy_first_nine_n}`",
        f"- v1.2.6 九日原始摘要核對：`{len(qa)} 項，0 失敗`",
        f"- v1.2.8 最新 Q5 日期：`{official_latest_date}`",
        f"- v1.2.8 最新日 Q5 訊號：`{len(official_latest)}`",
        f"- H10 已成熟且完成淨報酬：`{len(matured)}`",
        "- v1.2.7 舊訊號與 v1.2.8 官方普通股重新排名分開保存。",
        "- Drive 原始來源唯讀；GitHub 僅新增 audit branch。",
        "",
        "## 檔案",
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

    # 更新 ZIP，以包含最終 JSON 及 README。
    with zipfile.ZipFile(
        zip_path,
        "w",
        compression=zipfile.ZIP_DEFLATED,
        compresslevel=6,
    ) as archive:
        for path in sorted(out_dir.iterdir()):
            if path.is_file():
                archive.write(path, arcname=path.name)

    with zipfile.ZipFile(zip_path, "r") as archive:
        bad = archive.testzip()

        if bad:
            raise RuntimeError(f"最終 ZIP CRC 驗證失敗：{bad}")

    # GitHub 獨立 audit branch
    log("[GITHUB] 上傳本次成果及 ZIP。")

    github = upload_github(
        out_dir,
        zip_path,
        report,
        run_id,
        github_token,
    )

    # 最終輸出
    print("\n" + "=" * 104, flush=True)
    print("[PASS] SSO v1.2.8 執行完成", flush=True)
    print("=" * 104, flush=True)

    print(f"輸出資料夾：{out_dir}", flush=True)
    print(f"ZIP：{zip_path}", flush=True)
    print(f"ZIP SHA256：{sha256_file(zip_path)}", flush=True)

    print("", flush=True)
    print("v1.2.7 九日相容性：", flush=True)
    print(f"原 Q5 訊號：{legacy_first_nine_n:,}", flush=True)
    print(
        f"v1.2.6 摘要核對：{len(qa):,} 項；"
        f"失敗 {qa_failures:,}",
        flush=True,
    )

    print("", flush=True)
    print("官方普通股 Q5：", flush=True)
    print(
        f"官方普通股市場－代號鍵：{len(ordinary_frame):,}",
        flush=True,
    )
    print(
        f"官方 Q5 最新日：{official_latest_date}；"
        f"訊號數：{len(official_latest):,}",
        flush=True,
    )

    print("", flush=True)
    print("H=10 成熟樣本：", flush=True)
    print(f"v1.2.7 凍結訊號成熟：{len(legacy_matured):,}", flush=True)
    print(f"v1.2.8 官方普通股訊號成熟：{len(official_matured):,}", flush=True)
    print(f"合計完成淨報酬：{len(matured):,}", flush=True)

    print("", flush=True)
    print("GitHub：", flush=True)
    print(f"Audit branch：{github['branch']}", flush=True)
    print(f"Commit：{github['commit']}", flush=True)
    print(f"資料夾：{github['folder_url']}", flush=True)
    print(f"ZIP：{github['zip_url']}", flush=True)
    print(f"Commit URL：{github['commit_url']}", flush=True)
    print(f"main before：{github['main_before']}", flush=True)
    print(f"main after：{github['main_after']}", flush=True)

    print("", flush=True)
    print("[PASS] GitHub 每個檔案均完成 SHA read-back。", flush=True)
    print("[PASS] GitHub main 前後 SHA 相同。", flush=True)
    print("[PASS] Google Drive 原始行情及正式資料未修改。", flush=True)
    print("[PASS] 未成熟 H10 訊號未計算報酬。", flush=True)
    print("=" * 104, flush=True)

    return {
        "output_dir": str(out_dir),
        "zip_path": str(zip_path),
        "github": github,
        "report": report,
    }


if __name__ == "__main__":
    RESULT = main()