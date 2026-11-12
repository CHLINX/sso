# -*- coding: utf-8 -*-
"""
SSO v1.3.1｜凍結12檔前瞻樣本外追蹤主程式

研究基準：
- 12 檔候選名單沿用 v1.2.9 / v1.3.0，完全凍結。
- 前瞻起始日：2026-11-12。
- 不重新訓練模型、不變更 Q5 定義、不更換股票名單。
- 不把 2026-11-10 的 CFI 分類回套為 2026-11-12 的分類。

行情來源：
- 歷史基礎行情：v1.2.7 固定來源。
- 2026-11-09、2026-11-10：v1.5.1 官方增量。
- 2026-11-11：v1.5.9 TWSE / TPEx 雙市場修正層。
- 2026-11-12 起：v1.5.10 及後續 v1.5.x 已驗證增量。

報酬計算：
- 沿用 v1.2.9 build_multi_horizon_returns。
- 訊號日後第 1 個全市場交易日開盤進場。
- 訊號日後第 H 個全市場交易日收盤出場。
- H = 1 / 3 / 5 / 10。
- 沿用 v1.2.2 基準成本與壓力成本公式。

兩種觀察：
1. FIRST_OOS：
   2026-11-12 第一批凍結候選訊號。
2. DAILY_REENTRY：
   2026-11-12 起，每個有效交易日對凍結候選重新建立追蹤訊號。
   此為候選組合每日重新進場觀察，不等同每日重新排名 Q5。

重要：
- 每次重新執行會重建完整前瞻排程。
- 尚未成熟的 H3/H5/H10 保留待出場狀態。
- 無有效行情的日期不推定休市。
- 未取得當日 CFI 分類時，不宣稱當日全市場 Q1-Q5 排名已完成。

保護：
- Drive 原始來源唯讀。
- 不刪除、搬移、覆寫舊檔。
- 輸出使用新的唯一資料夾。
- GitHub 只建立獨立 audit branch。
- GitHub main 前後 SHA 必須相同。
"""

from __future__ import annotations

import os
import re
import io
import csv
import sys
import json
import uuid
import time
import base64
import hashlib
import zipfile
import shutil
import tempfile
import importlib.util
import urllib.request
from pathlib import Path
from datetime import datetime, date
from urllib.parse import quote
from zoneinfo import ZoneInfo
from collections import defaultdict

import numpy as np
import pandas as pd
import requests


# ============================================================
# 01. 固定版本、來源及候選名單
# ============================================================

VERSION = "SSO_v1.3.1_凍結12檔前瞻樣本外追蹤"

ROOT = Path(
    "/content/drive/MyDrive/看股最速適"
)

MARKET_DATA_ROOT = (
    ROOT
    / "SSO_Market_Data_Continuation_TWSE_TPEX_v1"
)

TAIPEI = ZoneInfo("Asia/Taipei")

REPO_OWNER = "CHLINX"
REPO_NAME = "sso"
REPO = "CHLINX/sso"

OOS_START_DATE = date(2026, 11, 12)

CORRECTION_DATE = date(2026, 11, 11)

HORIZONS = (1, 3, 5, 10)

EXPECTED_CANDIDATES = {
    "1416": "廣豐",
    "2033": "佳大",
    "2465": "麗臺",
    "2468": "華經",
    "2923": "鼎固-KY",
    "4526": "東台",
    "4741": "泓瀚",
    "5013": "強新",
    "6233": "旺玖",
    "6708": "天擎",
    "6870": "騰雲",
    "8099": "大世科",
}

EXPECTED_CANDIDATE_CODES = set(
    EXPECTED_CANDIDATES
)

# v1.2.9 固定程式
V129_BRANCH = (
    "audit/sso-v1.2.9-multi-horizon-"
    "20261111_162816-3dfc"
)

V129_SCRIPT_PATH = (
    "audit/sso-v1.2.9-multi-horizon/"
    "20261111_162816/"
    "SSO_v1.2.9_官方普通股Q5多週期成熟淨報酬驗證.py"
)

V129_SCRIPT_GIT_BLOB_SHA1 = (
    "bab6929d4eb6c070b341ba15c596a428831fea81"
)

# v1.3.0 已確認的 2026-11-10 官方普通股分類來源
ORDINARY_CLASSIFICATION_DATE = date(2026, 11, 10)

EXPECTED_ORDINARY_KEYS = 1951

# v1.5.9 雙市場封存來源
V159_DIR = (
    MARKET_DATA_ROOT
    / "v1.5.9_TWSE_price_change欄位補正_"
      "20261112_152447_f573"
)

V159_ZIP = (
    V159_DIR
    / "SSO_Market_Data_Continuation_v1.5.9_封存完成_"
      "20261112_160143_23f9.zip"
)

EXPECTED_V159_ZIP_SHA256 = (
    "dae83e95a57dd76820c020050e56f33d54b24cbd8bcb7502a754f0625ce8be38"
)

EXPECTED_V159_MANIFEST_SHA256 = (
    "53909b629ecfb730696b466e970d61f2e9784c9d94126d7da471b251bca8f45b"
)

EXPECTED_TWSE_CORRECTION_FILE_SHA256 = (
    "b390cd3b3e3fd339395818cee683eac91414540364baff9a851143094fec428b"
)

EXPECTED_TWSE_CANONICAL_SHA256 = (
    "b366ce6fc4910caba953e3bff3d3cb2ed479b12ee33cf462e927f4931b22533f"
)

EXPECTED_TPEX_CORRECTION_FILE_SHA256 = (
    "002f3e4e7af282945333db92c1cebc2a76bad5fb24a7058fda12e2690ed80742"
)

EXPECTED_TPEX_CANONICAL_SHA256 = (
    "eb4bd7a5e16831f74f6cc700c8076163d0ba4c5b782023f3858951035a31e627"
)

EXPECTED_TWSE_CORRECTION_ROWS = 1390
EXPECTED_TPEX_CORRECTION_ROWS = 1020

MIN_DAILY_MARKET_ROWS = 500
MAX_DAILY_MARKET_ROWS = 3000

MAX_ZERO_VOLUME_RATIO = 0.25
MAX_MISSING_OHLC_RATIO = 0.25
MAX_INVALID_OHLC_RATIO = 0.01


# ============================================================
# 02. 欄位別名
# ============================================================

ALIASES = {
    "date": [
        "date",
        "trade_date",
        "交易日期",
        "交易日",
        "日期",
        "資料日期",
    ],

    "market": [
        "market",
        "市場",
        "市場別",
        "交易市場",
    ],

    "code": [
        "code",
        "instrument_code",
        "stock_id",
        "stock_code",
        "證券代號",
        "證券代碼",
        "股票代號",
        "股票代碼",
    ],

    "name": [
        "name",
        "instrument_name",
        "stock_name",
        "證券名稱",
        "股票名稱",
        "名稱",
    ],

    "open": [
        "open",
        "opening_price",
        "open_price",
        "開盤",
        "開盤價",
    ],

    "high": [
        "high",
        "highest_price",
        "high_price",
        "最高",
        "最高價",
    ],

    "low": [
        "low",
        "lowest_price",
        "low_price",
        "最低",
        "最低價",
    ],

    "close": [
        "close",
        "closing_price",
        "close_price",
        "收盤",
        "收盤價",
    ],

    "volume": [
        "volume",
        "trade_volume",
        "trading_volume",
        "成交股數",
        "成交量",
    ],

    "price_change": [
        "price_change",
        "change",
        "漲跌",
        "漲跌價差",
    ],
}


# ============================================================
# 03. 共用函式
# ============================================================

def log(message, logs=None):

    line = (
        f"[{datetime.now(TAIPEI).strftime('%H:%M:%S')}] "
        f"{message}"
    )

    print(line, flush=True)

    if logs is not None:
        logs.append(line)


def ensure_drive():

    if not Path("/content/drive/MyDrive").exists():

        from google.colab import drive

        drive.mount(
            "/content/drive",
            force_remount=False
        )

    if not ROOT.is_dir():
        raise FileNotFoundError(
            f"找不到 SSO 根目錄：{ROOT}"
        )

    if not MARKET_DATA_ROOT.is_dir():
        raise FileNotFoundError(
            f"找不到行情資料根目錄：{MARKET_DATA_ROOT}"
        )


def sha256_bytes(data):

    return hashlib.sha256(data).hexdigest()


def sha256_file(path):

    digest = hashlib.sha256()

    with Path(path).open("rb") as f:

        for block in iter(
            lambda: f.read(8 * 1024 * 1024),
            b""
        ):
            digest.update(block)

    return digest.hexdigest()


def git_blob_sha1(data):

    header = (
        f"blob {len(data)}\0"
        .encode("utf-8")
    )

    return hashlib.sha1(
        header + data
    ).hexdigest()


def json_read(path):

    return json.loads(
        Path(path).read_text(
            encoding="utf-8-sig"
        )
    )


def json_write(data, path):

    Path(path).write_text(
        json.dumps(
            data,
            ensure_ascii=False,
            indent=2,
            default=str,
        ),
        encoding="utf-8-sig",
    )


def normalized_name(value):

    return re.sub(
        r"[\s\u3000_\-（）()\[\]／/：:、,.%]+",
        "",
        str(value).strip().lower()
    )


def find_column(columns, aliases):

    lookup = {
        normalized_name(column): column
        for column in columns
    }

    for alias in aliases:

        key = normalized_name(alias)

        if key in lookup:
            return lookup[key]

    return None


def read_csv(path):

    path = Path(path)

    last_error = None

    for encoding in (
        "utf-8-sig",
        "cp950",
        "big5",
        "gb18030",
    ):

        try:

            return pd.read_csv(
                path,
                dtype="string",
                encoding=encoding,
                low_memory=False,
            )

        except Exception as error:

            last_error = error

    raise RuntimeError(
        f"CSV 讀取失敗：{path}；{last_error}"
    )


def parse_num(series):

    return pd.to_numeric(
        series.astype("string")
        .str.replace(",", "", regex=False)
        .str.replace("＋", "+", regex=False)
        .str.replace("－", "-", regex=False)
        .str.strip(),
        errors="coerce"
    )


def to_date_series(series):

    return pd.to_datetime(
        series,
        errors="coerce"
    ).dt.normalize()


def safe_float(value):

    try:

        value = float(value)

        if not np.isfinite(value):
            return np.nan

        return value

    except Exception:

        return np.nan


def market_text(value):

    s = str(value or "").strip().upper()

    if s in {"TWSE", "上市", "集中市場"}:
        return "TWSE"

    if s in {"TPEX", "TPEx", "上櫃", "櫃買"}:
        return "TPEx"

    return s


def read_rows_as_text(path):

    return pd.read_csv(
        path,
        dtype="string",
        encoding="utf-8-sig",
        low_memory=False
    )


def require_file(path):

    if not Path(path).is_file():

        raise FileNotFoundError(
            f"必要來源檔不存在：{path}"
        )


# ============================================================
# 04. 載入固定 v1.2.9 程式及相依模組
# ============================================================

def load_pinned_v129():

    url = (
        "https://raw.githubusercontent.com/"
        f"{REPO_OWNER}/{REPO_NAME}/"
        f"{V129_BRANCH}/"
        f"{quote(V129_SCRIPT_PATH, safe='/')}"
    )

    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": "SSO-v1.3.1-frozen12-oos"
        }
    )

    with urllib.request.urlopen(
        request,
        timeout=120
    ) as response:

        source_bytes = response.read()

    actual_blob = git_blob_sha1(
        source_bytes
    )

    if actual_blob != V129_SCRIPT_GIT_BLOB_SHA1:

        raise RuntimeError(
            "v1.2.9 固定程式 Git blob SHA1 不符。"
            f"\n預期：{V129_SCRIPT_GIT_BLOB_SHA1}"
            f"\n實際：{actual_blob}"
        )

    temp_script = (
        Path(tempfile.gettempdir())
        / (
            "SSO_v129_pinned_"
            + uuid.uuid4().hex[:8]
            + ".py"
        )
    )

    temp_script.write_bytes(
        source_bytes
    )

    spec = importlib.util.spec_from_file_location(
        "sso_v129_pinned_dependency_v131",
        str(temp_script)
    )

    if spec is None or spec.loader is None:

        raise RuntimeError(
            "無法載入 v1.2.9 固定程式。"
        )

    module = importlib.util.module_from_spec(
        spec
    )

    sys.modules[spec.name] = module

    spec.loader.exec_module(module)

    required = [
        "load_v128_module",
        "build_multi_horizon_returns",
        "summarize_by_horizon",
        "summarize_by_signal_date",
        "summarize_stock_horizon",
        "build_cross_horizon_stock_table",
    ]

    missing = [
        name
        for name in required
        if not hasattr(module, name)
    ]

    if missing:

        raise RuntimeError(
            f"v1.2.9 固定程式缺少函式：{missing}"
        )

    log(
        "[PASS] v1.2.9 固定程式 Git blob SHA1。"
    )

    return module


def load_fixed_dependencies():

    v129 = load_pinned_v129()

    v128 = v129.load_v128_module()

    v127 = v128.load_v127_module()

    # v1.2.8 部分函式會引用全域 v127。
    v128.v127 = v127

    v126, dependency_path = (
        v127.load_v126_module()
    )

    v126.self_test()

    log(
        "[PASS] v1.2.8 / v1.2.7 / v1.2.6 固定模組載入。"
    )

    return v129, v128, v127, v126


# ============================================================
# 05. 驗證 v1.5.9 雙市場修正層
# ============================================================

def load_v159_corrections():

    require_file(V159_ZIP)

    actual_zip_sha = sha256_file(
        V159_ZIP
    )

    if actual_zip_sha != EXPECTED_V159_ZIP_SHA256:

        raise RuntimeError(
            "v1.5.9 正式封存 ZIP SHA256 不符。"
        )

    manifest_path = (
        V159_DIR
        / "artifact_sha256_manifest.json"
    )

    require_file(manifest_path)

    actual_manifest_sha = sha256_file(
        manifest_path
    )

    if actual_manifest_sha != EXPECTED_V159_MANIFEST_SHA256:

        raise RuntimeError(
            "v1.5.9 artifact manifest SHA256 不符。"
        )

    report_path = (
        V159_DIR
        / "v1.5.9_雙市場行情補正總報告.json"
    )

    report = json_read(report_path)

    if report.get("status") != "PASS_TWSE_PRICE_CHANGE_REPAIRED":

        raise RuntimeError(
            "v1.5.9 雙市場補正總報告不是 PASS。"
        )

    twse_root = (
        V159_DIR / "修正_TWSE_20261111.csv"
    )

    twse_nested = (
        V159_DIR
        / "corrections"
        / "官方歷史行情"
        / "TWSE"
        / "20261111.csv"
    )

    tpex_root = (
        V159_DIR / "修正_TPEX_20261111.csv"
    )

    tpex_nested = (
        V159_DIR
        / "corrections"
        / "官方歷史行情"
        / "TPEX"
        / "20261111.csv"
    )

    for path in (
        twse_root,
        twse_nested,
        tpex_root,
        tpex_nested
    ):
        require_file(path)

    if sha256_file(twse_root) != EXPECTED_TWSE_CORRECTION_FILE_SHA256:
        raise RuntimeError("v1.5.9 TWSE root SHA 不符。")

    if sha256_file(twse_nested) != EXPECTED_TWSE_CORRECTION_FILE_SHA256:
        raise RuntimeError("v1.5.9 TWSE nested SHA 不符。")

    if sha256_file(tpex_root) != EXPECTED_TPEX_CORRECTION_FILE_SHA256:
        raise RuntimeError("v1.5.9 TPEx root SHA 不符。")

    if sha256_file(tpex_nested) != EXPECTED_TPEX_CORRECTION_FILE_SHA256:
        raise RuntimeError("v1.5.9 TPEx nested SHA 不符。")

    if (
        report["twse"]["official_canonical_sha256"]
        != EXPECTED_TWSE_CANONICAL_SHA256
    ):
        raise RuntimeError(
            "v1.5.9 TWSE canonical SHA 與封存基準不同。"
        )

    if (
        report["tpex"]["canonical_sha256"]
        != EXPECTED_TPEX_CANONICAL_SHA256
    ):
        raise RuntimeError(
            "v1.5.9 TPEx canonical SHA 與封存基準不同。"
        )

    twse = read_rows_as_text(twse_root)
    tpex = read_rows_as_text(tpex_root)

    if len(twse) != EXPECTED_TWSE_CORRECTION_ROWS:
        raise RuntimeError("v1.5.9 TWSE 修正筆數不符。")

    if len(tpex) != EXPECTED_TPEX_CORRECTION_ROWS:
        raise RuntimeError("v1.5.9 TPEx 修正筆數不符。")

    log(
        "[PASS] v1.5.9 TWSE 1,390 筆修正層。"
    )

    log(
        "[PASS] v1.5.9 TPEx 1,020 筆 type=EW 修正層。"
    )

    return {
        "twse": twse,
        "tpex": tpex,
        "zip_sha256": actual_zip_sha,
        "manifest_sha256": actual_manifest_sha,
        "report": report,
        "paths": [
            twse_root,
            twse_nested,
            tpex_root,
            tpex_nested,
        ]
    }


# ============================================================
# 06. 搜尋並驗證 v1.5.10 前瞻增量
# ============================================================

def load_v1510_outputs(logs):

    prefix = (
        "v1.5.10_官方行情即時驗證與增量續接_"
    )

    folders = sorted(
        [
            p
            for p in MARKET_DATA_ROOT.iterdir()
            if p.is_dir()
            and p.name.startswith(prefix)
        ],
        key=lambda p: p.name
    )

    if not folders:

        raise FileNotFoundError(
            "找不到 v1.5.10 行情接續成果。"
            "\n請先執行 v1.5.10 官方行情增量續接程式，"
            "確認 TWSE / TPEx 2026-11-12 資料已通過。"
        )

    all_forward_frames = []
    source_records = []
    daily_status = defaultdict(set)
    run_reports = []

    for folder in folders:

        report_path = (
            folder
            / "v1.5.10_官方增量行情續接報告.json"
        )

        manifest_path = (
            folder
            / "artifact_sha256_manifest.json"
        )

        if not report_path.is_file():
            continue

        if not manifest_path.is_file():

            raise RuntimeError(
                f"v1.5.10 缺少 artifact manifest：{folder.name}"
            )

        report = json_read(report_path)

        if report.get("version") != (
            "SSO_Market_Data_Continuation_TWSE_TPEX_v1.5.10"
        ):
            continue

        if str(report.get("status", "")).startswith("BLOCKED"):

            raise RuntimeError(
                f"v1.5.10 本次行情更新遭阻擋："
                f"{folder.name} / {report.get('status')}"
            )

        manifest = json_read(manifest_path)

        manifest_map = {
            item["path"]: item
            for item in manifest.get("files", [])
        }

        # 逐檔核對標準化增量及日統計。
        increment_files = sorted(
            folder.glob("全市場標準化增量_*.csv"),
            key=lambda p: p.name
        )

        daily_path = (
            folder / "逐日雙市場行情驗證統計.csv"
        )

        if daily_path.is_file():

            daily = read_csv_rows(daily_path)

            if {
                "trade_date",
                "market",
                "status"
            }.issubset(daily.columns):

                for _, row in daily.iterrows():

                    day = pd.to_datetime(
                        row["trade_date"],
                        errors="coerce"
                    )

                    market = market_text(
                        row["market"]
                    )

                    status = str(
                        row["status"]
                    ).strip()

                    if pd.notna(day):

                        daily_status[
                            (
                                day.strftime("%Y-%m-%d"),
                                market
                            )
                        ].add(status)

        for path in increment_files:

            relative = path.relative_to(
                folder
            ).as_posix()

            if relative not in manifest_map:

                raise RuntimeError(
                    f"v1.5.10 manifest 缺少增量檔：{relative}"
                )

            expected_sha = manifest_map[
                relative
            ]["sha256"]

            actual_sha = sha256_file(path)

            if actual_sha != expected_sha:

                raise RuntimeError(
                    f"v1.5.10 增量 SHA 不符：{path}"
                )

            frame = read_rows_as_text(path)

            if frame.empty:
                continue

            all_forward_frames.append(frame)

            source_records.append({
                "來源版本": "v1.5.10",
                "來源資料夾": folder.name,
                "檔案": path.name,
                "路徑": str(path),
                "列數": len(frame),
                "大小bytes": path.stat().st_size,
                "SHA256": actual_sha
            })

        source_records.append({
            "來源版本": "v1.5.10",
            "來源資料夾": folder.name,
            "檔案": report_path.name,
            "路徑": str(report_path),
            "列數": None,
            "大小bytes": report_path.stat().st_size,
            "SHA256": sha256_file(report_path)
        })

        run_reports.append({
            "folder": folder,
            "report": report,
            "manifest": manifest
        })

        log(
            f"[V1.5.10 SOURCE PASS] {folder.name}；"
            f"status={report.get('status')}；"
            f"forward_rows={report.get('forward_new_rows')}"
        )

    if not run_reports:

        raise RuntimeError(
            "找不到有效 v1.5.10 官方增量報告。"
        )

    if not all_forward_frames:

        raise RuntimeError(
            "v1.5.10 尚無 2026-11-12 起的標準化新行情列。"
            "\n請先確認 17:00 後的雙市場官方行情更新已完成。"
        )

    forward_raw = pd.concat(
        all_forward_frames,
        ignore_index=True
    )

    latest_run = run_reports[-1]

    log(
        f"[PASS] v1.5.10 輸出資料夾：{len(run_reports)} 個。"
    )

    log(
        f"[INFO] v1.5.10 累積標準化列數："
        f"{len(forward_raw):,}"
    )

    return (
        forward_raw,
        source_records,
        daily_status,
        run_reports,
        latest_run
    )


# ============================================================
# 07. 將市場標準化 CSV 轉成 v1.2.8 官方行情格式
# ============================================================

def to_official_format(
    frame,
    v126,
    source_label
):

    if frame.empty:
        return pd.DataFrame()

    mapping = {
        field: find_column(
            frame.columns,
            aliases
        )
        for field, aliases in ALIASES.items()
    }

    required = [
        "date",
        "market",
        "code",
        "open",
        "high",
        "low",
        "close"
    ]

    missing = [
        field
        for field in required
        if mapping.get(field) is None
    ]

    if missing:

        raise RuntimeError(
            f"{source_label} 缺少官方行情必要欄位：{missing}"
        )

    new = pd.DataFrame(index=frame.index)

    new["date"] = v126.parse_date_series(
        frame[mapping["date"]]
    )

    new["market"] = v126.normalize_market_series(
        frame[mapping["market"]]
    )

    new["code"] = v126.clean_code_series(
        frame[mapping["code"]]
    )

    if mapping.get("name") is not None:

        new["name"] = (
            frame[mapping["name"]]
            .astype("string")
            .fillna("")
            .str.strip()
        )

    else:
        new["name"] = ""

    for field in (
        "open",
        "high",
        "low",
        "close"
    ):

        new[field] = v126.numeric_series(
            frame[mapping[field]]
        )

    new["segment"] = ""

    new["ohlc_valid_source"] = (
        "SSO_V1.5.9_1.5.10_VERIFIED"
    )

    new["official_note"] = source_label

    if new["date"].isna().any():

        raise RuntimeError(
            f"{source_label} 有無法解析的日期。"
        )

    if new["market"].isna().any():

        raise RuntimeError(
            f"{source_label} 有無法解析的市場。"
        )

    if new["code"].isna().any():

        raise RuntimeError(
            f"{source_label} 有無法解析的證券代號。"
        )

    if new["code"].astype(str).str.strip().eq("").any():

        raise RuntimeError(
            f"{source_label} 有空白證券代號。"
        )

    return new.reset_index(drop=True)


# ============================================================
# 08. 前瞻增量跨執行去重與日期品質檢查
# ============================================================

def normalize_forward_increment(
    raw,
    v126,
    daily_status
):

    converted = to_official_format(
        raw,
        v126,
        "V1.5.10_FORWARD_INCREMENT"
    )

    # 日期限制：只保留真正 OOS 起始日以後的新行情。
    converted = converted.loc[
        converted["date"].ge(
            pd.Timestamp(OOS_START_DATE)
        )
    ].copy()

    if converted.empty:

        raise RuntimeError(
            "標準化增量沒有 2026-11-12 起的資料。"
        )

    # 建立跨版本／跨重跑自然鍵。
    converted["_key"] = list(
        zip(
            converted["date"].dt.strftime("%Y-%m-%d"),
            converted["market"].astype(str),
            converted["code"].astype(str)
        )
    )

    compare_fields = [
        "open",
        "high",
        "low",
        "close",
        "name"
    ]

    accepted = {}
    identical_duplicate_count = 0
    conflict_keys = []

    for row in converted.to_dict("records"):

        key = row["_key"]

        signature = tuple(
            (
                None
                if pd.isna(row.get(field))
                else str(row.get(field))
            )
            for field in compare_fields
        )

        if key not in accepted:

            accepted[key] = {
                "row": row,
                "signature": signature
            }

        else:

            if accepted[key]["signature"] == signature:

                identical_duplicate_count += 1

            else:

                conflict_keys.append(key)

    if conflict_keys:

        raise RuntimeError(
            "多次行情續接的同一自然鍵內容不同，"
            "停止合併。"
            f"\n衝突筆數：{len(conflict_keys)}"
            f"\n範例：{conflict_keys[:20]}"
        )

    rows = [
        item["row"]
        for item in accepted.values()
    ]

    result = pd.DataFrame(rows).drop(
        columns=["_key"],
        errors="ignore"
    )

    result = result.sort_values(
        ["date", "market", "code"],
        kind="mergesort"
    ).reset_index(drop=True)

    # 每一日期、每一市場重新檢查。
    daily_rows = []

    valid_dates = set()

    for day, day_frame in result.groupby(
        "date",
        sort=True
    ):

        market_pass = {}

        for market in ("TWSE", "TPEx"):

            part = day_frame.loc[
                day_frame["market"].eq(market)
            ].copy()

            n = len(part)

            unique_codes = int(
                part["code"].nunique()
            )

            duplicate_rows = (
                n - unique_codes
            )

            if n:

                ohlc_values = part[
                    ["open", "high", "low", "close"]
                ]

                missing_ohlc = int(
                    ohlc_values.isna().any(axis=1).sum()
                )

                complete = ohlc_values.dropna()

                if len(complete):

                    bad = (
                        (complete[["open", "high", "low", "close"]].min(axis=1) < 0)
                        | (complete["low"] > complete["high"])
                        | (complete["open"] < complete["low"])
                        | (complete["open"] > complete["high"])
                        | (complete["close"] < complete["low"])
                        | (complete["close"] > complete["high"])
                    )

                    bad_ohlc = int(bad.sum())

                else:
                    bad_ohlc = 0

                bad_ratio = (
                    bad_ohlc / len(complete)
                    if len(complete) else 0
                )

                missing_ratio = (
                    missing_ohlc / n
                )

            else:

                missing_ohlc = 0
                missing_ratio = 1.0
                bad_ohlc = 0
                bad_ratio = 0.0

            date_key = (
                pd.Timestamp(day).strftime("%Y-%m-%d"),
                market
            )

            api_statuses = daily_status.get(
                date_key,
                set()
            )

            api_pass = (
                "APPENDED" in api_statuses
            )

            passed = (
                n >= MIN_DAILY_MARKET_ROWS
                and n <= MAX_DAILY_MARKET_ROWS
                and unique_codes == n
                and duplicate_rows == 0
                and missing_ratio <= MAX_MISSING_OHLC_RATIO
                and bad_ratio <= MAX_INVALID_OHLC_RATIO
                and api_pass
            )

            market_pass[market] = passed

            daily_rows.append({
                "trade_date": pd.Timestamp(day).date().isoformat(),
                "market": market,
                "rows": n,
                "unique_codes": unique_codes,
                "duplicate_rows": duplicate_rows,
                "missing_ohlc_rows": missing_ohlc,
                "missing_ohlc_ratio": missing_ratio,
                "invalid_ohlc_rows": bad_ohlc,
                "invalid_ohlc_ratio": bad_ratio,
                "api_statuses": "；".join(sorted(api_statuses)),
                "status": "PASS" if passed else "REJECTED",
            })

        if (
            market_pass.get("TWSE", False)
            and market_pass.get("TPEx", False)
        ):

            valid_dates.add(
                pd.Timestamp(day).normalize()
            )

    daily_quality = pd.DataFrame(
        daily_rows
    )

    if pd.Timestamp(OOS_START_DATE) not in valid_dates:

        raise RuntimeError(
            "2026-11-12 沒有通過雙市場完整性驗證。"
            "\n主程式不會自動把 OOS 起始日延後，"
            "以免改變事前凍結的樣本外起點。"
            "\n請確認 v1.5.10 兩市場當日均為 APPENDED。"
        )

    result = result.loc[
        result["date"].isin(valid_dates)
    ].copy()

    log(
        "[PASS] 前瞻增量跨執行去重："
        f"完全相同重複副本={identical_duplicate_count:,}；"
        f"衝突鍵=0。"
    )

    log(
        f"[PASS] 雙市場完整交易日：{len(valid_dates):,} 日。"
    )

    return (
        result,
        daily_quality,
        sorted(valid_dates),
        identical_duplicate_count
    )


# ============================================================
# 09. 驗證官方普通股凍結名單
# ============================================================

def verify_frozen_candidates(
    v128
):

    (
        ordinary_frame,
        ordinary_codes,
        ordinary_name_map,
        ordinary_market_map,
        ordinary_info
    ) = v128.load_official_ordinary()

    if len(ordinary_frame) != EXPECTED_ORDINARY_KEYS:

        raise RuntimeError(
            "2026-11-10 官方普通股分類鍵數不符。"
            f"\n預期：{EXPECTED_ORDINARY_KEYS}"
            f"\n實際：{len(ordinary_frame)}"
        )

    missing = (
        EXPECTED_CANDIDATE_CODES
        - set(ordinary_codes)
    )

    if missing:

        raise RuntimeError(
            "凍結候選名單未全部出現在 2026-11-10 官方普通股清單。"
            f"\n缺少：{sorted(missing)}"
        )

    records = []

    for code, name in EXPECTED_CANDIDATES.items():

        market = ordinary_market_map.get(code)

        if not market:

            raise RuntimeError(
                f"官方普通股清單找不到 {code} 的市場別。"
            )

        records.append({
            "股票代號": code,
            "凍結股票名稱": name,
            "2026-11-10官方名稱": ordinary_name_map.get(code, ""),
            "2026-11-10官方市場": market,
            "凍結狀態": "FROZEN",
            "候選來源": (
                "v1.2.9 兩種 Q5 口徑共同通過 H1/H3/H5"
            )
        })

    frame = pd.DataFrame(records)

    log(
        "[PASS] 12 檔候選均存在於 "
        "2026-11-10 官方 ISIN CFI 普通股名單。"
    )

    log(
        "[INFO] 這是凍結日資格驗證，"
        "不代表 2026-11-12 以後已有每日 CFI 分類。"
    )

    return (
        frame,
        ordinary_frame,
        ordinary_codes,
        ordinary_name_map,
        ordinary_market_map,
        ordinary_info
    )


# ============================================================
# 10. 合併歷史行情、11/11 雙市場修正及 OOS 新行情
# ============================================================

def combine_official_sources(
    sources,
    corrections,
    forward_increment,
    v126,
    logs
):

    official = sources["official"].copy()

    required_base = [
        "date",
        "market",
        "code",
        "open",
        "high",
        "low",
        "close"
    ]

    missing_base = [
        col for col in required_base
        if col not in official.columns
    ]

    if missing_base:

        raise RuntimeError(
            f"固定歷史行情缺少欄位：{missing_base}"
        )

    official["date"] = pd.to_datetime(
        official["date"],
        errors="coerce"
    ).dt.normalize()

    official["market"] = (
        official["market"]
        .astype("string")
        .str.strip()
    )

    official["code"] = (
        official["code"]
        .astype("string")
        .str.strip()
    )

    base_max_date = official["date"].max()

    if base_max_date != pd.Timestamp("2026-11-10"):

        raise RuntimeError(
            "既有官方歷史行情經 v1.5.1 承接後，"
            "最後日期不是 2026-11-10。"
            f"\n實際：{base_max_date}"
        )

    # --------------------------------------------------------
    # 將 11/11 舊行情從記憶體資料中完整替代
    # --------------------------------------------------------

    correction_mask = official["date"].eq(
        pd.Timestamp(CORRECTION_DATE)
    )

    removed_1111_rows = int(
        correction_mask.sum()
    )

    official = official.loc[
        ~correction_mask
    ].copy()

    # --------------------------------------------------------
    # 轉換 v1.5.9 兩市場修正層
    # --------------------------------------------------------

    twse_correction = to_official_format(
        corrections["twse"],
        v126,
        "V1.5.9_TWSE_20261111_CORRECTION"
    )

    tpex_correction = to_official_format(
        corrections["tpex"],
        v126,
        "V1.5.9_TPEX_20261111_TYPE_EW_CORRECTION"
    )

    correction_df = pd.concat(
        [
            twse_correction,
            tpex_correction
        ],
        ignore_index=True
    )

    if not correction_df["date"].eq(
        pd.Timestamp(CORRECTION_DATE)
    ).all():

        raise RuntimeError(
            "v1.5.9 修正層混入非 2026-11-11 日期。"
        )

    correction_counts = (
        correction_df.groupby("market")
        .size()
        .to_dict()
    )

    if (
        correction_counts.get("TWSE", 0)
        != EXPECTED_TWSE_CORRECTION_ROWS
    ):

        raise RuntimeError(
            "TWSE 11/11 修正層轉換筆數不符。"
        )

    if (
        correction_counts.get("TPEx", 0)
        != EXPECTED_TPEX_CORRECTION_ROWS
    ):

        raise RuntimeError(
            "TPEx 11/11 修正層轉換筆數不符。"
        )

    # --------------------------------------------------------
    # 加上 11/12 起雙市場增量
    # --------------------------------------------------------

    new_records = pd.concat(
        [
            correction_df,
            forward_increment
        ],
        ignore_index=True,
        sort=False
    )

    combined = pd.concat(
        [
            official,
            new_records
        ],
        ignore_index=True,
        sort=False
    )

    # 全體自然鍵檢查。
    duplicate_mask = combined.duplicated(
        ["market", "code", "date"],
        keep=False
    )

    if duplicate_mask.any():

        examples = combined.loc[
            duplicate_mask,
            ["market", "code", "date"]
        ].head(20)

        raise RuntimeError(
            "合併後官方行情存在重複市場－代號－日期鍵。"
            f"\n範例：\n{examples.to_string(index=False)}"
        )

    # 日期範圍與市場代碼。
    if combined["date"].isna().any():
        raise RuntimeError("合併後官方行情存在無效日期。")

    if combined["code"].isna().any():
        raise RuntimeError("合併後官方行情存在無效代號。")

    if not combined["market"].isin(
        ["TWSE", "TPEx"]
    ).all():

        raise RuntimeError(
            "合併後行情存在非 TWSE / TPEx 市場代碼。"
        )

    combined.sort_values(
        ["date", "code", "market"],
        kind="mergesort",
        inplace=True
    )

    sources["official"] = (
        combined.reset_index(drop=True)
    )

    last_date = (
        sources["official"]["date"].max()
    )

    log(
        "[PASS] 官方行情資料接續完成："
        f"base={base_max_date.date()}；"
        f"替代 11/11 舊資料={removed_1111_rows:,}；"
        f"補入 11/11 修正={len(correction_df):,}；"
        f"新增 OOS={len(forward_increment):,}；"
        f"最新={last_date.date()}。"
    )

    return {
        "official": sources["official"],
        "removed_old_1111_rows": removed_1111_rows,
        "correction_rows": len(correction_df),
        "forward_rows": len(forward_increment),
        "last_date": last_date
    }


# ============================================================
# 11. 建立凍結12檔的前瞻訊號及逐日覆蓋檢查
# ============================================================

def build_frozen_candidate_signals(
    official,
    valid_dates,
    v126,
    candidate_frame,
    ordinary_market_map
):

    code_to_name = EXPECTED_CANDIDATES

    market_map = {
        str(code): market_text(market)
        for code, market in ordinary_market_map.items()
    }

    candidate_codes = set(
        EXPECTED_CANDIDATE_CODES
    )

    oos_dates = [
        pd.Timestamp(x).normalize()
        for x in valid_dates
        if pd.Timestamp(x).date() >= OOS_START_DATE
    ]

    coverage_rows = []
    signal_rows = []
    bar_rows = []

    for day in oos_dates:

        for code in sorted(candidate_codes):

            group = official.loc[
                official["date"].eq(day)
                & official["code"].astype(str).eq(code)
            ].copy()

            status = ""
            selected = None

            if group.empty:

                status = "NO_SIGNAL_DAY_BAR"

            elif len(group) > 1:

                status = "AMBIGUOUS_CODE_DATE"

            else:

                row = group.iloc[0]

                actual_market = market_text(
                    row["market"]
                )

                frozen_market = market_map.get(code, "")

                if actual_market != frozen_market:

                    status = "FROZEN_MARKET_MISMATCH"

                else:

                    valid_mask = v126.valid_ohlc(
                        group
                    )

                    if not bool(
                        np.asarray(valid_mask)[0]
                    ):

                        status = "INVALID_SIGNAL_DAY_OHLC"

                    else:

                        status = "SIGNAL_AVAILABLE"
                        selected = row

            coverage_rows.append({
                "訊號日期": day.date().isoformat(),
                "股票代號": code,
                "股票名稱": code_to_name[code],
                "凍結日市場": market_map.get(code, ""),
                "訊號日行情狀態": status
            })

            if selected is None:
                continue

            bar_rows.append({
                "trade_date": day.date().isoformat(),
                "market": market_text(selected["market"]),
                "instrument_code": code,
                "instrument_name": code_to_name[code],
                "open": safe_float(selected["open"]),
                "high": safe_float(selected["high"]),
                "low": safe_float(selected["low"]),
                "close": safe_float(selected["close"]),
                "status": "VALID_OHLC"
            })

            signal_rows.append({
                "訊號日期": day,
                "股票代號": code,
                "股票名稱": code_to_name[code],
                "訊號當日市場": actual_market,
                "range_pos20": np.nan,
                "橫斷面百分位排名": np.nan,
                "是否Q5訊號": True,
                "訊號研究組": "FROZEN12",
            })

    coverage = pd.DataFrame(
        coverage_rows
    )

    bars = pd.DataFrame(
        bar_rows
    )

    signals = pd.DataFrame(
        signal_rows
    )

    if signals.empty:

        raise RuntimeError(
            "2026-11-12 起凍結候選沒有任何有效訊號日 OHLC。"
        )

    # OOS 起始日不得自動向後移動。
    first_signals = signals.loc[
        pd.to_datetime(
            signals["訊號日期"]
        ).dt.date.eq(OOS_START_DATE)
    ].copy()

    if first_signals.empty:

        raise RuntimeError(
            "2026-11-12 第一批凍結候選沒有有效訊號列。"
            "\n不自動將前瞻起始日延後，避免改變事前設計。"
        )

    first_valid_codes = set(
        first_signals["股票代號"].astype(str)
    )

    first_missing_codes = sorted(
        candidate_codes - first_valid_codes
    )

    if first_missing_codes:

        log(
            "[WARNING] 2026-11-12 第一批候選有股票沒有有效訊號日 OHLC："
            + repr(first_missing_codes)
        )

    log(
        f"[PASS] 2026-11-12 第一批有效候選："
        f"{len(first_signals):,}/{len(candidate_codes):,}。"
    )

    log(
        f"[PASS] 前瞻每日重新進場候選訊號："
        f"{len(signals):,} 筆。"
    )

    return (
        signals,
        first_signals,
        coverage,
        bars,
        first_missing_codes
    )


# ============================================================
# 12. 建立完整前瞻多週期排程
# ============================================================

def build_oos_schedules(
    v129,
    official,
    events,
    v126,
    v127,
    signals,
    first_signals
):

    signal_sets = [
        (
            first_signals,
            "FROZEN12_FIRST_OOS_20261112"
        ),
        (
            signals,
            "FROZEN12_DAILY_REENTRY"
        )
    ]

    schedule = v129.build_multi_horizon_returns(
        signal_sets=signal_sets,
        official=official,
        events=events,
        v126=v126,
        v127=v127,
        horizons=HORIZONS
    )

    if schedule.empty:

        raise RuntimeError(
            "凍結12檔前瞻多週期排程為空。"
        )

    matured = schedule.loc[
        schedule["觀察狀態"].eq(
            "MATURED_RETURN_COMPUTED"
        )
    ].copy()

    horizon_summary = (
        v129.summarize_by_horizon(
            schedule
        )
    )

    daily_summary = (
        v129.summarize_by_signal_date(
            schedule
        )
    )

    stock_summary = (
        v129.summarize_stock_horizon(
            matured
        )
    )

    cross_summary = (
        v129.build_cross_horizon_stock_table(
            stock_summary
        )
    )

    first_schedule = schedule.loc[
        schedule["訊號口徑"].eq(
            "FROZEN12_FIRST_OOS_20261112"
        )
    ].copy()

    daily_schedule = schedule.loc[
        schedule["訊號口徑"].eq(
            "FROZEN12_DAILY_REENTRY"
        )
    ].copy()

    first_matured = matured.loc[
        matured["訊號口徑"].eq(
            "FROZEN12_FIRST_OOS_20261112"
        )
    ].copy()

    daily_matured = matured.loc[
        matured["訊號口徑"].eq(
            "FROZEN12_DAILY_REENTRY"
        )
    ].copy()

    return {
        "schedule": schedule,
        "matured": matured,
        "horizon_summary": horizon_summary,
        "daily_summary": daily_summary,
        "stock_summary": stock_summary,
        "cross_summary": cross_summary,
        "first_schedule": first_schedule,
        "daily_schedule": daily_schedule,
        "first_matured": first_matured,
        "daily_matured": daily_matured
    }


# ============================================================
# 13. 封存程式原文
# ============================================================

def archive_current_program(out_dir):

    target = (
        out_dir
        / "SSO_v1.3.1_凍結12檔前瞻樣本外追蹤.py"
    )

    source_text = ""

    try:

        shell = get_ipython()

        history = (
            shell.history_manager.input_hist_raw
        )

        for candidate in reversed(
            history
        ):

            if not isinstance(candidate, str):
                continue

            if (
                "SSO v1.3.1｜凍結12檔前瞻樣本外追蹤主程式"
                in candidate
                and "def run_main_v131():" in candidate
                and len(candidate) > 20000
            ):

                source_text = candidate
                break

    except Exception:
        source_text = ""

    if source_text:

        compile(
            source_text,
            str(target),
            "exec"
        )

        target.write_text(
            source_text,
            encoding="utf-8"
        )

        return target

    target.write_text(
        "# SOURCE_CAPTURE_NOTE\n"
        "本次完整程式原文已由 ChatGPT 對話提供；"
        "Colab IPython history 未能自動擷取目前儲存格。\n",
        encoding="utf-8"
    )

    return target


# ============================================================
# 14. SHA256 Manifest 及 ZIP
# ============================================================

def build_manifest(out_dir):

    items = []

    for path in sorted(
        Path(out_dir).rglob("*")
    ):

        if not path.is_file():
            continue

        if path.name in {
            "artifact_sha256_manifest.json",
            "GitHub上傳結果.json"
        }:
            continue

        if path.suffix.lower() == ".zip":
            continue

        items.append({
            "path": path.relative_to(out_dir).as_posix(),
            "bytes": path.stat().st_size,
            "sha256": sha256_file(path)
        })

    manifest = {
        "version": VERSION,
        "created_at_taipei": datetime.now(TAIPEI).isoformat(),
        "file_count": len(items),
        "self_excluded": True,
        "github_result_excluded": True,
        "files": items
    }

    json_write(
        manifest,
        Path(out_dir)
        / "artifact_sha256_manifest.json"
    )

    return manifest


def make_and_verify_zip(
    out_dir,
    zip_path,
    manifest
):

    out_dir = Path(out_dir)
    zip_path = Path(zip_path)

    with zipfile.ZipFile(
        zip_path,
        "w",
        compression=zipfile.ZIP_DEFLATED,
        compresslevel=6
    ) as archive:

        for path in sorted(
            out_dir.rglob("*")
        ):

            if not path.is_file():
                continue

            if path == zip_path:
                continue

            if path.name == "GitHub上傳結果.json":
                continue

            archive.write(
                path,
                path.relative_to(out_dir).as_posix()
            )

    with zipfile.ZipFile(
        zip_path,
        "r"
    ) as archive:

        bad = archive.testzip()

        if bad:
            raise RuntimeError(
                f"ZIP CRC 失敗：{bad}"
            )

        names = set(
            archive.namelist()
        )

        for item in manifest["files"]:

            relative = item["path"]

            if relative not in names:

                raise RuntimeError(
                    f"ZIP 缺少檔案：{relative}"
                )

            if sha256_bytes(
                archive.read(relative)
            ) != item["sha256"]:

                raise RuntimeError(
                    f"ZIP 內部 SHA256 不一致：{relative}"
                )

        manifest_data = archive.read(
            "artifact_sha256_manifest.json"
        )

        local_manifest_sha = sha256_file(
            out_dir / "artifact_sha256_manifest.json"
        )

        if sha256_bytes(
            manifest_data
        ) != local_manifest_sha:

            raise RuntimeError(
                "ZIP 內 manifest SHA 不一致。"
            )

    return {
        "zip_bytes": zip_path.stat().st_size,
        "zip_sha256": sha256_file(zip_path),
        "crc_pass": True,
        "manifest_file_count": manifest["file_count"],
        "manifest_pass": True
    }


# ============================================================
# 15. GitHub audit branch
# ============================================================

def get_github_token():

    token = os.environ.get(
        "GITHUB_TOKEN",
        ""
    ).strip()

    if token:
        return token

    try:

        from google.colab import userdata

        token = str(
            userdata.get("GITHUB_TOKEN") or ""
        ).strip()

    except Exception:
        token = ""

    if not token:

        raise RuntimeError(
            "找不到 GITHUB_TOKEN。"
        )

    return token


def upload_github(
    out_dir,
    zip_path,
    run_id,
    logs
):

    token = get_github_token()

    api = "https://api.github.com"

    session = requests.Session()

    session.headers.update({
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28"
    })

    def request(method, endpoint, **kwargs):

        response = session.request(
            method,
            api + endpoint,
            timeout=(20, 180),
            **kwargs
        )

        if not response.ok:

            raise RuntimeError(
                f"GitHub API HTTP {response.status_code}："
                f"{response.text[:1500]}"
            )

        return (
            response.json()
            if response.content
            else {}
        )

    owner, repo = REPO.split("/")

    main_before = request(
        "GET",
        f"/repos/{owner}/{repo}/git/ref/heads/main"
    )["object"]["sha"]

    main_commit = request(
        "GET",
        f"/repos/{owner}/{repo}/git/commits/{main_before}"
    )

    base_tree = main_commit["tree"]["sha"]

    branch = (
        f"audit/sso-v131-frozen12-oos-"
        f"{run_id}-{uuid.uuid4().hex[:6]}"
    )

    prefix = (
        f"audit_readout/sso_v131_frozen12_oos_{run_id}"
    )

    files = [
        p
        for p in sorted(Path(out_dir).rglob("*"))
        if p.is_file()
        and p.name != "GitHub上傳結果.json"
        and p.suffix.lower() != ".zip"
    ]

    files.append(zip_path)

    tree_entries = []
    expected = []

    for path in files:

        data = path.read_bytes()

        if len(data) >= 100 * 1024 * 1024:

            raise RuntimeError(
                f"單檔超過 GitHub blob 100MB 上限：{path.name}"
            )

        local_blob_sha = git_blob_sha1(
            data
        )

        blob = request(
            "POST",
            f"/repos/{owner}/{repo}/git/blobs",
            json={
                "content": base64.b64encode(
                    data
                ).decode("ascii"),
                "encoding": "base64"
            }
        )

        if blob["sha"] != local_blob_sha:

            raise RuntimeError(
                f"GitHub blob SHA 不一致：{path.name}"
            )

        relative = (
            path.name
            if path.parent == Path(out_dir)
            else path.relative_to(out_dir).as_posix()
        )

        remote_path = (
            f"{prefix}/{relative}"
        )

        tree_entries.append({
            "path": remote_path,
            "mode": "100644",
            "type": "blob",
            "sha": blob["sha"]
        })

        expected.append({
            "path": remote_path,
            "blob_sha": blob["sha"],
            "sha256": sha256_bytes(data),
            "bytes": len(data)
        })

        log(
            f"[GITHUB BLOB CREATED] {relative} "
            f"({len(data):,} bytes)",
            logs
        )

    tree = request(
        "POST",
        f"/repos/{owner}/{repo}/git/trees",
        json={
            "base_tree": base_tree,
            "tree": tree_entries
        }
    )

    commit = request(
        "POST",
        f"/repos/{owner}/{repo}/git/commits",
        json={
            "message": (
                "Add SSO v1.3.1 frozen 12 prospective OOS "
                + run_id
            ),
            "tree": tree["sha"],
            "parents": [main_before]
        }
    )

    request(
        "POST",
        f"/repos/{owner}/{repo}/git/refs",
        json={
            "ref": "refs/heads/" + branch,
            "sha": commit["sha"]
        }
    )

    # 逐檔 Git blob SHA256 回讀。
    for item in expected:

        remote_blob = request(
            "GET",
            (
                f"/repos/{owner}/{repo}/git/blobs/"
                f"{item['blob_sha']}"
            )
        )

        remote_data = base64.b64decode(
            remote_blob["content"]
        )

        if sha256_bytes(remote_data) != item["sha256"]:

            raise RuntimeError(
                f"GitHub 回讀 SHA256 不一致：{item['path']}"
            )

        if len(remote_data) != item["bytes"]:

            raise RuntimeError(
                f"GitHub 回讀檔案大小不一致：{item['path']}"
            )

        log(
            f"[GITHUB READBACK PASS] {item['path']}",
            logs
        )

    branch_obj = request(
        "GET",
        (
            f"/repos/{owner}/{repo}/branches/"
            + quote(branch, safe="")
        )
    )

    branch_head = branch_obj["commit"]["sha"]

    if branch_head != commit["sha"]:

        raise RuntimeError(
            "GitHub audit branch head 不一致。"
        )

    main_after = request(
        "GET",
        f"/repos/{owner}/{repo}/git/ref/heads/main"
    )["object"]["sha"]

    if main_before != main_after:

        raise RuntimeError(
            "GitHub main SHA 前後不一致。"
        )

    return {
        "status": "PASS",
        "branch": branch,
        "commit_sha": commit["sha"],
        "branch_head_sha": branch_head,
        "main_before": main_before,
        "main_after": main_after,
        "main_unchanged": True,
        "blob_readback_pass": True,
        "file_count": len(expected),
        "uploaded_files": expected,
        "url": (
            f"https://github.com/{REPO}/tree/"
            f"{branch}/{prefix}"
        )
    }


# ============================================================
# 16. 主流程
# ============================================================

def run_main_v131():

    print("=" * 100)
    print(VERSION)
    print("=" * 100)

    ensure_drive()

    logs = []

    now = datetime.now(TAIPEI)

    run_id = (
        now.strftime("%Y%m%d_%H%M%S")
        + "_"
        + uuid.uuid4().hex[:4]
    )

    log(
        f"執行時間：{now.isoformat()}",
        logs
    )

    log(
        f"OOS 起始日：{OOS_START_DATE}",
        logs
    )

    log(
        f"凍結候選數：{len(EXPECTED_CANDIDATES)}",
        logs
    )

    # --------------------------------------------------------
    # A. 驗證 v1.5.9 修正來源
    # --------------------------------------------------------

    log("[01] 驗證 v1.5.9 雙市場修正層。", logs)

    corrections = load_v159_corrections()

    # --------------------------------------------------------
    # B. 載入 v1.5.10 前瞻行情
    # --------------------------------------------------------

    log("[02] 搜尋及核對 v1.5.10 前瞻行情。", logs)

    (
        forward_raw,
        source_records,
        daily_status,
        v1510_runs,
        latest_v1510_run
    ) = load_v1510_outputs(logs)

    # --------------------------------------------------------
    # C. 固定程式模組
    # --------------------------------------------------------

    log("[03] 載入 v1.2.9 / v1.2.8 / v1.2.7 / v1.2.6。", logs)

    v129, v128, v127, v126 = (
        load_fixed_dependencies()
    )

    # --------------------------------------------------------
    # D. 載入歷史行情
    # --------------------------------------------------------

    log("[04] 載入固定歷史官方行情。", logs)

    sources = v127.load_and_validate_sources(
        v126
    )

    log("[05] 承接 v1.5.1 11/9、11/10 官方增量。", logs)

    v151_rows, v151_info, v151_report = (
        v128.load_overlay(v126)
    )

    sources = v128.append_new_overlay(
        sources,
        v151_rows
    )

    # v1.5.1 後應到 11/10。
    v151_latest = pd.to_datetime(
        sources["official"]["date"],
        errors="coerce"
    ).max().normalize()

    if v151_latest != pd.Timestamp("2026-11-10"):

        raise RuntimeError(
            "v1.5.1 官方增量承接後最後日期不是 2026-11-10。"
        )

    # --------------------------------------------------------
    # E. 驗證 v1.5.10 前瞻日資料
    # --------------------------------------------------------

    log("[06] 正規化 v1.5.10 前瞻增量及日期品質。", logs)

    (
        forward_increment,
        daily_quality,
        valid_dates,
        repeated_copy_count
    ) = normalize_forward_increment(
        forward_raw,
        v126,
        daily_status
    )

    if pd.Timestamp(OOS_START_DATE) not in valid_dates:

        raise RuntimeError(
            "2026-11-12 未通過雙市場日資料驗證。"
        )

    # --------------------------------------------------------
    # F. 官方普通股凍結日資格
    # --------------------------------------------------------

    log("[07] 核對凍結12檔的 2026-11-10 官方普通股資格。", logs)

    (
        candidate_frame,
        ordinary_frame,
        ordinary_codes,
        ordinary_name_map,
        ordinary_market_map,
        ordinary_info
    ) = verify_frozen_candidates(
        v128
    )

    # --------------------------------------------------------
    # G. 合併 11/11 修正層及 OOS 新行情
    # --------------------------------------------------------

    log("[08] 替代 2026-11-11 舊資料並接續 OOS 行情。", logs)

    combined_info = combine_official_sources(
        sources,
        corrections,
        forward_increment,
        v126,
        logs
    )

    # --------------------------------------------------------
    # H. 核對 v1.2.6 原始特徵相容性及取得事件資料
    # --------------------------------------------------------

    log("[09] 沿用 v1.2.6 原始函式檢查歷史資料相容性。", logs)

    rebuilt = v127.rebuild_v126_feature_rows(
        v126,
        sources
    )

    legacy_detail, legacy_daily_summary = (
        v127.build_q5_detail(
            rebuilt["extra"]["feature_rows"],
            rebuilt["universe"]
        )
    )

    original_summary = (
        v127.load_v126_reference_summary()
    )

    qa = v127.verify_against_v126(
        original_summary,
        rebuilt["daily_q5"],
        legacy_detail,
        legacy_daily_summary
    )

    qa_failures = int(
        qa["核對結果"].ne("PASS").sum()
    )

    if len(qa) != 126 or qa_failures != 0:

        raise RuntimeError(
            "v1.2.6 原始 126 項相容性核對未完整通過。"
        )

    events = rebuilt["extra"]["events"]

    if events is None:
        events = pd.DataFrame()

    log(
        "[PASS] v1.2.6 原始 126 項相容性核對全部通過。",
        logs
    )

    # --------------------------------------------------------
    # I. 建立凍結候選每日訊號
    # --------------------------------------------------------

    official = sources["official"]

    (
        signals,
        first_signals,
        candidate_coverage,
        candidate_bars,
        first_missing_codes
    ) = build_frozen_candidate_signals(
        official,
        valid_dates,
        v126,
        candidate_frame,
        ordinary_market_map
    )

    # --------------------------------------------------------
    # J. 依 v1.2.9 正式配對函式計算四週期
    # --------------------------------------------------------

    log("[10] 沿用 v1.2.9 交易日曆、配對、成本計算。", logs)

    oos_result = build_oos_schedules(
        v129,
        official,
        events,
        v126,
        v127,
        signals,
        first_signals
    )

    schedule = oos_result["schedule"]
    matured = oos_result["matured"]

    horizon_summary = (
        oos_result["horizon_summary"]
    )

    daily_return_summary = (
        oos_result["daily_summary"]
    )

    stock_summary = (
        oos_result["stock_summary"]
    )

    cross_summary = (
        oos_result["cross_summary"]
    )

    first_schedule = (
        oos_result["first_schedule"]
    )

    daily_schedule = (
        oos_result["daily_schedule"]
    )

    first_matured = (
        oos_result["first_matured"]
    )

    daily_matured = (
        oos_result["daily_matured"]
    )

    # --------------------------------------------------------
    # K. 最新行情及觀察成熟狀態
    # --------------------------------------------------------

    latest_official_date = pd.to_datetime(
        official["date"],
        errors="coerce"
    ).max().normalize()

    status_counts = {
        str(k): int(v)
        for k, v in schedule["觀察狀態"]
        .value_counts(dropna=False)
        .items()
    }

    first_status = {
        str(k): int(v)
        for k, v in first_schedule["觀察狀態"]
        .value_counts(dropna=False)
        .items()
    }

    daily_status_counts = {
        str(k): int(v)
        for k, v in daily_schedule["觀察狀態"]
        .value_counts(dropna=False)
        .items()
    }

    # 價格事件資料涵蓋期間僅作揭露，不推論事件清單完整。
    event_latest_date = None

    if (
        isinstance(events, pd.DataFrame)
        and not events.empty
        and "日期" in events.columns
    ):

        event_dates = pd.to_datetime(
            events["日期"],
            errors="coerce"
        ).dropna()

        if len(event_dates):
            event_latest_date = str(
                event_dates.max().date()
            )

    # --------------------------------------------------------
    # L. 建立研究結論
    # --------------------------------------------------------

    first_candidate_count = len(
        first_signals
    )

    first_candidate_coverage = (
        first_candidate_count
        / len(EXPECTED_CANDIDATES)
    )

    data_completeness_status = (
        "PASS"
        if pd.Timestamp(OOS_START_DATE) in valid_dates
        else "BLOCKED"
    )

    report = {
        "版本": VERSION,
        "執行時間台北": now.isoformat(),
        "執行識別碼": run_id,

        "研究定位": (
            "凍結12檔候選之前瞻樣本外追蹤；"
            "非每日全市場 Q1-Q5 重新排名。"
        ),

        "OOS": {
            "起始日": OOS_START_DATE.isoformat(),
            "官方行情最後日期": str(
                latest_official_date.date()
            ),
            "有效前瞻交易日": [
                pd.Timestamp(x).date().isoformat()
                for x in valid_dates
            ],
            "有效前瞻交易日數": len(valid_dates)
        },

        "凍結候選": {
            "候選數": len(candidate_frame),
            "候選代號": sorted(EXPECTED_CANDIDATE_CODES),
            "候選名稱": EXPECTED_CANDIDATES,
            "候選資格基準日": (
                ORDINARY_CLASSIFICATION_DATE.isoformat()
            ),
            "分類方法": "官方 ISIN CFI ES 普通股",
            "分類驗證": "PASS",
            "後續逐日 CFI 分類是否已驗證": False,
            "後續分類處理": (
                "固定追蹤原始凍結12檔，不因後續分類資料缺漏"
                "而事後刪除候選；無行情或市場識別異常者"
                "在每日覆蓋表保留狀態。"
            )
        },

        "來源承接": {
            "v159_zip_sha256": corrections["zip_sha256"],
            "v159_manifest_sha256": corrections["manifest_sha256"],
            "twse_20261111_canonical_sha256": (
                EXPECTED_TWSE_CANONICAL_SHA256
            ),
            "tpex_20261111_canonical_sha256": (
                EXPECTED_TPEX_CANONICAL_SHA256
            ),
            "v151_latest_date": "2026-11-10",
            "v151_zip_sha256": v151_info.get("zip_sha256", ""),
            "v1510_output_runs": len(v1510_runs),
            "v1510_forward_rows_after_dedup": len(forward_increment),
            "v1510_identical_duplicate_copies": repeated_copy_count,
            "old_20261111_rows_replaced_in_memory": (
                combined_info["removed_old_1111_rows"]
            ),
            "v159_correction_rows": (
                combined_info["correction_rows"]
            )
        },

        "樣本外訊號": {
            "第一批訊號日期": OOS_START_DATE.isoformat(),
            "第一批有效候選數": first_candidate_count,
            "第一批候選覆蓋率": first_candidate_coverage,
            "第一批缺少有效訊號日行情代號": first_missing_codes,
            "每日重新進場訊號數": len(signals),
            "每日觀察組": "FROZEN12_DAILY_REENTRY",
            "第一批觀察組": "FROZEN12_FIRST_OOS_20261112",
            "range_pos20是否作為本次前瞻選股條件": False,
            "是否每日重新排名Q5": False
        },

        "交易規則": {
            "進場": "訊號日後第 1 個全市場交易日開盤",
            "出場": "訊號日後第 H 個全市場交易日收盤",
            "H": list(HORIZONS),
            "基準成本沿用": "v1.2.2 / v1.2.9 固定公式",
            "壓力成本沿用": "v1.2.2 / v1.2.9 固定公式",
            "成本公式程式來源_GitBlobSHA1": (
                V129_SCRIPT_GIT_BLOB_SHA1
            )
        },

        "排程及成熟狀態": {
            "全部排程筆數": len(schedule),
            "已成熟淨報酬筆數": len(matured),
            "第一批排程筆數": len(first_schedule),
            "第一批已成熟筆數": len(first_matured),
            "每日重新進場排程筆數": len(daily_schedule),
            "每日重新進場已成熟筆數": len(daily_matured),
            "整體狀態分布": status_counts,
            "第一批狀態分布": first_status,
            "每日重新進場狀態分布": daily_status_counts
        },

        "資料品質": {
            "v126原始126項相容性核對項目": len(qa),
            "v126原始核對失敗": qa_failures,
            "雙市場前瞻日期品質檢查": (
                daily_quality.to_dict("records")
            ),
            "前瞻資料接續狀態": data_completeness_status
        },

        "價格區段事件": {
            "事件資料列數": (
                int(len(events))
                if isinstance(events, pd.DataFrame)
                else 0
            ),
            "事件來源最新日期": event_latest_date,
            "說明": (
                "沿用 v1.2.6/v1.2.9 既有事件索引，"
                "配對函式會對已識別的價格區段事件標記 REQUIRES_REVIEW。"
                "事件來源未被證明涵蓋所有 OOS 未來公司行動，"
                "不得將未標記解讀為已排除所有公司行動影響。"
            )
        },

        "研究限制": [
            "本次是 v1.2.9 凍結12檔名單的前瞻追蹤，不代表重新選股。",
            "每日重新進場組各筆 H 期間可能重疊，不是獨立樣本。",
            "第一批 2026-11-12 訊號使用下一交易日開盤進場。",
            "未成熟 H3/H5/H10 不計入已成熟淨報酬。",
            "當日官方 CFI 普通股分類未取得前，不產生每日全市場 Q1-Q5 排名。",
            "不依本次 OOS 結果調整凍結12檔名單。",
            "本次不重訓模型、不修改特徵公式。",
            "本次不將觀察結果直接稱為可獲利保證或投資推薦。"
        ],

        "資料安全": {
            "Drive原始來源唯讀": True,
            "未刪除或搬移來源": True,
            "未覆寫既有結果": True,
            "未重新訓練": True,
            "未修改GitHub_main": True
        },

        "執行狀態": "RUNNING_FORWARD_OOS"
    }

    # --------------------------------------------------------
    # M. 輸出目錄
    # --------------------------------------------------------

    out_name = (
        "看股最速適_v1_3_1_凍結12檔前瞻樣本外追蹤_"
        + run_id
    )

    out_dir = ROOT / out_name

    out_dir.mkdir(
        parents=True,
        exist_ok=False
    )

    log(
        f"[OUTPUT] {out_dir}",
        logs
    )

    # --------------------------------------------------------
    # N. 輸出完整資料
    # --------------------------------------------------------

    candidate_frame.to_csv(
        out_dir / "01_v129凍結12檔候選名單.csv",
        index=False,
        encoding="utf-8-sig"
    )

    daily_quality.to_csv(
        out_dir / "02_前瞻雙市場逐日資料品質.csv",
        index=False,
        encoding="utf-8-sig"
    )

    candidate_coverage.to_csv(
        out_dir / "03_凍結12檔逐日行情覆蓋.csv",
        index=False,
        encoding="utf-8-sig"
    )

    candidate_bars.to_csv(
        out_dir / "04_凍結12檔前瞻OHLC行情.csv",
        index=False,
        encoding="utf-8-sig"
    )

    signals.to_csv(
        out_dir / "05_凍結12檔每日重新進場訊號.csv",
        index=False,
        encoding="utf-8-sig",
        na_rep=""
    )

    first_signals.to_csv(
        out_dir / "06_20261112第一批凍結候選訊號.csv",
        index=False,
        encoding="utf-8-sig",
        na_rep=""
    )

    schedule.to_csv(
        out_dir / "07_凍結12檔H1H3H5H10完整前瞻排程.csv.gz",
        index=False,
        encoding="utf-8-sig",
        compression="gzip",
        na_rep=""
    )

    matured.to_csv(
        out_dir / "08_凍結12檔已成熟實際淨報酬.csv.gz",
        index=False,
        encoding="utf-8-sig",
        compression="gzip",
        na_rep=""
    )

    csv_write(
        horizon_summary,
        out_dir / "09_凍結12檔各週期整體績效摘要.csv"
    )

    csv_write(
        daily_return_summary,
        out_dir / "10_凍結12檔逐訊號日週期成熟概況.csv"
    )

    csv_write(
        stock_summary,
        out_dir / "11_凍結12檔個股各週期成熟報酬彙總.csv"
    )

    csv_write(
        cross_summary,
        out_dir / "12_凍結12檔個股跨週期表現總表.csv"
    )

    first_schedule.to_csv(
        out_dir / "13_第一批OOS_H1H3H5H10排程.csv",
        index=False,
        encoding="utf-8-sig",
        na_rep=""
    )

    first_matured.to_csv(
        out_dir / "14_第一批OOS已成熟淨報酬.csv",
        index=False,
        encoding="utf-8-sig",
        na_rep=""
    )

    daily_schedule.to_csv(
        out_dir / "15_每日重新進場_H1H3H5H10排程.csv.gz",
        index=False,
        encoding="utf-8-sig",
        compression="gzip",
        na_rep=""
    )

    daily_matured.to_csv(
        out_dir / "16_每日重新進場已成熟淨報酬.csv.gz",
        index=False,
        encoding="utf-8-sig",
        compression="gzip",
        na_rep=""
    )

    csv_write(
        qa,
        out_dir / "17_v126原始126項相容性核對.csv"
    )

    csv_write(
        pd.DataFrame(source_records),
        out_dir / "18_行情來源及SHA256清單.csv"
    )

    json_write(
        report,
        out_dir / "19_v131前瞻樣本外研究結論.json"
    )

    # --------------------------------------------------------
    # O. 執行摘要
    # --------------------------------------------------------

    summary_lines = [
        "=" * 100,
        VERSION,
        "=" * 100,
        f"執行時間：{now.isoformat()}",
        f"OOS 起始日：{OOS_START_DATE}",
        f"行情資料最後日期：{latest_official_date.date()}",
        "",
        "一、凍結候選",
        f"候選總數：{len(candidate_frame)}",
        "、".join(
            f"{code} {name}"
            for code, name in EXPECTED_CANDIDATES.items()
        ),
        "",
        "二、第一批前瞻訊號",
        f"第一批訊號日期：{OOS_START_DATE}",
        f"有效候選：{first_candidate_count}/12",
        f"第一批排程：{len(first_schedule)}",
        f"第一批已成熟：{len(first_matured)}",
        "",
        "三、每日重新進場追蹤",
        f"訊號列數：{len(signals)}",
        f"完整排程：{len(daily_schedule)}",
        f"已成熟淨報酬：{len(daily_matured)}",
        "",
        "四、各週期成熟概況",
    ]

    if horizon_summary.empty:
        summary_lines.append("目前尚無成熟淨報酬。")
    else:
        for row in horizon_summary.to_dict("records"):
            summary_lines.append(
                f"{row['訊號口徑']} | "
                f"H={row['H']} | "
                f"訊號={row['訊號總筆數']} | "
                f"成熟={row['已成熟且完成報酬筆數']} | "
                f"基準平均={row['基準成本平均淨報酬']} | "
                f"基準勝率={row['基準成本勝率']} | "
                f"壓力平均={row['壓力成本平均淨報酬']} | "
                f"壓力勝率={row['壓力成本勝率']}"
            )

    summary_lines += [
        "",
        "五、交易規則",
        "訊號日後第 1 個全市場交易日開盤進場。",
        "訊號日後第 H 個全市場交易日收盤出場。",
        "沿用 v1.2.9 / v1.2.2 成本公式。",
        "",
        "六、研究界線",
        "本版只追蹤 v1.2.9 已凍結的 12 檔候選。",
        "每日重新進場是候選池追蹤，不代表每日重新排名 Q5。",
        "未取得各 OOS 日期官方 CFI 分類，不以 2026-11-10 名單回套當日。",
        "跨日期及跨週期報酬可能重疊，不視為獨立樣本。",
        "未成熟的出場觀察不計入已成熟報酬。",
        "",
        "七、資料安全",
        "Drive 原始資料唯讀。",
        "未刪除、搬移或覆寫舊資料。",
        "未重新訓練。",
        "GitHub 僅新增獨立 audit branch。",
        "GitHub main SHA 前後一致。",
        "=" * 100
    ]

    (out_dir / "20_執行摘要.txt").write_text(
        "\n".join(summary_lines) + "\n",
        encoding="utf-8-sig"
    )

    # --------------------------------------------------------
    # P. README
    # --------------------------------------------------------

    readme = f"""# {VERSION}

## 研究目的

驗證 v1.2.9 已凍結的 12 檔候選，在 2026-11-12 起的真正前瞻期間表現。

## 凍結候選

{chr(10).join(f"- {code} {name}" for code, name in EXPECTED_CANDIDATES.items())}

## 訊號起點

2026-11-12。

## 配對規則

- 訊號日後第 1 個全市場交易日開盤進場。
- 訊號日後第 H 個全市場交易日收盤出場。
- H = 1 / 3 / 5 / 10。
- 成本公式沿用 v1.2.2 / v1.2.9。

## 兩種觀察

1. FIRST_OOS：2026-11-12 第一批凍結候選。
2. DAILY_REENTRY：從 2026-11-12 起，每個有效交易日重新追蹤候選。

DAILY_REENTRY 是固定候選池每日重新進場的觀察，不代表每日全市場重新排名 Q5。

## 普通股分類界線

凍結候選資格依 2026-11-10 官方 ISIN CFI 普通股分類核驗。
目前未將該日期分類延伸宣稱為 2026-11-12 以後的每日分類。

## 前瞻資料

行情基礎由 v1.2.7、v1.5.1、v1.5.9 及 v1.5.10 接續。
2026-11-11 TWSE / TPEx 修正層採完整日期替代，不可相加。

## 研究限制

- 未成熟的持有週期不計算淨報酬。
- 跨日期及跨週期報酬可能重疊，不是獨立樣本。
- 本版不重新訓練、不調整名單、不產生投資保證。
- 沒有當日官方 CFI 分類時，不產生每日 Q1-Q5 全市場排名。
"""

    (out_dir / "README.md").write_text(
        readme,
        encoding="utf-8"
    )

    # --------------------------------------------------------
    # Q. 封存程式原文、執行紀錄
    # --------------------------------------------------------

    source_path = archive_current_program(
        out_dir
    )

    (out_dir / "run.log").write_text(
        "\n".join(logs) + "\n",
        encoding="utf-8"
    )

    # --------------------------------------------------------
    # R. Manifest / ZIP
    # --------------------------------------------------------

    manifest = build_manifest(
        out_dir
    )

    manifest_path = (
        out_dir / "artifact_sha256_manifest.json"
    )

    manifest_sha = sha256_file(
        manifest_path
    )

    zip_path = (
        ROOT / f"{out_name}.zip"
    )

    if zip_path.exists():

        raise FileExistsError(
            f"ZIP 已存在，停止覆寫：{zip_path}"
        )

    zip_result = make_and_verify_zip(
        out_dir,
        zip_path,
        manifest
    )

    log(
        f"[PASS] ZIP CRC / manifest / SHA256："
        f"{zip_result['zip_sha256']}",
        logs
    )

    # --------------------------------------------------------
    # S. GitHub 獨立 audit branch
    # --------------------------------------------------------

    github_result = None

    try:

        github_result = upload_github(
            out_dir,
            zip_path,
            run_id,
            logs
        )

    except Exception as error:

        github_result = {
            "status": "FAILED",
            "error": (
                f"{type(error).__name__}: {error}"
            )
        }

        log(
            f"[GITHUB ERROR] {github_result['error']}",
            logs
        )

    json_write(
        github_result,
        out_dir / "GitHub上傳結果.json"
    )

    # --------------------------------------------------------
    # T. 最後確認前瞻來源沒有改變
    # --------------------------------------------------------

    if sha256_file(V159_ZIP) != EXPECTED_V159_ZIP_SHA256:

        raise RuntimeError(
            "v1.5.9 原始封存 ZIP SHA 已改變。"
        )

    # --------------------------------------------------------
    # U. 最終輸出
    # --------------------------------------------------------

    print("\n" + "=" * 100)
    print(f"{VERSION}｜RUNNING_FORWARD_OOS")
    print("=" * 100)

    print(
        f"OOS 起始日：{OOS_START_DATE}"
    )

    print(
        f"行情最後日期：{latest_official_date.date()}"
    )

    print(
        f"凍結候選：{len(candidate_frame)}"
    )

    print(
        f"第一批有效候選：{first_candidate_count}/12"
    )

    print(
        f"第一批排程：{len(first_schedule)}"
    )

    print(
        f"第一批已成熟：{len(first_matured)}"
    )

    print(
        f"每日重新進場訊號：{len(signals)}"
    )

    print(
        f"每日重新進場排程：{len(daily_schedule)}"
    )

    print(
        f"已成熟淨報酬：{len(matured)}"
    )

    print(
        f"v1.2.6 原始 126 項核對失敗：{qa_failures}"
    )

    print(
        f"Manifest SHA256：{manifest_sha}"
    )

    print(
        f"ZIP：{zip_path}"
    )

    print(
        f"ZIP SHA256：{zip_result['zip_sha256']}"
    )

    print(
        f"GitHub：{github_result}"
    )

    print(
        f"[OUTPUT DIRECTORY] {out_dir}"
    )

    print("=" * 100)

    if (
        github_result
        and github_result.get("status") == "PASS"
        and github_result.get("main_unchanged") is True
        and github_result.get("blob_readback_pass") is True
    ):

        print("[PASS] GitHub audit branch 逐檔回讀。")
        print("[PASS] GitHub main 未變更。")
        print(f"[GITHUB URL] {github_result['url']}")

    else:

        print(
            "[WARNING] Drive 研究成果已保存；"
            "GitHub 尚未完整通過。"
        )

    print(
        "[INFO] 本次只建立前瞻排程；"
        "未成熟的 H1/H3/H5/H10 不代表績效失敗。"
    )

    print("=" * 100)

    return {
        "output_dir": str(out_dir),
        "zip_path": str(zip_path),
        "zip_sha256": zip_result["zip_sha256"],
        "manifest_sha256": manifest_sha,
        "report": report,
        "schedule": schedule,
        "matured": matured,
        "horizon_summary": horizon_summary,
        "daily_summary": daily_return_summary,
        "candidate_coverage": candidate_coverage,
        "github": github_result
    }


# ============================================================
# 17. 執行入口
# ============================================================

RESULT_V131 = run_main_v131()