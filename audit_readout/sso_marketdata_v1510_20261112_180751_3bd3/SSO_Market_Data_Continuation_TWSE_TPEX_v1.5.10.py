# -*- coding: utf-8 -*-
"""
SSO_Market_Data_Continuation_TWSE_TPEX_v1.5.10

2026-11-12 起，雙市場官方行情增量接續與下游資料驗證。

一、基準修正資料
    v1.5.9 已封存的 2026-11-11：
    TWSE 1,390 筆，price_change 已補正。
    TPEx 1,020 筆，type=EW，price_change 缺欄維持 None。

二、官方新行情
    TWSE：MI_INDEX / ALLBUT0999
    TPEx：afterTrading/otc / EW

三、接續邏輯
    讀取既有 v1.5.x 標準化增量。
    讀取歷史待重查日期。
    2026-11-11 以 v1.5.9 雙市場修正層替代。
    從最後成功日期之後查詢新行情。
    已成功日期不重複下載。

四、資料驗證
    原始 API 回應 GZIP 保存。
    行情欄位、日期、市場、代號、筆數、OHLC、
    成交量、零成交量比例、缺值及重複鍵值檢查。
    CSV 寫入後重新讀取。
    逐股 overlay 回讀。
    既有資料接續及替代層檢查。
    SSO 下游讀取預檢。

五、保護
    不修改舊版 Drive 檔案。
    不刪除任何歷史資料。
    不重訓模型、不產生選股分數。
    GitHub 僅上傳獨立 audit branch，不修改 main。
"""

from __future__ import annotations

import os
import re
import csv
import io
import json
import gzip
import time
import uuid
import math
import base64
import hashlib
import zipfile
import statistics
import shutil

from pathlib import Path
from datetime import (
    datetime,
    date,
    timedelta,
    time as dt_time
)
from zoneinfo import ZoneInfo
from collections import defaultdict
from urllib.parse import quote

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# ============================================================
# 1. 基本設定
# ============================================================

VERSION = "SSO_Market_Data_Continuation_TWSE_TPEX_v1.5.10"

ROOT = Path(
    "/content/drive/MyDrive/看股最速適/"
    "SSO_Market_Data_Continuation_TWSE_TPEX_v1"
)

REPO = "CHLINX/sso"
TZ = ZoneInfo("Asia/Taipei")

MARKET_CLOSE_CUTOFF = dt_time(17, 0)

CORRECTION_DATE = date(2026, 11, 11)

V159_DIR = (
    ROOT
    / "v1.5.9_TWSE_price_change欄位補正_"
      "20261112_152447_f573"
)

V159_SEALED_ZIP = (
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

EXPECTED_TWSE_CORRECTION_CANONICAL_SHA256 = (
    "b366ce6fc4910caba953e3bff3d3cb2ed479b12ee33cf462e927f4931b22533f"
)

EXPECTED_TPEX_CORRECTION_FILE_SHA256 = (
    "002f3e4e7af282945333db92c1cebc2a76bad5fb24a7058fda12e2690ed80742"
)

EXPECTED_TPEX_CORRECTION_CANONICAL_SHA256 = (
    "eb4bd7a5e16831f74f6cc700c8076163d0ba4c5b782023f3858951035a31e627"
)

EXPECTED_TWSE_CORRECTION_ROWS = 1390
EXPECTED_TPEX_CORRECTION_ROWS = 1020

MIN_ROWS_ABSOLUTE = 500
MAX_ROWS_ABSOLUTE = 3000

MIN_COUNT_RATIO = 0.55
MAX_COUNT_RATIO = 1.60

MAX_ZERO_VOLUME_RATIO = 0.25
MAX_MISSING_OHLC_RATIO = 0.25
MAX_INVALID_PRICE_RATIO = 0.01

MIN_CODE_LENGTH = 2
MAX_CODE_LENGTH = 16


# ============================================================
# 2. 欄位定義
# ============================================================

ALIASES = {
    "trade_date": [
        "trade_date", "date", "日期", "交易日期",
        "資料日期", "年月日"
    ],

    "market": [
        "market", "市場", "市場別", "交易市場"
    ],

    "instrument_code": [
        "instrument_code", "code", "stock_id",
        "stock_code", "證券代號", "證券代碼",
        "代號", "股票代號"
    ],

    "instrument_name": [
        "instrument_name", "name", "stock_name",
        "證券名稱", "名稱", "股票名稱"
    ],

    "open": [
        "open", "opening_price", "open_price",
        "開盤", "開盤價", "開盤價格"
    ],

    "high": [
        "high", "highest_price", "high_price",
        "最高", "最高價", "最高價格"
    ],

    "low": [
        "low", "lowest_price", "low_price",
        "最低", "最低價", "最低價格"
    ],

    "close": [
        "close", "closing_price", "close_price",
        "收盤", "收盤價", "收盤價格"
    ],

    "volume": [
        "volume", "trade_volume", "trading_volume",
        "成交股數", "成交量"
    ],

    "trade_value": [
        "trade_value", "traded_value", "turnover",
        "amount", "成交金額", "成交金額元",
        "成交金額(元)"
    ],

    "price_change": [
        "price_change", "change", "漲跌", "漲跌價差"
    ],

    "transactions": [
        "transactions", "transaction_count",
        "transaction", "成交筆數", "筆數",
        "交易筆數"
    ],

    "source": [
        "source", "source_archive_member",
        "來源", "資料來源"
    ]
}

NUMERIC_FIELDS = {
    "open", "high", "low", "close",
    "volume", "trade_value",
    "price_change", "transactions"
}

INTEGER_FIELDS = {
    "volume", "trade_value", "transactions"
}

COMPARE_FIELDS = [
    "trade_date",
    "market",
    "instrument_code",
    "instrument_name",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "trade_value",
    "price_change",
    "transactions",
    "source"
]

CORE_REQUIRED_FIELDS = [
    "trade_date",
    "market",
    "instrument_code",
    "open",
    "high",
    "low",
    "close",
    "volume"
]


# ============================================================
# 3. 共用基礎函式
# ============================================================

def log(message, logs=None):

    line = (
        f"[{datetime.now(TZ).strftime('%H:%M:%S')}] "
        f"{message}"
    )

    print(line, flush=True)

    if logs is not None:
        logs.append(line)


def mount_drive():

    if not Path("/content/drive/MyDrive").exists():

        from google.colab import drive

        drive.mount(
            "/content/drive",
            force_remount=False
        )

    if not ROOT.exists():
        raise FileNotFoundError(
            f"資料根目錄不存在：{ROOT}"
        )


def sha256_bytes(data):

    return hashlib.sha256(data).hexdigest()


def sha256_file(path):

    return sha256_bytes(
        Path(path).read_bytes()
    )


def norm(value):

    return re.sub(
        r"[\s\u3000_\-（）()\[\]／/：:、,.%]+",
        "",
        str(value).strip().lower()
    )


def find_col(columns, aliases):

    lookup = {
        norm(column): column
        for column in columns
    }

    for alias in aliases:

        key = norm(alias)

        if key in lookup:
            return lookup[key]

    return None


def market_norm(value):

    value = str(value or "").strip().upper()

    if value in {
        "TWSE", "上市", "集中市場"
    }:
        return "TWSE"

    if value in {
        "TPEX", "TPEX", "上櫃", "櫃買"
    }:
        return "TPEX"

    return value


def parse_date(value):

    if value is None:
        return None

    s = str(value).strip()

    if not s:
        return None

    s = s.split("T")[0].split(" ")[0]

    m = re.fullmatch(
        r"(\d{2,4})[/-](\d{1,2})[/-](\d{1,2})",
        s
    )

    if m:

        year, month, day = map(
            int,
            m.groups()
        )

        if year < 1911:
            year += 1911

        try:
            return date(year, month, day)
        except ValueError:
            return None

    m = re.fullmatch(
        r"(\d{4})(\d{2})(\d{2})",
        s
    )

    if m:

        try:
            return date(
                *map(int, m.groups())
            )
        except ValueError:
            return None

    return None


def safe_num(value, integer=False):

    if value is None:
        return None

    s = (
        str(value)
        .strip()
        .replace(",", "")
        .replace("＋", "+")
        .replace("－", "-")
    )

    if s in {
        "", "-", "--", "---",
        "X", "x", "N/A", "nan", "None"
    }:
        return None

    try:

        number = float(s)

        if not math.isfinite(number):
            return None

        if integer:
            return int(round(number))

        return number

    except Exception:
        return None


def normalize_value(field, value):

    if field == "trade_date":

        d = parse_date(value)

        return d.isoformat() if d else ""

    if field == "market":
        return market_norm(value)

    if field == "instrument_code":
        return str(value or "").strip()

    if field in NUMERIC_FIELDS:

        return safe_num(
            value,
            integer=field in INTEGER_FIELDS
        )

    return str(value or "").strip()


def canonical_record(record):

    return {
        field: normalize_value(
            field,
            record.get(field)
        )
        for field in COMPARE_FIELDS
    }


def canonical_records_sha256(records):

    ordered = sorted(
        [
            canonical_record(record)
            for record in records
        ],
        key=lambda x: (
            x["trade_date"],
            x["market"],
            x["instrument_code"]
        )
    )

    raw = json.dumps(
        ordered,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str
    ).encode("utf-8")

    return sha256_bytes(raw)


def record_key(record):

    return (
        market_norm(record.get("market")),
        str(record.get("trade_date", "")),
        str(record.get("instrument_code", "")).strip()
    )


def record_signature(record):

    """
    比較實際行情數值。
    source 僅為來源描述，不以來源文字差異視為行情數值衝突。
    """

    fields = [
        "trade_date",
        "market",
        "instrument_code",
        "instrument_name",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "trade_value",
        "price_change",
        "transactions"
    ]

    return tuple(
        normalize_value(
            field,
            record.get(field)
        )
        for field in fields
    )


def write_json(path, obj):

    path = Path(path)

    path.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    path.write_text(
        json.dumps(
            obj,
            ensure_ascii=False,
            indent=2,
            default=str
        ),
        encoding="utf-8"
    )


# ============================================================
# 4. CSV 讀寫及正規化
# ============================================================

def read_csv_rows(path):

    raw = Path(path).read_bytes()

    decoded = None

    for encoding in (
        "utf-8-sig",
        "cp950",
        "big5",
        "gb18030"
    ):

        try:
            decoded = raw.decode(encoding)
            break

        except Exception:
            continue

    if decoded is None:
        raise RuntimeError(
            f"無法解碼 CSV：{path}"
        )

    reader = csv.DictReader(
        io.StringIO(decoded)
    )

    columns = list(
        reader.fieldnames or []
    )

    rows = list(reader)

    if not columns:
        raise RuntimeError(
            f"CSV 缺少表頭：{path}"
        )

    return columns, rows


def read_header(path):

    columns, _ = read_csv_rows(path)

    return columns


def make_mapping(columns):

    return {
        field: find_col(
            columns,
            aliases
        )
        for field, aliases in ALIASES.items()
    }


def normalize_csv_rows(
    path,
    market=None,
    required_fields=None
):

    columns, rows = read_csv_rows(path)

    mapping = make_mapping(columns)

    required_fields = (
        required_fields
        if required_fields is not None
        else CORE_REQUIRED_FIELDS
    )

    missing = [
        field
        for field in required_fields
        if mapping.get(field) is None
    ]

    if missing:
        raise RuntimeError(
            f"{Path(path).name} 缺少必要欄位：{missing}"
        )

    records = []

    for row in rows:

        record = {}

        for field in COMPARE_FIELDS:

            column = mapping.get(field)

            value = (
                row.get(column)
                if column is not None
                else None
            )

            record[field] = normalize_value(
                field,
                value
            )

        records.append(record)

    codes = [
        r["instrument_code"]
        for r in records
    ]

    if any(not code for code in codes):

        raise RuntimeError(
            f"{Path(path).name} 存在空白證券代號。"
        )

    if len(codes) != len(set(codes)):

        raise RuntimeError(
            f"{Path(path).name} 存在重複證券代號。"
        )

    if market is not None:

        wrong_market = [
            r["instrument_code"]
            for r in records
            if r["market"] != market
        ]

        if wrong_market:

            raise RuntimeError(
                f"{Path(path).name} 市場別錯誤："
                f"{wrong_market[:10]}"
            )

    return columns, records


def ensure_columns(columns):

    columns = list(columns)

    for field in COMPARE_FIELDS:

        if find_col(
            columns,
            ALIASES[field]
        ) is None:

            columns.append(field)

    return columns


def record_to_target(record, columns):

    result = {}

    for column in columns:

        matched = None

        for field, aliases in ALIASES.items():

            if norm(column) in {
                norm(x) for x in aliases
            }:

                matched = field
                break

        value = (
            record.get(matched)
            if matched is not None
            else ""
        )

        result[column] = (
            ""
            if value is None
            else value
        )

    return result


def write_csv(path, records, columns):

    path = Path(path)

    path.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    columns = ensure_columns(columns)

    with path.open(
        "w",
        encoding="utf-8-sig",
        newline=""
    ) as f:

        writer = csv.DictWriter(
            f,
            fieldnames=columns,
            extrasaction="ignore"
        )

        writer.writeheader()

        for record in records:

            writer.writerow(
                record_to_target(
                    record,
                    columns
                )
            )


def verify_csv_roundtrip(
    path,
    expected_records,
    market=None,
    day=None
):

    _, actual_records = normalize_csv_rows(
        path,
        market=market,
        required_fields=CORE_REQUIRED_FIELDS
    )

    if len(actual_records) != len(expected_records):

        raise RuntimeError(
            f"{Path(path).name} 寫入後筆數不符："
            f"{len(actual_records)} != {len(expected_records)}"
        )

    expected_sha = canonical_records_sha256(
        expected_records
    )

    actual_sha = canonical_records_sha256(
        actual_records
    )

    if expected_sha != actual_sha:

        raise RuntimeError(
            f"{Path(path).name} 寫入後 canonical SHA 不一致。"
        )

    if day is not None:

        if any(
            r["trade_date"] != day.isoformat()
            for r in actual_records
        ):

            raise RuntimeError(
                f"{Path(path).name} 交易日期不一致。"
            )

    return {
        "rows": len(actual_records),
        "canonical_sha256": actual_sha,
        "file_sha256": sha256_file(path)
    }


# ============================================================
# 5. 版本資料夾及既有欄位
# ============================================================

def version_dirs():

    items = []

    for path in ROOT.iterdir():

        if not path.is_dir():
            continue

        match = re.match(
            r"v1\.5\.(\d+)_",
            path.name
        )

        if match:

            items.append((
                int(match.group(1)),
                path
            ))

    items.sort(
        key=lambda x: (
            x[0],
            x[1].name
        )
    )

    return [
        item[1]
        for item in items
    ]


def load_baseline_and_schema(dirs):

    baseline_values = {
        "TWSE": [],
        "TPEX": []
    }

    standard_columns = None

    overlay_columns = {
        "TWSE": None,
        "TPEX": None
    }

    # 歷史筆數基準沿用 v1.5.1。
    for directory in dirs:

        if not directory.name.startswith("v1.5.1_"):
            continue

        stat_files = list(
            directory.glob(
                "**/新增九個交易日_逐日雙市場統計.csv"
            )
        )

        for path in stat_files:

            try:

                columns, rows = read_csv_rows(path)

                market_col = find_col(
                    columns,
                    ALIASES["market"]
                )

                rows_col = find_col(
                    columns,
                    [
                        "rows", "row_count",
                        "筆數", "資料筆數"
                    ]
                )

                if not market_col or not rows_col:
                    continue

                for row in rows:

                    market = market_norm(
                        row.get(market_col)
                    )

                    number = safe_num(
                        row.get(rows_col),
                        integer=True
                    )

                    if (
                        market in baseline_values
                        and number is not None
                        and number >= MIN_ROWS_ABSOLUTE
                    ):

                        baseline_values[market].append(
                            number
                        )

            except Exception as error:

                print(
                    f"[BASELINE WARN] {path.name}: {error}"
                )

    # 找出既有標準化增量欄位。
    for directory in reversed(dirs):

        std_files = sorted(
            directory.glob(
                "**/全市場標準化增量_*.csv"
            ),
            key=lambda p: str(p)
        )

        if standard_columns is None and std_files:

            standard_columns = read_header(
                std_files[-1]
            )

        overlay_dir = (
            directory
            / "overlay"
            / "官方歷史行情"
        )

        if overlay_dir.exists():

            for market, pattern in (
                ("TWSE", "*_TWSE.csv"),
                ("TPEX", "*_TPEx.csv")
            ):

                if overlay_columns[market] is None:

                    files = sorted(
                        overlay_dir.glob(pattern),
                        key=lambda p: str(p)
                    )

                    if files:

                        overlay_columns[market] = (
                            read_header(files[0])
                        )

        if (
            standard_columns
            and all(overlay_columns.values())
        ):
            break

    if standard_columns is None:

        raise RuntimeError(
            "找不到既有全市場標準化增量欄位格式。"
        )

    # 保留舊欄位，補足後續下游必要欄位。
    standard_columns = ensure_columns(
        standard_columns
    )

    for market in ("TWSE", "TPEX"):

        if overlay_columns[market] is None:

            overlay_columns[market] = list(
                standard_columns
            )

        overlay_columns[market] = ensure_columns(
            overlay_columns[market]
        )

    baseline = {}

    baseline_source = {}

    for market in ("TWSE", "TPEX"):

        values = baseline_values[market]

        if values:

            baseline[market] = int(
                statistics.median(values)
            )

            baseline_source[market] = (
                "v1.5.1 歷史逐日統計中位數"
            )

        else:

            baseline[market] = (
                1400
                if market == "TWSE"
                else 1020
            )

            baseline_source[market] = (
                "歷史統計未取得，使用既有保守基準"
            )

    return (
        baseline,
        baseline_source,
        standard_columns,
        overlay_columns
    )


# ============================================================
# 6. v1.5.9 雙市場修正資料驗證
# ============================================================

def load_v159_corrections(logs):

    log(
        "[CORRECTION] 核對 v1.5.9 已封存雙市場修正成果。",
        logs
    )

    if not V159_DIR.is_dir():

        raise FileNotFoundError(
            f"找不到 v1.5.9 修正資料夾：{V159_DIR}"
        )

    if not V159_SEALED_ZIP.is_file():

        raise FileNotFoundError(
            f"找不到 v1.5.9 正式封存 ZIP：{V159_SEALED_ZIP}"
        )

    zip_sha = sha256_file(
        V159_SEALED_ZIP
    )

    if zip_sha != EXPECTED_V159_ZIP_SHA256:

        raise RuntimeError(
            "v1.5.9 正式 ZIP SHA256 不符。"
        )

    manifest_path = (
        V159_DIR
        / "artifact_sha256_manifest.json"
    )

    if sha256_file(manifest_path) != EXPECTED_V159_MANIFEST_SHA256:

        raise RuntimeError(
            "v1.5.9 artifact manifest SHA256 不符。"
        )

    report_path = (
        V159_DIR
        / "v1.5.9_雙市場行情補正總報告.json"
    )

    source_report = json.loads(
        report_path.read_text(encoding="utf-8")
    )

    if source_report.get("status") != "PASS_TWSE_PRICE_CHANGE_REPAIRED":

        raise RuntimeError(
            "v1.5.9 雙市場總報告不是 PASS。"
        )

    twse_info = source_report["twse"]
    tpex_info = source_report["tpex"]

    if twse_info.get("status") != "PASS":
        raise RuntimeError("v1.5.9 TWSE 修正未通過。")

    if tpex_info.get("status") != "PASS":
        raise RuntimeError("v1.5.9 TPEx 修正未通過。")

    if not source_report.get(
        "validation", {}
    ).get("twse_official_roundtrip_pass", True):

        raise RuntimeError(
            "v1.5.9 TWSE 官方 round-trip 未通過。"
        )

    if not source_report.get(
        "validation", {}
    ).get("tpex_root_nested_canonical_pass", True):

        raise RuntimeError(
            "v1.5.9 TPEx root/nested 驗證未通過。"
        )

    # --------------------------------------------------------
    # TWSE correction
    # --------------------------------------------------------

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

    twse_raw = (
        V159_DIR
        / "raw"
        / "TWSE"
        / "20261111.json.gz"
    )

    for p in (
        twse_root,
        twse_nested,
        twse_raw
    ):

        if not p.is_file():
            raise FileNotFoundError(
                f"v1.5.9 TWSE 證據不存在：{p}"
            )

    for p in (twse_root, twse_nested):

        if sha256_file(p) != EXPECTED_TWSE_CORRECTION_FILE_SHA256:

            raise RuntimeError(
                f"v1.5.9 TWSE CSV SHA 不符：{p.name}"
            )

    if gzip.decompress(twse_raw.read_bytes()) is None:
        raise RuntimeError("v1.5.9 TWSE raw GZIP 解壓失敗。")

    _, twse_root_records = normalize_csv_rows(
        twse_root,
        market="TWSE",
        required_fields=COMPARE_FIELDS
    )

    _, twse_nested_records = normalize_csv_rows(
        twse_nested,
        market="TWSE",
        required_fields=COMPARE_FIELDS
    )

    twse_root_sha = canonical_records_sha256(
        twse_root_records
    )

    twse_nested_sha = canonical_records_sha256(
        twse_nested_records
    )

    if (
        len(twse_root_records)
        != EXPECTED_TWSE_CORRECTION_ROWS
    ):
        raise RuntimeError("v1.5.9 TWSE 修正筆數不符。")

    if (
        twse_root_sha
        != EXPECTED_TWSE_CORRECTION_CANONICAL_SHA256
    ):
        raise RuntimeError("v1.5.9 TWSE root canonical SHA 不符。")

    if (
        twse_nested_sha
        != EXPECTED_TWSE_CORRECTION_CANONICAL_SHA256
    ):
        raise RuntimeError("v1.5.9 TWSE nested canonical SHA 不符。")

    if twse_root_sha != twse_nested_sha:
        raise RuntimeError("v1.5.9 TWSE root/nested 不一致。")

    # 確認 price_change 逐筆明細。
    twse_detail = (
        V159_DIR
        / "TWSE_20261111_price_change逐筆補正核驗.csv"
    )

    _, twse_detail_rows = read_csv_rows(
        twse_detail
    )

    if len(twse_detail_rows) != EXPECTED_TWSE_CORRECTION_ROWS:
        raise RuntimeError("v1.5.9 TWSE price_change 明細筆數不符。")

    if any(
        str(row.get("status", "")).strip() != "PASS"
        for row in twse_detail_rows
    ):
        raise RuntimeError("v1.5.9 TWSE price_change 逐筆明細有失敗列。")

    log(
        "[PASS] v1.5.9 TWSE："
        f"{len(twse_root_records):,} 筆；"
        "price_change 逐筆補正 PASS。"
    )

    # --------------------------------------------------------
    # TPEx correction
    # --------------------------------------------------------

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

    for p in (tpex_root, tpex_nested):

        if not p.is_file():
            raise FileNotFoundError(
                f"v1.5.9 TPEx 證據不存在：{p}"
            )

        if sha256_file(p) != EXPECTED_TPEX_CORRECTION_FILE_SHA256:

            raise RuntimeError(
                f"v1.5.9 TPEx CSV SHA 不符：{p.name}"
            )

    # TPEx 原始 CSV 的 price_change 欄位缺漏允許存在。
    # 正規化後必須保持 None，不自行製造資料。
    tpex_required = [
        field
        for field in COMPARE_FIELDS
        if field != "price_change"
    ]

    _, tpex_root_records = normalize_csv_rows(
        tpex_root,
        market="TPEX",
        required_fields=tpex_required
    )

    _, tpex_nested_records = normalize_csv_rows(
        tpex_nested,
        market="TPEX",
        required_fields=tpex_required
    )

    tpex_root_sha = canonical_records_sha256(
        tpex_root_records
    )

    tpex_nested_sha = canonical_records_sha256(
        tpex_nested_records
    )

    if len(tpex_root_records) != EXPECTED_TPEX_CORRECTION_ROWS:
        raise RuntimeError("v1.5.9 TPEx 修正筆數不符。")

    if (
        tpex_root_sha
        != EXPECTED_TPEX_CORRECTION_CANONICAL_SHA256
    ):
        raise RuntimeError("v1.5.9 TPEx root canonical SHA 不符。")

    if (
        tpex_nested_sha
        != EXPECTED_TPEX_CORRECTION_CANONICAL_SHA256
    ):
        raise RuntimeError("v1.5.9 TPEx nested canonical SHA 不符。")

    if tpex_root_sha != tpex_nested_sha:
        raise RuntimeError("v1.5.9 TPEx root/nested 不一致。")

    tpex_price_change_missing = (
        find_col(
            read_header(tpex_root),
            ALIASES["price_change"]
        ) is None
    )

    if tpex_price_change_missing:

        if any(
            r["price_change"] is not None
            for r in tpex_root_records
        ):
            raise RuntimeError(
                "TPEx 缺少 price_change 欄，"
                "但正規化後出現非 None 值。"
            )

    if any(
        "type_EW" not in str(r.get("source", ""))
        for r in tpex_root_records
    ):
        raise RuntimeError(
            "TPEx 修正來源不是完整 type=EW。"
        )

    log(
        "[PASS] v1.5.9 TPEx："
        f"{len(tpex_root_records):,} 筆；"
        "type=EW；canonical SHA 通過。"
    )

    # --------------------------------------------------------
    # Return
    # --------------------------------------------------------

    return {
        "source_directory": str(V159_DIR),
        "source_zip_sha256": zip_sha,
        "source_manifest_sha256": EXPECTED_V159_MANIFEST_SHA256,

        "twse": {
            "root": twse_root,
            "nested": twse_nested,
            "raw": twse_raw,
            "records": twse_root_records,
            "rows": len(twse_root_records),
            "file_sha256": sha256_file(twse_root),
            "canonical_sha256": twse_root_sha
        },

        "tpex": {
            "root": tpex_root,
            "nested": tpex_nested,
            "records": tpex_root_records,
            "rows": len(tpex_root_records),
            "file_sha256": sha256_file(tpex_root),
            "canonical_sha256": tpex_root_sha,
            "price_change_missing": tpex_price_change_missing
        }
    }


# ============================================================
# 7. 既有資料日期、已完成日期及歷史鍵值
# ============================================================

def read_previous_state(dirs, logs):

    latest = {
        "TWSE": None,
        "TPEX": None
    }

    completed = {
        "TWSE": set(),
        "TPEX": set()
    }

    previous_map = {}

    previous_rows = 0
    previous_bad_rows = 0

    duplicate_identical = 0
    conflict_map = defaultdict(list)

    correction_rows_seen = 0

    for directory in dirs:

        files = sorted(
            directory.glob(
                "**/全市場標準化增量_*.csv"
            ),
            key=lambda p: str(p)
        )

        for path in files:

            try:

                _, records = normalize_csv_rows(
                    path,
                    market=None,
                    required_fields=CORE_REQUIRED_FIELDS
                )

                for record in records:

                    market = market_norm(
                        record.get("market")
                    )

                    day = parse_date(
                        record.get("trade_date")
                    )

                    code = str(
                        record.get("instrument_code", "")
                    ).strip()

                    if (
                        market not in {"TWSE", "TPEX"}
                        or day is None
                        or not code
                    ):

                        previous_bad_rows += 1
                        continue

                    previous_rows += 1

                    completed[market].add(day)

                    if (
                        latest[market] is None
                        or day > latest[market]
                    ):

                        latest[market] = day

                    key = (
                        market,
                        day.isoformat(),
                        code
                    )

                    # 2026-11-11 已有正式修正層。
                    # 先排除舊日期資料，稍後由 v1.5.9 完整替代。
                    if day == CORRECTION_DATE:

                        correction_rows_seen += 1
                        continue

                    if key not in previous_map:

                        previous_map[key] = record

                    else:

                        old_signature = record_signature(
                            previous_map[key]
                        )

                        new_signature = record_signature(
                            record
                        )

                        if old_signature == new_signature:

                            duplicate_identical += 1

                        else:

                            conflict_map[key].append({
                                "existing": previous_map[key],
                                "other": record,
                                "source_file": str(path)
                            })

            except Exception as error:

                previous_bad_rows += 1

                log(
                    f"[PREVIOUS DATA WARN] "
                    f"{path.name}：{error}",
                    logs
                )

    scan = {
        "previous_rows_scanned": previous_rows,
        "previous_bad_rows": previous_bad_rows,
        "unique_previous_keys_excluding_correction": len(previous_map),
        "identical_duplicate_copies": duplicate_identical,
        "conflict_key_count": len(conflict_map),
        "old_20261111_rows_replaced": correction_rows_seen
    }

    log(
        "[PREVIOUS DATA SCAN] "
        f"rows={previous_rows:,}；"
        f"bad={previous_bad_rows:,}；"
        f"identical duplicates={duplicate_identical:,}；"
        f"conflicts={len(conflict_map):,}",
        logs
    )

    return (
        latest,
        completed,
        previous_map,
        conflict_map,
        scan
    )


def read_previous_pending(dirs, logs):

    pending = {}

    for directory in dirs:

        reports = list(
            directory.glob(
                "**/v1.5.*_官方增量行情續接報告.json"
            )
        )

        for path in reports:

            try:

                obj = json.loads(
                    path.read_text(encoding="utf-8")
                )

                items = []

                for key in (
                    "unresolved_dates",
                    "unresolved_weekday_no_data",
                    "deferred_not_yet_due_dates"
                ):

                    values = obj.get(key) or []

                    if isinstance(values, list):
                        items.extend(values)

                for item in items:

                    market = market_norm(
                        item.get("market")
                    )

                    day = parse_date(
                        item.get("trade_date")
                    )

                    if (
                        market not in {"TWSE", "TPEX"}
                        or day is None
                    ):
                        continue

                    key = (
                        market,
                        day
                    )

                    pending[key] = {
                        "market": market,
                        "trade_date": day.isoformat(),
                        "status": item.get(
                            "status",
                            "RETRY_REQUIRED"
                        ),
                        "previous_raw_file": item.get(
                            "raw_file", ""
                        ),
                        "previous_response_sha256": item.get(
                            "response_sha256", ""
                        ),
                        "previous_error": item.get(
                            "error", ""
                        )
                    }

            except Exception as error:

                log(
                    f"[PENDING READ WARN] "
                    f"{path.name}：{error}",
                    logs
                )

    # 2026-11-11 已由正式修正層取代，不再列為待重查。
    pending.pop(("TWSE", CORRECTION_DATE), None)
    pending.pop(("TPEX", CORRECTION_DATE), None)

    log(
        f"[PASS] 讀取既有待重查日期：{len(pending)} 筆。",
        logs
    )

    return pending


# ============================================================
# 8. 行情品質檢查
# ============================================================

def validate_records(
    records,
    market,
    day,
    expected_rows
):

    errors = []

    n = len(records)

    codes = [
        str(r.get("instrument_code", "")).strip()
        for r in records
    ]

    unique_codes = len(set(codes))

    duplicate_count = n - unique_codes

    invalid_codes = [
        code
        for code in codes
        if not re.fullmatch(
            rf"[0-9A-Za-z.\-]{{{MIN_CODE_LENGTH},{MAX_CODE_LENGTH}}}",
            code
        )
    ]

    if n < MIN_ROWS_ABSOLUTE:
        errors.append(
            f"資料筆數低於絕對下限：{n}"
        )

    if n > MAX_ROWS_ABSOLUTE:
        errors.append(
            f"資料筆數超過絕對上限：{n}"
        )

    if expected_rows:

        lower = expected_rows * MIN_COUNT_RATIO
        upper = expected_rows * MAX_COUNT_RATIO

        if not lower <= n <= upper:

            errors.append(
                "資料筆數偏離歷史母體："
                f"本次={n}；基準={expected_rows}；"
                f"允許={lower:.0f}~{upper:.0f}"
            )

    if duplicate_count:
        errors.append(
            f"重複證券代號列數={duplicate_count}"
        )

    if invalid_codes:
        errors.append(
            f"不合法證券代號列數={len(invalid_codes)}"
        )

    wrong_date = sum(
        r.get("trade_date") != day.isoformat()
        for r in records
    )

    wrong_market = sum(
        market_norm(r.get("market")) != market
        for r in records
    )

    if wrong_date:
        errors.append(
            f"交易日期不符列數={wrong_date}"
        )

    if wrong_market:
        errors.append(
            f"市場別不符列數={wrong_market}"
        )

    missing_volume = sum(
        r.get("volume") is None
        for r in records
    )

    negative_volume = sum(
        r.get("volume") is not None
        and r["volume"] < 0
        for r in records
    )

    if missing_volume:
        errors.append(
            f"成交量缺值列數={missing_volume}"
        )

    if negative_volume:
        errors.append(
            f"負成交量列數={negative_volume}"
        )

    zero_volume = sum(
        r.get("volume") == 0
        for r in records
    )

    zero_ratio = (
        zero_volume / n
        if n else 0
    )

    if zero_ratio > MAX_ZERO_VOLUME_RATIO:

        errors.append(
            f"零成交量比例異常：{zero_ratio:.2%}"
        )

    missing_ohlc = sum(
        any(
            r.get(k) is None
            for k in ("open", "high", "low", "close")
        )
        for r in records
    )

    missing_ohlc_ratio = (
        missing_ohlc / n
        if n else 0
    )

    if missing_ohlc_ratio > MAX_MISSING_OHLC_RATIO:

        errors.append(
            f"OHLC 缺值比例異常："
            f"{missing_ohlc_ratio:.2%}"
        )

    bad_price = 0
    complete_price = 0

    for record in records:

        values = [
            record.get(k)
            for k in ("open", "high", "low", "close")
        ]

        if any(v is None for v in values):
            continue

        open_price, high_price, low_price, close_price = values

        complete_price += 1

        if (
            min(values) < 0
            or low_price > high_price
            or not low_price <= open_price <= high_price
            or not low_price <= close_price <= high_price
        ):
            bad_price += 1

    bad_price_ratio = (
        bad_price / complete_price
        if complete_price else 0
    )

    if bad_price_ratio > MAX_INVALID_PRICE_RATIO:

        errors.append(
            f"OHLC 價格關係異常："
            f"{bad_price}/{complete_price} "
            f"({bad_price_ratio:.2%})"
        )

    negative_trade_value = sum(
        r.get("trade_value") is not None
        and r["trade_value"] < 0
        for r in records
    )

    negative_transactions = sum(
        r.get("transactions") is not None
        and r["transactions"] < 0
        for r in records
    )

    if negative_trade_value:
        errors.append(
            f"負成交金額列數={negative_trade_value}"
        )

    if negative_transactions:
        errors.append(
            f"負成交筆數列數={negative_transactions}"
        )

    source_ew_fail = 0

    if market == "TPEX":

        source_ew_fail = sum(
            "type_EW" not in str(r.get("source", ""))
            for r in records
        )

        if source_ew_fail:
            errors.append(
                f"TPEx 非 type=EW 來源列數={source_ew_fail}"
            )

    missing_price_change = sum(
        r.get("price_change") is None
        for r in records
    )

    stats = {
        "rows": n,
        "unique_codes": unique_codes,
        "duplicate_rows": duplicate_count,
        "invalid_code_rows": len(invalid_codes),
        "wrong_date_rows": wrong_date,
        "wrong_market_rows": wrong_market,
        "missing_volume_rows": missing_volume,
        "negative_volume_rows": negative_volume,
        "zero_volume_rows": zero_volume,
        "zero_volume_ratio": round(zero_ratio, 6),
        "missing_ohlc_rows": missing_ohlc,
        "missing_ohlc_ratio": round(missing_ohlc_ratio, 6),
        "complete_ohlc_rows": complete_price,
        "invalid_ohlc_rows": bad_price,
        "invalid_ohlc_ratio": round(bad_price_ratio, 6),
        "negative_trade_value_rows": negative_trade_value,
        "negative_transactions_rows": negative_transactions,
        "missing_price_change_rows": missing_price_change,
        "tpex_ew_source_fail_rows": source_ew_fail,
        "expected_rows": expected_rows,
        "count_ratio": (
            round(n / expected_rows, 6)
            if expected_rows else None
        ),
        "validation_errors": errors
    }

    return len(errors) == 0, stats


# ============================================================
# 9. 官方 API 解析
# ============================================================

def make_session():

    session = requests.Session()

    retry = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=1.0,
        status_forcelist=[
            429, 500, 502, 503, 504
        ],
        allowed_methods=["GET"]
    )

    adapter = HTTPAdapter(
        max_retries=retry
    )

    session.mount("https://", adapter)
    session.mount("http://", adapter)

    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 "
            "(compatible; SSO-Market-Data/1.5.10)"
        ),
        "Accept": "application/json,text/plain,*/*"
    })

    return session


def extract_tables(payload):

    result = []

    if not isinstance(payload, dict):
        return result

    tables = payload.get("tables")

    if isinstance(tables, list):

        for table in tables:

            if (
                isinstance(table, dict)
                and isinstance(table.get("fields"), list)
                and isinstance(table.get("data"), list)
            ):
                result.append(table)

    if (
        isinstance(payload.get("fields"), list)
        and isinstance(payload.get("data"), list)
    ):
        result.append(payload)

    data_obj = payload.get("data")

    if (
        isinstance(data_obj, dict)
        and isinstance(data_obj.get("fields"), list)
        and isinstance(data_obj.get("data"), list)
    ):
        result.append(data_obj)

    return result


def parse_payload(payload, market, day):

    field_aliases = {
        "code": ALIASES["instrument_code"],
        "name": ALIASES["instrument_name"],
        "open": ALIASES["open"],
        "high": ALIASES["high"],
        "low": ALIASES["low"],
        "close": ALIASES["close"],
        "volume": ALIASES["volume"],
        "trade_value": ALIASES["trade_value"],
        "transactions": ALIASES["transactions"],
        "change": ALIASES["price_change"],
        "sign": [
            "漲跌(+/-)",
            "漲跌符號",
            "漲跌記號"
        ]
    }

    required = [
        "code", "open", "high",
        "low", "close", "volume"
    ]

    candidates = []

    for table in extract_tables(payload):

        fields = table["fields"]

        mapping = {
            key: find_col(fields, aliases)
            for key, aliases in field_aliases.items()
        }

        if all(
            mapping.get(key) is not None
            for key in required
        ):
            candidates.append(
                (table, mapping)
            )

    if not candidates:

        tables = extract_tables(payload)

        if not any(
            table.get("data")
            for table in tables
        ):
            return (
                [],
                "NO_DATA",
                "官方端點目前沒有行情列",
                "",
                {}
            )

        raise RuntimeError(
            f"{market} 找不到完整 OHLC/成交量欄位表。"
        )

    candidates.sort(
        key=lambda item: len(
            item[0].get("data", [])
        ),
        reverse=True
    )

    table, mapping = candidates[0]

    fields = table["fields"]

    indexes = {
        key: (
            fields.index(value)
            if value in fields
            else None
        )
        for key, value in mapping.items()
    }

    records = []

    parse_stats = {
        "table_rows": len(table["data"]),
        "blank_code_rows": 0,
        "invalid_code_rows": 0,
        "malformed_rows": 0
    }

    for row in table["data"]:

        if not isinstance(row, (list, tuple)):

            parse_stats["malformed_rows"] += 1
            continue

        def cell(key):

            index = indexes.get(key)

            if (
                index is not None
                and index < len(row)
            ):
                return row[index]

            return None

        code = re.sub(
            r"\s+",
            "",
            str(cell("code") or "")
        ).strip()

        if not code:

            parse_stats["blank_code_rows"] += 1
            continue

        if not re.fullmatch(
            r"[0-9A-Za-z.\-]{2,16}",
            code
        ):

            parse_stats["invalid_code_rows"] += 1
            continue

        change = safe_num(
            cell("change")
        )

        if (
            indexes.get("sign") is not None
            and change is not None
        ):

            sign = str(
                cell("sign") or ""
            ).strip()

            if sign == "-":
                change = -abs(change)

            elif sign == "+":
                change = abs(change)

        records.append({
            "trade_date": day.isoformat(),
            "market": market,
            "instrument_code": code,
            "instrument_name": str(
                cell("name") or ""
            ).strip(),
            "open": safe_num(cell("open")),
            "high": safe_num(cell("high")),
            "low": safe_num(cell("low")),
            "close": safe_num(cell("close")),
            "volume": safe_num(
                cell("volume"),
                integer=True
            ),
            "trade_value": safe_num(
                cell("trade_value"),
                integer=True
            ),
            "price_change": change,
            "transactions": safe_num(
                cell("transactions"),
                integer=True
            ),
            "source": (
                "TWSE_MI_INDEX"
                if market == "TWSE"
                else "TPEX_afterTrading_otc_type_EW"
            )
        })

    title = str(
        table.get(
            "title",
            table.get("note", "")
        )
    )

    return (
        records,
        "PARSED",
        "",
        title,
        parse_stats
    )


# ============================================================
# 10. 官方 API 下載、原始回應保存及立即驗證
# ============================================================

def download_and_validate(
    session,
    market,
    day,
    expected_rows,
    out_dir,
    logs
):

    if market == "TWSE":

        url = (
            "https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX"
        )

        params = {
            "date": day.strftime("%Y%m%d"),
            "type": "ALLBUT0999",
            "response": "json"
        }

        query_type = "ALLBUT0999"

    else:

        url = (
            "https://www.tpex.org.tw/www/zh-tw/afterTrading/otc"
        )

        params = {
            "date": day.strftime("%Y/%m/%d"),
            "type": "EW",
            "response": "json"
        }

        query_type = "EW"

    meta = {
        "trade_date": day.isoformat(),
        "market": market,
        "query_type": query_type,
        "url": url,
        "params": params,
        "http_status": None,
        "response_sha256": "",
        "raw_gzip_sha256": "",
        "raw_file": "",
        "table_title": "",
        "status": "",
        "validation": {},
        "error": ""
    }

    try:

        response = session.get(
            url,
            params=params,
            timeout=(25, 120)
        )

        meta["http_status"] = response.status_code

        raw = response.content

        meta["response_sha256"] = sha256_bytes(raw)

        raw_dir = (
            Path(out_dir)
            / "raw"
            / market
        )

        raw_dir.mkdir(
            parents=True,
            exist_ok=True
        )

        raw_path = (
            raw_dir
            / f"{day.strftime('%Y%m%d')}.json.gz"
        )

        compressed = gzip.compress(
            raw,
            compresslevel=6,
            mtime=0
        )

        raw_path.write_bytes(compressed)

        meta["raw_gzip_sha256"] = sha256_bytes(
            compressed
        )

        meta["raw_file"] = (
            raw_path.relative_to(out_dir).as_posix()
        )

        if gzip.decompress(compressed) != raw:

            raise RuntimeError(
                "官方原始回應 GZIP 解壓比對失敗。"
            )

        response.raise_for_status()

        content_type = response.headers.get(
            "Content-Type",
            ""
        )

        if (
            "html" in content_type.lower()
            or raw.lstrip().lower().startswith(
                (b"<html", b"<!doctype html")
            )
        ):

            raise RuntimeError(
                "官方回應為 HTML，不是 JSON 行情。"
            )

        payload = response.json()

        (
            records,
            parse_status,
            parse_error,
            title,
            parse_stats
        ) = parse_payload(
            payload,
            market,
            day
        )

        meta["table_title"] = title
        meta["parse_stats"] = parse_stats

        if parse_status == "NO_DATA":

            meta["status"] = "WAITING_FOR_OFFICIAL_DATA"

            meta["error"] = (
                f"{parse_error}；原始回應已保存；"
                "不推定休市、不寫入增量，後續保留重查。"
            )

            meta["validation"] = {
                "rows": 0,
                "validation_errors": []
            }

            log(
                f"[WAITING] {market} {day}："
                "官方目前沒有行情列。",
                logs
            )

            return [], meta

        passed, stats = validate_records(
            records,
            market,
            day,
            expected_rows
        )

        stats["parse_stats"] = parse_stats

        meta["validation"] = stats

        invalid_limit = max(
            10,
            int(
                parse_stats["table_rows"] * 0.05
            )
        )

        if (
            parse_stats["invalid_code_rows"]
            > invalid_limit
        ):

            passed = False

            stats["validation_errors"].append(
                "官方原始表格不合法代號過多："
                f"{parse_stats['invalid_code_rows']}"
            )

        if not passed:

            meta["status"] = "REJECTED"

            meta["error"] = "; ".join(
                stats["validation_errors"]
            )

            log(
                f"[REJECTED] {market} {day}："
                f"{meta['error']}",
                logs
            )

            return [], meta

        meta["status"] = "PASS"
        meta["error"] = ""

        log(
            f"[DOWNLOAD + CHECK PASS] {market} {day}："
            f"{stats['rows']:,} 筆；"
            f"唯一代號={stats['unique_codes']:,}；"
            f"零成交量={stats['zero_volume_ratio']:.2%}；"
            f"OHLC缺值={stats['missing_ohlc_ratio']:.2%}；"
            f"price_change缺值={stats['missing_price_change_rows']:,}",
            logs
        )

        return records, meta

    except Exception as error:

        meta["status"] = "ERROR"

        meta["error"] = (
            f"{type(error).__name__}: {error}"
        )

        log(
            f"[DOWNLOAD ERROR] {market} {day}："
            f"{meta['error']}",
            logs
        )

        return [], meta


# ============================================================
# 11. 逐筆資料鍵值檢查
# ============================================================

def verify_all_forward_records(records):

    keys = [
        record_key(record)
        for record in records
    ]

    duplicates = (
        len(keys)
        - len(set(keys))
    )

    if duplicates:

        raise RuntimeError(
            f"本次雙市場增量出現重複鍵值：{duplicates}"
        )


# ============================================================
# 12. 當前程式來源封存
# ============================================================

def save_current_source(out_dir, logs):

    target = (
        Path(out_dir)
        / f"{VERSION}.py"
    )

    source_text = ""

    try:

        ip = get_ipython()

        history = (
            ip.history_manager.input_hist_raw
        )

        for candidate in reversed(
            history[-10:]
        ):

            if (
                VERSION in candidate
                and "def run_v1510" in candidate
            ):

                source_text = candidate
                break

    except Exception:
        source_text = ""

    if source_text:

        target.write_text(
            source_text,
            encoding="utf-8"
        )

        log(
            "[SOURCE PASS] 完整 v1.5.10 程式已封存。",
            logs
        )

        return target

    # 不因 Colab history 無法取得而丟失本次行情成果。
    note = (
        "本次執行程式已完整提供於 ChatGPT 對話；"
        "Colab IPython history 無法擷取目前儲存格。"
    )

    target.write_text(
        "# SOURCE CAPTURE NOTE\n"
        + note
        + "\n",
        encoding="utf-8"
    )

    log(
        "[SOURCE WARNING] Colab 未能擷取完整 cell source；"
        "程式仍由對話保存。",
        logs
    )

    return target


# ============================================================
# 13. 待重查紀錄輸出
# ============================================================

def write_api_index(path, api_meta):

    fields = [
        "trade_date",
        "market",
        "query_type",
        "url",
        "params",
        "http_status",
        "response_sha256",
        "raw_gzip_sha256",
        "raw_file",
        "table_title",
        "status",
        "error",
        "validation"
    ]

    with Path(path).open(
        "w",
        encoding="utf-8-sig",
        newline=""
    ) as f:

        writer = csv.DictWriter(
            f,
            fieldnames=fields
        )

        writer.writeheader()

        for item in api_meta:

            row = dict(item)

            for key in (
                "params",
                "validation"
            ):

                row[key] = json.dumps(
                    row.get(key, {}),
                    ensure_ascii=False,
                    default=str
                )

            writer.writerow(row)


# ============================================================
# 14. SHA256 manifest 及 ZIP
# ============================================================

def build_artifact_manifest(out_dir):

    items = []

    exclude_names = {
        "artifact_sha256_manifest.json",
        "GitHub上傳結果.json"
    }

    for path in sorted(
        Path(out_dir).rglob("*")
    ):

        if not path.is_file():
            continue

        if path.name in exclude_names:
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
        "created_at_taipei": datetime.now(TZ).isoformat(),
        "file_count": len(items),
        "self_excluded": True,
        "zip_files_excluded": True,
        "github_result_excluded": True,
        "files": items
    }

    write_json(
        Path(out_dir)
        / "artifact_sha256_manifest.json",
        manifest
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
    ) as zf:

        for path in sorted(out_dir.rglob("*")):

            if not path.is_file():
                continue

            if path == zip_path:
                continue

            if path.name == "GitHub上傳結果.json":
                continue

            if path.suffix.lower() == ".zip":
                continue

            zf.write(
                path,
                path.relative_to(out_dir).as_posix()
            )

    with zipfile.ZipFile(
        zip_path,
        "r"
    ) as zf:

        bad_member = zf.testzip()

        if bad_member:

            raise RuntimeError(
                f"ZIP CRC 驗證失敗：{bad_member}"
            )

        names = set(zf.namelist())

        for item in manifest["files"]:

            relative = item["path"]

            if relative not in names:

                raise RuntimeError(
                    f"ZIP 缺少封存檔案：{relative}"
                )

            data = zf.read(relative)

            if sha256_bytes(data) != item["sha256"]:

                raise RuntimeError(
                    f"ZIP 內檔案 SHA256 不一致：{relative}"
                )

    return {
        "zip_bytes": zip_path.stat().st_size,
        "zip_sha256": sha256_file(zip_path),
        "crc_pass": True,
        "manifest_file_count": manifest["file_count"],
        "manifest_readback_pass": True
    }


# ============================================================
# 15. GitHub 獨立 audit branch
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


def github_upload(
    files,
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

    def req(method, path, **kwargs):

        response = session.request(
            method,
            api + path,
            timeout=(20, 180),
            **kwargs
        )

        if not response.ok:

            raise RuntimeError(
                f"GitHub API HTTP {response.status_code}: "
                f"{response.text[:1500]}"
            )

        return (
            response.json()
            if response.content
            else {}
        )

    owner, repo = REPO.split("/")

    # main 只讀取，不修改。
    main_before = req(
        "GET",
        f"/repos/{owner}/{repo}/git/ref/heads/main"
    )["object"]["sha"]

    main_commit = req(
        "GET",
        f"/repos/{owner}/{repo}/git/commits/{main_before}"
    )

    base_tree = main_commit["tree"]["sha"]

    branch = (
        f"audit/sso-marketdata-v1510-{run_id}"
    )

    prefix = (
        f"audit_readout/sso_marketdata_v1510_{run_id}"
    )

    entries = []
    expected = []

    for path in files:

        path = Path(path)

        if not path.is_file():
            continue

        data = path.read_bytes()

        local_blob_sha = hashlib.sha1(
            b"blob "
            + str(len(data)).encode()
            + b"\x00"
            + data
        ).hexdigest()

        blob = req(
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

        relative = path.relative_to(
            Path(OUTPUT_DIR)
        ).as_posix()

        remote_path = (
            f"{prefix}/{relative}"
        )

        entries.append({
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

    tree = req(
        "POST",
        f"/repos/{owner}/{repo}/git/trees",
        json={
            "base_tree": base_tree,
            "tree": entries
        }
    )

    commit = req(
        "POST",
        f"/repos/{owner}/{repo}/git/commits",
        json={
            "message": (
                f"SSO market data v1.5.10 "
                f"validated incremental update {run_id}"
            ),
            "tree": tree["sha"],
            "parents": [main_before]
        }
    )

    req(
        "POST",
        f"/repos/{owner}/{repo}/git/refs",
        json={
            "ref": f"refs/heads/{branch}",
            "sha": commit["sha"]
        }
    )

    # 逐檔回讀。
    for item in expected:

        remote_blob = req(
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

        log(
            f"[GITHUB READBACK PASS] {item['path']}",
            logs
        )

    # Branch head 回讀。
    branch_ref = req(
        "GET",
        (
            f"/repos/{owner}/{repo}/branches/"
            + quote(branch, safe="")
        )
    )

    branch_head = branch_ref["commit"]["sha"]

    if branch_head != commit["sha"]:

        raise RuntimeError(
            "GitHub audit branch head 與本次 commit 不一致。"
        )

    # main 前後 SHA。
    main_after = req(
        "GET",
        f"/repos/{owner}/{repo}/git/ref/heads/main"
    )["object"]["sha"]

    if main_before != main_after:

        raise RuntimeError(
            "GitHub main 前後 SHA 不一致。"
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

def run_v1510():

    mount_drive()

    logs = []

    now = datetime.now(TZ)
    today = now.date()

    same_day_allowed = (
        now.time() >= MARKET_CLOSE_CUTOFF
    )

    target_end_date = (
        today
        if same_day_allowed
        else today - timedelta(days=1)
    )

    run_id = (
        now.strftime("%Y%m%d_%H%M%S")
        + "_"
        + uuid.uuid4().hex[:4]
    )

    out_name = (
        f"v1.5.10_官方行情即時驗證與增量續接_{run_id}"
    )

    out_dir = ROOT / out_name

    # --------------------------------------------------------
    # A. 啟動及門檻
    # --------------------------------------------------------

    log(
        f"啟動：{VERSION}",
        logs
    )

    log(
        f"台灣時間：{now.isoformat()}；"
        f"當日查詢門檻：17:00；"
        f"本次查詢截止日：{target_end_date}",
        logs
    )

    if not same_day_allowed:

        log(
            "[SAME-DAY GATE] 尚未到 17:00，"
            f"跳過 {today} 當日行情查詢。",
            logs
        )

    # --------------------------------------------------------
    # B. 載入歷史資料、schema、baseline
    # --------------------------------------------------------

    dirs = version_dirs()

    (
        baseline,
        baseline_source,
        standard_cols,
        overlay_cols
    ) = load_baseline_and_schema(dirs)

    (
        latest,
        completed,
        previous_map,
        previous_conflicts,
        previous_scan
    ) = read_previous_state(
        dirs,
        logs
    )

    pending = read_previous_pending(
        dirs,
        logs
    )

    # --------------------------------------------------------
    # C. 驗證 v1.5.9 雙市場修正層
    # --------------------------------------------------------

    corrections = load_v159_corrections(
        logs
    )

    correction_records = {
        "TWSE": corrections["twse"]["records"],
        "TPEX": corrections["tpex"]["records"]
    }

    correction_pass = True

    # 兩市場 11/11 都已經由完整修正層替代。
    for market in ("TWSE", "TPEX"):

        completed[market].add(
            CORRECTION_DATE
        )

        if (
            latest[market] is None
            or CORRECTION_DATE > latest[market]
        ):

            latest[market] = CORRECTION_DATE

        pending.pop(
            (market, CORRECTION_DATE),
            None
        )

    # --------------------------------------------------------
    # D. 建立本次全新輸出目錄
    # --------------------------------------------------------

    out_dir.mkdir(
        parents=True,
        exist_ok=False
    )

    log(
        f"資料根目錄：{ROOT}",
        logs
    )

    log(
        f"本次輸出目錄：{out_dir}",
        logs
    )

    for market in ("TWSE", "TPEX"):

        log(
            f"{market} 前次最後成功行情日："
            f"{latest[market]}",
            logs
        )

        log(
            f"{market} 歷史合理筆數基準："
            f"{baseline[market]}；"
            f"來源={baseline_source[market]}",
            logs
        )

    log(
        "[PASS] v1.5.9 2026-11-11 雙市場修正已接續。",
        logs
    )

    # --------------------------------------------------------
    # E. 複製兩市場修正層
    # --------------------------------------------------------

    correction_output = {}

    for market in ("TWSE", "TPEX"):

        source = corrections[
            market.lower()
        ]

        root_target = (
            out_dir
            / f"修正_{market}_20261111.csv"
        )

        nested_target = (
            out_dir
            / "corrections"
            / "官方歷史行情"
            / market
            / "20261111.csv"
        )

        nested_target.parent.mkdir(
            parents=True,
            exist_ok=True
        )

        shutil.copy2(
            source["root"],
            root_target
        )

        shutil.copy2(
            source["nested"],
            nested_target
        )

        if (
            sha256_file(root_target)
            != sha256_file(source["root"])
        ):

            raise RuntimeError(
                f"{market} root 修正層複製 SHA 不一致。"
            )

        if (
            sha256_file(nested_target)
            != sha256_file(source["nested"])
        ):

            raise RuntimeError(
                f"{market} nested 修正層複製 SHA 不一致。"
            )

        # 複製後回讀。
        required = (
            COMPARE_FIELDS
            if market == "TWSE"
            else [
                f for f in COMPARE_FIELDS
                if f != "price_change"
            ]
        )

        _, root_records = normalize_csv_rows(
            root_target,
            market=market,
            required_fields=required
        )

        _, nested_records = normalize_csv_rows(
            nested_target,
            market=market,
            required_fields=required
        )

        root_sha = canonical_records_sha256(
            root_records
        )

        nested_sha = canonical_records_sha256(
            nested_records
        )

        expected_sha = (
            EXPECTED_TWSE_CORRECTION_CANONICAL_SHA256
            if market == "TWSE"
            else EXPECTED_TPEX_CORRECTION_CANONICAL_SHA256
        )

        if (
            root_sha != expected_sha
            or nested_sha != expected_sha
        ):

            raise RuntimeError(
                f"{market} 新修正層 canonical SHA 不符。"
            )

        if root_sha != nested_sha:

            raise RuntimeError(
                f"{market} root/nested 修正層不一致。"
            )

        correction_output[market] = {
            "root": root_target,
            "nested": nested_target,
            "records": root_records,
            "rows": len(root_records),
            "root_sha256": sha256_file(root_target),
            "nested_sha256": sha256_file(nested_target),
            "canonical_sha256": root_sha
        }

        log(
            f"[CORRECTION CARRY-FORWARD PASS] "
            f"{market}：{len(root_records):,} 筆；"
            f"canonical={root_sha}",
            logs
        )

    # 複製 TWSE 11/11 原始官方回應。
    source_twse_raw = corrections["twse"]["raw"]

    target_twse_raw = (
        out_dir
        / "raw"
        / "TWSE"
        / "20261111.json.gz"
    )

    target_twse_raw.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    shutil.copy2(
        source_twse_raw,
        target_twse_raw
    )

    if (
        sha256_file(target_twse_raw)
        != sha256_file(source_twse_raw)
    ):

        raise RuntimeError(
            "TWSE 11/11 官方 raw GZIP 複製 SHA 不一致。"
        )

    # --------------------------------------------------------
    # F. 設定待重查日期
    # --------------------------------------------------------

    deferred_pending = {
        key: dict(item)
        for key, item in pending.items()
        if key[1] > target_end_date
    }

    retry_queue = {
        key: dict(item)
        for key, item in pending.items()
        if key[1] <= target_end_date
    }

    # 移除已由 v1.5.9 修正層完整替代的 11/11。
    retry_queue.pop(
        ("TWSE", CORRECTION_DATE),
        None
    )

    retry_queue.pop(
        ("TPEX", CORRECTION_DATE),
        None
    )

    deferred_items = sorted(
        [
            dict(item)
            for item in deferred_pending.values()
        ],
        key=lambda x: (
            x.get("trade_date", ""),
            x.get("market", "")
        )
    )

    if deferred_items:

        log(
            f"[PENDING DEFERRED] "
            f"{len(deferred_items)} 筆日期尚未到查詢門檻。",
            logs
        )

    # --------------------------------------------------------
    # G. 前瞻增量下載
    # --------------------------------------------------------

    forward_records = []

    api_meta = []
    daily_summary = []

    rejected_count = 0
    download_error_count = 0
    waiting_count = 0
    duplicate_existing_count = 0

    session = make_session()

    for market in ("TWSE", "TPEX"):

        candidate_days = set()

        # 前次未完成日期。
        for key in list(retry_queue):

            if key[0] == market:
                candidate_days.add(key[1])

        # 最後成功日期之後的新日期。
        cursor = latest[market] + timedelta(days=1)

        while cursor <= target_end_date:

            candidate_days.add(cursor)

            cursor += timedelta(days=1)

        for day in sorted(candidate_days):

            if day > target_end_date:
                continue

            key = (market, day)

            # 已有成功日期不重複下載。
            if day in completed[market]:

                retry_queue.pop(key, None)
                continue

            records, meta = download_and_validate(
                session,
                market,
                day,
                baseline[market],
                out_dir,
                logs
            )

            api_meta.append(meta)

            if meta["status"] == "PASS":

                # 不允許新下載的鍵值與歷史資料重複。
                existing_keys = [
                    record_key(r)
                    for r in records
                    if record_key(r) in previous_map
                ]

                if existing_keys:

                    duplicate_existing_count += len(
                        existing_keys
                    )

                    meta["status"] = "REJECTED_DUPLICATE_WITH_HISTORY"

                    meta["error"] = (
                        "新下載資料與既有增量鍵值重複："
                        f"{len(existing_keys)} 筆；"
                        "為避免雙重計算，本日不寫入增量。"
                    )

                    meta["validation"]["validation_errors"].append(
                        meta["error"]
                    )

                    retry_queue[key] = {
                        "market": market,
                        "trade_date": day.isoformat(),
                        "status": meta["status"],
                        "raw_file": meta.get("raw_file", ""),
                        "response_sha256": meta.get(
                            "response_sha256", ""
                        ),
                        "error": meta["error"]
                    }

                    daily_summary.append({
                        "trade_date": day.isoformat(),
                        "market": market,
                        "rows": 0,
                        "unique_codes": 0,
                        "status": meta["status"],
                        "error": meta["error"]
                    })

                    log(
                        f"[BLOCK DUPLICATE] {market} {day}："
                        f"{len(existing_keys)} 筆與既有資料重疊。",
                        logs
                    )

                    continue

                forward_records.extend(
                    records
                )

                completed[market].add(day)

                retry_queue.pop(key, None)

                v = meta["validation"]

                daily_summary.append({
                    "trade_date": day.isoformat(),
                    "market": market,
                    "rows": v["rows"],
                    "unique_codes": v["unique_codes"],
                    "zero_volume_rows": v["zero_volume_rows"],
                    "zero_volume_ratio": v["zero_volume_ratio"],
                    "missing_ohlc_rows": v["missing_ohlc_rows"],
                    "missing_ohlc_ratio": v["missing_ohlc_ratio"],
                    "missing_price_change_rows": (
                        v["missing_price_change_rows"]
                    ),
                    "duplicate_rows": v["duplicate_rows"],
                    "status": "APPENDED",
                    "error": ""
                })

            else:

                if meta["status"] == "REJECTED":
                    rejected_count += 1

                elif meta["status"] == "ERROR":
                    download_error_count += 1

                elif meta["status"] == "WAITING_FOR_OFFICIAL_DATA":
                    waiting_count += 1

                else:
                    rejected_count += 1

                retry_queue[key] = {
                    "market": market,
                    "trade_date": day.isoformat(),
                    "status": meta["status"],
                    "raw_file": meta.get("raw_file", ""),
                    "response_sha256": meta.get(
                        "response_sha256", ""
                    ),
                    "error": meta.get("error", "")
                }

                daily_summary.append({
                    "trade_date": day.isoformat(),
                    "market": market,
                    "rows": 0,
                    "unique_codes": 0,
                    "status": meta["status"],
                    "error": meta.get("error", "")
                })

            time.sleep(0.3)

    verify_all_forward_records(
        forward_records
    )

    forward_records.sort(
        key=lambda r: (
            r["trade_date"],
            r["market"],
            r["instrument_code"]
        )
    )

    # --------------------------------------------------------
    # H. 全市場標準化增量寫入
    # --------------------------------------------------------

    forward_dates = sorted({
        r["trade_date"]
        for r in forward_records
    })

    if forward_dates:

        std_path = (
            out_dir
            / (
                "全市場標準化增量_"
                f"{forward_dates[0].replace('-', '')}_"
                f"{forward_dates[-1].replace('-', '')}.csv"
            )
        )

    else:

        std_path = (
            out_dir
            / f"全市場標準化增量_無新增_{run_id}.csv"
        )

    write_csv(
        std_path,
        forward_records,
        standard_cols
    )

    standard_roundtrip = normalize_csv_rows(
        std_path,
        market=None,
        required_fields=CORE_REQUIRED_FIELDS
    )[1]

    if len(standard_roundtrip) != len(forward_records):

        raise RuntimeError(
            "全市場標準化增量寫入後筆數不一致。"
        )

    if (
        canonical_records_sha256(standard_roundtrip)
        != canonical_records_sha256(forward_records)
    ):

        raise RuntimeError(
            "全市場標準化增量寫入後 canonical SHA 不一致。"
        )

    log(
        f"[WRITE CHECK PASS] 全市場標準化增量："
        f"{len(forward_records):,} 筆。",
        logs
    )

    # --------------------------------------------------------
    # I. 逐股 overlay
    #     含 11/11 修正列 + 本次新行情列
    # --------------------------------------------------------

    overlay_records = (
        correction_records["TWSE"]
        + correction_records["TPEX"]
        + forward_records
    )

    overlay_groups = defaultdict(list)

    for record in overlay_records:

        overlay_groups[
            (
                record["market"],
                record["instrument_code"]
            )
        ].append(record)

    overlay_root = (
        out_dir
        / "overlay"
        / "官方歷史行情"
    )

    overlay_index = []

    for (market, code), rows in sorted(
        overlay_groups.items()
    ):

        suffix = (
            "TWSE"
            if market == "TWSE"
            else "TPEx"
        )

        filename = f"{code}_{suffix}.csv"

        path = overlay_root / filename

        rows.sort(
            key=lambda r: r["trade_date"]
        )

        keys = [
            record_key(r)
            for r in rows
        ]

        if len(keys) != len(set(keys)):

            raise RuntimeError(
                f"逐股 overlay 存在重複鍵值：{filename}"
            )

        cols = overlay_cols[market]

        write_csv(
            path,
            rows,
            cols
        )

        reread = normalize_csv_rows(
            path,
            market=market,
            required_fields=CORE_REQUIRED_FIELDS
        )[1]

        if len(reread) != len(rows):

            raise RuntimeError(
                f"逐股 overlay 寫入後筆數不符：{filename}"
            )

        if (
            canonical_records_sha256(reread)
            != canonical_records_sha256(rows)
        ):

            raise RuntimeError(
                f"逐股 overlay canonical SHA 不符：{filename}"
            )

        dates = sorted({
            r["trade_date"]
            for r in rows
        })

        overlay_index.append({
            "instrument_code": code,
            "market": market,
            "overlay_file": (
                f"官方歷史行情/{filename}"
            ),
            "rows": len(rows),
            "first_date": dates[0],
            "last_date": dates[-1],
            "sha256": sha256_file(path),
            "canonical_sha256": (
                canonical_records_sha256(reread)
            )
        })

    log(
        f"[OVERLAY PASS] "
        f"逐股 overlay={len(overlay_index):,} 檔；"
        f"含雙市場 11/11 修正層及本次新行情。",
        logs
    )

    # --------------------------------------------------------
    # J. 雙市場修正替代規則
    # --------------------------------------------------------

    correction_manifest = {
        "version": VERSION,
        "correction_date": CORRECTION_DATE.isoformat(),
        "source_version": "v1.5.9",
        "source_sealed_zip_sha256": EXPECTED_V159_ZIP_SHA256,
        "markets": {
            "TWSE": {
                "rows": correction_output["TWSE"]["rows"],
                "file_sha256": correction_output["TWSE"]["root_sha256"],
                "canonical_sha256": (
                    correction_output["TWSE"]["canonical_sha256"]
                ),
                "replacement_file": (
                    "corrections/官方歷史行情/TWSE/20261111.csv"
                ),
                "rule": (
                    "以完整官方 1,390 列替代同日舊資料，"
                    "不可相加。"
                )
            },
            "TPEX": {
                "rows": correction_output["TPEX"]["rows"],
                "file_sha256": correction_output["TPEX"]["root_sha256"],
                "canonical_sha256": (
                    correction_output["TPEX"]["canonical_sha256"]
                ),
                "replacement_file": (
                    "corrections/官方歷史行情/TPEX/20261111.csv"
                ),
                "rule": (
                    "以既有 type=EW 1,020 列替代同日舊資料，"
                    "不可相加。"
                ),
                "price_change_policy": (
                    "原始資料缺欄時正規化為 None；不補造數值。"
                )
            }
        },
        "old_files_modified": False,
        "old_files_deleted": False
    }

    write_json(
        out_dir / "v1.5.10_雙市場修正替代規則.json",
        correction_manifest
    )

    # --------------------------------------------------------
    # K. 既有資料 + 修正層 + 新行情接續預檢
    # --------------------------------------------------------

    effective_map = dict(previous_map)

    # 加入 11/11 正式替代層。
    correction_key_count = 0

    for market in ("TWSE", "TPEX"):

        for record in correction_records[market]:

            key = record_key(record)

            effective_map[key] = record

            correction_key_count += 1

    # 加入本次新行情。
    forward_key_conflicts = []

    for record in forward_records:

        key = record_key(record)

        if key in effective_map:

            forward_key_conflicts.append({
                "key": key,
                "record": record
            })

            continue

        effective_map[key] = record

    if forward_key_conflicts:

        raise RuntimeError(
            "合併既有資料時發現新行情鍵值衝突："
            f"{len(forward_key_conflicts)} 筆。"
        )

    effective_keys = list(effective_map.keys())

    if len(effective_keys) != len(set(effective_keys)):

        raise RuntimeError(
            "下游有效資料集合存在重複鍵值。"
        )

    # 修正日的舊資料必須已被完整替代。
    replacement_counts = {
        "TWSE": sum(
            1
            for r in effective_map.values()
            if (
                r["market"] == "TWSE"
                and r["trade_date"] == CORRECTION_DATE.isoformat()
            )
        ),
        "TPEX": sum(
            1
            for r in effective_map.values()
            if (
                r["market"] == "TPEX"
                and r["trade_date"] == CORRECTION_DATE.isoformat()
            )
        )
    }

    if (
        replacement_counts["TWSE"]
        != EXPECTED_TWSE_CORRECTION_ROWS
    ):

        raise RuntimeError(
            "SSO 有效資料集合中的 TWSE 11/11 替代筆數不符。"
        )

    if (
        replacement_counts["TPEX"]
        != EXPECTED_TPEX_CORRECTION_ROWS
    ):

        raise RuntimeError(
            "SSO 有效資料集合中的 TPEx 11/11 替代筆數不符。"
        )

    # 現行目標日期逐市場完整性。
    current_day_market = {}

    for market in ("TWSE", "TPEX"):

        rows = [
            r
            for r in effective_map.values()
            if (
                r["market"] == market
                and r["trade_date"] == target_end_date.isoformat()
            )
        ]

        codes = [
            r["instrument_code"]
            for r in rows
        ]

        current_day_market[market] = {
            "rows": len(rows),
            "unique_codes": len(set(codes)),
            "pass": (
                len(rows) >= MIN_ROWS_ABSOLUTE
                and len(rows) == len(set(codes))
            )
        }

    # 下游必要欄位。
    schema_ready = all(
        find_col(
            standard_cols,
            ALIASES[field]
        ) is not None
        for field in (
            "trade_date",
            "market",
            "instrument_code",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "price_change"
        )
    )

    history_conflict_count = len(
        previous_conflicts
    )

    due_unresolved = [
        item
        for key, item in retry_queue.items()
        if key[1] <= target_end_date
    ]

    current_day_both_pass = (
        current_day_market["TWSE"]["pass"]
        and current_day_market["TPEX"]["pass"]
    )

    downstream_ready = (
        correction_pass
        and schema_ready
        and history_conflict_count == 0
        and len(forward_key_conflicts) == 0
        and len(due_unresolved) == 0
        and (
            current_day_both_pass
            if same_day_allowed
            else True
        )
    )

    downstream_report = {
        "version": VERSION,
        "created_at_taipei": datetime.now(TZ).isoformat(),
        "target_end_date": target_end_date.isoformat(),

        "v159_correction_source_pass": correction_pass,

        "replacement_20261111": {
            "TWSE_rows": replacement_counts["TWSE"],
            "TWSE_expected_rows": EXPECTED_TWSE_CORRECTION_ROWS,
            "TPEX_rows": replacement_counts["TPEX"],
            "TPEX_expected_rows": EXPECTED_TPEX_CORRECTION_ROWS,
            "old_same_day_rows_replaced": True,
            "correction_layer_added_as_new_increment": False
        },

        "forward_increment": {
            "rows": len(forward_records),
            "dates": forward_dates,
            "TWSE_rows": sum(
                r["market"] == "TWSE"
                for r in forward_records
            ),
            "TPEX_rows": sum(
                r["market"] == "TPEX"
                for r in forward_records
            )
        },

        "current_target_day": {
            "date": target_end_date.isoformat(),
            "same_day_gate_pass": same_day_allowed,
            "TWSE": current_day_market["TWSE"],
            "TPEX": current_day_market["TPEX"],
            "both_markets_pass": current_day_both_pass
        },

        "historical_continuity": {
            "previous_unique_keys": len(previous_map),
            "identical_duplicate_copies": (
                previous_scan["identical_duplicate_copies"]
            ),
            "historical_conflict_keys": history_conflict_count,
            "old_20261111_rows_replaced": (
                previous_scan["old_20261111_rows_replaced"]
            ),
            "effective_unique_keys_after_replacement": (
                len(effective_map)
            )
        },

        "schema": {
            "standard_columns": standard_cols,
            "price_change_available": (
                find_col(
                    standard_cols,
                    ALIASES["price_change"]
                ) is not None
            ),
            "schema_ready": schema_ready
        },

        "pending": {
            "due_unresolved_count": len(due_unresolved),
            "due_unresolved_dates": due_unresolved
        },

        "downstream_preflight_ready": downstream_ready,

        "model_execution": {
            "training_performed": False,
            "scoring_performed": False,
            "Q5_performance_test_performed": False,
            "reason": (
                "本版本屬官方行情資料續接及資料品質驗證；"
                "不在行情更新程序內重訓模型或產生選股分數。"
            )
        }
    }

    write_json(
        out_dir / "SSO下游資料接續預檢報告.json",
        downstream_report
    )

    log(
        f"[SSO PREFLIGHT] downstream_ready={downstream_ready}；"
        f"historical_conflicts={history_conflict_count}；"
        f"pending={len(due_unresolved)}",
        logs
    )

    # --------------------------------------------------------
    # L. 最後日期及待重查紀錄
    # --------------------------------------------------------

    updated_latest = {
        market: latest[market]
        for market in latest
    }

    for record in forward_records:

        market = record["market"]

        d = parse_date(
            record["trade_date"]
        )

        if (
            updated_latest[market] is None
            or d > updated_latest[market]
        ):

            updated_latest[market] = d

    state = {
        "version": VERSION,
        "updated_at_taipei": now.isoformat(),
        "market_last_date": {
            market: (
                day.isoformat()
                if day is not None
                else None
            )
            for market, day in updated_latest.items()
        },
        "correction_20261111": {
            "TWSE_pass": True,
            "TWSE_rows": EXPECTED_TWSE_CORRECTION_ROWS,
            "TPEX_pass": True,
            "TPEX_rows": EXPECTED_TPEX_CORRECTION_ROWS
        },
        "tpex_query_type": "EW",
        "unresolved_dates_count": len(retry_queue)
    }

    write_json(
        out_dir / "latest_market_dates.json",
        state
    )

    unresolved_dates = sorted(
        [
            dict(item)
            for item in retry_queue.values()
        ],
        key=lambda x: (
            x.get("trade_date", ""),
            x.get("market", "")
        )
    )

    write_json(
        out_dir / "待重查日期清單.json",
        {
            "version": VERSION,
            "created_at_taipei": now.isoformat(),
            "count": len(unresolved_dates),
            "items": unresolved_dates,
            "deferred_not_yet_due_count": len(deferred_items),
            "deferred_not_yet_due_items": deferred_items,
            "rule": (
                "未取得官方資料、品質拒絕、下載錯誤及"
                "重複鍵值日期均保留；不推定休市；"
                "後續執行持續重新查詢。"
            )
        }
    )

    # --------------------------------------------------------
    # M. 逐日統計及 API 索引
    # --------------------------------------------------------

    daily_path = (
        out_dir / "逐日雙市場行情驗證統計.csv"
    )

    daily_fields = [
        "trade_date",
        "market",
        "rows",
        "unique_codes",
        "zero_volume_rows",
        "zero_volume_ratio",
        "missing_ohlc_rows",
        "missing_ohlc_ratio",
        "missing_price_change_rows",
        "duplicate_rows",
        "status",
        "error"
    ]

    with daily_path.open(
        "w",
        encoding="utf-8-sig",
        newline=""
    ) as f:

        writer = csv.DictWriter(
            f,
            fieldnames=daily_fields,
            extrasaction="ignore"
        )

        writer.writeheader()
        writer.writerows(daily_summary)

    api_index_path = (
        out_dir / "官方API下載及即時驗證索引.csv"
    )

    write_api_index(
        api_index_path,
        api_meta
    )

    overlay_index_path = (
        out_dir / "個股增量覆蓋檔索引.csv"
    )

    overlay_index_fields = [
        "instrument_code",
        "market",
        "overlay_file",
        "rows",
        "first_date",
        "last_date",
        "sha256",
        "canonical_sha256"
    ]

    with overlay_index_path.open(
        "w",
        encoding="utf-8-sig",
        newline=""
    ) as f:

        writer = csv.DictWriter(
            f,
            fieldnames=overlay_index_fields
        )

        writer.writeheader()
        writer.writerows(overlay_index)

    # --------------------------------------------------------
    # N. 修正替代索引
    # --------------------------------------------------------

    replacement_index_path = (
        out_dir / "雙市場20261111修正替代索引.csv"
    )

    replacement_rows = []

    for market in ("TWSE", "TPEX"):

        item = correction_output[market]

        replacement_rows.append({
            "trade_date": CORRECTION_DATE.isoformat(),
            "market": market,
            "rows": item["rows"],
            "unique_codes": item["rows"],
            "replacement_file": (
                f"corrections/官方歷史行情/"
                f"{market}/20261111.csv"
            ),
            "root_sha256": item["root_sha256"],
            "nested_sha256": item["nested_sha256"],
            "canonical_sha256": item["canonical_sha256"],
            "replacement_rule": (
                "完整同日替代，不可與舊資料相加。"
            ),
            "status": "PASS"
        })

    with replacement_index_path.open(
        "w",
        encoding="utf-8-sig",
        newline=""
    ) as f:

        writer = csv.DictWriter(
            f,
            fieldnames=list(replacement_rows[0].keys())
        )

        writer.writeheader()
        writer.writerows(replacement_rows)

    # --------------------------------------------------------
    # O. 執行結論
    # --------------------------------------------------------

    if not correction_pass:

        status = "BLOCKED_CORRECTION_FAILED"

    elif (
        rejected_count
        or download_error_count
        or waiting_count
        or unresolved_dates
        or duplicate_existing_count
    ):

        status = "PARTIAL"

    elif not forward_records:

        status = "NO_NEW_ROWS"

    else:

        status = "SUCCESS"

    report = {
        "version": VERSION,
        "run_id": run_id,
        "run_time_taipei": now.isoformat(),
        "status": status,

        "market_close_gate": {
            "timezone": "Asia/Taipei",
            "cutoff_local": "17:00",
            "same_day_download_allowed": same_day_allowed,
            "target_end_date": target_end_date.isoformat()
        },

        "previous_market_last_date": {
            market: (
                day.isoformat()
                if day is not None
                else None
            )
            for market, day in latest.items()
        },

        "market_last_date_after_update": state["market_last_date"],

        "baseline_daily_rows": baseline,
        "baseline_source": baseline_source,

        "previous_data_scan": previous_scan,

        "correction_source": corrections["source_directory"],
        "correction": correction_manifest,

        "forward_new_rows": len(forward_records),

        "forward_new_by_market": {
            market: sum(
                r["market"] == market
                for r in forward_records
            )
            for market in ("TWSE", "TPEX")
        },

        "forward_new_instruments": len(overlay_groups),

        "forward_trade_dates": forward_dates,

        "api_requests": len(api_meta),

        "api_pass_count": sum(
            item["status"] == "PASS"
            for item in api_meta
        ),

        "rejected_download_count": rejected_count,
        "download_error_count": download_error_count,
        "waiting_official_data_count": waiting_count,

        "duplicate_existing_count": duplicate_existing_count,

        "unresolved_dates": unresolved_dates,
        "deferred_not_yet_due_dates": deferred_items,

        "downstream_preflight": downstream_report,

        "immediate_validation": {
            "enabled": True,
            "before_write": True,
            "after_write": True,
            "checks": [
                "v1.5.9 封存 ZIP SHA256",
                "v1.5.9 artifact manifest SHA256",
                "TWSE 2026-11-11 correction canonical SHA",
                "TPEx 2026-11-11 correction canonical SHA",
                "官方 API HTTP 狀態及 JSON",
                "官方原始 GZIP 解壓核對",
                "官方行情欄位",
                "最低及最高列數",
                "歷史母體筆數範圍",
                "證券代號格式",
                "唯一代號及重複代號",
                "日期及市場一致性",
                "成交量缺值及負值",
                "零成交量比例",
                "OHLC 缺值比例",
                "OHLC 高低價格關係",
                "成交金額負值",
                "成交筆數負值",
                "TPEx type=EW 來源",
                "標準化增量寫入後再讀取",
                "逐股 overlay 寫入後再讀取",
                "雙市場修正層完整替代",
                "SSO 下游資料接續預檢",
                "ZIP CRC 及 SHA256",
                "GitHub blob 逐檔回讀",
                "GitHub main SHA 前後核對"
            ],
            "failed_data_written_to_increment": False,
            "raw_response_preserved": True
        },

        "preservation": {
            "v1_5_9_original_files_modified": False,
            "v1_5_8_original_files_modified": False,
            "v1_5_7_original_files_modified": False,
            "v1_5_6_original_files_modified": False,
            "old_history_files_deleted": False,
            "old_tpex_al_data_physically_deleted": False,
            "github_main_modified": False,
            "model_training_performed": False
        }
    }

    report_path = (
        out_dir
        / "v1.5.10_官方增量行情續接報告.json"
    )

    write_json(
        report_path,
        report
    )

    # --------------------------------------------------------
    # P. README
    # --------------------------------------------------------

    readme = f"""# {VERSION}

## 執行結論

{status}

## 行情查詢門檻

- 台灣時間：{now.isoformat()}
- 當日查詢門檻：17:00
- 本次查詢截止日：{target_end_date}

## 2026-11-11 雙市場修正承接

- TWSE：{EXPECTED_TWSE_CORRECTION_ROWS:,} 列
- TWSE canonical SHA256：
  {EXPECTED_TWSE_CORRECTION_CANONICAL_SHA256}

- TPEx：{EXPECTED_TPEX_CORRECTION_ROWS:,} 列
- TPEx 查詢類型：EW
- TPEx canonical SHA256：
  {EXPECTED_TPEX_CORRECTION_CANONICAL_SHA256}

兩市場均採完整日期替代，不可與舊版同日資料相加。

## 本次新行情

- 前瞻新增行情：{len(forward_records):,} 列
- TWSE 新增：{sum(r["market"] == "TWSE" for r in forward_records):,}
- TPEx 新增：{sum(r["market"] == "TPEX" for r in forward_records):,}
- 新增交易日：{forward_dates}

## 官方查詢

- TWSE：MI_INDEX / ALLBUT0999
- TPEx：afterTrading/otc / EW
- 原始 API 回應均保存為 gzip。
- 官方尚無資料的日期保留待重查，不推定休市。

## 資料接續

- 讀取既有 v1.5.x 標準化增量。
- 2026-11-11 雙市場資料以 v1.5.9 修正層替代。
- 新日期資料寫入全市場標準化增量。
- 新增日期與修正日期寫入逐股 overlay。
- 不修改任何舊版 Drive 資料。

## SSO 下游預檢

- downstream_preflight_ready：
  {downstream_ready}
- 既有資料衝突鍵值：
  {history_conflict_count}
- 待重查日期：
  {len(unresolved_dates)}

本版本只完成行情資料接續及資料品質檢查。
不執行模型訓練、Q5 回測或選股分數計算。

## 原始資料保護

- 不修改 v1.5.9
- 不修改 v1.5.8
- 不修改 v1.5.7
- 不修改 v1.5.6
- 不刪除歷史資料
- 不修改 GitHub main
"""

    (out_dir / "README.md").write_text(
        readme,
        encoding="utf-8"
    )

    # --------------------------------------------------------
    # Q. 封存實際執行程式
    # --------------------------------------------------------

    source_path = save_current_source(
        out_dir,
        logs
    )

    # --------------------------------------------------------
    # R. 執行紀錄固定快照
    # --------------------------------------------------------

    log_path = (
        out_dir / "run.log"
    )

    log_path.write_text(
        "\n".join(logs) + "\n",
        encoding="utf-8"
    )

    # --------------------------------------------------------
    # S. Manifest 及 ZIP
    # --------------------------------------------------------

    artifact_manifest = build_artifact_manifest(
        out_dir
    )

    manifest_path = (
        out_dir / "artifact_sha256_manifest.json"
    )

    manifest_sha256 = sha256_file(
        manifest_path
    )

    zip_path = (
        out_dir
        / f"{out_name}.zip"
    )

    zip_result = make_and_verify_zip(
        out_dir,
        zip_path,
        artifact_manifest
    )

    log(
        f"[ZIP PASS] {zip_path.name}；"
        f"bytes={zip_result['zip_bytes']:,}；"
        f"SHA256={zip_result['zip_sha256']}",
        logs
    )

    log(
        "[ZIP CRC + MANIFEST PASS] "
        "封存檔案及內部 SHA256 核對通過。",
        logs
    )

    # --------------------------------------------------------
    # T. GitHub 上傳
    # --------------------------------------------------------

    upload_files = [
        zip_path,
        source_path,
        out_dir / "README.md",
        report_path,
        out_dir / "SSO下游資料接續預檢報告.json",
        out_dir / "v1.5.10_雙市場修正替代規則.json",
        out_dir / "latest_market_dates.json",
        out_dir / "待重查日期清單.json",
        out_dir / "逐日雙市場行情驗證統計.csv",
        out_dir / "官方API下載及即時驗證索引.csv",
        out_dir / "個股增量覆蓋檔索引.csv",
        out_dir / "雙市場20261111修正替代索引.csv",
        manifest_path,
        std_path,
        correction_output["TWSE"]["root"],
        correction_output["TWSE"]["nested"],
        correction_output["TPEX"]["root"],
        correction_output["TPEX"]["nested"],
        target_twse_raw
    ]

    # 本次 API 原始回應存在時一併上傳。
    for market in ("TWSE", "TPEX"):

        raw_dir = (
            out_dir / "raw" / market
        )

        if raw_dir.exists():

            upload_files.extend(
                sorted(raw_dir.glob("*.json.gz"))
            )

    # 去除重複路徑。
    unique_upload_files = []

    seen_paths = set()

    for p in upload_files:

        p = Path(p)

        if not p.is_file():
            continue

        if p in seen_paths:
            continue

        seen_paths.add(p)
        unique_upload_files.append(p)

    github_result = None

    try:

        github_result = github_upload(
            unique_upload_files,
            run_id,
            logs
        )

        write_json(
            out_dir / "GitHub上傳結果.json",
            github_result
        )

    except Exception as error:

        github_result = {
            "status": "FAILED",
            "error": (
                f"{type(error).__name__}: {error}"
            )
        }

        write_json(
            out_dir / "GitHub上傳結果.json",
            github_result
        )

        log(
            f"[GITHUB ERROR] {github_result['error']}",
            logs
        )

    # --------------------------------------------------------
    # U. 舊資料保護再確認
    # --------------------------------------------------------

    if (
        sha256_file(V159_SEALED_ZIP)
        != EXPECTED_V159_ZIP_SHA256
    ):

        raise RuntimeError(
            "v1.5.9 原始封存 ZIP SHA 已改變。"
        )

    if (
        sha256_file(V159_DIR / "修正_TWSE_20261111.csv")
        != EXPECTED_TWSE_CORRECTION_FILE_SHA256
    ):

        raise RuntimeError(
            "v1.5.9 TWSE 原始修正檔 SHA 已改變。"
        )

    if (
        sha256_file(V159_DIR / "修正_TPEX_20261111.csv")
        != EXPECTED_TPEX_CORRECTION_FILE_SHA256
    ):

        raise RuntimeError(
            "v1.5.9 TPEx 原始修正檔 SHA 已改變。"
        )

    # --------------------------------------------------------
    # V. 最終輸出
    # --------------------------------------------------------

    print("\n" + "=" * 88)

    print(
        f"{VERSION}：{status}"
    )

    print(
        "v1.5.9 雙市場修正承接：PASS"
    )

    print(
        f"TWSE 2026-11-11 修正："
        f"{EXPECTED_TWSE_CORRECTION_ROWS:,} 筆"
    )

    print(
        f"TPEx 2026-11-11 修正："
        f"{EXPECTED_TPEX_CORRECTION_ROWS:,} 筆"
    )

    print(
        f"前瞻新增行情：{len(forward_records):,} 列"
    )

    print(
        f"TWSE 前瞻新增："
        f"{sum(r['market'] == 'TWSE' for r in forward_records):,}"
    )

    print(
        f"TPEx 前瞻新增："
        f"{sum(r['market'] == 'TPEX' for r in forward_records):,}"
    )

    print(
        f"官方 API 請求：{len(api_meta)}"
    )

    print(
        f"待官方資料：{waiting_count}"
    )

    print(
        f"品質拒絕：{rejected_count}"
    )

    print(
        f"下載錯誤：{download_error_count}"
    )

    print(
        f"待重查日期：{len(unresolved_dates)}"
    )

    print(
        f"SSO 下游資料預檢：{downstream_ready}"
    )

    print(
        f"TWSE 最後行情日：{state['market_last_date']['TWSE']}"
    )

    print(
        f"TPEx 最後行情日：{state['market_last_date']['TPEX']}"
    )

    print(
        f"全市場增量：{std_path}"
    )

    print(
        f"ZIP：{zip_path}"
    )

    print(
        f"ZIP SHA256：{zip_result['zip_sha256']}"
    )

    print(
        f"Manifest SHA256：{manifest_sha256}"
    )

    print(
        f"GitHub 結果：{github_result}"
    )

    print("=" * 88)

    if (
        github_result
        and github_result.get("status") == "PASS"
        and github_result.get("main_unchanged") is True
        and github_result.get("blob_readback_pass") is True
    ):

        print(
            "[PASS] GitHub audit branch 已逐檔回讀。"
        )

        print(
            "[PASS] GitHub main 未變更。"
        )

        print(
            f"[GITHUB URL] {github_result['url']}"
        )

    else:

        print(
            "[WARNING] Drive 成果已保存；"
            "GitHub 尚未完整通過。"
        )

    print(
        f"[OUTPUT DIRECTORY] {out_dir}"
    )

    print("=" * 88)

    return report


# ============================================================
# 17. 執行入口
# ============================================================

RESULT_V1510 = run_v1510()