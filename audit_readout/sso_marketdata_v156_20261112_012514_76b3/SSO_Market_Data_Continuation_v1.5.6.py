# -*- coding: utf-8 -*-
# SSO_Market_Data_Continuation_TWSE_TPEX_v1.5.6
# 保留官方行情下載，擴充資料檢查、修正承接、待重查及封存驗證。

from __future__ import annotations

import os
import re
import csv
import io
import json
import gzip
import time
import uuid
import base64
import hashlib
import zipfile
import statistics
import shutil
from pathlib import Path
from datetime import datetime, date, timedelta, time as dt_time
from zoneinfo import ZoneInfo
from collections import defaultdict

import requests
import pandas as pd
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# ============================================================
# 1. 基本設定
# ============================================================

VERSION = "SSO_Market_Data_Continuation_TWSE_TPEX_v1.5.6"

ROOT = Path(
    "/content/drive/MyDrive/看股最速適/"
    "SSO_Market_Data_Continuation_TWSE_TPEX_v1"
)

REPO = "CHLINX/sso"
TZ = ZoneInfo("Asia/Taipei")

MARKET_CLOSE_CUTOFF = dt_time(17, 0)

CORRECTION_MARKET = "TPEX"
CORRECTION_DATE = date(2026, 11, 11)
EXPECTED_CORRECTION_ROWS = 1020

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
        "instrument_code", "code", "stock_id", "stock_code",
        "證券代號", "證券代碼", "代號", "股票代號"
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
        "amount", "成交金額", "成交金額元", "成交金額(元)"
    ],
    "price_change": [
        "price_change", "change", "漲跌", "漲跌價差"
    ],
    "transactions": [
        "transactions", "transaction_count", "transaction",
        "成交筆數", "筆數", "交易筆數"
    ],
    "source": [
        "source", "source_archive_member", "來源", "資料來源"
    ]
}


# ============================================================
# 3. 基礎工具
# ============================================================

def mount_drive():
    if not Path("/content/drive/MyDrive").exists():
        from google.colab import drive
        drive.mount("/content/drive", force_remount=False)

    ROOT.mkdir(parents=True, exist_ok=True)


def log(message, logs):
    line = (
        f"[{datetime.now(TZ).strftime('%H:%M:%S')}] "
        f"{message}"
    )
    print(line, flush=True)
    logs.append(line)


def norm(value):
    return re.sub(
        r"[\s\u3000_\-（）()【】\[\]／/：:、,.%]+",
        "",
        str(value).strip().lower()
    )


def market_norm(value):
    value = str(value).strip().upper()

    if value in {"TWSE", "上市", "集中市場"}:
        return "TWSE"

    if value in {"TPEX", "TPEx", "上櫃", "櫃買"}:
        return "TPEX"

    return value


def parse_date(value):
    if value is None:
        return None

    s = str(value).strip()

    if not s:
        return None

    m = re.fullmatch(
        r"(\d{2,3})[/-]?(\d{1,2})[/-]?(\d{1,2})",
        s
    )

    if m:
        y, mo, da = map(int, m.groups())

        if y < 1911:
            y += 1911

        try:
            return date(y, mo, da)
        except Exception:
            return None

    try:
        return pd.to_datetime(s, errors="raise").date()
    except Exception:
        return None


def safe_num(value, integer=False):
    # 修正舊版 0 被當成空值的問題。
    if value is None:
        return None

    s = (
        str(value)
        .strip()
        .replace(",", "")
        .replace("＋", "+")
        .replace("－", "-")
    )

    if s in {"", "-", "--", "---", "X", "x", "N/A", "nan", "None"}:
        return None

    try:
        number = float(s)

        if not pd.notna(number):
            return None

        if integer:
            return int(round(number))

        return number

    except Exception:
        return None


def find_col(columns, aliases):
    lookup = {norm(c): c for c in columns}

    for alias in aliases:
        key = norm(alias)

        if key in lookup:
            return lookup[key]

    return None


def find_value(row, key):
    column = find_col(row.keys(), ALIASES[key])

    if column is None:
        return None

    return row.get(column)


def sha256_bytes(data):
    return hashlib.sha256(data).hexdigest()


def sha256_file(path):
    return sha256_bytes(Path(path).read_bytes())


def canonical_records_sha256(records):
    ordered = sorted(
        records,
        key=lambda x: (
            x.get("trade_date", ""),
            x.get("market", ""),
            x.get("instrument_code", "")
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


def read_header(path):
    raw = Path(path).read_bytes()

    for encoding in ("utf-8-sig", "cp950", "big5", "gb18030"):
        try:
            return next(
                csv.reader(io.StringIO(raw.decode(encoding)))
            )
        except Exception:
            continue

    raise RuntimeError(f"無法辨識 CSV 表頭：{path}")


def validate_schema(columns, label, standard=True):
    required = [
        "trade_date", "open", "high", "low", "close", "volume"
    ]

    if standard:
        required += ["market", "instrument_code"]

    missing = []

    for key in required:
        if find_col(columns, ALIASES[key]) is None:
            missing.append(key)

    if missing:
        raise RuntimeError(
            f"{label} 欄位不完整；缺少={missing}；"
            f"現有欄位={list(columns)}"
        )


def record_to_target(record, columns):
    result = {}

    for column in columns:
        matched_key = None

        for key, aliases in ALIASES.items():
            if norm(column) in {norm(x) for x in aliases}:
                matched_key = key
                break

        value = record.get(matched_key, "") if matched_key else ""

        result[column] = "" if value is None else value

    return result


def write_csv(path, records, columns):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open(
        "w",
        encoding="utf-8-sig",
        newline=""
    ) as f:

        writer = csv.DictWriter(
            f,
            fieldnames=list(columns),
            extrasaction="ignore"
        )

        writer.writeheader()

        for record in records:
            writer.writerow(
                record_to_target(record, columns)
            )


def read_normalized_csv(path):
    path = Path(path)

    with path.open(
        "r",
        encoding="utf-8-sig",
        newline=""
    ) as f:

        reader = csv.DictReader(f)

        columns = reader.fieldnames or []
        validate_schema(columns, path.name, standard=True)

        records = []

        for row in reader:
            record = {
                "trade_date": (
                    parse_date(find_value(row, "trade_date")).isoformat()
                    if parse_date(find_value(row, "trade_date"))
                    else ""
                ),
                "market": market_norm(find_value(row, "market")),
                "instrument_code": str(
                    find_value(row, "instrument_code") or ""
                ).strip(),
                "instrument_name": str(
                    find_value(row, "instrument_name") or ""
                ).strip(),
                "open": safe_num(find_value(row, "open")),
                "high": safe_num(find_value(row, "high")),
                "low": safe_num(find_value(row, "low")),
                "close": safe_num(find_value(row, "close")),
                "volume": safe_num(
                    find_value(row, "volume"),
                    integer=True
                ),
                "trade_value": safe_num(
                    find_value(row, "trade_value"),
                    integer=True
                ),
                "price_change": safe_num(
                    find_value(row, "price_change")
                ),
                "transactions": safe_num(
                    find_value(row, "transactions"),
                    integer=True
                ),
                "source": str(
                    find_value(row, "source") or ""
                ).strip()
            }

            records.append(record)

    return columns, records


# ============================================================
# 4. 版本資料夾與既有欄位
# ============================================================

def version_dirs():
    dirs = []

    for path in ROOT.iterdir():
        if not path.is_dir():
            continue

        match = re.match(r"v1\.5\.(\d+)_", path.name)

        if match:
            dirs.append((int(match.group(1)), path))

    dirs.sort(key=lambda x: (x[0], x[1].name))

    return [x[1] for x in dirs]


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

    # 歷史筆數基準只取 v1.5.1。
    for directory in dirs:
        if not directory.name.startswith("v1.5.1_"):
            continue

        stat_files = list(
            directory.glob("**/新增九個交易日_逐日雙市場統計.csv")
        )

        for path in stat_files:
            try:
                df = pd.read_csv(
                    path,
                    dtype=str,
                    encoding="utf-8-sig"
                )

                market_col = find_col(df.columns, ALIASES["market"])
                rows_col = find_col(
                    df.columns,
                    ["rows", "row_count", "筆數", "資料筆數"]
                )

                if not market_col or not rows_col:
                    continue

                for _, row in df.iterrows():
                    market = market_norm(row[market_col])
                    number = safe_num(row[rows_col], integer=True)

                    if (
                        market in baseline_values
                        and number is not None
                        and number >= MIN_ROWS_ABSOLUTE
                    ):
                        baseline_values[market].append(number)

            except Exception as error:
                print(f"[BASELINE WARN] {path.name}: {error}")

    # 讀取既有正式 CSV 欄位。
    for directory in reversed(dirs):
        std_files = list(
            directory.glob("**/全市場標準化增量_*.csv")
        )

        if standard_columns is None and std_files:
            standard_columns = read_header(std_files[-1])

        overlay_dir = directory / "overlay" / "官方歷史行情"

        if overlay_dir.exists():
            for market, pattern in (
                ("TWSE", "*_TWSE.csv"),
                ("TPEX", "*_TPEx.csv")
            ):
                if overlay_columns[market] is None:
                    files = list(overlay_dir.glob(pattern))

                    if files:
                        overlay_columns[market] = read_header(files[0])

        if standard_columns and all(overlay_columns.values()):
            break

    if standard_columns is None:
        raise RuntimeError("找不到既有全市場標準化增量欄位格式。")

    for market in ("TWSE", "TPEX"):
        if overlay_columns[market] is None:
            overlay_columns[market] = list(standard_columns)

    validate_schema(
        standard_columns,
        "既有全市場標準化增量",
        standard=True
    )

    for market in ("TWSE", "TPEX"):
        validate_schema(
            overlay_columns[market],
            f"{market} 個股覆蓋檔",
            standard=True
        )

    baseline = {}

    for market in ("TWSE", "TPEX"):
        values = baseline_values[market]

        if values:
            baseline[market] = int(statistics.median(values))
        else:
            baseline[market] = (
                1400 if market == "TWSE" else 1020
            )

    return baseline, standard_columns, overlay_columns


# ============================================================
# 5. 下載資料品質檢查
# ============================================================

def validate_records(
    records,
    market,
    day,
    expected_rows,
    exact_rows=None,
    require_tpex_ew=False
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
        code for code in codes
        if not re.fullmatch(
            rf"[0-9A-Za-z.\-]{{{MIN_CODE_LENGTH},{MAX_CODE_LENGTH}}}",
            code
        )
    ]

    if n < MIN_ROWS_ABSOLUTE:
        errors.append(f"資料筆數低於下限：{n}")

    if n > MAX_ROWS_ABSOLUTE:
        errors.append(f"資料筆數超過上限：{n}")

    if expected_rows:
        lower = expected_rows * MIN_COUNT_RATIO
        upper = expected_rows * MAX_COUNT_RATIO

        if not lower <= n <= upper:
            errors.append(
                f"資料筆數偏離歷史母體：本次={n}；"
                f"基準={expected_rows}；允許={lower:.0f}~{upper:.0f}"
            )

    if exact_rows is not None and n != exact_rows:
        errors.append(
            f"指定修正資料筆數不符：應為 {exact_rows}，實際 {n}"
        )

    if duplicate_count:
        errors.append(f"重複證券代號列數={duplicate_count}")

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
        errors.append(f"交易日期不符列數={wrong_date}")

    if wrong_market:
        errors.append(f"市場代碼不符列數={wrong_market}")

    missing_volume = sum(
        r.get("volume") is None
        for r in records
    )

    negative_volume = sum(
        r.get("volume") is not None and r["volume"] < 0
        for r in records
    )

    if missing_volume:
        errors.append(f"成交量缺值列數={missing_volume}")

    if negative_volume:
        errors.append(f"負成交量列數={negative_volume}")

    zero_volume = sum(
        r.get("volume") == 0
        for r in records
    )

    zero_ratio = zero_volume / n if n else 0

    if zero_ratio > MAX_ZERO_VOLUME_RATIO:
        errors.append(
            f"零成交量比例異常：{zero_ratio:.2%}"
        )

    missing_ohlc = sum(
        any(r.get(k) is None for k in ("open", "high", "low", "close"))
        for r in records
    )

    missing_ohlc_ratio = missing_ohlc / n if n else 0

    if missing_ohlc_ratio > MAX_MISSING_OHLC_RATIO:
        errors.append(
            f"OHLC 缺值比例異常：{missing_ohlc_ratio:.2%}"
        )

    bad_price = 0
    complete_price = 0

    for record in records:
        values = [
            record.get(k)
            for k in ("open", "high", "low", "close")
        ]

        if any(value is None for value in values):
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
            f"{bad_price}/{complete_price} ({bad_price_ratio:.2%})"
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
        errors.append(f"負成交金額列數={negative_trade_value}")

    if negative_transactions:
        errors.append(f"負成交筆數列數={negative_transactions}")

    source_ew_fail = 0

    if require_tpex_ew:
        source_ew_fail = sum(
            "type_EW" not in str(r.get("source", ""))
            for r in records
        )

        if source_ew_fail:
            errors.append(
                f"修正資料來源不是 TPEx type=EW 的列數={source_ew_fail}"
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
# 6. 官方 API 解析
# ============================================================

def make_session():
    session = requests.Session()

    retry = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=1.0,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )

    adapter = HTTPAdapter(max_retries=retry)

    session.mount("https://", adapter)
    session.mount("http://", adapter)

    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 "
            "(compatible; SSO-Market-Data/1.5.5)"
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
        "sign": ["漲跌(+/-)", "漲跌符號", "漲跌記號"]
    }

    required = [
        "code", "open", "high", "low", "close", "volume"
    ]

    candidates = []

    for table in extract_tables(payload):
        fields = table["fields"]

        mapping = {
            key: find_col(fields, aliases)
            for key, aliases in field_aliases.items()
        }

        if all(mapping.get(key) is not None for key in required):
            candidates.append((table, mapping))

    if not candidates:
        tables = extract_tables(payload)

        if not any(table.get("data") for table in tables):
            return [], "NO_DATA", "官方端點沒有行情列", "", {}

        raise RuntimeError(
            f"{market} 找不到完整 OHLC/成交量欄位表"
        )

    candidates.sort(
        key=lambda item: len(item[0].get("data", [])),
        reverse=True
    )

    table, mapping = candidates[0]
    fields = table["fields"]

    indexes = {
        key: fields.index(value) if value in fields else None
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

            if index is not None and index < len(row):
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

        if not re.fullmatch(r"[0-9A-Za-z.\-]{2,16}", code):
            parse_stats["invalid_code_rows"] += 1
            continue

        change = safe_num(cell("change"))

        if indexes.get("sign") is not None and change is not None:
            sign = str(cell("sign") or "").strip()

            if sign == "-":
                change = -abs(change)
            elif sign == "+":
                change = abs(change)

        records.append({
            "trade_date": day.isoformat(),
            "market": market,
            "instrument_code": code,
            "instrument_name": str(cell("name") or "").strip(),
            "open": safe_num(cell("open")),
            "high": safe_num(cell("high")),
            "low": safe_num(cell("low")),
            "close": safe_num(cell("close")),
            "volume": safe_num(cell("volume"), integer=True),
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
        table.get("title", table.get("note", ""))
    )

    return records, "PARSED", "", title, parse_stats


# ============================================================
# 7. 下載後立即驗證
# ============================================================

def download_and_validate(
    session,
    market,
    day,
    expected_rows,
    out_dir,
    logs,
    exact_rows=None,
    require_tpex_ew=False
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
        "validation": {}
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

        raw_dir = Path(out_dir) / "raw" / market
        raw_dir.mkdir(parents=True, exist_ok=True)

        raw_path = raw_dir / f"{day.strftime('%Y%m%d')}.json.gz"

        compressed = gzip.compress(
            raw,
            compresslevel=6,
            mtime=0
        )

        raw_path.write_bytes(compressed)

        meta["raw_gzip_sha256"] = sha256_bytes(compressed)
        meta["raw_file"] = str(
            raw_path.relative_to(out_dir)
        ).replace("\\", "/")

        # 立即確認 gzip 原始封存可正常解壓。
        if gzip.decompress(compressed) != raw:
            raise RuntimeError("gzip 原始回應解壓比對失敗")

        response.raise_for_status()

        content_type = response.headers.get(
            "Content-Type", ""
        )

        if (
            "html" in content_type.lower()
            or raw.lstrip().lower().startswith(
                (b"<html", b"<!doctype html")
            )
        ):
            raise RuntimeError("官方回應為 HTML，不是 JSON 行情")

        payload = response.json()

        (
            records,
            parse_status,
            parse_error,
            title,
            parse_stats
        ) = parse_payload(payload, market, day)

        meta["table_title"] = title
        meta["parse_stats"] = parse_stats

        if parse_status == "NO_DATA":
            meta["status"] = "WAITING_FOR_OFFICIAL_DATA"

            meta["error"] = (
                f"{parse_error}；原始回應已保存，"
                "不推定休市、不寫入增量，後續將重新查詢。"
            )

            meta["validation"] = {
                "rows": 0,
                "validation_errors": []
            }

            log(
                f"[WAITING] {market} {day}："
                "官方目前沒有行情列，保留原始回應。",
                logs
            )

            return [], meta

        passed, stats = validate_records(
            records,
            market,
            day,
            expected_rows,
            exact_rows=exact_rows,
            require_tpex_ew=require_tpex_ew
        )

        stats["parse_stats"] = parse_stats

        meta["validation"] = stats

        # 解析期間有大量不合法代號時，拒絕寫入。
        invalid_limit = max(
            10,
            int(parse_stats["table_rows"] * 0.05)
        )

        if parse_stats["invalid_code_rows"] > invalid_limit:
            passed = False
            stats["validation_errors"].append(
                "原始表格不合法證券代號過多："
                f"{parse_stats['invalid_code_rows']}"
            )

        if not passed:
            meta["status"] = "REJECTED"
            meta["error"] = "; ".join(
                stats["validation_errors"]
            )

            log(
                f"[REJECTED] {market} {day}：{meta['error']}",
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
            f"OHLC缺值={stats['missing_ohlc_ratio']:.2%}",
            logs
        )

        return records, meta

    except Exception as error:
        meta["status"] = "ERROR"
        meta["error"] = (
            f"{type(error).__name__}: {error}"
        )

        log(
            f"[DOWNLOAD ERROR] {market} {day}：{meta['error']}",
            logs
        )

        return [], meta


# ============================================================
# 8. 既有行情日期及待重查日期
# ============================================================

def read_previous_state(dirs):
    latest = {
        "TWSE": None,
        "TPEX": None
    }

    completed = {
        "TWSE": set(),
        "TPEX": set()
    }

    previous_rows = 0
    previous_bad_rows = 0

    for directory in dirs:
        files = list(
            directory.glob("**/全市場標準化增量_*.csv")
        )

        for path in files:
            try:
                _, records = read_normalized_csv(path)

                for record in records:
                    market = market_norm(record.get("market"))
                    day = parse_date(record.get("trade_date"))
                    code = str(
                        record.get("instrument_code", "")
                    ).strip()

                    if market not in latest or day is None or not code:
                        previous_bad_rows += 1
                        continue

                    previous_rows += 1

                    completed[market].add(day)

                    if (
                        latest[market] is None
                        or day > latest[market]
                    ):
                        latest[market] = day

            except Exception as error:
                print(
                    f"[PREVIOUS DATA WARN] {path.name}: {error}"
                )

    if any(value is None for value in latest.values()):
        raise RuntimeError(
            f"無法辨識雙市場最後行情日期：{latest}"
        )

    return latest, completed, {
        "previous_rows_scanned": previous_rows,
        "previous_bad_rows": previous_bad_rows
    }


def read_previous_pending(dirs):
    pending = {}

    for directory in dirs:
        report_files = list(
            directory.glob("**/v1.5.*_官方增量行情續接報告.json")
        )

        for path in report_files:
            try:
                obj = json.loads(
                    path.read_text(encoding="utf-8")
                )

                items = []

                for item_key in (
                    "unresolved_dates",
                    "unresolved_weekday_no_data",
                    "deferred_not_yet_due_dates"
                ):
                    values = obj.get(item_key) or []

                    if isinstance(values, list):
                        items.extend(values)

                for item in items:
                    market = market_norm(item.get("market"))
                    day = parse_date(item.get("trade_date"))

                    if market not in {"TWSE", "TPEX"} or day is None:
                        continue

                    key = (market, day)

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
                        )
                    }

            except Exception as error:
                print(
                    f"[PENDING READ WARN] {path.name}: {error}"
                )

    return pending


# ============================================================
# 9. 既有 TPEx 修正資料承接
# ============================================================

def load_existing_correction(
    dirs,
    baseline,
    logs
):
    candidates = []

    for directory in reversed(dirs):
        paths = [
            directory / "修正_TPEX_20261111.csv",
            (
                directory
                / "corrections"
                / "官方歷史行情"
                / "TPEX"
                / "20261111.csv"
            )
        ]

        for path in paths:
            if not path.is_file():
                continue

            try:
                raw_sha = sha256_file(path)

                columns, records = read_normalized_csv(path)

                passed, stats = validate_records(
                    records,
                    "TPEX",
                    CORRECTION_DATE,
                    baseline["TPEX"],
                    exact_rows=EXPECTED_CORRECTION_ROWS,
                    require_tpex_ew=True
                )

                if not passed:
                    log(
                        f"[CORRECTION SOURCE INVALID] "
                        f"{path}：{stats['validation_errors']}",
                        logs
                    )
                    continue

                canonical_sha = canonical_records_sha256(records)

                manifest_source = None

                manifests = list(
                    directory.glob("**/v1.5.*_修正替代規則.json")
                )

                for manifest_path in manifests:
                    try:
                        manifest_obj = json.loads(
                            manifest_path.read_text(encoding="utf-8")
                        )

                        if (
                            manifest_obj.get("correction_pass") is True
                            and manifest_obj.get("correct_query_type") == "EW"
                            and manifest_obj.get("correction_date")
                            == CORRECTION_DATE.isoformat()
                        ):
                            manifest_source = str(manifest_path)
                            break

                    except Exception:
                        continue

                candidates.append({
                    "path": path,
                    "directory": directory,
                    "columns": columns,
                    "records": records,
                    "stats": stats,
                    "source_file_sha256": raw_sha,
                    "canonical_sha256": canonical_sha,
                    "source_manifest": manifest_source
                })

                log(
                    f"[CORRECTION SOURCE CHECK PASS] "
                    f"{path.name}：{len(records):,} 筆；"
                    f"唯一代號={stats['unique_codes']:,}；"
                    f"SHA256={raw_sha}",
                    logs
                )

            except Exception as error:
                log(
                    f"[CORRECTION SOURCE ERROR] "
                    f"{path}：{error}",
                    logs
                )

    if not candidates:
        log(
            "[CORRECTION SOURCE] 找不到可完整驗證的既有修正檔；"
            "將使用官方 API 重新下載修正日期。",
            logs
        )
        return None

    hashes = {
        item["canonical_sha256"]
        for item in candidates
    }

    if len(hashes) != 1:
        log(
            "[CORRECTION SOURCE CONFLICT] "
            "不同版本的修正資料內容不一致；"
            "不沿用衝突檔，改由官方 API 重新下載。",
            logs
        )
        return None

    selected = candidates[0]

    log(
        "[CORRECTION REUSE PASS] "
        f"採用 {selected['directory'].name} 的既有修正資料。",
        logs
    )

    return selected


# ============================================================
# 10. 寫入後再次驗證
# ============================================================

def verify_csv_roundtrip(path, expected_records, market, day):
    columns, actual_records = read_normalized_csv(path)

    if len(actual_records) != len(expected_records):
        raise RuntimeError(
            f"{path.name} 寫入後筆數不符："
            f"{len(actual_records)} != {len(expected_records)}"
        )

    expected_keys = {
        (
            r["trade_date"],
            r["market"],
            r["instrument_code"]
        )
        for r in expected_records
    }

    actual_keys = {
        (
            r["trade_date"],
            r["market"],
            r["instrument_code"]
        )
        for r in actual_records
    }

    if expected_keys != actual_keys:
        raise RuntimeError(
            f"{path.name} 寫入後證券鍵值不一致"
        )

    if any(
        r["trade_date"] != day.isoformat()
        or r["market"] != market
        for r in actual_records
    ):
        raise RuntimeError(
            f"{path.name} 寫入後日期或市場錯誤"
        )

    return {
        "rows": len(actual_records),
        "unique_keys": len(actual_keys),
        "sha256": sha256_file(path)
    }


def verify_all_forward_records(records):
    keys = [
        (
            r["market"],
            r["trade_date"],
            r["instrument_code"]
        )
        for r in records
    ]

    duplicates = len(keys) - len(set(keys))

    if duplicates:
        raise RuntimeError(
            f"本次雙市場增量出現重複鍵值：{duplicates}"
        )


# ============================================================
# 11. JSON、來源程式及 ZIP 封存
# ============================================================

def write_json(path, obj):
    Path(path).write_text(
        json.dumps(
            obj,
            ensure_ascii=False,
            indent=2,
            default=str
        ),
        encoding="utf-8"
    )


def save_current_source(out_dir, logs):

    target = (
        Path(out_dir)
        / "SSO_Market_Data_Continuation_v1.5.6.py"
    )

    target.write_text(
        __SSO_MARKET_DATA_SOURCE__,
        encoding="utf-8"
    )

    log(
        "[SOURCE PASS] "
        "v1.5.6 完整實際執行程式已封存。",
        logs
    )

    return target


def build_artifact_manifest(out_dir):
    out_dir = Path(out_dir)

    items = []

    for path in sorted(out_dir.rglob("*")):
        if not path.is_file():
            continue

        if path.name == "artifact_sha256_manifest.json":
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
        "files": items
    }

    write_json(
        out_dir / "artifact_sha256_manifest.json",
        manifest
    )

    return manifest


def make_and_verify_zip(out_dir, zip_path, manifest):
    out_dir = Path(out_dir)
    zip_path = Path(zip_path)

    with zipfile.ZipFile(
        zip_path,
        "w",
        compression=zipfile.ZIP_DEFLATED,
        compresslevel=6
    ) as zf:

        for path in sorted(out_dir.rglob("*")):
            if not path.is_file() or path == zip_path:
                continue

            zf.write(
                path,
                path.relative_to(out_dir).as_posix()
            )

    with zipfile.ZipFile(zip_path, "r") as zf:
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
# 12. GitHub 獨立 audit branch
# ============================================================

def github_upload(files, run_id, logs):
    token = os.environ.get("GITHUB_TOKEN", "").strip()

    if not token:
        try:
            from google.colab import userdata
            token = str(
                userdata.get("GITHUB_TOKEN") or ""
            ).strip()
        except Exception:
            token = ""

    if not token:
        raise RuntimeError("找不到 GITHUB_TOKEN")

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
            timeout=(20, 120),
            **kwargs
        )

        if not response.ok:
            raise RuntimeError(
                f"GitHub API HTTP {response.status_code}: "
                f"{response.text[:1500]}"
            )

        return response.json() if response.content else {}

    owner, repo = REPO.split("/")

    # 記錄 main 原始 SHA。
    main_ref = req(
        "GET",
        f"/repos/{owner}/{repo}/git/ref/heads/main"
    )

    main_sha = main_ref["object"]["sha"]

    main_commit = req(
        "GET",
        f"/repos/{owner}/{repo}/git/commits/{main_sha}"
    )

    base_tree = main_commit["tree"]["sha"]

    branch = f"audit/sso-marketdata-v156-{run_id}"

    prefix = f"audit_readout/sso_marketdata_v156_{run_id}"

    entries = []
    expected = []

    for path in files:
        path = Path(path)
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
                "content": base64.b64encode(data).decode("ascii"),
                "encoding": "base64"
            }
        )

        if blob["sha"] != local_blob_sha:
            raise RuntimeError(
                f"GitHub blob SHA 不一致：{path.name}"
            )

        remote_path = f"{prefix}/{path.name}"

        entries.append({
            "path": remote_path,
            "mode": "100644",
            "type": "blob",
            "sha": blob["sha"]
        })

        expected.append({
            "path": remote_path,
            "sha": blob["sha"],
            "sha256": sha256_bytes(data),
            "bytes": len(data)
        })

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
                f"SSO market data v1.5.6 "
                f"validated incremental update {run_id}"
            ),
            "tree": tree["sha"],
            "parents": [main_sha]
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

    # 逐檔 GitHub blob 回讀。
    for item in expected:
        remote_blob = req(
            "GET",
            f"/repos/{owner}/{repo}/git/blobs/{item['sha']}"
        )

        remote_data = base64.b64decode(
            remote_blob["content"]
        )

        if sha256_bytes(remote_data) != item["sha256"]:
            raise RuntimeError(
                f"GitHub 回讀 SHA256 不一致：{item['path']}"
            )

    # 確認 branch head。
    branch_ref = req(
        "GET",
        f"/repos/{owner}/{repo}/git/ref/heads/"
        + branch.replace("/", "%2F")
    )

    branch_head = branch_ref["object"]["sha"]

    if branch_head != commit["sha"]:
        raise RuntimeError(
            "GitHub audit branch head 與本次 commit 不一致"
        )

    # 確認 main 完全沒變更。
    main_after = req(
        "GET",
        f"/repos/{owner}/{repo}/git/ref/heads/main"
    )["object"]["sha"]

    main_unchanged = main_sha == main_after

    result = {
        "branch": branch,
        "commit_sha": commit["sha"],
        "branch_head_sha": branch_head,
        "main_before": main_sha,
        "main_after": main_after,
        "main_unchanged": main_unchanged,
        "file_count": len(expected),
        "blob_readback_pass": True,
        "branch_head_readback_pass": True,
        "url": (
            f"https://github.com/{REPO}/tree/"
            f"{branch}/{prefix}"
        )
    }

    log(
        f"[GITHUB PASS] branch={branch}；"
        f"files={len(expected)}",
        logs
    )

    log(
        f"[GITHUB READBACK PASS] "
        "全部檔案 SHA256 已回讀核對。",
        logs
    )

    log(
        f"[GITHUB MAIN] unchanged={main_unchanged}",
        logs
    )

    return result


# ============================================================
# 13. 主流程
# ============================================================

def run_v156():
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

    log(f"啟動：{VERSION}", logs)

    log(
        f"台灣時間：{now.isoformat()}；"
        f"當日查詢門檻：17:00；"
        f"本次查詢截止日：{target_end_date}",
        logs
    )

    if not same_day_allowed:
        log(
            f"[SAME-DAY GATE] 尚未到 17:00，"
            f"跳過 {today} 當日行情查詢。",
            logs
        )

    dirs = version_dirs()

    baseline, standard_cols, overlay_cols = (
        load_baseline_and_schema(dirs)
    )

    latest, completed, previous_scan = (
        read_previous_state(dirs)
    )

    pending = read_previous_pending(dirs)

    out_name = (
        f"v1.5.6_官方行情即時驗證與增量續接_{run_id}"
    )

    out_dir = ROOT / out_name
    out_dir.mkdir(parents=True, exist_ok=False)

    log(f"資料根目錄：{ROOT}", logs)

    log(
        f"TWSE 最後行情日：{latest['TWSE']}",
        logs
    )

    log(
        f"TPEx 最後行情日：{latest['TPEX']}",
        logs
    )

    log(
        f"歷史合理筆數基準："
        f"TWSE={baseline['TWSE']}；"
        f"TPEX={baseline['TPEX']}",
        logs
    )

    log(
        f"既有增量掃描："
        f"{previous_scan['previous_rows_scanned']:,} 列；"
        f"異常列={previous_scan['previous_bad_rows']:,}",
        logs
    )

    # --------------------------------------------------------
    # A. 已知修正資料承接
    # --------------------------------------------------------

    correction_source = load_existing_correction(
        dirs,
        baseline,
        logs
    )

    correction_records = []
    correction_meta = None

    correction_reused = False
    correction_pass = False

    correction_source_version = None
    correction_source_path = None
    correction_source_file_sha256 = None
    correction_canonical_sha256 = None

    if correction_source is not None:
        correction_records = correction_source["records"]

        correction_reused = True
        correction_pass = True

        correction_source_version = (
            correction_source["directory"].name
        )

        correction_source_path = str(
            correction_source["path"]
        )

        correction_source_file_sha256 = (
            correction_source["source_file_sha256"]
        )

        correction_canonical_sha256 = (
            correction_source["canonical_sha256"]
        )

        # 原始來源檔以原始位元組複製，不改動格式。
        root_correction_path = (
            out_dir / "修正_TPEX_20261111.csv"
        )

        shutil.copy2(
            correction_source["path"],
            root_correction_path
        )

        # 個股 overlay 另依既有正式欄位輸出。
        nested_correction_path = (
            out_dir
            / "corrections"
            / "官方歷史行情"
            / "TPEX"
            / "20261111.csv"
        )

        write_csv(
            nested_correction_path,
            correction_records,
            overlay_cols["TPEX"]
        )

        # 重新讀取兩份輸出檔核對。
        verify_csv_roundtrip(
            root_correction_path,
            correction_records,
            "TPEX",
            CORRECTION_DATE
        )

        verify_csv_roundtrip(
            nested_correction_path,
            correction_records,
            "TPEX",
            CORRECTION_DATE
        )

        log(
            f"[CORRECTION CARRY-FORWARD PASS] "
            f"沿用 {len(correction_records):,} 筆既有修正資料；"
            f"來源={correction_source_version}",
            logs
        )

    else:
        log(
            "[CORRECTION DOWNLOAD] "
            "既有修正檔不存在或未通過檢查；"
            "使用官方 API 重新下載 2026-11-11 TPEx type=EW。",
            logs
        )

        session = make_session()

        correction_records, correction_meta = (
            download_and_validate(
                session,
                CORRECTION_MARKET,
                CORRECTION_DATE,
                baseline["TPEX"],
                out_dir,
                logs,
                exact_rows=EXPECTED_CORRECTION_ROWS,
                require_tpex_ew=True
            )
        )

        correction_pass = (
            correction_meta.get("status") == "PASS"
        )

        if correction_pass:
            root_correction_path = (
                out_dir / "修正_TPEX_20261111.csv"
            )

            nested_correction_path = (
                out_dir
                / "corrections"
                / "官方歷史行情"
                / "TPEX"
                / "20261111.csv"
            )

            write_csv(
                root_correction_path,
                correction_records,
                standard_cols
            )

            write_csv(
                nested_correction_path,
                correction_records,
                overlay_cols["TPEX"]
            )

            correction_source_version = VERSION
            correction_source_path = "本次官方 API 重新下載"
            correction_source_file_sha256 = sha256_file(
                root_correction_path
            )

            correction_canonical_sha256 = (
                canonical_records_sha256(correction_records)
            )

            verify_csv_roundtrip(
                root_correction_path,
                correction_records,
                "TPEX",
                CORRECTION_DATE
            )

            verify_csv_roundtrip(
                nested_correction_path,
                correction_records,
                "TPEX",
                CORRECTION_DATE
            )

            log(
                f"[CORRECTION DOWNLOAD PASS] "
                f"重新取得 {len(correction_records):,} 筆。",
                logs
            )

        else:
            log(
                "[CORRECTION BLOCK] "
                "已知錯誤日期修正未通過；"
                "停止後續增量下載。",
                logs
            )

    correction_export_sha256 = None

    if correction_pass:
        correction_export_sha256 = sha256_file(
            out_dir / "修正_TPEX_20261111.csv"
        )

    # --------------------------------------------------------
    # B. 前瞻增量下載
    # --------------------------------------------------------

    forward_records = []
    api_meta = []
    daily_summary = []

    rejected_count = 0
    download_error_count = 0
    waiting_count = 0

    # 已成功日期及待重查日期採雙向核對。
    # 成功日期不重複下載；之前未取得資料的日期會重新查詢。
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
            "[PENDING DEFERRED] "
            f"{len(deferred_items)} 筆日期尚未到查詢門檻；"
            "保留紀錄，待日期到達後再查詢。",
            logs
        )

    if correction_pass:
        session = make_session()

        for market in ("TWSE", "TPEX"):
            candidate_days = set()

            # 先納入前次未完成的日期。
            for key in list(retry_queue):
                if key[0] == market:
                    candidate_days.add(key[1])

            # 再納入最後成功日期之後的新日期。
            cursor = latest[market] + timedelta(days=1)

            while cursor <= target_end_date:
                candidate_days.add(cursor)
                cursor += timedelta(days=1)

            for day in sorted(candidate_days):
                if day > target_end_date:
                    continue

                # 已有成功資料者不重複下載。
                if day in completed[market]:
                    retry_queue.pop((market, day), None)
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

                key = (market, day)

                if meta["status"] == "PASS":
                    forward_records.extend(records)

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
                        "duplicate_rows": v["duplicate_rows"],
                        "status": "APPENDED"
                    })

                else:
                    if meta["status"] == "REJECTED":
                        rejected_count += 1
                    elif meta["status"] == "ERROR":
                        download_error_count += 1
                    elif meta["status"] == "WAITING_FOR_OFFICIAL_DATA":
                        waiting_count += 1

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
                        "zero_volume_rows": 0,
                        "zero_volume_ratio": 0,
                        "missing_ohlc_rows": 0,
                        "missing_ohlc_ratio": 0,
                        "duplicate_rows": 0,
                        "status": meta["status"]
                    })

                time.sleep(0.3)

    else:
        log(
            "[FORWARD DOWNLOAD BLOCKED] "
            "已知修正資料尚未通過，不下載後續行情。",
            logs
        )

    verify_all_forward_records(forward_records)

    # --------------------------------------------------------
    # C. 前瞻資料寫入
    # --------------------------------------------------------

    forward_dates = sorted({
        record["trade_date"]
        for record in forward_records
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

    # 寫入後立即再讀取檢查。
    standard_roundtrip = read_normalized_csv(std_path)[1]

    if len(standard_roundtrip) != len(forward_records):
        raise RuntimeError(
            "全市場標準化增量寫入後筆數不一致。"
        )

    groups = defaultdict(list)

    for record in forward_records:
        groups[
            (
                record["market"],
                record["instrument_code"]
            )
        ].append(record)

    overlay_root = (
        out_dir / "overlay" / "官方歷史行情"
    )

    overlay_index = []

    for (market, code), rows in sorted(groups.items()):
        suffix = "TWSE" if market == "TWSE" else "TPEx"

        filename = f"{code}_{suffix}.csv"

        path = overlay_root / filename

        write_csv(
            path,
            rows,
            overlay_cols[market]
        )

        # 個股檔寫入後再讀取驗證。
        _, reread_rows = read_normalized_csv(path)

        if len(reread_rows) != len(rows):
            raise RuntimeError(
                f"個股 overlay 寫入筆數不一致：{filename}"
            )

        dates = sorted(
            row["trade_date"]
            for row in rows
        )

        overlay_index.append({
            "instrument_code": code,
            "market": market,
            "overlay_file": f"官方歷史行情/{filename}",
            "new_rows": len(rows),
            "first_new_date": dates[0],
            "last_new_date": dates[-1],
            "sha256": sha256_file(path)
        })

    log(
        f"[WRITE CHECK PASS] "
        f"標準化增量={len(forward_records):,} 列；"
        f"個股 overlay={len(overlay_index):,} 檔。",
        logs
    )

    # --------------------------------------------------------
    # D. 修正替代規則及最後日期
    # --------------------------------------------------------

    correction_manifest = {
        "version": VERSION,
        "correction_date": CORRECTION_DATE.isoformat(),
        "market": "TPEX",
        "previous_v1_5_2_query_type": "AL",
        "correct_query_type": "EW",
        "correction_pass": bool(correction_pass),
        "correction_reused": bool(correction_reused),
        "correction_source_version": correction_source_version,
        "correction_source_path": correction_source_path,
        "correction_rows": len(correction_records),
        "correction_new_rows": (
            0 if correction_reused else len(correction_records)
        ),
        "correction_unique_codes": len({
            r["instrument_code"]
            for r in correction_records
        }),
        "correction_source_file_sha256": (
            correction_source_file_sha256
        ),
        "correction_export_file_sha256": (
            correction_export_sha256
        ),
        "correction_canonical_sha256": (
            correction_canonical_sha256
        ),
        "old_v1_5_2_file_modified": False,
        "replacement_rule": (
            "分析 2026-11-11 TPEx 行情時，"
            "以本次 corrections/官方歷史行情/TPEX/20261111.csv "
            "替代 v1.5.2 同日期 TPEx 全部資料列；不可將兩者相加。"
        ),
        "reason": (
            "v1.5.2 使用 type=AL，資料母體包含權證及牛熊證；"
            "本次以 type=EW 與既有母體一致。"
        )
    }

    write_json(
        out_dir / "v1.5.6_修正替代規則.json",
        correction_manifest
    )

    # 最新資料日期由實際已成功取得的資料推算。
    updated_latest = {
        market: latest[market]
        for market in latest
    }

    for market in ("TWSE", "TPEX"):
        if completed[market]:
            updated_latest[market] = max(
                completed[market]
            )

    if correction_pass and (
        updated_latest["TPEX"] is None
        or CORRECTION_DATE > updated_latest["TPEX"]
    ):
        updated_latest["TPEX"] = CORRECTION_DATE

    state = {
        "version": VERSION,
        "updated_at_taipei": now.isoformat(),
        "market_last_date": {
            market: day.isoformat()
            for market, day in updated_latest.items()
        },
        "correction_20261111_tpex_pass": bool(correction_pass),
        "correction_rows": len(correction_records),
        "tpex_type": "EW",
        "unresolved_dates_count": len(retry_queue)
    }

    write_json(
        out_dir / "latest_market_dates.json",
        state
    )

    # --------------------------------------------------------
    # E. 待重查清單
    # --------------------------------------------------------

    unresolved_dates = sorted(
        retry_queue.values(),
        key=lambda x: (
            x["trade_date"],
            x["market"]
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
                "未取得官方資料、品質拒絕及下載錯誤均保留；"
                "不推定休市；後續執行持續重新查詢。"
            )
        }
    )

    # 將舊版 correction API 驗證紀錄與本次 API 紀錄分開。
    if correction_meta:
        api_meta.insert(0, correction_meta)

    pd.DataFrame(daily_summary).to_csv(
        out_dir / "逐日雙市場行情驗證統計.csv",
        index=False,
        encoding="utf-8-sig"
    )

    pd.DataFrame(api_meta).to_csv(
        out_dir / "官方API下載及即時驗證索引.csv",
        index=False,
        encoding="utf-8-sig"
    )

    pd.DataFrame(overlay_index).to_csv(
        out_dir / "個股增量覆蓋檔索引.csv",
        index=False,
        encoding="utf-8-sig"
    )

    # --------------------------------------------------------
    # F. 執行結論
    # --------------------------------------------------------

    if not correction_pass:
        status = "BLOCKED_CORRECTION_FAILED"

    elif (
        rejected_count
        or download_error_count
        or unresolved_dates
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
            market: day.isoformat()
            for market, day in latest.items()
        },

        "market_last_date_after_update": state["market_last_date"],

        "baseline_daily_rows_v151": baseline,

        "previous_data_scan": previous_scan,

        "tpex_query_type": "EW",

        "correction": correction_manifest,

        "forward_new_rows": len(forward_records),

        "forward_new_instruments": len(groups),

        "forward_trade_dates": forward_dates,

        "api_requests": len(api_meta),

        "rejected_download_count": rejected_count,

        "download_error_count": download_error_count,

        "waiting_official_data_count": waiting_count,

        "unresolved_dates": unresolved_dates,

        "deferred_not_yet_due_dates": deferred_items,

        "deferred_not_yet_due_count": len(
            deferred_items
        ),

        "immediate_validation": {
            "enabled": True,
            "before_write": True,
            "after_write": True,
            "checks": [
                "HTTP 狀態與 JSON 回應",
                "gzip 原始回應解壓及 SHA256",
                "官方行情欄位",
                "最低及最高列數",
                "歷史母體筆數範圍",
                "證券代號格式",
                "唯一代號與重複代號",
                "日期及市場一致性",
                "成交量缺值及負值",
                "零成交量比例",
                "OHLC 缺值比例",
                "OHLC 高低價格關係",
                "成交金額負值",
                "成交筆數負值",
                "已知修正資料來源 type=EW",
                "寫入後 CSV 再讀取",
                "個股 overlay 筆數核對",
                "待重查日期保留"
            ],
            "failed_data_written_to_increment": False,
            "raw_response_preserved": True
        },

        "preservation": {
            "v2_8_original_zip_modified": False,
            "v1_4_2_original_zip_modified": False,
            "v1_5_1_original_files_modified": False,
            "v1_5_2_original_files_modified": False,
            "v1_5_3_original_files_modified": False,
            "zero_volume_rows_deleted": False,
            "missing_ohlc_rows_deleted": False,
            "duplicate_rows_removed": False,
            "old_tpex_al_data_physically_deleted": False,
            "main_branch_modified": False
        }
    }

    write_json(
        out_dir / "v1.5.6_官方增量行情續接報告.json",
        report
    )

    # --------------------------------------------------------
    # G. README
    # --------------------------------------------------------

    readme = f"""# {VERSION}

## 執行結論

{status}

## 行情日期

- TWSE 最後行情日：{updated_latest["TWSE"]}
- TPEx 最後行情日：{updated_latest["TPEX"]}

## 官方下載

- TWSE：保留官方 API 增量下載
- TPEx：保留官方 API 增量下載
- TPEx 查詢參數：EW
- 當日下載門檻：台灣時間 17:00

## 已知修正資料承接

- 修正日期：2026-11-11
- 市場：TPEx
- 來源：{correction_source_version}
- 修正資料筆數：{len(correction_records):,}
- 新下載修正筆數：{0 if correction_reused else len(correction_records):,}
- 沿用既有修正檔：{correction_reused}
- 修正資料唯一代號：{len({r["instrument_code"] for r in correction_records}):,}

修正資料使用規則：

2026-11-11 TPEx 資料以本次修正層替代 v1.5.2 同日期 TPEx 全部資料列，不可將兩者相加。

原有 v1.5.2、v1.5.3 檔案均不修改。

## 本次新增檢查

1. 數值 0 與缺值區分。
2. 日期、市場與證券代號檢查。
3. 重複代號檢查。
4. 成交量、成交金額及成交筆數負值檢查。
5. OHLC 缺值比例及高低價格關係檢查。
6. 修正資料列數及 type=EW 來源檢查。
7. CSV 寫入後重新讀取核對。
8. ZIP CRC 與封存檔案 SHA256 核對。
9. GitHub blob 回讀及 main SHA 核對。

## 待重查機制

空回應、品質拒絕及下載錯誤不會自動視為休市。

未通過或尚未取得資料的日期會保留在待重查清單。

後續日期即使成功，也不會刪除之前未解決的日期紀錄。

已經成功取得的日期不會重複下載。

## 新增資料

- 前瞻新增行情：{len(forward_records):,} 列
- 修正資料：{len(correction_records):,} 列
- 修正資料是否沿用舊檔：{correction_reused}
- 前瞻個股 overlay：{len(overlay_index):,} 檔
- 待重查日期：{len(unresolved_dates):,} 筆
- 尚未到查詢門檻日期：{len(deferred_items):,} 筆
- 尚未到期日期不計為資料缺漏，仍保留後續查詢。

## 原始資料保護

- 不修改 v2.8
- 不修改 v1.4.2
- 不修改 v1.5.1
- 不修改 v1.5.2
- 不修改 v1.5.3
- 不修改 GitHub main
- 不刪除零成交量資料
- 不刪除 OHLC 缺值資料
- 不刪除原始下載回應

## GitHub

Repository：{REPO}

使用獨立 audit branch，不直接修改 main。
"""

    (out_dir / "README.md").write_text(
        readme,
        encoding="utf-8"
    )

    # --------------------------------------------------------
    # H. 封存完整來源與雜湊
    # --------------------------------------------------------

    save_current_source(out_dir, logs)

    (out_dir / "run.log").write_text(
        "\n".join(logs) + "\n",
        encoding="utf-8"
    )

    artifact_manifest = build_artifact_manifest(out_dir)

    write_json(
        out_dir / "artifact_sha256_manifest.json",
        artifact_manifest
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
    # I. GitHub 上傳
    # --------------------------------------------------------

    upload_files = [
        zip_path,
        out_dir / "README.md",
        out_dir / "v1.5.6_官方增量行情續接報告.json",
        out_dir / "v1.5.6_修正替代規則.json",
        out_dir / "待重查日期清單.json",
        out_dir / "artifact_sha256_manifest.json",
        out_dir / "逐日雙市場行情驗證統計.csv",
        out_dir / "官方API下載及即時驗證索引.csv",
        out_dir / "個股增量覆蓋檔索引.csv",
        out_dir / "latest_market_dates.json",
        out_dir / "run.log",
        std_path
    ]

    correction_csv = (
        out_dir / "修正_TPEX_20261111.csv"
    )

    if correction_csv.exists():
        upload_files.append(correction_csv)

    source_path = (
        out_dir / "SSO_Market_Data_Continuation_v1.5.6.py"
    )

    if source_path.exists():
        upload_files.append(source_path)

    github_result = None

    try:
        github_result = github_upload(
            upload_files,
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
            "error": f"{type(error).__name__}: {error}"
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
    # J. 最終輸出
    # --------------------------------------------------------

    print("\n" + "=" * 88)

    print(f"{VERSION}：{status}")

    print(
        f"已知 TPEx 2026-11-11 修正："
        f"{'PASS' if correction_pass else 'FAILED'}"
    )

    print(
        f"修正資料筆數：{len(correction_records):,}"
    )

    print(
        f"修正資料沿用既有檔案：{correction_reused}"
    )

    print(
        f"修正來源：{correction_source_version}"
    )

    print(
        f"修正原始檔 SHA256："
        f"{correction_source_file_sha256}"
    )

    print(
        f"修正標準化資料 SHA256："
        f"{correction_canonical_sha256}"
    )

    print(
        f"前瞻新增行情：{len(forward_records):,} 列"
    )

    print(
        f"前瞻新增個股 overlay：{len(overlay_index):,} 檔"
    )

    print(
        f"品質檢查拒絕：{rejected_count}"
    )

    print(
        f"下載／解析錯誤：{download_error_count}"
    )

    print(
        f"待官方資料：{waiting_count}"
    )

    print(
        f"待重查日期總數：{len(unresolved_dates)}"
    )

    print(
        f"查詢截止日期：{target_end_date}"
    )

    print(
        f"TWSE 最後行情日：{state['market_last_date']['TWSE']}"
    )

    print(
        f"TPEx 最後行情日：{state['market_last_date']['TPEX']}"
    )

    print(f"ZIP：{zip_path}")

    print(
        f"ZIP SHA256：{zip_result['zip_sha256']}"
    )

    if github_result and github_result.get("branch"):
        print(
            f"GitHub branch：{github_result['branch']}"
        )

        print(
            f"GitHub URL：{github_result['url']}"
        )

        print(
            f"GitHub main 未變更："
            f"{github_result['main_unchanged']}"
        )

    else:
        print(f"GitHub：{github_result}")

    print("=" * 88)

    return report


# ============================================================
# 14. 執行入口
# ============================================================

RESULT_V156 = run_v156()