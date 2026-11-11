
# -*- coding: utf-8 -*-
# =============================================================================
# SSO v1.3.1｜歷史 OHLC 與逐日證券分類來源精確驗證
#
# 目的：只查證 Drive 既有歷史資料，不重跑 v1.3.0 的行情/特徵/Q1-Q5/報酬。
#
# 核心修正：
# 1. 不再以檔名或路徑文字直接認定 OHLC 或證券分類。
# 2. 直接 CSV/CSV.GZ/TSV/TXT 與 ZIP 內表格成員，實際讀取標頭及資料列。
# 3. OHLC 必須具備日期、代號、開高低收欄位，並檢驗有效資料列。
# 4. CFI 歷史分類必須有 CFI 欄位及分類快照/生效日期；listed_date 不算。
# 5. 分開辨識 CFI 單日名錄、listed_date 靜態名錄、有效期間資料。
# 6. 以解壓後內容 SHA256 去除完全相同的副本；ZIP 成員與實體檔可互相比對。
# 7. 不修改、不刪除、不搬移、不覆寫任何原始來源。
# 8. 成果 ZIP + 獨立 GitHub audit branch + 每檔 Git blob SHA read-back。
# 9. GitHub main SHA 前後核對，絕不更新 main。
#
# 本程式不呼叫官方 API、不重新訓練、不重算股票績效。
# =============================================================================

import os
import re
import io
import csv
import sys
import json
import gzip
import uuid
import time
import math
import hashlib
import zipfile
import tempfile
import subprocess
import warnings

from pathlib import Path
from datetime import datetime
from contextlib import contextmanager
from collections import defaultdict, Counter
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore", category=FutureWarning)

# =============================================================================
# 01. 固定設定
# =============================================================================

VERSION = "SSO v1.3.1｜歷史 OHLC 與逐日證券分類來源精確驗證"

ROOT = Path("/content/drive/MyDrive/看股最速適")

TAIPEI = ZoneInfo("Asia/Taipei")

REPO_OWNER = "CHLINX"
REPO_NAME = "sso"
REPO_URL = "https://github.com/CHLINX/sso.git"
MAIN_BRANCH = "main"

V130_BRANCH = (
    "audit/sso-v1.3.0-q1-q5-history-"
    "20261111_204711_H10FIX-4d23"
)

V130_ARTIFACT_DIR = (
    "audit/sso-v1.3.0-q1-q5-history/20261111_204711_H10FIX"
)

# 只掃描表格及可結構化檢查的 JSON；其他檔案保留在實體清單中。
TABLE_EXTENSIONS = (
    ".csv", ".csv.gz", ".tsv", ".tab", ".txt",
    ".json", ".json.gz", ".xlsx", ".xls", ".parquet",
)

ARCHIVE_EXTENSION = ".zip"

CHUNK_SIZE = 100_000
SAMPLE_BYTES = 512 * 1024
MAX_JSON_BYTES = 64 * 1024 * 1024
MAX_HEADER_COLUMNS = 500
PROGRESS_EVERY = 100

RUN_ID = datetime.now(TAIPEI).strftime("%Y%m%d_%H%M%S")

OUT_DIR = (
    ROOT
    / f"看股最速適_v1_3_1_歷史來源精確驗證_{RUN_ID}"
)

ZIP_PATH = ROOT / f"{OUT_DIR.name}.zip"


# =============================================================================
# 02. 通用工具
# =============================================================================

def log(message):
    now = datetime.now(TAIPEI).strftime("%H:%M:%S")
    print(f"[{now}] {message}", flush=True)


def sha256_bytes(data):
    return hashlib.sha256(data).hexdigest()


def sha256_file(path, chunk=8 * 1024 * 1024):
    h = hashlib.sha256()

    with open(path, "rb") as f:
        for block in iter(lambda: f.read(chunk), b""):
            h.update(block)

    return h.hexdigest()


def csv_write(frame, path):
    frame.to_csv(
        path,
        index=False,
        encoding="utf-8-sig",
        na_rep="",
    )


def json_write(obj, path):
    Path(path).write_text(
        json.dumps(
            obj,
            ensure_ascii=False,
            indent=2,
            default=str,
        ),
        encoding="utf-8-sig",
    )


def safe_int(value, default=0):
    try:
        return int(value)
    except Exception:
        return default


def norm_col(value):
    text = str(value).replace("\ufeff", "").strip().lower()
    text = re.sub(r"[\s\-./（）()\[\]{}：:]+", "_", text)
    return text.strip("_")


def nonempty_text(series):
    s = series.astype("string").str.strip()

    return (
        s.notna()
        & ~s.isin(["", "nan", "None", "<NA>", "-", "--", "N/A"])
    )


def parse_dates(series):
    """支援西元日期、民國日期、YYYYMMDD、民國 yyyMMdd。"""

    s = series.astype("string").str.strip()

    s = s.replace({
        "": pd.NA,
        "-": pd.NA,
        "--": pd.NA,
        "nan": pd.NA,
        "None": pd.NA,
    })

    out = pd.Series(
        pd.NaT,
        index=s.index,
        dtype="datetime64[ns]",
    )

    if s.notna().sum() == 0:
        return out

    # 先處理民國斜線日期，避免被 pandas 誤解析成西元 115 年。
    roc_slash = s.str.fullmatch(
        r"\d{2,3}[/-]\d{1,2}[/-]\d{1,2}",
        na=False,
    )

    if roc_slash.any():
        converted = []

        for raw in s.loc[roc_slash].tolist():
            parts = re.split(r"[/-]", str(raw))
            year = int(parts[0])

            if year < 1911:
                year += 1911

            try:
                converted.append(
                    pd.Timestamp(
                        year=year,
                        month=int(parts[1]),
                        day=int(parts[2]),
                    )
                )
            except Exception:
                converted.append(pd.NaT)

        parsed_roc = pd.Series(
            converted,
            index=s.index[roc_slash],
        )

        good = parsed_roc.notna()

        out.loc[parsed_roc.index[good]] = (
            pd.to_datetime(parsed_roc.loc[good]).dt.normalize()
        )

    # 民國 yyyMMdd / yyMMdd 及西元 YYYYMMDD。
    compact = s.str.replace(
        r"\.0$",
        "",
        regex=True,
    )

    compact = (
        compact
        .str.replace(r"[./年\-]", "", regex=True)
        .str.replace("月", "")
        .str.replace("日", "")
    )

    roc_compact = (
        compact.str.fullmatch(r"\d{6,7}", na=False)
        & out.isna()
    )

    if roc_compact.any():
        converted = []

        for raw in compact.loc[roc_compact].tolist():
            value = str(raw)

            try:
                if len(value) == 7:
                    year = int(value[:3]) + 1911
                    month = int(value[3:5])
                    day = int(value[5:7])
                else:
                    year = int(value[:2]) + 1911
                    month = int(value[2:4])
                    day = int(value[4:6])

                converted.append(
                    pd.Timestamp(
                        year=year,
                        month=month,
                        day=day,
                    )
                )

            except Exception:
                converted.append(pd.NaT)

        parsed_roc = pd.Series(
            converted,
            index=s.index[roc_compact],
        )

        good = parsed_roc.notna()

        out.loc[parsed_roc.index[good]] = (
            pd.to_datetime(parsed_roc.loc[good]).dt.normalize()
        )

    west_compact = (
        compact.str.fullmatch(r"\d{8}", na=False)
        & out.isna()
    )

    if west_compact.any():
        parsed_west = pd.to_datetime(
            compact.loc[west_compact],
            format="%Y%m%d",
            errors="coerce",
        )

        good = parsed_west.notna()

        out.loc[parsed_west.index[good]] = (
            parsed_west.loc[good].dt.normalize()
        )

    missing = out.isna() & s.notna()

    if missing.any():
        try:
            general = pd.to_datetime(
                s.loc[missing],
                errors="coerce",
                format="mixed",
            )
        except TypeError:
            general = pd.to_datetime(
                s.loc[missing],
                errors="coerce",
            )

        good = general.notna()

        out.loc[general.index[good]] = (
            general.loc[good].dt.normalize()
        )

    return out


def numeric_values(series):
    s = series.astype("string").str.strip()

    s = s.str.replace(",", "", regex=False)
    s = s.str.replace("＋", "+", regex=False)
    s = s.str.replace("－", "-", regex=False)

    s = s.replace({
        "": pd.NA,
        "-": pd.NA,
        "--": pd.NA,
        "X": pd.NA,
        "x": pd.NA,
        "N/A": pd.NA,
    })

    return pd.to_numeric(s, errors="coerce")


def is_table_name(name):
    lower = str(name).lower()
    return any(lower.endswith(ext) for ext in TABLE_EXTENSIONS)


def is_supported_csv_name(name):
    lower = str(name).lower()

    return lower.endswith((
        ".csv",
        ".csv.gz",
        ".tsv",
        ".tab",
        ".txt",
    ))


def infer_market_from_path(text):
    # ZIP 成員名稱比上層封存資料夾名稱精確；先判斷最末段。
    raw = str(text).replace("\\", "/")
    last = raw.split("::")[-1].split("/")[-1].lower()

    twse_last = any(
        x in last
        for x in ["twse", "上市市場", "上市行情"]
    )

    tpex_last = any(
        x in last
        for x in ["tpex", "otc", "上櫃市場", "上櫃行情"]
    )

    if twse_last and not tpex_last:
        return "TWSE_PATH_INFERRED"

    if tpex_last and not twse_last:
        return "TPEX_PATH_INFERRED"

    lower = raw.lower()

    twse = any(
        x in lower
        for x in ["twse", "上市市場", "上市行情"]
    )

    tpex = any(
        x in lower
        for x in ["tpex", "otc", "上櫃市場", "上櫃行情"]
    )

    if twse and not tpex:
        return "TWSE_PATH_INFERRED"

    if tpex and not twse:
        return "TPEX_PATH_INFERRED"

    return "UNKNOWN"


# =============================================================================
# 03. 欄位語意識別
# =============================================================================

DATE_ALIASES = {
    "trade_date", "tradedate", "date",
    "日期", "交易日期", "交易日",
    "資料日期", "資料日", "行情日期",
    "年月日", "成交日期", "交易年月日",
    "snapshot_date", "as_of_date", "asofdate",
    "effective_date", "生效日期", "生效日",
    "分類日期", "分類基準日", "基準日", "資料基準日",
}

LISTING_DATE_ALIASES = {
    "listed_date", "listing_date", "list_date", "date_listed",
    "上市日期", "上櫃日期", "掛牌日期",
    "上市日", "上櫃日", "初次上市日期",
    "delisted_date", "delisting_date",
    "下市日期", "下櫃日期", "終止上市日期",
}

EFFECTIVE_DATE_ALIASES = {
    "snapshot_date", "as_of_date", "asofdate",
    "effective_date", "trade_date", "tradedate",
    "valid_from", "valid_to", "valid_start", "valid_end",
    "start_date", "end_date", "effective_from", "effective_to",
    "classification_date", "class_date", "reference_date",
    "生效日期", "生效日", "分類日期", "分類基準日",
    "資料基準日", "有效日期", "有效起日", "有效迄日",
    "有效起始日", "有效終止日", "生效起日", "生效迄日",
    "起始生效日", "終止生效日",
    "基準日", "資料日期", "交易日期", "交易日", "日期",
}

CODE_ALIASES = {
    "code", "symbol", "ticker",
    "instrument_code", "stock_code",
    "security_code",
    "證券代號", "股票代號",
    "證券代碼", "股票代碼",
    "代號", "代碼", "完整證券代號", "證券編號",
}

MARKET_ALIASES = {
    "market", "market_code", "market_name",
    "exchange", "market_type",
    "市場", "市場別", "市場代碼",
    "交易市場", "證券市場", "交易所",
}

OHLC_ALIASES = {
    "open": {
        "open", "開盤", "開盤價",
        "開盤價格", "open_price",
    },
    "high": {
        "high", "最高", "最高價",
        "最高價格", "high_price",
    },
    "low": {
        "low", "最低", "最低價",
        "最低價格", "low_price",
    },
    "close": {
        "close", "closing", "收盤",
        "收盤價", "收盤價格", "close_price",
    },
}

CFI_ALIASES = {
    "cfi", "cfi_code", "isin_cfi",
    "cfi分類", "分類代碼", "金融工具分類代碼",
}

CLASS_ALIASES = {
    "security_type", "security_category",
    "instrument_type", "instrument_category",
    "classification", "class_code",
    "product_type", "product_category",
    "證券種類", "證券類別", "商品種類",
    "商品類型", "金融商品種類", "有價證券種類",
    "證券分類", "商品分類", "類別代碼",
}


def find_column(columns, aliases):
    normalized = {
        norm_col(c): c
        for c in columns
    }

    # 固定偏好主要交易欄位，避免同時有 date / trade_date 時任意選取。
    preferred = {
        "trade_date": 0,
        "交易日期": 0,
        "交易年月日": 0,
        "snapshot_date": 1,
        "as_of_date": 1,
        "effective_date": 2,
        "date": 3,
        "日期": 4,

        "instrument_code": 0,
        "security_code": 1,
        "stock_code": 2,
        "code": 3,
        "證券代號": 0,
        "股票代號": 1,
        "完整證券代號": 2,

        "market": 0,
        "market_code": 1,
        "exchange": 2,

        "cfi_code": 0,
        "isin_cfi": 1,
        "cfi": 2,

        "open": 0,
        "high": 0,
        "low": 0,
        "close": 0,
    }

    ordered_aliases = sorted(
        aliases,
        key=lambda x: (
            preferred.get(norm_col(x), 50),
            len(norm_col(x)),
            norm_col(x),
        ),
    )

    for alias in ordered_aliases:
        key = norm_col(alias)

        if key in normalized:
            return normalized[key]

    # 只對英文帶底線別名做保守的包含比對。
    for alias in ordered_aliases:
        key = norm_col(alias)

        if len(key) >= 8 and re.fullmatch(r"[a-z0-9_]+", key):
            for norm, raw in normalized.items():
                if norm.startswith(key + "_"):
                    return raw

    return None


def identify_schema(columns):
    cols = list(columns)

    date_col = find_column(cols, DATE_ALIASES)

    # 不以 listed_date / delisted_date 當行情日期。
    if date_col is not None and norm_col(date_col) in LISTING_DATE_ALIASES:
        date_col = None

    code_col = find_column(cols, CODE_ALIASES)
    market_col = find_column(cols, MARKET_ALIASES)

    ohlc_cols = {
        name: find_column(cols, aliases)
        for name, aliases in OHLC_ALIASES.items()
    }

    cfi_col = find_column(cols, CFI_ALIASES)
    class_col = find_column(cols, CLASS_ALIASES)

    listing_date_cols = [
        c for c in cols
        if norm_col(c) in LISTING_DATE_ALIASES
    ]

    effective_date_cols = [
        c for c in cols
        if norm_col(c) in EFFECTIVE_DATE_ALIASES
        and norm_col(c) not in LISTING_DATE_ALIASES
    ]

    interval_from = find_column(
        cols,
        {
            "valid_from", "valid_start", "effective_from",
            "start_date", "有效起日", "有效起始日", "生效起日",
        },
    )

    interval_to = find_column(
        cols,
        {
            "valid_to", "valid_end", "effective_to",
            "end_date", "有效迄日", "有效終止日", "生效迄日",
        },
    )

    selected = []

    for col in [
        date_col, code_col, market_col, cfi_col, class_col,
        *ohlc_cols.values(), *listing_date_cols,
        *effective_date_cols, interval_from, interval_to,
    ]:
        if col is not None and col not in selected:
            selected.append(col)

    return {
        "date_col": date_col,
        "code_col": code_col,
        "market_col": market_col,
        "ohlc_cols": ohlc_cols,
        "cfi_col": cfi_col,
        "class_col": class_col,
        "listing_date_cols": listing_date_cols,
        "effective_date_cols": effective_date_cols,
        "interval_from": interval_from,
        "interval_to": interval_to,
        "selected_columns": selected,
    }


# =============================================================================
# 04. 讀取來源串流（實體檔或 ZIP 成員）
# =============================================================================

@contextmanager
def open_candidate_stream(candidate):
    """回傳解壓後的位元組串流；SHA256 對解壓後內容計算。"""

    if candidate["source_kind"] == "FILE":
        path = Path(candidate["path"])
        raw = open(path, "rb")

        try:
            if str(path).lower().endswith((".gz", ".csv.gz", ".json.gz")):
                with gzip.GzipFile(fileobj=raw, mode="rb") as gz:
                    yield gz
            else:
                yield raw
        finally:
            raw.close()

        return

    archive_path = Path(candidate["archive_path"])
    member = candidate["member"]

    with zipfile.ZipFile(archive_path, "r") as zf:
        with zf.open(member, "r") as raw:
            if str(member).lower().endswith((".gz", ".csv.gz", ".json.gz")):
                with gzip.GzipFile(fileobj=raw, mode="rb") as gz:
                    yield gz
            else:
                yield raw


class HashingReader:
    """讓 pandas 串流讀取時同步計算解壓後內容 SHA256。"""

    def __init__(self, raw):
        self.raw = raw
        self.digest = hashlib.sha256()

    def read(self, size=-1):
        data = self.raw.read(size)

        if data:
            self.digest.update(data)

        return data

    def readline(self, size=-1):
        data = self.raw.readline(size)

        if data:
            self.digest.update(data)

        return data

    def readinto(self, buffer):
        data = self.raw.read(len(buffer))
        n = len(data)
        buffer[:n] = data

        if data:
            self.digest.update(data)

        return n

    def readable(self):
        return True

    def seekable(self):
        return False

    def __iter__(self):
        return self

    def __next__(self):
        data = self.readline()

        if not data:
            raise StopIteration

        return data


def decode_sample(sample):
    best = None
    best_score = float("inf")

    for encoding in (
        "utf-8-sig", "utf-8", "cp950",
        "big5", "gb18030", "utf-16",
    ):
        try:
            decoded = sample.decode(
                encoding,
                errors="replace",
            )

            replacement = decoded.count("\ufffd")
            score = replacement / max(1, len(decoded))

            if score < best_score:
                best, best_score = (decoded, encoding), score

            if replacement == 0:
                return decoded, encoding

        except Exception:
            continue

    if best is None:
        return sample.decode("utf-8", errors="replace"), "utf-8"

    return best


def detect_delimiter(text, filename):
    if str(filename).lower().endswith((".tsv", ".tab")):
        return "\t"

    lines = [
        x for x in text.splitlines()[:30]
        if x.strip()
    ]

    if not lines:
        return ","

    sample = "\n".join(lines[:20])

    try:
        return csv.Sniffer().sniff(
            sample,
            delimiters=",\t;|",
        ).delimiter

    except Exception:
        scores = {}

        for sep in [",", "\t", ";", "|"]:
            counts = [
                line.count(sep)
                for line in lines[:10]
            ]

            scores[sep] = (
                sum(counts),
                len(set(counts)),
            )

        return max(
            scores,
            key=lambda x: (
                scores[x][0],
                -scores[x][1],
            ),
        )


def read_header(candidate):
    with open_candidate_stream(candidate) as stream:
        sample = stream.read(SAMPLE_BYTES)

    decoded, encoding = decode_sample(sample)

    sep = detect_delimiter(
        decoded,
        candidate.get("member") or candidate.get("path", ""),
    )

    try:
        header = pd.read_csv(
            io.StringIO(decoded),
            sep=sep,
            nrows=0,
            engine="python",
            on_bad_lines="skip",
        )

        columns = [
            str(x).replace("\ufeff", "").strip()
            for x in header.columns
        ]

    except Exception as exc:
        raise RuntimeError(
            f"標頭解析失敗：{type(exc).__name__}: {exc}"
        )

    if len(columns) > MAX_HEADER_COLUMNS:
        raise RuntimeError(
            f"欄位數超過限制：{len(columns)}"
        )

    return columns, encoding, sep


# =============================================================================
# 05. 資料列精確盤點
# =============================================================================

def cfi_valid_mask(series):
    text = (
        series.astype("string")
        .str.strip()
        .str.upper()
    )

    return text.str.fullmatch(
        r"[A-Z0-9]{6}",
        na=False,
    )


def scan_csv_candidate(candidate):
    result = {
        "來源類型": (
            "ZIP成員"
            if candidate["source_kind"] == "ZIP_MEMBER"
            else "實體檔案"
        ),
        "相對路徑": candidate.get("relative_path", ""),
        "ZIP路徑": candidate.get("archive_relative", ""),
        "ZIP成員": candidate.get("member", ""),
        "副檔名": candidate.get("extension", ""),
        "來源大小bytes": candidate.get("source_size", 0),
        "解壓後大小bytes": candidate.get(
            "uncompressed_size",
            candidate.get("source_size", 0),
        ),
        "ZIP_CRC32": candidate.get("crc32", ""),
        "內容SHA256": "",
        "重複內容群組數": 1,
        "完全相同內容副本數": 1,
        "欄位": "",
        "日期欄位": "",
        "代號欄位": "",
        "市場欄位": "",
        "開盤欄位": "",
        "最高欄位": "",
        "最低欄位": "",
        "收盤欄位": "",
        "CFI欄位": "",
        "證券分類欄位": "",
        "上市日期欄位": "",
        "分類有效日期欄位": "",
        "資料列數": 0,
        "有效日期列數": 0,
        "有效代號列數": 0,
        "有效OHLC列數": 0,
        "有效CFI列數": 0,
        "不同交易日期數": 0,
        "最早交易日期": "",
        "最晚交易日期": "",
        "分類日期數": 0,
        "分類最早日期": "",
        "分類最晚日期": "",
        "分類有效期間起日欄": "",
        "分類有效期間迄日欄": "",
        "市場來源判斷": "UNKNOWN",
        "OHLC判斷": "非OHLC或資料不足",
        "分類判斷": "無日期化分類證據",
        "CFI樣本值": "",
        "分類樣本值": "",
        "內容檢查狀態": "",
        "錯誤訊息": "",
    }

    try:
        columns, encoding, sep = read_header(candidate)
        schema = identify_schema(columns)

        result["欄位"] = "；".join(columns)
        result["日期欄位"] = schema["date_col"] or ""
        result["代號欄位"] = schema["code_col"] or ""
        result["市場欄位"] = schema["market_col"] or ""
        result["開盤欄位"] = schema["ohlc_cols"]["open"] or ""
        result["最高欄位"] = schema["ohlc_cols"]["high"] or ""
        result["最低欄位"] = schema["ohlc_cols"]["low"] or ""
        result["收盤欄位"] = schema["ohlc_cols"]["close"] or ""
        result["CFI欄位"] = schema["cfi_col"] or ""
        result["證券分類欄位"] = schema["class_col"] or ""
        result["上市日期欄位"] = "；".join(
            schema["listing_date_cols"]
        )
        result["分類有效日期欄位"] = "；".join(
            schema["effective_date_cols"]
        )

        result["分類有效期間起日欄"] = (
            schema["interval_from"] or ""
        )

        result["分類有效期間迄日欄"] = (
            schema["interval_to"] or ""
        )

        market_path_text = (
            candidate.get("member")
            or candidate.get("relative_path", "")
        )

        infer_market = infer_market_from_path(
            market_path_text
        )

        result["市場來源判斷"] = (
            "資料欄位提供市場"
            if schema["market_col"]
            else infer_market
        )

        selected = schema["selected_columns"]

        has_ohlc_header = (
            schema["date_col"] is not None
            and schema["code_col"] is not None
            and all(schema["ohlc_cols"].values())
        )

        has_price_header = (
            schema["date_col"] is not None
            and schema["code_col"] is not None
            and schema["ohlc_cols"]["close"] is not None
        )

        has_class_schema = (
            schema["cfi_col"] is not None
            or schema["class_col"] is not None
        )

        relevant = bool(
            schema["date_col"]
            or schema["code_col"]
            or has_ohlc_header
            or has_class_schema
        )

        trade_dates = set()
        class_dates = set()

        trade_min = None
        trade_max = None
        class_min = None
        class_max = None

        row_count = 0
        valid_date_rows = 0
        valid_code_rows = 0
        valid_ohlc_rows = 0
        valid_cfi_rows = 0

        valid_interval_rows = 0
        interval_min = None
        interval_max = None

        cfi_values = Counter()
        class_values = Counter()

        if not relevant:
            # 無日期/代號/分類結構的 CSV 仍計算解壓後 SHA。
            with open_candidate_stream(candidate) as raw:
                hreader = HashingReader(raw)

                while hreader.read(8 * 1024 * 1024):
                    pass

                result["內容SHA256"] = hreader.digest.hexdigest()

            result["內容檢查狀態"] = (
                "已讀標頭及完整內容雜湊；不含行情/分類核心欄位"
            )

            return result

        usecols = [
            c for c in selected
            if c in columns
        ]

        if not usecols:
            usecols = columns

        with open_candidate_stream(candidate) as raw:
            hreader = HashingReader(raw)

            reader = pd.read_csv(
                hreader,
                sep=sep,
                encoding=encoding,
                dtype="string",
                keep_default_na=False,
                chunksize=CHUNK_SIZE,
                usecols=usecols,
                on_bad_lines="skip",
                low_memory=False,
            )

            for chunk in reader:
                row_count += len(chunk)

                # 交易日期：只使用交易日期欄，不使用 listed_date。
                if (
                    schema["date_col"]
                    and schema["date_col"] in chunk.columns
                ):
                    dates = parse_dates(
                        chunk[schema["date_col"]]
                    )

                    valid_dates = dates.dropna()
                    valid_date_rows += int(len(valid_dates))

                    if len(valid_dates):
                        trade_dates.update(
                            valid_dates.dt.strftime(
                                "%Y-%m-%d"
                            ).unique().tolist()
                        )

                        dmin = valid_dates.min()
                        dmax = valid_dates.max()

                        trade_min = (
                            dmin if trade_min is None
                            else min(trade_min, dmin)
                        )

                        trade_max = (
                            dmax if trade_max is None
                            else max(trade_max, dmax)
                        )

                if (
                    schema["code_col"]
                    and schema["code_col"] in chunk.columns
                ):
                    valid_code_rows += int(
                        nonempty_text(
                            chunk[schema["code_col"]]
                        ).sum()
                    )

                # 完整 OHLC 欄位與合理價格檢查。
                if has_ohlc_header:
                    oc = schema["ohlc_cols"]

                    op = numeric_values(chunk[oc["open"]])
                    hi = numeric_values(chunk[oc["high"]])
                    lo = numeric_values(chunk[oc["low"]])
                    cl = numeric_values(chunk[oc["close"]])

                    valid_ohlc = (
                        op.gt(0)
                        & hi.gt(0)
                        & lo.gt(0)
                        & cl.gt(0)
                        & hi.ge(
                            pd.concat(
                                [op, cl],
                                axis=1,
                            ).max(axis=1)
                        )
                        & lo.le(
                            pd.concat(
                                [op, cl],
                                axis=1,
                            ).min(axis=1)
                        )
                        & hi.ge(lo)
                    )

                    valid_ohlc_rows += int(
                        valid_ohlc.sum()
                    )

                # CFI 格式及分類欄位實際值。
                if (
                    schema["cfi_col"]
                    and schema["cfi_col"] in chunk.columns
                ):
                    cfi_series = (
                        chunk[schema["cfi_col"]]
                        .astype("string")
                        .str.strip()
                        .str.upper()
                    )

                    mask = cfi_valid_mask(cfi_series)
                    valid_cfi_rows += int(mask.sum())

                    for value, count in (
                        cfi_series.loc[mask]
                        .value_counts()
                        .head(30)
                        .items()
                    ):
                        cfi_values[str(value)] += int(count)

                if (
                    schema["class_col"]
                    and schema["class_col"] in chunk.columns
                ):
                    values = (
                        chunk[schema["class_col"]]
                        .astype("string")
                        .str.strip()
                    )

                    for value, count in (
                        values[nonempty_text(values)]
                        .value_counts()
                        .head(30)
                        .items()
                    ):
                        class_values[str(value)] += int(count)

                # 分類日期只取語意上明確的快照/生效日期。
                # listed_date / delisted_date 不在此欄位清單中。
                for col in schema["effective_date_cols"]:
                    if col not in chunk.columns:
                        continue

                    parsed = parse_dates(
                        chunk[col]
                    ).dropna()

                    if len(parsed):
                        class_dates.update(
                            parsed.dt.strftime(
                                "%Y-%m-%d"
                            ).unique().tolist()
                        )

                        dmin = parsed.min()
                        dmax = parsed.max()

                        class_min = (
                            dmin if class_min is None
                            else min(class_min, dmin)
                        )

                        class_max = (
                            dmax if class_max is None
                            else max(class_max, dmax)
                        )

                # 有效起迄期間必須兩欄都有可解析資料。
                if (
                    schema["interval_from"]
                    and schema["interval_to"]
                ):
                    if (
                        schema["interval_from"] in chunk.columns
                        and schema["interval_to"] in chunk.columns
                    ):
                        starts = parse_dates(
                            chunk[schema["interval_from"]]
                        )

                        ends = parse_dates(
                            chunk[schema["interval_to"]]
                        )

                        paired = (
                            starts.notna()
                            & ends.notna()
                            & ends.ge(starts)
                        )

                        valid_interval_rows += int(
                            paired.sum()
                        )

                        if paired.any():
                            local_min = starts.loc[paired].min()
                            local_max = ends.loc[paired].max()

                            interval_min = (
                                local_min
                                if interval_min is None
                                else min(interval_min, local_min)
                            )

                            interval_max = (
                                local_max
                                if interval_max is None
                                else max(interval_max, local_max)
                            )

            result["內容SHA256"] = (
                hreader.digest.hexdigest()
            )

        result["資料列數"] = row_count
        result["有效日期列數"] = valid_date_rows
        result["有效代號列數"] = valid_code_rows
        result["有效OHLC列數"] = valid_ohlc_rows
        result["有效CFI列數"] = valid_cfi_rows

        result["有效分類區間列數"] = valid_interval_rows

        result["分類區間最早起日"] = (
            interval_min.strftime("%Y-%m-%d")
            if interval_min is not None else ""
        )

        result["分類區間最晚迄日"] = (
            interval_max.strftime("%Y-%m-%d")
            if interval_max is not None else ""
        )

        result["不同交易日期數"] = len(trade_dates)
        result["最早交易日期"] = (
            min(trade_dates) if trade_dates else ""
        )
        result["最晚交易日期"] = (
            max(trade_dates) if trade_dates else ""
        )

        result["分類日期數"] = len(class_dates)
        result["分類最早日期"] = (
            min(class_dates) if class_dates else ""
        )
        result["分類最晚日期"] = (
            max(class_dates) if class_dates else ""
        )

        result["CFI樣本值"] = "；".join(
            f"{k}:{v}"
            for k, v in cfi_values.most_common(20)
        )

        result["分類樣本值"] = "；".join(
            f"{k}:{v}"
            for k, v in class_values.most_common(20)
        )

        # OHLC 結論：完整四價、交易日、代號、有效資料列及跨日範圍。
        if (
            has_ohlc_header
            and valid_ohlc_rows > 0
            and len(trade_dates) >= 2
            and valid_code_rows > 0
        ):
            if schema["market_col"]:
                result["OHLC判斷"] = (
                    "完整OHLC_有市場欄位_跨日期資料"
                )
            elif infer_market != "UNKNOWN":
                result["OHLC判斷"] = (
                    "完整OHLC_由路徑推定市場_跨日期資料"
                )
            else:
                result["OHLC判斷"] = (
                    "完整OHLC_市場未知_跨日期資料"
                )

        elif has_ohlc_header:
            result["OHLC判斷"] = (
                "具OHLC欄位但有效資料/日期不足"
            )

        elif has_price_header:
            result["OHLC判斷"] = (
                "僅有部分價格欄位_非完整OHLC"
            )

        else:
            result["OHLC判斷"] = "非完整OHLC"

        # 分類結論：listed_date 僅記錄上市時間，不是分類有效日期。
        if schema["cfi_col"] and valid_cfi_rows > 0:

            if (
                schema["interval_from"]
                and schema["interval_to"]
                and valid_interval_rows > 0
            ):
                result["分類判斷"] = (
                    "CFI_具有效起迄資料列_歷史分類候選"
                )

            elif len(class_dates) >= 2:
                result["分類判斷"] = (
                    "CFI_多日期快照_歷史分類候選"
                )

            elif len(class_dates) == 1:
                result["分類判斷"] = (
                    "CFI_單一日期快照_不足以建立長期歷史分類"
                )

            elif schema["listing_date_cols"]:
                result["分類判斷"] = (
                    "CFI靜態名錄_只有上市/下市日期_不是歷史分類快照"
                )

            else:
                result["分類判斷"] = (
                    "CFI欄位存在_但無分類有效日期證據"
                )

        elif schema["class_col"]:

            if len(class_dates) >= 2:
                result["分類判斷"] = (
                    "非CFI分類欄位_多日期分類候選_須核對語意"
                )

            elif len(class_dates) == 1:
                result["分類判斷"] = (
                    "非CFI分類欄位_單一日期"
                )

            else:
                result["分類判斷"] = (
                    "非CFI分類欄位_沒有有效日期證據"
                )

        else:
            result["分類判斷"] = "無日期化分類證據"

        result["內容檢查狀態"] = (
            "完整串流讀取及內容SHA256完成"
        )

        return result

    except Exception as exc:
        result["內容檢查狀態"] = "讀取或解析失敗"
        result["錯誤訊息"] = (
            f"{type(exc).__name__}: {str(exc)[:1200]}"
        )

        return result


# =============================================================================
# 06. JSON 結構化盤點（只讀取小型 JSON；大型 JSON 留作待處理）
# =============================================================================

def scan_json_candidate(candidate):
    row = {
        "來源類型": (
            "ZIP成員"
            if candidate["source_kind"] == "ZIP_MEMBER"
            else "實體檔案"
        ),
        "相對路徑": candidate.get("relative_path", ""),
        "ZIP路徑": candidate.get("archive_relative", ""),
        "ZIP成員": candidate.get("member", ""),
        "來源大小bytes": candidate.get("source_size", 0),
        "內容SHA256": "",
        "JSON頂層型態": "",
        "欄位或鍵": "",
        "是否含CFI鍵": False,
        "是否含分類日期鍵": False,
        "是否僅含上市日期鍵": False,
        "JSON檢查狀態": "",
        "錯誤訊息": "",
    }

    try:
        raw = bytearray()
        h = hashlib.sha256()

        with open_candidate_stream(candidate) as stream:
            while True:
                block = stream.read(8 * 1024 * 1024)

                if not block:
                    break

                h.update(block)

                if len(raw) <= MAX_JSON_BYTES:
                    raw.extend(block)

        row["內容SHA256"] = h.hexdigest()

        if len(raw) > MAX_JSON_BYTES:
            row["JSON檢查狀態"] = (
                "超過JSON解析大小上限；已完成SHA256"
            )
            return row

        full_text = bytes(raw).decode(
            "utf-8-sig",
            errors="replace",
        )

        obj = json.loads(full_text)

        row["JSON頂層型態"] = type(obj).__name__

        records = []

        if (
            isinstance(obj, list)
            and obj
            and isinstance(obj[0], dict)
        ):
            records = obj

        elif isinstance(obj, dict):
            row["欄位或鍵"] = "；".join(
                list(map(str, obj.keys()))[:200]
            )

            for value in obj.values():
                if (
                    isinstance(value, list)
                    and value
                    and isinstance(value[0], dict)
                ):
                    records = value
                    break

            if not records:
                records = [obj]

        if records:
            keys = list(records[0].keys())
            norm = {norm_col(k) for k in keys}

            row["欄位或鍵"] = "；".join(
                map(str, keys[:200])
            )

            row["是否含CFI鍵"] = bool(
                norm & CFI_ALIASES
            )

            row["是否含分類日期鍵"] = bool(
                norm & EFFECTIVE_DATE_ALIASES
            )

            row["是否僅含上市日期鍵"] = bool(
                (norm & LISTING_DATE_ALIASES)
                and not (norm & EFFECTIVE_DATE_ALIASES)
            )

        else:
            norm = (
                {norm_col(k) for k in obj.keys()}
                if isinstance(obj, dict)
                else set()
            )

            row["是否含CFI鍵"] = bool(
                norm & CFI_ALIASES
            )

            row["是否含分類日期鍵"] = bool(
                norm & EFFECTIVE_DATE_ALIASES
            )

            row["是否僅含上市日期鍵"] = bool(
                (norm & LISTING_DATE_ALIASES)
                and not (norm & EFFECTIVE_DATE_ALIASES)
            )

        row["JSON檢查狀態"] = (
            "JSON完整解析完成；鍵名僅作候選判斷"
        )

        return row

    except Exception as exc:
        row["JSON檢查狀態"] = "JSON解析失敗"
        row["錯誤訊息"] = (
            f"{type(exc).__name__}: {str(exc)[:1000]}"
        )

        return row


# =============================================================================
# 07. 建立實體檔案及 ZIP 成員清單
# =============================================================================

def enumerate_sources(root, out_dir):
    physical = []
    archives = []
    table_candidates = []
    json_candidates = []

    walk_dirs = 0
    walk_files = 0

    excluded_dirs = {
        ".ipynb_checkpoints",
        "__pycache__",
        ".Trash",
        ".trash",
        ".git",
        "node_modules",
    }

    for current, dirs, files in os.walk(
        root,
        topdown=True,
        followlinks=False,
    ):
        walk_dirs += 1

        dirs[:] = [
            d for d in dirs
            if d not in excluded_dirs
            and not d.startswith(".tmp")
            and (Path(current) / d).resolve() != out_dir.resolve()
        ]

        for filename in files:
            path = Path(current) / filename
            walk_files += 1

            if out_dir.resolve() in path.resolve().parents:
                continue

            if path == ZIP_PATH:
                continue

            if filename.startswith("~$") or filename.endswith(".tmp"):
                continue

            try:
                stat = path.stat()
            except Exception:
                continue

            relative = str(path.relative_to(root))
            lower = filename.lower()

            if lower.endswith(ARCHIVE_EXTENSION):
                archives.append({
                    "path": path,
                    "relative_path": relative,
                    "size": int(stat.st_size),
                    "mtime": datetime.fromtimestamp(
                        stat.st_mtime,
                        TAIPEI,
                    ).isoformat(),
                })

            elif is_table_name(lower):
                candidate = {
                    "source_kind": "FILE",
                    "path": path,
                    "relative_path": relative,
                    "extension": next(
                        (
                            e for e in TABLE_EXTENSIONS
                            if lower.endswith(e)
                        ),
                        path.suffix.lower(),
                    ),
                    "source_size": int(stat.st_size),
                    "uncompressed_size": int(stat.st_size),
                    "mtime": datetime.fromtimestamp(
                        stat.st_mtime,
                        TAIPEI,
                    ).isoformat(),
                }

                if lower.endswith((".json", ".json.gz")):
                    json_candidates.append(candidate)

                elif is_supported_csv_name(lower):
                    table_candidates.append(candidate)

                else:
                    physical.append({
                        "相對路徑": relative,
                        "類型": "其他結構化格式未解析",
                        "大小bytes": int(stat.st_size),
                        "修改時間": datetime.fromtimestamp(
                            stat.st_mtime,
                            TAIPEI,
                        ).isoformat(),
                    })

            else:
                physical.append({
                    "相對路徑": relative,
                    "類型": "其他檔案",
                    "大小bytes": int(stat.st_size),
                    "修改時間": datetime.fromtimestamp(
                        stat.st_mtime,
                        TAIPEI,
                    ).isoformat(),
                })

    archive_member_rows = []
    zip_errors = []

    log(
        f"[SCAN] 實體檔案 {walk_files:,}；資料夾 {walk_dirs:,}；"
        f"ZIP容器 {len(archives):,}；"
        f"直接表格 {len(table_candidates) + len(json_candidates):,}。"
    )

    log("[SCAN] 建立 ZIP 內部表格成員索引。")

    for idx, archive in enumerate(archives, start=1):

        try:
            with zipfile.ZipFile(
                archive["path"],
                "r",
            ) as zf:

                for info in zf.infolist():
                    if info.is_dir():
                        continue

                    member = info.filename
                    lower = member.lower()

                    if not is_table_name(lower):
                        continue

                    if (
                        "__macosx/" in lower
                        or lower.endswith("/.ds_store")
                    ):
                        continue

                    candidate = {
                        "source_kind": "ZIP_MEMBER",
                        "archive_path": archive["path"],
                        "archive_relative": archive["relative_path"],
                        "member": member,
                        "relative_path": (
                            archive["relative_path"]
                            + "::"
                            + member
                        ),
                        "extension": next(
                            (
                                e for e in TABLE_EXTENSIONS
                                if lower.endswith(e)
                            ),
                            Path(lower).suffix,
                        ),
                        "source_size": int(info.compress_size),
                        "uncompressed_size": int(info.file_size),
                        "crc32": f"{info.CRC:08x}",
                        "mtime": archive["mtime"],
                    }

                    archive_member_rows.append({
                        "ZIP路徑": archive["relative_path"],
                        "ZIP成員": member,
                        "副檔名": candidate["extension"],
                        "壓縮大小bytes": int(info.compress_size),
                        "解壓大小bytes": int(info.file_size),
                        "CRC32": f"{info.CRC:08x}",
                        "壓縮方法": str(info.compress_type),
                    })

                    if lower.endswith((".json", ".json.gz")):
                        json_candidates.append(candidate)

                    elif is_supported_csv_name(lower):
                        table_candidates.append(candidate)

        except Exception as exc:
            zip_errors.append({
                "ZIP路徑": archive["relative_path"],
                "錯誤": (
                    f"{type(exc).__name__}: {str(exc)[:1000]}"
                ),
            })

        if idx % 50 == 0 or idx == len(archives):
            log(
                f"[ZIP INDEX] {idx:,}/{len(archives):,}；"
                f"表格成員 {len(archive_member_rows):,}。"
            )

    return {
        "physical": pd.DataFrame(physical),
        "archives": archives,
        "table_candidates": table_candidates,
        "json_candidates": json_candidates,
        "zip_members": pd.DataFrame(archive_member_rows),
        "zip_errors": pd.DataFrame(zip_errors),
        "walk_dirs": walk_dirs,
        "walk_files": walk_files,
    }


# =============================================================================
# 08. 主掃描與完全內容去重
# =============================================================================

def run_content_audit(index):
    candidates = index["table_candidates"]
    json_candidates = index["json_candidates"]

    table_rows = []
    json_rows = []

    hash_groups = defaultdict(list)
    hash_master_rows = {}

    log(
        f"[CONTENT] 開始檢查 CSV/TSV/TXT：{len(candidates):,} 項。"
    )

    for i, candidate in enumerate(candidates, start=1):
        row = scan_csv_candidate(candidate)
        row["來源識別"] = f"T{i:06d}"
        row["修改時間"] = candidate.get("mtime", "")

        table_rows.append(row)

        sha = row.get("內容SHA256", "")

        if sha:
            hash_groups[sha].append(row["來源識別"])

            if sha not in hash_master_rows:
                hash_master_rows[sha] = row

        if i % PROGRESS_EVERY == 0 or i == len(candidates):
            log(
                f"[CONTENT] {i:,}/{len(candidates):,}；"
                f"已辨識SHA內容群組 {len(hash_groups):,}。"
            )

    log(
        f"[CONTENT] 開始檢查 JSON：{len(json_candidates):,} 項。"
    )

    for i, candidate in enumerate(json_candidates, start=1):
        row = scan_json_candidate(candidate)
        row["來源識別"] = f"J{i:06d}"

        json_rows.append(row)

        sha = row.get("內容SHA256", "")

        if sha:
            hash_groups[sha].append(row["來源識別"])

        if i % PROGRESS_EVERY == 0 or i == len(json_candidates):
            log(f"[JSON] {i:,}/{len(json_candidates):,}")

    table_frame = pd.DataFrame(table_rows)
    json_frame = pd.DataFrame(json_rows)

    # 完全內容去重：SHA256 是解壓後原始表格內容，不是 ZIP 容器雜湊。
    duplicate_count = {}
    representative_ids = {}

    for sha, ids in hash_groups.items():
        for source_id in ids:
            duplicate_count[source_id] = len(ids)

        if ids:
            representative_ids[ids[0]] = True

    if not table_frame.empty:
        table_frame["完全相同內容副本數"] = (
            table_frame["來源識別"]
            .map(duplicate_count)
            .fillna(1)
            .astype(int)
        )

        table_frame["內容唯一"] = (
            table_frame["完全相同內容副本數"].eq(1)
        )

        table_frame["是否唯一內容代表"] = (
            table_frame["來源識別"]
            .map(representative_ids)
            .fillna(False)
        )

    if not json_frame.empty:
        json_frame["完全相同內容副本數"] = (
            json_frame["來源識別"]
            .map(duplicate_count)
            .fillna(1)
            .astype(int)
        )

        json_frame["內容唯一"] = (
            json_frame["完全相同內容副本數"].eq(1)
        )

        json_frame["是否唯一內容代表"] = (
            json_frame["來源識別"]
            .map(representative_ids)
            .fillna(False)
        )

    group_rows = []

    all_rows = pd.concat(
        [table_frame, json_frame],
        ignore_index=True,
        sort=False,
    )

    if not all_rows.empty:
        for sha, group in (
            all_rows.loc[
                all_rows["內容SHA256"].astype(str).ne("")
            ]
            .groupby("內容SHA256", sort=False)
        ):
            group_rows.append({
                "內容SHA256": sha,
                "完全相同副本數": int(len(group)),
                "來源識別": "；".join(
                    group["來源識別"].astype(str).tolist()[:100]
                ),
                "來源位置": " || ".join(
                    (
                        group.get(
                            "相對路徑",
                            pd.Series(dtype=str),
                        ).fillna("").astype(str)
                        + group.get(
                            "ZIP路徑",
                            pd.Series(dtype=str),
                        ).fillna("").astype(str)
                        + "::"
                        + group.get(
                            "ZIP成員",
                            pd.Series(dtype=str),
                        ).fillna("").astype(str)
                    ).tolist()[:20]
                ),
                "唯一內容": len(group) == 1,
            })

    hash_group_frame = pd.DataFrame(group_rows)

    return table_frame, json_frame, hash_group_frame


# =============================================================================
# 09. 結果分類、日期覆蓋及優先來源
# =============================================================================

def build_result_sets(table_frame, json_frame):
    if table_frame.empty:
        table_frame = pd.DataFrame()

    if table_frame.empty:
        ohlc = table_frame.copy()
        class_hist = table_frame.copy()
        static_roster = table_frame.copy()
        partial_price = table_frame.copy()

    else:
        ohlc = table_frame.loc[
            table_frame["OHLC判斷"]
            .astype(str)
            .str.startswith("完整OHLC")
            & pd.to_numeric(
                table_frame["有效OHLC列數"],
                errors="coerce",
            ).gt(0)
            & pd.to_numeric(
                table_frame["不同交易日期數"],
                errors="coerce",
            ).ge(2)
        ].copy()

        class_hist = table_frame.loc[
            table_frame["分類判斷"]
            .astype(str)
            .str.contains("歷史分類候選", na=False)
        ].copy()

        static_roster = table_frame.loc[
            table_frame["分類判斷"]
            .astype(str)
            .str.contains(
                "靜態名錄|只有上市/下市日期|單一日期快照",
                regex=True,
                na=False,
            )
        ].copy()

        partial_price = table_frame.loc[
            table_frame["OHLC判斷"]
            .astype(str)
            .str.contains(
                "部分價格|具OHLC欄位但",
                regex=True,
                na=False,
            )
        ].copy()

    # 真正跨入 2020 年以前的 OHLC：以內容日期，不以檔名日期。
    if not ohlc.empty:
        ohlc["最早交易日期_dt"] = pd.to_datetime(
            ohlc["最早交易日期"],
            errors="coerce",
        )

        ohlc_pre2020 = ohlc.loc[
            ohlc["最早交易日期_dt"].lt(
                pd.Timestamp("2020-01-01")
            )
        ].copy()

        ohlc_pre2020.drop(
            columns=["最早交易日期_dt"],
            inplace=True,
        )

    else:
        ohlc_pre2020 = ohlc.copy()

    # 每個 SHA256 群組只保留一筆代表來源。
    if not ohlc.empty:
        ohlc_unique = ohlc.loc[
            ohlc["是否唯一內容代表"].fillna(False)
        ].copy()

    else:
        ohlc_unique = ohlc.copy()

    if not class_hist.empty:
        class_unique = class_hist.loc[
            class_hist["是否唯一內容代表"].fillna(False)
        ].copy()

    else:
        class_unique = class_hist.copy()

    return {
        "ohlc": ohlc,
        "ohlc_pre2020": ohlc_pre2020,
        "ohlc_unique": ohlc_unique,
        "class_hist": class_hist,
        "class_unique": class_unique,
        "static_roster": static_roster,
        "partial_price": partial_price,
    }


# =============================================================================
# 10. GitHub 上傳（獨立 branch）
# =============================================================================

def run_git(args, cwd=None, env=None, input_bytes=None):
    p = subprocess.run(
        ["git"] + list(args),
        cwd=str(cwd) if cwd is not None else None,
        env=env,
        input=input_bytes,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )

    if p.returncode != 0:
        raise RuntimeError(
            "Git 指令失敗：git "
            + " ".join(map(str, args))
            + "\n"
            + p.stderr.decode(
                "utf-8",
                errors="replace",
            )[-5000:]
        )

    return p.stdout.decode(
        "utf-8",
        errors="replace",
    ).strip()


def get_github_token():
    token = os.environ.get("GITHUB_TOKEN", "").strip()

    if token:
        return token

    try:
        from google.colab import userdata

        token = str(
            userdata.get("GITHUB_TOKEN") or ""
        ).strip()

        if token:
            return token

    except Exception:
        pass

    raise RuntimeError(
        "找不到 GITHUB_TOKEN。分析成果已保留在 Drive；GitHub 尚未上傳。"
    )


def upload_github(out_dir, zip_path, run_id, token, summary):
    files = [
        p for p in sorted(out_dir.iterdir())
        if p.is_file()
    ] + [zip_path]

    too_big = [
        (p.name, p.stat().st_size)
        for p in files
        if p.stat().st_size >= 100 * 1024 * 1024
    ]

    if too_big:
        raise RuntimeError(
            "GitHub 單檔超過 100MB：" + repr(too_big)
        )

    branch = (
        f"audit/sso-v1.3.1-history-source-"
        f"{run_id}-{uuid.uuid4().hex[:4]}"
    )

    remote_dir = (
        f"audit/sso-v1.3.1-history-source/{run_id}"
    )

    index_lines = [
        "# SSO v1.3.1｜歷史 OHLC 與逐日證券分類來源精確驗證",
        "",
        f"- 執行時間：`{summary['執行時間台北']}`",
        f"- 實體檔案：`{summary['實體檔案數']:,}`",
        f"- ZIP容器：`{summary['ZIP容器數']:,}`",
        f"- 直接表格：`{summary['直接表格數']:,}`",
        f"- ZIP表格成員：`{summary['ZIP表格成員數']:,}`",
        f"- 完整OHLC跨日期候選：`{summary['完整OHLC跨日期候選數']:,}`",
        f"- 2020年前OHLC候選：`{summary['2020年前OHLC候選數']:,}`",
        f"- CFI歷史分類候選：`{summary['CFI歷史分類候選數']:,}`",
        f"- CFI靜態名錄：`{summary['CFI靜態名錄數']:,}`",
        "- 只讀取既有 Drive；不改動原始檔案。",
        "- GitHub 僅新增 audit branch，不更新 main。",
        "",
        "## 檔案 SHA256",
    ]

    for p in files:
        index_lines.append(
            f"- `{p.name}` — "
            f"{p.stat().st_size:,} bytes — "
            f"`{sha256_file(p)}`"
        )

    index_bytes = (
        "\n".join(index_lines) + "\n"
    ).encode("utf-8")

    with tempfile.TemporaryDirectory(
        prefix="sso_v131_git_"
    ) as td:

        temp = Path(td)
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

        log("[GITHUB 1/7] 建立獨立暫存 repository。")

        run_git(
            ["init", "--bare", str(bare)],
            env=env,
        )

        run_git(
            ["remote", "add", "origin", REPO_URL],
            cwd=bare,
            env=env,
        )

        log("[GITHUB 2/7] 讀取 main 基準 SHA。")

        run_git(
            [
                "-c", "protocol.version=2",
                "fetch",
                "--depth=1",
                "--filter=blob:none",
                "origin",
                f"refs/heads/{MAIN_BRANCH}",
            ],
            cwd=bare,
            env=env,
        )

        main_before = run_git(
            ["rev-parse", "FETCH_HEAD^{commit}"],
            cwd=bare,
            env=env,
        )

        run_git(
            ["read-tree", main_before],
            cwd=bare,
            env=env,
        )

        log(f"[PASS] main before：{main_before}")

        uploads = [
            (p, f"{remote_dir}/{p.name}", None)
            for p in files
        ]

        uploads.append((
            None,
            f"{remote_dir}/README_GitHub上傳索引.md",
            index_bytes,
        ))

        local_blobs = {}

        log("[GITHUB 3/7] 建立成果 Git blobs。")

        for i, (local_path, remote_path, inline) in enumerate(
            uploads,
            start=1,
        ):

            if local_path is not None:
                blob = run_git(
                    ["hash-object", "-w", str(local_path)],
                    cwd=bare,
                    env=env,
                )

                size = local_path.stat().st_size
                name = local_path.name

            else:
                blob = run_git(
                    ["hash-object", "-w", "--stdin"],
                    cwd=bare,
                    env=env,
                    input_bytes=inline,
                )

                size = len(inline)
                name = "README_GitHub上傳索引.md"

            run_git(
                [
                    "update-index",
                    "--add",
                    "--cacheinfo",
                    "100644",
                    blob,
                    remote_path,
                ],
                cwd=bare,
                env=env,
            )

            local_blobs[remote_path] = blob

            log(
                f"[ADD {i}/{len(uploads)}] "
                f"{name} | {size:,} bytes"
            )

        log("[GITHUB 4/7] 建立 audit commit。")

        tree_sha = run_git(
            ["write-tree"],
            cwd=bare,
            env=env,
        )

        commit_sha = run_git(
            [
                "-c", "user.name=SSO Audit Uploader",
                "-c", "user.email=sso-audit-uploader@users.noreply.github.com",
                "commit-tree",
                tree_sha,
                "-p",
                main_before,
                "-m",
                f"Add SSO v1.3.1 historical source audit ({run_id})",
            ],
            cwd=bare,
            env=env,
        )

        log("[GITHUB 5/7] 推送獨立 audit branch。")

        run_git(
            [
                "push",
                "origin",
                f"{commit_sha}:refs/heads/{branch}",
            ],
            cwd=bare,
            env=env,
        )

        remote_branch = run_git(
            [
                "ls-remote",
                "origin",
                f"refs/heads/{branch}",
            ],
            cwd=bare,
            env=env,
        ).split()

        if (
            not remote_branch
            or remote_branch[0] != commit_sha
        ):
            raise RuntimeError(
                "GitHub audit branch commit SHA read-back 失敗。"
            )

        log("[GITHUB 6/7] 逐檔 Git blob SHA read-back。")

        run_git(
            [
                "fetch",
                "--depth=1",
                "origin",
                f"refs/heads/{branch}",
            ],
            cwd=bare,
            env=env,
        )

        for path, expected_blob in local_blobs.items():
            actual_blob = run_git(
                [
                    "rev-parse",
                    f"FETCH_HEAD:{path}",
                ],
                cwd=bare,
                env=env,
            )

            if actual_blob != expected_blob:
                raise RuntimeError(
                    "GitHub blob SHA 不符：" + path
                )

        log(
            f"[PASS] GitHub 已核對 {len(local_blobs):,} 個 Git blobs。"
        )

        log("[GITHUB 7/7] 確認 main SHA 前後一致。")

        remote_main = run_git(
            [
                "ls-remote",
                "origin",
                f"refs/heads/{MAIN_BRANCH}",
            ],
            cwd=bare,
            env=env,
        ).split()

        main_after = (
            remote_main[0]
            if remote_main else ""
        )

        if main_after != main_before:
            raise RuntimeError(
                f"GitHub main SHA 前後不同："
                f"{main_before} -> {main_after}"
            )

    return {
        "branch": branch,
        "commit": commit_sha,
        "main_before": main_before,
        "main_after": main_after,
        "uploaded_files": len(uploads),
        "folder_url": (
            f"https://github.com/{REPO_OWNER}/{REPO_NAME}"
            f"/tree/{branch}/{remote_dir}"
        ),
        "zip_url": (
            f"https://github.com/{REPO_OWNER}/{REPO_NAME}"
            f"/blob/{branch}/{remote_dir}/{zip_path.name}"
        ),
        "commit_url": (
            f"https://github.com/{REPO_OWNER}/{REPO_NAME}"
            f"/commit/{commit_sha}"
        ),
    }


# =============================================================================
# 11. 封存目前完整程式碼
# =============================================================================

def archive_current_program(out_dir):
    target = (
        out_dir
        / "SSO_v1.3.1_歷史OHLC與逐日證券分類來源精確驗證.py"
    )

    source = None

    try:
        shell = get_ipython()
        history = shell.history_manager.input_hist_raw

        for candidate in reversed(history):
            if (
                isinstance(candidate, str)
                and "SSO v1.3.1" in candidate
                and "def run_v131()" in candidate
                and "def scan_csv_candidate(" in candidate
                and len(candidate) > 25000
            ):
                source = candidate
                break

    except Exception:
        pass

    if not source:
        try:
            history = globals().get("In", [])

            for candidate in reversed(history):
                if (
                    isinstance(candidate, str)
                    and "SSO v1.3.1" in candidate
                    and "def run_v131()" in candidate
                    and len(candidate) > 25000
                ):
                    source = candidate
                    break

        except Exception:
            pass

    if not source:
        raise RuntimeError(
            "無法從 Colab 執行歷史取得完整 v1.3.1 程式原文，停止封存。"
        )

    compile(
        source,
        str(target),
        "exec",
    )

    target.write_text(
        source,
        encoding="utf-8",
    )

    return target


# =============================================================================
# 12. 主流程
# =============================================================================

def run_v131():
    if not ROOT.is_dir():
        raise FileNotFoundError(
            f"找不到 Google Drive 資料根目錄：{ROOT}"
        )

    if OUT_DIR.exists() or ZIP_PATH.exists():
        raise FileExistsError(
            "輸出路徑已存在，為避免覆寫而停止。"
        )

    OUT_DIR.mkdir(
        parents=True,
        exist_ok=False,
    )

    print("=" * 104)

    log(VERSION)
    log(f"研究根目錄：{ROOT}")
    log("只讀取歷史來源；不重跑 v1.3.0 報酬計算。")
    log("OHLC 需由內容欄位證明；CFI 歷史分類需有快照/生效日期。")
    log("listed_date 不視為分類生效日。")

    print("=" * 104)

    index = enumerate_sources(
        ROOT,
        OUT_DIR,
    )

    table_frame, json_frame, hash_group_frame = (
        run_content_audit(index)
    )

    sets = build_result_sets(
        table_frame,
        json_frame,
    )

    zip_member_frame = index["zip_members"]
    physical_frame = index["physical"]
    zip_error_frame = index["zip_errors"]

    direct_table_count = sum(
        1 for c in index["table_candidates"]
        if c["source_kind"] == "FILE"
    ) + sum(
        1 for c in index["json_candidates"]
        if c["source_kind"] == "FILE"
    )

    zip_table_count = sum(
        1 for c in index["table_candidates"]
        if c["source_kind"] == "ZIP_MEMBER"
    ) + sum(
        1 for c in index["json_candidates"]
        if c["source_kind"] == "ZIP_MEMBER"
    )

    unparsed_structured_direct = int(
        (index["physical"]["類型"] == "其他結構化格式未解析").sum()
    ) if not index["physical"].empty else 0

    unparsed_structured_zip = int(
        index["zip_members"]["副檔名"]
        .astype(str)
        .str.lower()
        .isin([".xlsx", ".xls", ".parquet"])
        .sum()
    ) if not index["zip_members"].empty else 0

    successful = int(
        table_frame["內容檢查狀態"]
        .astype(str)
        .str.contains("完成", na=False)
        .sum()
    ) if not table_frame.empty else 0

    # hash_group_frame 每列就是一個 SHA256 內容群組；
    # 包含單一檔及重複副本群組。
    sha_unique = int(len(hash_group_frame))

    class_hist_unique_count = len(sets["class_unique"])

    static_roster_unique_count = len(
        sets["static_roster"].loc[
            sets["static_roster"]["是否唯一內容代表"].fillna(False)
        ]
    ) if not sets["static_roster"].empty else 0

    summary = {
        "版本": VERSION,
        "執行時間台北": datetime.now(TAIPEI).isoformat(),
        "執行識別碼": RUN_ID,
        "掃描根目錄": str(ROOT),
        "資料夾數": int(index["walk_dirs"]),
        "實體檔案數": int(index["walk_files"]),
        "ZIP容器數": int(len(index["archives"])),
        "直接表格數": int(direct_table_count),
        "ZIP表格成員數": int(zip_table_count),
        "CSV類候選內容檢查數": int(len(table_frame)),
        "JSON候選內容檢查數": int(len(json_frame)),
        "CSV類成功完整讀取數": int(successful),
        "未解析其他結構化實體檔案數": unparsed_structured_direct,
        "未解析其他結構化ZIP成員數": unparsed_structured_zip,
        "SHA256唯一內容數": int(sha_unique),
        "SHA256重複內容群組數": int(
            hash_group_frame["完全相同副本數"].gt(1).sum()
        ) if not hash_group_frame.empty else 0,
        "完整OHLC跨日期候選數": int(len(sets["ohlc"])),
        "完整OHLC唯一內容數": int(len(sets["ohlc_unique"])),
        "2020年前OHLC候選數": int(len(sets["ohlc_pre2020"])),
        "CFI歷史分類候選數": int(len(sets["class_hist"])),
        "CFI歷史分類唯一內容數": int(class_hist_unique_count),
        "CFI靜態名錄數": int(static_roster_unique_count),
        "非完整OHLC或部分價格來源數": int(len(sets["partial_price"])),
        "ZIP容器索引錯誤數": int(len(zip_error_frame)),
        "歷史OHLC已依內容欄位判定": True,
        "上市日期不當作分類有效日期": True,
        "SHA256以解壓後內容計算": True,
        "研究限制": [
            "OHLC欄位完整及跨日期只代表行情資料候選，不自動代表官方來源或已調整公司行動。",
            "CFI多日期快照/有效期間只代表分類歷史候選，仍須核對資料來源、CFI語意與日期連續性。",
            "listed_date、delisted_date 僅表示證券生命週期，不等於該日CFI分類快照。",
            "若沒有逐日或有效期間分類證據，不把2026-11-10普通股名單回套歷史。",
            "ZIP內表格成員以解壓後內容SHA256去重；同一資料在多個封存ZIP不重複計為獨立來源。",
        ],
    }

    # 所有候選表格與 JSON 清單。
    archive_frame = pd.DataFrame([
        {
            "相對路徑": a["relative_path"],
            "檔案大小bytes": a["size"],
            "修改時間": a["mtime"],
        }
        for a in index["archives"]
    ])

    csv_write(
        index["physical"],
        OUT_DIR / "00_其他實體檔案清單.csv",
    )

    csv_write(
        archive_frame,
        OUT_DIR / "01_ZIP容器清單.csv",
    )

    csv_write(
        index["zip_members"],
        OUT_DIR / "02_ZIP內表格成員完整清單.csv",
    )

    csv_write(
        table_frame,
        OUT_DIR / "03_表格內容及SHA256精確檢查.csv",
    )

    csv_write(
        json_frame,
        OUT_DIR / "04_JSON結構化內容檢查.csv",
    )

    csv_write(
        hash_group_frame,
        OUT_DIR / "05_解壓後內容SHA256去重群組.csv",
    )

    csv_write(
        sets["ohlc"],
        OUT_DIR / "06_完整OHLC跨日期來源候選.csv",
    )

    csv_write(
        sets["ohlc_unique"],
        OUT_DIR / "07_完整OHLC唯一內容來源.csv",
    )

    csv_write(
        sets["ohlc_pre2020"],
        OUT_DIR / "08_內容證實2020年前OHLC來源.csv",
    )

    csv_write(
        sets["class_hist"],
        OUT_DIR / "09_CFI多日期或有效期間歷史分類候選.csv",
    )

    csv_write(
        sets["static_roster"],
        OUT_DIR / "10_CFI靜態名錄及單日分類來源.csv",
    )

    csv_write(
        sets["partial_price"],
        OUT_DIR / "11_部分價格欄位及未達完整OHLC來源.csv",
    )

    csv_write(
        zip_error_frame,
        OUT_DIR / "12_ZIP容器索引錯誤.csv",
    )

    json_write(
        summary,
        OUT_DIR / "13_v131精確驗證結論.json",
    )

    # 精簡摘要。
    summary_lines = [
        "=" * 104,
        VERSION,
        "=" * 104,
        f"執行時間：{summary['執行時間台北']}",
        f"實體檔案：{summary['實體檔案數']:,}",
        f"ZIP容器：{summary['ZIP容器數']:,}",
        f"直接表格：{direct_table_count:,}",
        f"ZIP內表格成員：{zip_table_count:,}",
        f"內容檢查完成：{successful:,}",
        f"未解析其他結構化實體檔案：{unparsed_structured_direct:,}",
        f"未解析其他結構化ZIP成員：{unparsed_structured_zip:,}",
        f"SHA256唯一內容：{sha_unique:,}",
        "",
        "一、OHLC 精確判定",
        f"完整 OHLC 跨日期候選：{len(sets['ohlc']):,}",
        f"完整 OHLC 唯一內容：{len(sets['ohlc_unique']):,}",
        f"內容日期證實早於 2020-01-01：{len(sets['ohlc_pre2020']):,}",
        "完整 OHLC 必須同時有交易日期、證券代號、開盤、最高、最低、收盤欄位，且有有效資料列與至少兩個不同日期。",
        "若沒有市場欄位，另標記是否可由路徑推定 TWSE/TPEx；不把推定市場等同欄位證據。",
        "",
        "二、證券分類歷史證據",
        f"CFI 多日期快照或有效期間候選：{len(sets['class_hist']):,}",
        f"CFI 歷史分類唯一內容：{len(sets['class_unique']):,}",
        f"CFI 靜態名錄/單日分類：{static_roster_unique_count:,}",
        "listed_date / 上市日期 / 上櫃日期 / delisted_date 不視為分類生效日期。",
        "有多個上市日期不等於有多個 CFI 分類快照。",
        "分類候選仍須核對來源、CFI 值語意及日期連續性。",
        "",
        "三、SHA256 去重",
        "SHA256 對 CSV 解壓後內容計算；相同檔案放在不同資料夾或不同 ZIP，不重複計為獨立來源。",
        f"完全相同內容群組：{len(hash_group_frame):,}",
        "",
        "四、ZIP 及讀取錯誤",
        f"ZIP 容器索引錯誤：{len(zip_error_frame):,}",
        "ZIP 成員逐一讀取；完整串流讀取時會觸發該成員的 CRC 檢查。",
        "",
        "五、研究限制",
        "OHLC 欄位完整不等於官方資料、公司行動調整或價格尺度連續性已驗證。",
        "有歷史 CFI 快照候選仍不等於每個交易日均完整；須以日期及證券鍵檢查覆蓋率。",
        "不把單日普通股分類回套至歷史日期。",
        "本版只盤點與驗證既有來源，不重新計算 v1.3.0 Q1-Q5 績效。",
        "",
        "六、資料安全",
        "Drive 原始來源唯讀；沒有刪除、搬移、覆寫或解壓回寫原始檔案。",
        "GitHub 僅新增獨立 audit branch；main SHA 前後必須一致。",
        "=" * 104,
    ]

    (OUT_DIR / "14_執行摘要.txt").write_text(
        "\n".join(summary_lines) + "\n",
        encoding="utf-8-sig",
    )

    # 完整程式原文；先寫程式，再建立 README/ZIP。
    program_path = archive_current_program(OUT_DIR)

    readme = [
        "# SSO v1.3.1｜歷史 OHLC 與逐日證券分類來源精確驗證",
        "",
        f"- 執行時間：`{summary['執行時間台北']}`",
        f"- 完整 OHLC 跨日期候選：`{len(sets['ohlc']):,}`",
        f"- 2020 年前 OHLC 候選：`{len(sets['ohlc_pre2020']):,}`",
        f"- CFI 多日期/有效期間分類候選：`{len(sets['class_hist']):,}`",
        f"- CFI 靜態名錄：`{static_roster_unique_count:,}`",
        "",
        "判定採內容欄位及資料列，不以檔名/路徑推定 OHLC 或分類。",
        "listed_date 不視為分類有效日期。SHA256 對解壓後內容計算。",
        "Drive 原始來源唯讀。GitHub 僅新增 audit branch，不更新 main。",
        "",
        "## 成果檔案 SHA256",
    ]

    for path in sorted(OUT_DIR.iterdir()):
        if path.is_file():
            readme.append(
                f"- `{path.name}` — "
                f"{path.stat().st_size:,} bytes — "
                f"`{sha256_file(path)}`"
            )

    (OUT_DIR / "README_稽核說明.md").write_text(
        "\n".join(readme) + "\n",
        encoding="utf-8",
    )

    # ZIP。
    log("[ZIP] 建立成果 ZIP。")

    with zipfile.ZipFile(
        ZIP_PATH,
        "w",
        compression=zipfile.ZIP_DEFLATED,
        compresslevel=6,
    ) as zf:

        for path in sorted(OUT_DIR.iterdir()):
            if path.is_file():
                zf.write(
                    path,
                    arcname=path.name,
                )

    with zipfile.ZipFile(ZIP_PATH, "r") as zf:
        bad = zf.testzip()

    if bad:
        raise RuntimeError(
            "成果 ZIP CRC 失敗：" + bad
        )

    zip_sha = sha256_file(ZIP_PATH)

    log(
        f"[PASS] ZIP CRC；SHA256={zip_sha}；"
        f"大小={ZIP_PATH.stat().st_size:,} bytes。"
    )

    # GitHub。
    log("[GITHUB] 準備上傳獨立 audit branch。")

    token = get_github_token()

    github = upload_github(
        OUT_DIR,
        ZIP_PATH,
        RUN_ID,
        token,
        summary,
    )

    verification = {
        "版本": VERSION,
        "執行識別碼": RUN_ID,
        "輸出資料夾": str(OUT_DIR),
        "ZIP": str(ZIP_PATH),
        "ZIP_SHA256": zip_sha,
        "ZIP_bytes": ZIP_PATH.stat().st_size,
        "GitHub": github,
        "驗證": {
            "content_inspection": True,
            "ohlc_requires_actual_fields_and_valid_rows": True,
            "listed_date_not_classification_effective_date": True,
            "decompressed_content_sha256_dedup": True,
            "github_blob_readback": True,
            "main_sha_unchanged": (
                github["main_before"] == github["main_after"]
            ),
            "drive_source_readonly": True,
        },
    }

    verification_path = (
        ROOT / f"{OUT_DIR.name}_GitHub上傳核驗.json"
    )

    json_write(
        verification,
        verification_path,
    )

    # 終端輸出。
    print("\n" + "=" * 104)
    print("[PASS] SSO v1.3.1 歷史來源精確驗證完成")
    print("=" * 104)

    print(f"實體檔案：{summary['實體檔案數']:,}")
    print(f"ZIP容器：{summary['ZIP容器數']:,}")
    print(f"直接表格：{direct_table_count:,}")
    print(f"ZIP內表格成員：{zip_table_count:,}")
    print(f"完整OHLC跨日期候選：{len(sets['ohlc']):,}")
    print(f"完整OHLC唯一內容：{len(sets['ohlc_unique']):,}")
    print(f"2020年前OHLC候選：{len(sets['ohlc_pre2020']):,}")
    print(f"CFI歷史分類候選：{len(sets['class_hist']):,}")
    print(f"CFI靜態名錄：{static_roster_unique_count:,}")
    print(f"SHA256內容群組：{len(hash_group_frame):,}")
    print(f"ZIP SHA256：{zip_sha}")
    print(f"輸出資料夾：{OUT_DIR}")
    print(f"ZIP：{ZIP_PATH}")
    print(f"GitHub branch：{github['branch']}")
    print(f"GitHub commit：{github['commit']}")
    print(f"GitHub 資料夾：{github['folder_url']}")
    print(f"GitHub ZIP：{github['zip_url']}")
    print(f"GitHub commit URL：{github['commit_url']}")
    print(f"main before：{github['main_before']}")
    print(f"main after：{github['main_after']}")
    print(f"上傳核驗：{verification_path}")

    print("[PASS] Drive 原始來源唯讀。")
    print("[PASS] GitHub 逐檔 SHA read-back。")
    print("[PASS] GitHub main SHA 前後一致。")
    print("=" * 104)

    return {
        "output_dir": str(OUT_DIR),
        "zip_path": str(ZIP_PATH),
        "zip_sha256": zip_sha,
        "github": github,
        "summary": summary,
        "ohlc": sets["ohlc"],
        "historical_classification": sets["class_hist"],
    }


# =============================================================================
# 13. 執行
# =============================================================================

RESULT = run_v131()