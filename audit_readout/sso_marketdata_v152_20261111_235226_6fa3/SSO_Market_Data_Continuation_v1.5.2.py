# -*- coding: utf-8 -*-
# SSO_Market_Data_Continuation_TWSE_TPEX_v1.5.2
# 從各市場最後留有行情日期的下一日起，向前補充官方每日行情。
# 不回頭檢查舊行情、不修改任何既有歷史底稿或增量檔。

from __future__ import annotations

import os
import re
import csv
import json
import gzip
import time
import uuid
import base64
import hashlib
import zipfile
import io
import sys

from pathlib import Path
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo
from collections import defaultdict

import requests
import pandas as pd


# =============================================================================
# 1. 基本設定
# =============================================================================

VERSION = "SSO_Market_Data_Continuation_TWSE_TPEX_v1.5.2"

ROOT = Path(
    "/content/drive/MyDrive/看股最速適/"
    "SSO_Market_Data_Continuation_TWSE_TPEX_v1"
)

REPO = "CHLINX/sso"

MIN_ROWS = 500

TZ = ZoneInfo("Asia/Taipei")


# =============================================================================
# 2. Google Drive
# =============================================================================

def mount_drive():

    if not Path("/content/drive/MyDrive").exists():

        try:
            from google.colab import drive
            drive.mount("/content/drive", force_remount=False)

        except Exception as e:
            raise RuntimeError(f"Google Drive 掛載失敗：{e}")

    ROOT.mkdir(parents=True, exist_ok=True)


# =============================================================================
# 3. 共用函式
# =============================================================================

def log(msg, logs):

    line = (
        f"[{datetime.now(TZ).strftime('%H:%M:%S')}] "
        f"{msg}"
    )

    print(line, flush=True)

    logs.append(line)


def norm_label(x):

    s = re.sub(r"<[^>]*>", "", str(x)).strip().lower()

    return re.sub(
        r"[\s\u3000_\-（）()【】\[\]／/：:、,.%]+",
        "",
        s
    )


def market_name(x):

    s = str(x).strip().upper()

    if s in {"TWSE", "上市", "集中市場"}:
        return "TWSE"

    if s in {"TPEX", "上櫃", "櫃買"}:
        return "TPEX"

    return s


def parse_date_value(v):

    s = str(v).strip()

    if not s:
        return None

    try:

        d = pd.to_datetime(s, errors="raise")

        return d.date()

    except Exception:
        pass

    m = re.fullmatch(
        r"(\d{3,4})[/-]?(\d{1,2})[/-]?(\d{1,2})",
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

    return None


def read_csv_header_bytes(raw):

    text = raw.decode("utf-8-sig", errors="replace")

    return next(csv.reader(io.StringIO(text)))


def read_small_csv(path, nrows=5):

    for enc in ("utf-8-sig", "cp950", "big5", "gb18030"):

        try:

            return pd.read_csv(
                path,
                dtype=str,
                encoding=enc,
                nrows=nrows,
                keep_default_na=False
            )

        except Exception:
            continue

    raise RuntimeError(f"無法讀取 CSV：{path}")


def find_col(cols, aliases):

    lookup = {
        norm_label(c): c
        for c in cols
    }

    for a in aliases:

        k = norm_label(a)

        if k in lookup:
            return lookup[k]

    return None


FIELD_ALIASES = {

    "trade_date": [
        "trade_date",
        "date",
        "日期",
        "交易日期",
        "資料日期",
        "年月日"
    ],

    "market": [
        "market",
        "市場",
        "市場別",
        "交易市場"
    ],

    "instrument_code": [
        "instrument_code",
        "code",
        "stock_id",
        "stock_code",
        "證券代號",
        "證券代碼",
        "代號",
        "股票代號"
    ],

    "instrument_name": [
        "instrument_name",
        "name",
        "stock_name",
        "證券名稱",
        "名稱",
        "股票名稱"
    ],

    "open": [
        "open",
        "opening_price",
        "open_price",
        "開盤",
        "開盤價",
        "開盤價格"
    ],

    "high": [
        "high",
        "highest_price",
        "high_price",
        "最高",
        "最高價",
        "最高價格"
    ],

    "low": [
        "low",
        "lowest_price",
        "low_price",
        "最低",
        "最低價",
        "最低價格"
    ],

    "close": [
        "close",
        "closing_price",
        "close_price",
        "收盤",
        "收盤價",
        "收盤價格"
    ],

    "volume": [
        "volume",
        "trade_volume",
        "trading_volume",
        "成交股數",
        "成交量"
    ],

    "trade_value": [
        "trade_value",
        "turnover",
        "amount",
        "成交金額",
        "成交金額元",
        "成交金額(元)"
    ],

    "price_change": [
        "price_change",
        "change",
        "漲跌",
        "漲跌價差",
        "漲跌點數"
    ],

    "transactions": [
        "transactions",
        "transaction_count",
        "成交筆數",
        "筆數",
        "交易筆數"
    ],

    "source": [
        "source",
        "來源",
        "資料來源",
        "source_api",
        "來源端點"
    ],
}


def validate_target_schema(cols, name, per_security=False):

    found = {
        k: find_col(cols, v)
        for k, v in FIELD_ALIASES.items()
    }

    must = [
        "trade_date",
        "open",
        "high",
        "low",
        "close",
        "volume"
    ]

    if not per_security:

        must += [
            "market",
            "instrument_code"
        ]

    missing = [
        k for k in must
        if found.get(k) is None
    ]

    if missing:

        raise RuntimeError(
            f"{name} 欄位格式不相容，"
            f"缺少必要欄位：{missing}；"
            f"現有欄位={cols}"
        )

    return found


def map_record_to_schema(record, columns):

    out = {}

    for col in columns:

        target = norm_label(col)

        value = ""

        for canonical, aliases in FIELD_ALIASES.items():

            if target in {
                norm_label(a)
                for a in aliases
            }:

                value = record.get(canonical, "")

                break

        out[col] = "" if value is None else value

    return out


def write_rows_csv(path, rows, columns):

    path = Path(path)

    path.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    with path.open(
        "w",
        encoding="utf-8-sig",
        newline=""
    ) as f:

        w = csv.DictWriter(
            f,
            fieldnames=list(columns),
            extrasaction="ignore"
        )

        w.writeheader()

        for row in rows:

            w.writerow(
                map_record_to_schema(row, columns)
            )


def safe_num(v, integer=False):

    s = (
        str(v)
        .strip()
        .replace(",", "")
        .replace("＋", "+")
        .replace("－", "-")
    )

    if s in {
        "",
        "--",
        "---",
        "-",
        "X",
        "x",
        "N/A",
        "nan",
        "None"
    }:

        return None

    s = s.replace("%", "")

    try:

        n = float(s)

        if integer:
            return int(round(n))

        return n

    except Exception:
        return None


def code_clean(v):

    return re.sub(
        r"\s+",
        "",
        str(v or "")
    ).strip()


# =============================================================================
# 4. 找出前次最後行情日期
#
# 只讀取 v1.5.x 增量成果。
# 不掃描 v2.8 十年歷史底稿。
# 不逐筆回查任何舊行情。
# =============================================================================

def find_previous_increment_sources(root):

    dirs = [
        p for p in root.iterdir()
        if p.is_dir()
        and re.match(r"v1\.5\.\d+", p.name)
    ]

    dirs.sort(
        key=lambda p: p.stat().st_mtime
    )

    return dirs


def read_market_dates_from_df(df, latest):

    if df.empty:
        return

    cols = list(df.columns)

    dc = find_col(
        cols,
        FIELD_ALIASES["trade_date"]
    )

    mc = find_col(
        cols,
        FIELD_ALIASES["market"]
    )

    if not dc or not mc:
        return

    for _, r in df[[dc, mc]].iterrows():

        d = parse_date_value(r[dc])

        m = market_name(r[mc])

        if d and m in latest:

            if latest[m] is None or d > latest[m]:

                latest[m] = d


def load_previous_state(root, logs):

    latest = {
        "TWSE": None,
        "TPEX": None
    }

    dirs = find_previous_increment_sources(root)

    # 優先讀取先前版本已封存的市場日期狀態。
    for d in dirs:

        state_files = (
            list(d.glob("latest_market_dates.json"))
            +
            list(d.glob("**/latest_market_dates.json"))
        )

        for sf in state_files:

            try:

                obj = json.loads(
                    sf.read_text(encoding="utf-8")
                )

                dates = obj.get(
                    "market_last_date",
                    obj.get("latest_market_dates", {})
                )

                for mk in latest:

                    parsed = parse_date_value(
                        dates.get(mk)
                    )

                    if parsed and (
                        latest[mk] is None
                        or parsed > latest[mk]
                    ):

                        latest[mk] = parsed

            except Exception as e:

                log(
                    f"[STATE WARN] {sf}: {e}",
                    logs
                )

    # 讀取既有 v1.5.x 標準化增量 CSV 的日期欄位。
    # 不載入舊行情的完整內容，只需要日期與市場欄位。
    for d in dirs:

        for p in d.glob("**/全市場標準化增量_*.csv"):

            try:

                df = pd.read_csv(
                    p,
                    dtype=str,
                    encoding="utf-8-sig",
                    usecols=lambda c: norm_label(c) in {
                        "tradedate",
                        "date",
                        "日期",
                        "交易日期",
                        "資料日期",
                        "market",
                        "市場",
                        "市場別",
                        "交易市場"
                    },
                    keep_default_na=False
                )

                read_market_dates_from_df(
                    df,
                    latest
                )

                log(
                    f"[LAST DATE SOURCE] {p.name}",
                    logs
                )

            except Exception as e:

                log(
                    f"[DATE CSV WARN] {p}: {e}",
                    logs
                )

    # 若沒有 v1.5.x 成果，才使用 v1.4.2 標準化行情作為日期依據。
    if not any(latest.values()):

        v142_dirs = [
            p for p in root.iterdir()
            if p.is_dir()
            and p.name.startswith("v1.4.2")
        ]

        for d in v142_dirs:

            for p in d.glob("**/雙市場逐日官方行情.csv"):

                try:

                    df = pd.read_csv(
                        p,
                        dtype=str,
                        encoding="utf-8-sig",
                        usecols=lambda c: norm_label(c) in {
                            "tradedate",
                            "date",
                            "日期",
                            "交易日期",
                            "資料日期",
                            "market",
                            "市場",
                            "市場別",
                            "交易市場"
                        },
                        keep_default_na=False
                    )

                    read_market_dates_from_df(
                        df,
                        latest
                    )

                    log(
                        f"[LAST DATE SOURCE V142] {p}",
                        logs
                    )

                except Exception as e:

                    log(
                        f"[V142 CSV WARN] {e}",
                        logs
                    )

        if not any(latest.values()):

            for z in root.glob("**/v1.4.2*.zip"):

                try:

                    with zipfile.ZipFile(z) as zf:

                        names = [
                            n for n in zf.namelist()
                            if n.endswith("雙市場逐日官方行情.csv")
                        ]

                        if names:

                            raw = zf.read(names[0])

                            df = pd.read_csv(
                                io.BytesIO(raw),
                                dtype=str,
                                encoding="utf-8-sig",
                                usecols=lambda c: norm_label(c) in {
                                    "tradedate",
                                    "date",
                                    "日期",
                                    "交易日期",
                                    "資料日期",
                                    "market",
                                    "市場",
                                    "市場別",
                                    "交易市場"
                                },
                                keep_default_na=False
                            )

                            read_market_dates_from_df(
                                df,
                                latest
                            )

                            log(
                                f"[LAST DATE SOURCE V142 ZIP] {z}",
                                logs
                            )

                            break

                except Exception as e:

                    log(
                        f"[V142 ZIP WARN] {z}: {e}",
                        logs
                    )

    if not any(latest.values()):

        raise RuntimeError(
            "找不到既有官方增量行情的最後日期；"
            "為避免從錯誤日期開始，程式停止。"
        )

    missing = [
        m for m, d in latest.items()
        if d is None
    ]

    if missing:

        raise RuntimeError(
            f"既有資料未能辨識以下市場的最後行情日期：{missing}；"
            "不會自行猜測。"
        )

    return latest, dirs


# =============================================================================
# 5. 一次性格式檢查
#
# 只取既有標準化檔與代表性個股檔的欄位表頭。
# 不重新檢查歷史行情內容。
# =============================================================================

def find_schema_templates(root, dirs):

    standard_cols = None

    overlay_cols = {}

    for d in reversed(dirs):

        std_files = list(
            d.glob("**/全市場標準化增量_*.csv")
        )

        if std_files and standard_cols is None:

            standard_cols = read_csv_header_bytes(
                std_files[-1].read_bytes()
            )

        overlay_dir = (
            d / "overlay" / "官方歷史行情"
        )

        if overlay_dir.exists():

            for market, patt in (
                ("TWSE", "*_TWSE.csv"),
                ("TPEX", "*_TPEx.csv")
            ):

                fs = list(
                    overlay_dir.glob(patt)
                )

                if fs and market not in overlay_cols:

                    overlay_cols[market] = (
                        read_csv_header_bytes(
                            fs[0].read_bytes()
                        )
                    )

        if standard_cols and len(overlay_cols) == 2:
            break

    # 若既有目錄未解壓，直接從前次封存 ZIP 取得欄位表頭。
    if standard_cols is None or len(overlay_cols) < 2:

        zips = []

        for d in dirs:

            zips.extend(d.glob("*.zip"))

            zips.extend(d.glob("**/*.zip"))

        zips = sorted(
            set(zips),
            key=lambda p: p.stat().st_mtime
        )

        for zp in reversed(zips):

            try:

                with zipfile.ZipFile(zp) as zf:

                    names = zf.namelist()

                    if standard_cols is None:

                        n = next(
                            (
                                x for x in names
                                if Path(x).name.startswith(
                                    "全市場標準化增量_"
                                )
                                and x.endswith(".csv")
                            ),
                            None
                        )

                        if n:

                            standard_cols = (
                                read_csv_header_bytes(
                                    zf.read(n)
                                )
                            )

                    for market, suffix in (
                        ("TWSE", "_TWSE.csv"),
                        ("TPEX", "_TPEx.csv")
                    ):

                        if market not in overlay_cols:

                            n = next(
                                (
                                    x for x in names
                                    if "overlay/官方歷史行情/" in x
                                    and x.endswith(suffix)
                                ),
                                None
                            )

                            if n:

                                overlay_cols[market] = (
                                    read_csv_header_bytes(
                                        zf.read(n)
                                    )
                                )

                if standard_cols and len(overlay_cols) == 2:
                    break

            except Exception:
                continue

    if standard_cols is None:

        standard_cols = [
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

    for market in ("TWSE", "TPEX"):

        if market not in overlay_cols:

            overlay_cols[market] = list(
                standard_cols
            )

    validate_target_schema(
        standard_cols,
        "全市場標準化增量",
        per_security=False
    )

    for market in ("TWSE", "TPEX"):

        validate_target_schema(
            overlay_cols[market],
            f"{market} 個股覆蓋檔",
            per_security=True
        )

    return standard_cols, overlay_cols


# =============================================================================
# 6. 官方行情下載與解析
# =============================================================================

def make_session():

    s = requests.Session()

    s.headers.update({

        "User-Agent": (
            "Mozilla/5.0 "
            "(compatible; SSO-Market-Data-Continuation/1.5.2)"
        ),

        "Accept": "application/json,text/plain,*/*"

    })

    return s


def extract_table_candidates(payload):

    out = []

    if isinstance(payload, dict):

        tables = payload.get("tables")

        if isinstance(tables, list):

            for t in tables:

                if (
                    isinstance(t, dict)
                    and isinstance(t.get("fields"), list)
                    and isinstance(t.get("data"), list)
                ):

                    out.append(t)

        if (
            isinstance(payload.get("fields"), list)
            and isinstance(payload.get("data"), list)
        ):

            out.append(payload)

        data_obj = payload.get("data")

        if (
            isinstance(data_obj, dict)
            and isinstance(data_obj.get("fields"), list)
            and isinstance(data_obj.get("data"), list)
        ):

            out.append(data_obj)

    return out


def parse_market_payload(payload, market, requested_date):

    aliases = {

        "code": [
            "證券代號",
            "證券代碼",
            "代號",
            "股票代號",
            "code"
        ],

        "name": [
            "證券名稱",
            "名稱",
            "股票名稱",
            "name"
        ],

        "open": [
            "開盤價",
            "開盤",
            "openingprice",
            "open"
        ],

        "high": [
            "最高價",
            "最高",
            "highestprice",
            "high"
        ],

        "low": [
            "最低價",
            "最低",
            "lowestprice",
            "low"
        ],

        "close": [
            "收盤價",
            "收盤",
            "closingprice",
            "close"
        ],

        "volume": [
            "成交股數",
            "成交量",
            "tradevolume",
            "volume"
        ],

        "trade_value": [
            "成交金額",
            "成交金額元",
            "成交金額(元)",
            "tradevalue",
            "turnover"
        ],

        "transactions": [
            "成交筆數",
            "筆數",
            "transaction",
            "transactions"
        ],

        "change": [
            "漲跌價差",
            "漲跌",
            "change",
            "pricechange"
        ],

        "sign": [
            "漲跌(+/-)",
            "漲跌符號",
            "漲跌記號"
        ],

    }

    required = [
        "code",
        "open",
        "high",
        "low",
        "close",
        "volume"
    ]

    selected = None

    idx = None

    for table in extract_table_candidates(payload):

        fields = table.get("fields", [])

        loc = {
            k: find_col(fields, vals)
            for k, vals in aliases.items()
        }

        if all(
            loc.get(k) is not None
            for k in required
        ):

            selected = table

            idx = loc

            break

    if selected is None:

        tables = extract_table_candidates(payload)

        has_rows = any(
            t.get("data")
            for t in tables
        )

        if not has_rows:

            return (
                [],
                "NO_DATA",
                "官方端點未回傳行情列"
            )

        raise RuntimeError(
            f"{market} 官方行情欄位格式無法辨識；"
            f"tables={[(t.get('title'), t.get('fields')) for t in tables[:3]]}"
        )

    fields = selected["fields"]

    indexes = {
        k: fields.index(v) if v in fields else None
        for k, v in idx.items()
    }

    rows = selected.get("data", [])

    records = []

    for row in rows:

        if not isinstance(row, (list, tuple)):
            continue

        def cell(k):

            i = indexes.get(k)

            if i is not None and i < len(row):
                return row[i]

            return ""

        code = code_clean(
            cell("code")
        )

        if not code:
            continue

        if not re.fullmatch(
            r"[0-9A-Za-z.\-]{2,16}",
            code
        ):
            continue

        change = safe_num(
            cell("change")
        )

        # TWSE 的漲跌符號與漲跌價差分開提供。
        if (
            indexes.get("sign") is not None
            and change is not None
        ):

            sign = str(
                cell("sign")
            ).strip()

            if sign == "-":
                change = -abs(change)

            elif sign == "+":
                change = abs(change)

        records.append({

            "trade_date": requested_date.isoformat(),

            "market": market,

            "instrument_code": code,

            "instrument_name": str(
                cell("name")
            ).strip(),

            "open": safe_num(
                cell("open")
            ),

            "high": safe_num(
                cell("high")
            ),

            "low": safe_num(
                cell("low")
            ),

            "close": safe_num(
                cell("close")
            ),

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
                else "TPEX_afterTrading_otc"
            ),

        })

    if not records:

        return (
            [],
            "NO_DATA",
            "資料表存在但沒有可辨識證券代號的行情列"
        )

    return records, "OK", ""


def fetch_one(session, market, day, out_dir, logs):

    if market == "TWSE":

        url = (
            "https://www.twse.com.tw/"
            "rwd/zh/afterTrading/MI_INDEX"
        )

        params = {
            "date": day.strftime("%Y%m%d"),
            "type": "ALLBUT0999",
            "response": "json"
        }

    else:

        url = (
            "https://www.tpex.org.tw/"
            "www/zh-tw/afterTrading/otc"
        )

        params = {
            "date": day.strftime("%Y/%m/%d"),
            "type": "AL",
            "response": "json"
        }

    meta = {

        "trade_date": day.isoformat(),

        "market": market,

        "url": url,

        "params": json.dumps(
            params,
            ensure_ascii=False
        ),

        "http_status": "",

        "content_type": "",

        "sha256": "",

        "raw_file": "",

        "row_count": 0,

        "status": "",

        "error": ""

    }

    try:

        resp = session.get(
            url,
            params=params,
            timeout=(15, 60)
        )

        meta["http_status"] = resp.status_code

        meta["content_type"] = resp.headers.get(
            "Content-Type",
            ""
        )

        raw = resp.content

        meta["sha256"] = hashlib.sha256(
            raw
        ).hexdigest()

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

        raw_path.write_bytes(
            gzip.compress(
                raw,
                compresslevel=6,
                mtime=0
            )
        )

        meta["raw_file"] = str(
            raw_path.relative_to(out_dir)
        ).replace("\\", "/")

        resp.raise_for_status()

        if (
            "html" in meta["content_type"].lower()
            or raw.lstrip().lower().startswith(
                (b"<!doctype html", b"<html")
            )
        ):

            raise RuntimeError(
                "HTTP 200 但回應為 HTML，疑似官方網站防護頁"
            )

        payload = resp.json()

        records, status, error = parse_market_payload(
            payload,
            market,
            day
        )

        meta["row_count"] = len(records)

        meta["status"] = status

        meta["error"] = error

        if (
            status == "OK"
            and len(records) < MIN_ROWS
        ):

            meta["status"] = "LOW_ROW_COUNT"

            meta["error"] = (
                f"行情列數 {len(records)} "
                f"少於最低完整性門檻 {MIN_ROWS}，"
                "不寫入增量"
            )

            records = []

        return records, meta

    except Exception as e:

        meta["status"] = "ERROR"

        meta["error"] = (
            f"{type(e).__name__}: {e}"
        )

        return [], meta


# =============================================================================
# 7. 輸出與封存
# =============================================================================

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


def save_source_from_history(out_dir, logs):

    try:

        ip = get_ipython()

        hist = ip.history_manager.input_hist_raw

        for cell in reversed(hist):

            if (
                "SSO_Market_Data_Continuation_TWSE_TPEX_v1.5.2" in cell
                and "def run_v152" in cell
                and len(cell) > 5000
            ):

                (
                    Path(out_dir)
                    / "SSO_Market_Data_Continuation_v1.5.2.py"
                ).write_text(
                    cell,
                    encoding="utf-8"
                )

                log(
                    "[SOURCE SAVED] 已封存本次完整程式碼",
                    logs
                )

                return

    except Exception as e:

        log(
            f"[SOURCE WARN] 無法自動封存 notebook 程式碼：{e}",
            logs
        )


def make_zip(out_dir, zip_path):

    with zipfile.ZipFile(
        zip_path,
        "w",
        compression=zipfile.ZIP_DEFLATED,
        compresslevel=6
    ) as zf:

        for p in sorted(
            Path(out_dir).rglob("*")
        ):

            if p.is_file() and p != zip_path:

                zf.write(
                    p,
                    p.relative_to(out_dir).as_posix()
                )


# =============================================================================
# 8. GitHub 獨立 audit branch
#
# 不修改 main。
# 上傳 ZIP、報告、標準化增量與索引。
# 逐檔回讀 Git blob SHA。
# =============================================================================

def github_upload(
    out_dir,
    zip_path,
    files_to_upload,
    run_id,
    logs
):

    token = os.environ.get(
        "GITHUB_TOKEN",
        ""
    ).strip()

    if not token:

        try:

            from google.colab import userdata

            token = str(
                userdata.get("GITHUB_TOKEN") or ""
            ).strip()

        except Exception:
            token = ""

    if not token:

        raise RuntimeError(
            "找不到 GITHUB_TOKEN"
        )

    api = "https://api.github.com"

    s = requests.Session()

    s.headers.update({

        "Authorization": f"Bearer {token}",

        "Accept": "application/vnd.github+json",

        "X-GitHub-Api-Version": "2022-11-28"

    })

    def req(method, path, **kwargs):

        r = s.request(
            method,
            api + path,
            timeout=(15, 120),
            **kwargs
        )

        if not r.ok:

            raise RuntimeError(
                f"GitHub API {method} {path} "
                f"HTTP {r.status_code}: {r.text[:1000]}"
            )

        return (
            r.json()
            if r.content
            else {}
        )

    owner, repo = REPO.split("/")

    # main 上傳前 SHA。
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

    branch = (
        f"audit/sso-marketdata-v152-{run_id}"
    )

    prefix = (
        f"audit_readout/sso_marketdata_v152_{run_id}"
    )

    tree_entries = []

    local_meta = []

    for p in files_to_upload:

        p = Path(p)

        data = p.read_bytes()

        b64 = base64.b64encode(
            data
        ).decode("ascii")

        blob = req(
            "POST",
            f"/repos/{owner}/{repo}/git/blobs",
            json={
                "content": b64,
                "encoding": "base64"
            }
        )

        local_sha = hashlib.sha1(
            b"blob "
            + str(len(data)).encode()
            + b"\0"
            + data
        ).hexdigest()

        if blob["sha"] != local_sha:

            raise RuntimeError(
                f"本機與 GitHub blob SHA 不一致：{p.name}"
            )

        tree_entries.append({

            "path": f"{prefix}/{p.name}",

            "mode": "100644",

            "type": "blob",

            "sha": blob["sha"]

        })

        local_meta.append({

            "path": f"{prefix}/{p.name}",

            "blob_sha": blob["sha"],

            "bytes": len(data),

            "sha256": hashlib.sha256(
                data
            ).hexdigest()

        })

    tree = req(
        "POST",
        f"/repos/{owner}/{repo}/git/trees",
        json={
            "base_tree": base_tree,
            "tree": tree_entries
        }
    )

    commit = req(
        "POST",
        f"/repos/{owner}/{repo}/git/commits",
        json={

            "message": (
                "SSO market data v1.5.2 "
                f"incremental update {run_id}"
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

    # GitHub Blob 回讀驗證。
    verified = []

    for x in local_meta:

        blob = req(
            "GET",
            f"/repos/{owner}/{repo}/git/blobs/{x['blob_sha']}"
        )

        remote = base64.b64decode(
            blob["content"]
        )

        if hashlib.sha256(
            remote
        ).hexdigest() != x["sha256"]:

            raise RuntimeError(
                f"GitHub 回讀驗證失敗：{x['path']}"
            )

        verified.append({

            "path": x["path"],

            "blob_sha": x["blob_sha"],

            "bytes": x["bytes"],

            "readback_pass": True

        })

    # main 上傳後 SHA。
    main_after = req(
        "GET",
        f"/repos/{owner}/{repo}/git/ref/heads/main"
    )["object"]["sha"]

    return {

        "repository": REPO,

        "branch": branch,

        "commit_sha": commit["sha"],

        "main_before": main_sha,

        "main_after": main_after,

        "main_unchanged": main_sha == main_after,

        "files": verified,

        "branch_url": (
            f"https://github.com/{REPO}/tree/{branch}/{prefix}"
        )

    }


# =============================================================================
# 9. 主程式
# =============================================================================

def run_v152():

    mount_drive()

    logs = []

    now = datetime.now(TZ)

    run_id = (
        now.strftime("%Y%m%d_%H%M%S")
        + "_"
        + uuid.uuid4().hex[:4]
    )

    # 取得各市場前次最後留有行情日期。
    latest, prev_dirs = load_previous_state(
        ROOT,
        logs
    )

    # 沿用 v1.5.1 的欄位結構，只檢查一次。
    standard_cols, overlay_cols = find_schema_templates(
        ROOT,
        prev_dirs
    )

    # 各市場分別從下一個日曆日起向前查詢。
    start_by_market = {
        m: latest[m] + timedelta(days=1)
        for m in latest
    }

    today = now.date()

    start_for_name = min(
        start_by_market.values()
    )

    out_name = (
        "v1.5.2_官方雙市場行情增量續接_"
        f"{start_for_name.strftime('%Y%m%d')}_"
        f"{today.strftime('%Y%m%d')}_"
        f"{run_id}"
    )

    out_dir = ROOT / out_name

    out_dir.mkdir(
        parents=True,
        exist_ok=False
    )

    log(f"版本：{VERSION}", logs)

    log(f"資料根目錄：{ROOT}", logs)

    log(f"本次輸出：{out_dir}", logs)

    log(
        f"TWSE 最後行情日：{latest['TWSE']}；"
        f"本次起始：{start_by_market['TWSE']}",
        logs
    )

    log(
        f"TPEx 最後行情日：{latest['TPEX']}；"
        f"本次起始：{start_by_market['TPEX']}",
        logs
    )

    log(
        f"本次查詢截止：{today}",
        logs
    )

    log(
        "只讀取既有 v1.5.x 增量日期與代表欄位格式；"
        "不掃描、不回查舊歷史行情。",
        logs
    )

    if all(
        d > today
        for d in start_by_market.values()
    ):

        log(
            "[NO UPDATE] 兩市場都已銜接至今日或更新日期。",
            logs
        )

        return

    session = make_session()

    all_records = []

    api_meta = []

    daily_summary = []

    cursor = min(
        start_by_market.values()
    )

    # 逐日向前查詢，不從舊日期倒查。
    # 週末也會查詢，以免漏掉補班交易日。
    while cursor <= today:

        for market in ("TWSE", "TPEX"):

            if cursor < start_by_market[market]:
                continue

            records, meta = fetch_one(
                session,
                market,
                cursor,
                out_dir,
                logs
            )

            api_meta.append(meta)

            if meta["status"] == "OK":

                all_records.extend(records)

                vols = [
                    r["volume"]
                    for r in records
                ]

                daily_summary.append({

                    "trade_date": cursor.isoformat(),

                    "market": market,

                    "rows": len(records),

                    "unique_codes": len(
                        set(
                            r["instrument_code"]
                            for r in records
                        )
                    ),

                    "zero_volume_rows": sum(
                        v == 0
                        for v in vols
                    ),

                    "missing_open": sum(
                        r["open"] is None
                        for r in records
                    ),

                    "missing_high": sum(
                        r["high"] is None
                        for r in records
                    ),

                    "missing_low": sum(
                        r["low"] is None
                        for r in records
                    ),

                    "missing_close": sum(
                        r["close"] is None
                        for r in records
                    ),

                    "status": "APPENDED"

                })

                log(
                    f"[PASS] {market} {cursor}："
                    f"{len(records):,} 筆",
                    logs
                )

            else:

                daily_summary.append({

                    "trade_date": cursor.isoformat(),

                    "market": market,

                    "rows": 0,

                    "unique_codes": 0,

                    "zero_volume_rows": 0,

                    "missing_open": 0,

                    "missing_high": 0,

                    "missing_low": 0,

                    "missing_close": 0,

                    "status": meta["status"]

                })

                log(
                    f"[{meta['status']}] {market} {cursor}："
                    f"{meta['error']}",
                    logs
                )

            time.sleep(0.25)

        cursor += timedelta(days=1)

    # -------------------------------------------------------------------------
    # 一次性格式接合檢查
    # -------------------------------------------------------------------------

    if all_records:

        probe = map_record_to_schema(
            all_records[0],
            standard_cols
        )

        for key in (
            "trade_date",
            "market",
            "instrument_code",
            "close",
            "volume"
        ):

            col = find_col(
                standard_cols,
                FIELD_ALIASES[key]
            )

            if col is None:

                raise RuntimeError(
                    f"標準化格式接合檢查失敗：缺少欄位 {key}"
                )

        for key in (
            "trade_date",
            "market",
            "instrument_code"
        ):

            col = find_col(
                standard_cols,
                FIELD_ALIASES[key]
            )

            if probe.get(col, "") in (None, ""):

                raise RuntimeError(
                    f"標準化格式接合檢查失敗："
                    f"欄位 {key} 未正確寫入"
                )

        for market in ("TWSE", "TPEX"):

            mr = next(
                (
                    r for r in all_records
                    if r["market"] == market
                ),
                None
            )

            if mr:

                for key in (
                    "trade_date",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume"
                ):

                    c = find_col(
                        overlay_cols[market],
                        FIELD_ALIASES[key]
                    )

                    if c is None:

                        raise RuntimeError(
                            f"{market} 個股覆蓋格式接合檢查失敗："
                            f"缺少 {key}"
                        )

        log(
            "[FORMAT PASS] 全市場標準化檔與 "
            "TWSE/TPEx 個股覆蓋檔格式一次檢查完成。",
            logs
        )

    # -------------------------------------------------------------------------
    # 全市場標準化增量 CSV
    # -------------------------------------------------------------------------

    new_dates = sorted({
        r["trade_date"]
        for r in all_records
    })

    if new_dates:

        std_path = (
            out_dir
            / "normalized"
            / (
                "全市場標準化增量_"
                f"{new_dates[0].replace('-', '')}_"
                f"{new_dates[-1].replace('-', '')}.csv"
            )
        )

        write_rows_csv(
            std_path,
            all_records,
            standard_cols
        )

    else:

        std_path = (
            out_dir
            / "normalized"
            / f"全市場標準化增量_無新增_{run_id}.csv"
        )

        write_rows_csv(
            std_path,
            [],
            standard_cols
        )

    # -------------------------------------------------------------------------
    # 個股／市場增量覆蓋檔
    #
    # 只寫本次新增日期，不改寫既有 v1.5.1 覆蓋檔。
    # -------------------------------------------------------------------------

    groups = defaultdict(list)

    for r in all_records:

        groups[
            (
                r["market"],
                r["instrument_code"]
            )
        ].append(r)

    overlay_index = []

    overlay_root = (
        out_dir
        / "overlay"
        / "官方歷史行情"
    )

    for (market, code), rows in sorted(
        groups.items()
    ):

        suffix = (
            "TWSE"
            if market == "TWSE"
            else "TPEx"
        )

        filename = f"{code}_{suffix}.csv"

        write_rows_csv(
            overlay_root / filename,
            rows,
            overlay_cols[market]
        )

        ds = sorted(
            r["trade_date"]
            for r in rows
        )

        overlay_index.append({

            "instrument_code": code,

            "market": market,

            "overlay_file": (
                f"官方歷史行情/{filename}"
            ),

            "new_rows": len(rows),

            "first_new_date": ds[0],

            "last_new_date": ds[-1]

        })

    # -------------------------------------------------------------------------
    # 更新各市場最後日期狀態
    # -------------------------------------------------------------------------

    updated_latest = {
        m: latest[m]
        for m in latest
    }

    for r in all_records:

        d = parse_date_value(
            r["trade_date"]
        )

        m = r["market"]

        if d and (
            updated_latest[m] is None
            or d > updated_latest[m]
        ):

            updated_latest[m] = d

    state = {

        "version": VERSION,

        "updated_at_taipei": now.isoformat(),

        "market_last_date": {

            m: d.isoformat() if d else None

            for m, d in updated_latest.items()

        },

        "policy": (
            "append-only; no backward checks; "
            "no old rows modified"
        )

    }

    write_json(
        out_dir / "latest_market_dates.json",
        state
    )

    # -------------------------------------------------------------------------
    # 統計及索引
    # -------------------------------------------------------------------------

    daily_df = pd.DataFrame(
        daily_summary
    )

    api_df = pd.DataFrame(
        api_meta
    )

    idx_df = pd.DataFrame(
        overlay_index
    )

    daily_df.to_csv(
        out_dir / "逐日雙市場新增行情統計.csv",
        index=False,
        encoding="utf-8-sig"
    )

    api_df.to_csv(
        out_dir / "官方API回應索引.csv",
        index=False,
        encoding="utf-8-sig"
    )

    idx_df.to_csv(
        out_dir / "個股增量覆蓋檔索引.csv",
        index=False,
        encoding="utf-8-sig"
    )

    # -------------------------------------------------------------------------
    # 報告
    # -------------------------------------------------------------------------

    status = (

        "SUCCESS"

        if all_records
        and all(
            x["status"] in {"APPENDED", "NO_DATA"}
            for x in daily_summary
        )

        else (

            "PARTIAL"
            if all_records

            else "NO_NEW_ROWS"

        )

    )

    report = {

        "version": VERSION,

        "run_id": run_id,

        "run_time_taipei": now.isoformat(),

        "status": status,

        "previous_market_last_date": {
            m: d.isoformat()
            for m, d in latest.items()
        },

        "market_start_date": {
            m: d.isoformat()
            for m, d in start_by_market.items()
        },

        "requested_through": today.isoformat(),

        "new_rows": len(all_records),

        "new_instruments": len(groups),

        "new_trade_dates": new_dates,

        "market_last_date_after_update": (
            state["market_last_date"]
        ),

        "api_requests": len(api_meta),

        "api_status_counts": (
            api_df["status"].value_counts().to_dict()
            if not api_df.empty
            else {}
        ),

        "format": {

            "standard_columns": standard_cols,

            "twse_overlay_columns": overlay_cols["TWSE"],

            "tpex_overlay_columns": overlay_cols["TPEX"],

            "format_check": (
                "PASS"
                if all_records
                else "NO_NEW_DATA_TO_VALIDATE"
            )

        },

        "preservation": {

            "original_v2_8_zip_modified": False,

            "previous_v1_5_x_outputs_modified": False,

            "zero_volume_rows_preserved": True,

            "missing_ohlc_rows_preserved": True,

            "duplicate_rows_removed": False,

            "instrument_category_filter": False,

            "all_official_instrument_types_retained": True

        },

        "scope": (
            "從每個市場最後留有行情的日期之次日起，"
            "只向前逐日查詢到執行日；"
            "未回查、未修訂任何舊日期。"
        ),

        "api_endpoints": {

            "TWSE": (
                "https://www.twse.com.tw/"
                "rwd/zh/afterTrading/MI_INDEX"
            ),

            "TPEX": (
                "https://www.tpex.org.tw/"
                "www/zh-tw/afterTrading/otc?type=AL"
            )

        }

    }

    write_json(
        out_dir / "v1.5.2_官方增量行情續接報告.json",
        report
    )

    readme = f"""# {VERSION}

- 執行日期：{now.isoformat()}
- TWSE 前次最後行情日：{latest['TWSE']}；本次自 {start_by_market['TWSE']} 起
- TPEx 前次最後行情日：{latest['TPEX']}；本次自 {start_by_market['TPEX']} 起
- 查詢截止：{today}
- 新增行情列數：{len(all_records):,}
- 新增個股／市場檔：{len(groups):,}

## 更新方式

只讀取既有 v1.5.x 增量行情的日期與代表性欄位格式；
不掃描十年歷史底稿、不向前檢查舊行情。

各市場從最後一個有行情日期的下一個日曆日起，
逐日呼叫官方日期端點。
週末也會查詢，以免漏掉補班交易日。

本次資料只寫入新的 append-only overlay：
`overlay/官方歷史行情/`。

格式沿用既有 v1.5.1 覆蓋檔欄位；
全市場標準化增量沿用既有標準化 CSV 欄位。

原始官方回應另以 gzip 保存。

零成交量、OHLC 空值及官方重複列均保留，
不刪除、不補值、不篩除 ETF、ETN、TDR、權證等官方商品。

既有 v2.8 ZIP、v1.4.2 ZIP、v1.5.1 ZIP 及舊增量均不修改。

## 官方來源

- TWSE：`https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX`
- TPEx：`https://www.tpex.org.tw/www/zh-tw/afterTrading/otc`，`type=AL`

## 使用方式

分析資料以 v2.8 歷史底稿 + 既有 v1.5.1 overlay
+ 本次 v1.5.2 overlay，依日期、代號及市場銜接。
"""

    (
        out_dir / "README.md"
    ).write_text(
        readme,
        encoding="utf-8"
    )

    save_source_from_history(
        out_dir,
        logs
    )

    (out_dir / "run.log").write_text(
        "\n".join(logs) + "\n",
        encoding="utf-8"
    )

    # -------------------------------------------------------------------------
    # ZIP
    # -------------------------------------------------------------------------

    zip_path = (
        out_dir
        / f"{out_name}.zip"
    )

    make_zip(
        out_dir,
        zip_path
    )

    log(
        f"[ZIP] {zip_path} "
        f"({zip_path.stat().st_size:,} bytes)",
        logs
    )

    (out_dir / "run.log").write_text(
        "\n".join(logs) + "\n",
        encoding="utf-8"
    )

    # -------------------------------------------------------------------------
    # GitHub 獨立 audit branch
    # -------------------------------------------------------------------------

    upload_result = None

    upload_files = [

        zip_path,

        out_dir / "v1.5.2_官方增量行情續接報告.json",

        out_dir / "README.md",

        std_path,

        out_dir / "逐日雙市場新增行情統計.csv",

        out_dir / "個股增量覆蓋檔索引.csv",

        out_dir / "官方API回應索引.csv"

    ]

    source_path = (
        out_dir
        / "SSO_Market_Data_Continuation_v1.5.2.py"
    )

    if source_path.exists():

        upload_files.append(
            source_path
        )

    try:

        upload_result = github_upload(
            out_dir,
            zip_path,
            upload_files,
            run_id,
            logs
        )

        write_json(
            out_dir / "GitHub上傳結果.json",
            upload_result
        )

        log(
            f"[GITHUB PASS] {upload_result['branch']} "
            f"/ commit={upload_result['commit_sha']}",
            logs
        )

        log(
            f"[GITHUB MAIN] "
            f"unchanged={upload_result['main_unchanged']}",
            logs
        )

        log(
            f"[GITHUB URL] {upload_result['branch_url']}",
            logs
        )

    except Exception as e:

        upload_result = {

            "status": "FAILED",

            "error": (
                f"{type(e).__name__}: {e}"
            )

        }

        write_json(
            out_dir / "GitHub上傳結果.json",
            upload_result
        )

        log(
            f"[GITHUB WARN] {upload_result['error']}",
            logs
        )

    (out_dir / "run.log").write_text(
        "\n".join(logs) + "\n",
        encoding="utf-8"
    )

    # -------------------------------------------------------------------------
    # 結果
    # -------------------------------------------------------------------------

    print("\n" + "=" * 88)

    print(
        f"{VERSION} 完成：{status}"
    )

    print(
        f"新增行情：{len(all_records):,} 列；"
        f"新增個股／市場檔：{len(groups):,}"
    )

    print(
        f"TWSE 最後行情日："
        f"{state['market_last_date']['TWSE']}"
    )

    print(
        f"TPEx 最後行情日："
        f"{state['market_last_date']['TPEX']}"
    )

    print(
        f"輸出資料夾：{out_dir}"
    )

    print(
        f"ZIP：{zip_path}"
    )

    if (
        upload_result
        and upload_result.get("branch")
    ):

        print(
            f"GitHub audit branch："
            f"{upload_result['branch']}"
        )

        print(
            f"GitHub URL："
            f"{upload_result['branch_url']}"
        )

        print(
            f"GitHub main 未變更："
            f"{upload_result['main_unchanged']}"
        )

    else:

        print(
            f"GitHub：{upload_result}"
        )

    print("=" * 88)

    return report


# =============================================================================
# 10. 執行
# =============================================================================

RESULT_V152 = run_v152()