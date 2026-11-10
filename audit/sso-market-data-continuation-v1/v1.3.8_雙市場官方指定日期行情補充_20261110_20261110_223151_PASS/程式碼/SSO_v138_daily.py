# -*- coding: utf-8 -*-
"""
SSO_Market_Data_Continuation_TWSE_TPEX v1.3.8
2026-11-10 上市＋上櫃官方指定日期行情補充（v1.3.7 修正版）

原則：
1. 只使用 TWSE、TPEx 官方指定日期歷史行情端點。
2. TWSE、TPEx 分開擷取、分開解析、分開核對，不因單一市場失敗而跳過另一市場。
3. 兩市場都通過指定日期及基本資料品質檢查，才輸出正式標準化 CSV。
4. 任一市場失敗時，只建立「待補核驗」診斷封存，保存已取得的原始回應，不冒充目標日期行情。
5. 只新增獨立資料夾，不覆蓋既有資料；不修改正式資料庫、不重訓、不回測、不選股。
6. GitHub 使用獨立 audit branch；main SHA 必須符合基準，並於上傳前後核對。
7. Colab notebook 傳入的 -f kernel.json 等參數不影響程式執行（本程式不使用 argparse）。
"""

import os
import re
import json
import csv
import gzip
import time
import hashlib
import zipfile
import traceback
import base64
from pathlib import Path
from datetime import datetime, date
from zoneinfo import ZoneInfo
from urllib.parse import quote

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# ============================================================
# 01. 設定
# ============================================================

VERSION = "v1.3.8"
TARGET_DATE = date(2026, 11, 10)
TZ = ZoneInfo("Asia/Taipei")
RUN_TIME = datetime.now(TZ)
STAMP = RUN_TIME.strftime("%Y%m%d_%H%M%S")

PROJECT_ROOT = Path("/content/drive/MyDrive/看股最速適")
CONTINUATION_ROOT = PROJECT_ROOT / "SSO_Market_Data_Continuation_TWSE_TPEX_v1"

OUTPUT_BASE_NAME = (
    f"{VERSION}_雙市場官方指定日期行情補充_"
    f"{TARGET_DATE:%Y%m%d}_{STAMP}"
)

REPO = "CHLINX/sso"
EXPECTED_MAIN_SHA = "aaabc2433fb5df2bb68af06461178305d6bd30d1"

# v1.3.7 使用的 STOCK_DAY_ALL / tpex_mainboard_daily_close_quotes
# 是最新行情快照，沒有指定日期查詢能力。本版改用官方歷史查詢端點。
TWSE_URL = "https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX"
TPEX_URL = (
    "https://www.tpex.org.tw/web/stock/aftertrading/"
    "otc_quotes_no1430/stk_wn1430_result.php"
)

MIN_ROWS = {"TWSE": 500, "TPEX": 500}
REQUEST_TIMEOUT = (20, 120)

CODE_KEYS = [
    "證券代號", "證券代碼", "股票代號", "代號",
    "Code", "SecuritiesCompanyCode"
]
NAME_KEYS = [
    "證券名稱", "股票名稱", "名稱",
    "Name", "CompanyName", "SecuritiesCompanyName"
]
VOLUME_KEYS = [
    "成交股數", "成交量", "TradeVolume", "TradingShares"
]
OHLC_KEYS = {
    "open": ["開盤價", "開盤", "OpeningPrice", "Open"],
    "high": ["最高價", "最高", "HighestPrice", "High"],
    "low": ["最低價", "最低", "LowestPrice", "Low"],
    "close": ["收盤價", "收盤", "ClosingPrice", "Close"],
}


# ============================================================
# 02. 基礎工具
# ============================================================

def log(message):
    print(
        f"[{datetime.now(TZ).strftime('%H:%M:%S')}] {message}",
        flush=True
    )


def sha256_bytes(data):
    return hashlib.sha256(data).hexdigest()


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def json_text(obj):
    return json.dumps(
        obj,
        ensure_ascii=False,
        indent=2,
        default=str
    )


def save_json(path, obj):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json_text(obj), encoding="utf-8")


def save_gzip(path, raw_bytes):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    compressed = gzip.compress(
        raw_bytes,
        compresslevel=6,
        mtime=0
    )

    path.write_bytes(compressed)

    return {
        "path": str(path),
        "bytes_original": len(raw_bytes),
        "bytes_gzip": len(compressed),
        "sha256_original": sha256_bytes(raw_bytes),
        "sha256_gzip": sha256_bytes(compressed),
    }


def mount_drive():
    if not Path("/content/drive/MyDrive").exists():
        from google.colab import drive
        drive.mount("/content/drive")

    if not PROJECT_ROOT.exists():
        raise FileNotFoundError(
            f"找不到專案根目錄：{PROJECT_ROOT}"
        )

    if not CONTINUATION_ROOT.exists():
        raise FileNotFoundError(
            "找不到獨立行情續補資料夾："
            f"{CONTINUATION_ROOT}"
        )


def get_github_token():
    token = (
        os.environ.get("GITHUB_TOKEN")
        or os.environ.get("GH_TOKEN")
    )

    if token:
        return token

    try:
        from google.colab import userdata
        return userdata.get("GITHUB_TOKEN")
    except Exception:
        return None


def make_session():
    session = requests.Session()

    retry = Retry(
        total=4,
        connect=4,
        read=4,
        status=4,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )

    adapter = HTTPAdapter(
        max_retries=retry,
        pool_connections=4,
        pool_maxsize=4,
    )

    session.mount("https://", adapter)
    session.mount("http://", adapter)

    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 "
            "(compatible; SSO-Market-Data-Continuation/1.3.8)"
        ),
        "Accept": "application/json,text/plain,*/*",
        "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    })

    return session


def twse_params():
    return {
        "date": TARGET_DATE.strftime("%Y%m%d"),
        "type": "ALLBUT0999",
        "response": "json",
    }


def tpex_params():
    roc_year = TARGET_DATE.year - 1911
    roc_date = f"{roc_year}/{TARGET_DATE:%m/%d}"

    return {
        "l": "zh-tw",
        "o": "json",
        "d": roc_date,
        "se": "EW",
        "s": "0,asc,0",
    }


# ============================================================
# 03. 官方來源擷取（兩市場互不連鎖）
# ============================================================

def fetch_source(session, market, url, params):
    log(f"[GET] {market} 指定日期官方歷史行情")

    item = {
        "market": market,
        "base_url": url,
        "params": params,
        "requested_trade_date": TARGET_DATE.isoformat(),
        "retrieved_at": datetime.now(TZ).isoformat(),
        "http_status": None,
        "response_url": None,
        "response_headers": {},
        "raw_bytes": b"",
        "payload": None,
        "error": None,
    }

    try:
        headers = {
            "Referer": (
                "https://www.tpex.org.tw/"
                if market == "TPEX"
                else "https://www.twse.com.tw/"
            )
        }

        response = session.get(
            url,
            params=params,
            headers=headers,
            timeout=REQUEST_TIMEOUT
        )

        item["http_status"] = response.status_code
        item["response_url"] = response.url
        item["raw_bytes"] = response.content or b""

        item["response_headers"] = {
            k: v
            for k, v in response.headers.items()
            if k.lower() in (
                "content-type",
                "content-length",
                "date",
                "last-modified",
                "etag",
                "cache-control",
                "age",
                "expires",
            )
        }

        if not response.content:
            item["error"] = "官方 API 回傳空內容"
            return item

        leading = response.content.lstrip()[:100].lower()

        if (
            leading.startswith(b"<!doctype html")
            or leading.startswith(b"<html")
        ):
            item["error"] = (
                "HTTP 回應內容為 HTML，不是 JSON，"
                "可能遭遇網站安全限制"
            )
            return item

        try:
            item["payload"] = response.json()
        except Exception as exc:
            item["error"] = (
                f"JSON 解析失敗：{type(exc).__name__}: {exc}"
            )
            return item

        if response.status_code >= 400:
            item["error"] = (
                f"HTTP {response.status_code}: "
                f"{response.text[:800]}"
            )
            return item

        log(
            f"[HTTP] {market} status={response.status_code} "
            f"bytes={len(response.content):,} "
            f"URL={response.url}"
        )

    except Exception as exc:
        item["error"] = (
            f"{type(exc).__name__}: {exc}"
        )

    return item


# ============================================================
# 04. JSON 表格解析
# ============================================================

def norm_key(value):
    return re.sub(
        r"[\s_\-（）()：:]+",
        "",
        str(value or "")
    ).lower()


def find_field(fields, aliases):
    lookup = {
        norm_key(v): v
        for v in fields
    }

    for alias in aliases:
        key = norm_key(alias)

        if key in lookup:
            return lookup[key]

    return None


def extract_table(payload, market):
    if not isinstance(payload, dict):
        raise ValueError(
            f"{market} JSON 根節點不是物件"
        )

    stat = payload.get("stat")

    if (
        stat is not None
        and str(stat).strip().upper() not in ("OK", "SUCCESS")
    ):
        raise ValueError(
            f"{market} 官方回應 stat={stat!r}，不是成功狀態"
        )

    tables = payload.get("tables")

    if not isinstance(tables, list):
        if (
            isinstance(payload.get("fields"), list)
            and isinstance(payload.get("data"), list)
        ):
            tables = [payload]

        elif isinstance(payload.get("aaData"), list):
            raise ValueError(
                f"{market} 回傳 aaData，但沒有欄位表頭，"
                "無法安全對應欄位"
            )

        else:
            raise ValueError(
                f"{market} JSON 找不到 tables 或 fields/data"
            )

    candidates = []

    for table in tables:
        if not isinstance(table, dict):
            continue

        fields = table.get("fields")
        data = table.get("data")

        if (
            not isinstance(fields, list)
            or not isinstance(data, list)
            or not fields
            or not data
        ):
            continue

        code_field = find_field(fields, CODE_KEYS)
        name_field = find_field(fields, NAME_KEYS)
        volume_field = find_field(fields, VOLUME_KEYS)

        close_field = find_field(
            fields,
            OHLC_KEYS["close"]
        )

        open_field = find_field(
            fields,
            OHLC_KEYS["open"]
        )

        high_field = find_field(
            fields,
            OHLC_KEYS["high"]
        )

        low_field = find_field(
            fields,
            OHLC_KEYS["low"]
        )

        # 個股日行情表必須至少具備代號、收盤價及成交股數。
        if code_field and close_field and volume_field:
            candidates.append((
                table,
                fields,
                data,
                {
                    "code": code_field,
                    "name": name_field,
                    "volume": volume_field,
                    "open": open_field,
                    "high": high_field,
                    "low": low_field,
                    "close": close_field,
                }
            ))

    if not candidates:
        titles = [
            str(t.get("title", ""))
            for t in tables
            if isinstance(t, dict)
        ]

        raise ValueError(
            f"{market} 找不到同時具備代號、收盤價、成交股數的個股行情表；"
            f"表格標題={titles[:10]}"
        )

    # 若回應含多個相似表格，取資料列最多者；原始 JSON 仍完整封存。
    table, fields, data, mapping = max(
        candidates,
        key=lambda x: len(x[2])
    )

    rows = []
    malformed = 0

    for values in data:
        if isinstance(values, dict):
            row = {
                str(k): v
                for k, v in values.items()
            }

        elif isinstance(values, (list, tuple)):
            if len(values) != len(fields):
                malformed += 1

            row = {
                str(fields[i]): (
                    values[i]
                    if i < len(values)
                    else ""
                )
                for i in range(len(fields))
            }

        else:
            malformed += 1
            continue

        rows.append(row)

    if not rows:
        raise ValueError(
            f"{market} 指定日期行情表沒有資料列"
        )

    return {
        "rows": rows,
        "fields": [str(x) for x in fields],
        "table_title": str(table.get("title", "")),
        "mapping": mapping,
        "malformed_rows": malformed,
    }


def parse_date_value(value):
    if value is None:
        return None

    text = str(value).strip()

    if not text:
        return None

    # 民國年文字格式，例如 115年11月10日。
    match = re.search(
        r"(\d{3,4})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日",
        text
    )

    if match:
        year, month, day_value = map(
            int,
            match.groups()
        )

        if year < 1912:
            year += 1911

        try:
            return date(
                year,
                month,
                day_value
            ).isoformat()
        except ValueError:
            return None

    digits = re.sub(r"\D", "", text)

    try:
        if len(digits) == 7:
            return date(
                int(digits[:3]) + 1911,
                int(digits[3:5]),
                int(digits[5:7]),
            ).isoformat()

        if len(digits) == 8:
            year = int(digits[:4])

            if year >= 1912:
                return date(
                    year,
                    int(digits[4:6]),
                    int(digits[6:8])
                ).isoformat()

    except ValueError:
        return None

    return None


def collect_payload_dates(payload):
    found = set()

    if not isinstance(payload, dict):
        return []

    # 只取回應 metadata，不掃描個股資料列，
    # 避免把其他欄位誤當資料日期。
    date_keys = {
        "date",
        "reportdate",
        "report_date",
        "datadate",
        "data_date",
        "tradedate",
        "trade_date",
        "資料日期",
        "日期",
    }

    normalized_date_keys = {
        norm_key(x)
        for x in date_keys
    }

    for key, value in payload.items():
        key_norm = norm_key(key)

        if key_norm in normalized_date_keys:
            parsed = parse_date_value(value)

            if parsed:
                found.add(parsed)

        if (
            key_norm in ("title", "標題")
            and isinstance(value, str)
        ):
            parsed = parse_date_value(value)

            if parsed:
                found.add(parsed)

    tables = payload.get("tables", [])

    if isinstance(tables, list):
        for table in tables:
            if not isinstance(table, dict):
                continue

            for key in (
                "title",
                "date",
                "reportDate",
                "資料日期",
                "日期",
            ):
                parsed = parse_date_value(
                    table.get(key)
                )

                if parsed:
                    found.add(parsed)

    return sorted(found)


def validate_market_result(item):
    market = item["market"]

    result = {
        "market": market,
        "requested_date": TARGET_DATE.isoformat(),
        "passed": False,
        "error": item.get("error"),
        "http_status": item.get("http_status"),
        "response_url": item.get("response_url"),
        "response_dates": [],
        "date_confirmation_basis": None,
        "row_count": 0,
        "table_title": None,
        "malformed_rows": None,
    }

    if item.get("error"):
        return result, None

    payload = item.get("payload")

    try:
        parsed = extract_table(
            payload,
            market
        )

        rows = parsed["rows"]

        result["row_count"] = len(rows)
        result["table_title"] = parsed["table_title"]
        result["malformed_rows"] = parsed["malformed_rows"]

        result["response_dates"] = collect_payload_dates(
            payload
        )

        if len(rows) < MIN_ROWS[market]:
            raise ValueError(
                f"{market} 只有 {len(rows)} 筆，"
                f"低於最低門檻 {MIN_ROWS[market]}"
            )

        if parsed["malformed_rows"]:
            raise ValueError(
                f"{market} 有 {parsed['malformed_rows']} 列"
                "欄位數與表頭不一致"
            )

        target = TARGET_DATE.isoformat()
        dates = result["response_dates"]

        if dates and (
            len(dates) != 1
            or dates[0] != target
        ):
            raise ValueError(
                f"{market} 回應 metadata 日期={dates}，"
                f"與指定日期 {target} 不一致"
            )

        if dates:
            result["date_confirmation_basis"] = (
                "官方回應 metadata 日期與指定日期相符"
            )
        else:
            result["date_confirmation_basis"] = (
                "官方歷史端點明確日期查詢參數；"
                "回應未另附日期 metadata"
            )

        result["passed"] = True

        return result, parsed

    except Exception as exc:
        result["error"] = (
            f"{type(exc).__name__}: {exc}"
        )

        return result, None


# ============================================================
# 05. 個股代號、數值與品質檢核
# ============================================================

def find_value_by_keys(row, candidate_names):
    lookup = {
        norm_key(k): v
        for k, v in row.items()
    }

    for name in candidate_names:
        key = norm_key(name)

        if key in lookup:
            return lookup[key]

    return None


def to_number(value):
    if value is None:
        return None

    text = (
        str(value)
        .strip()
        .replace(",", "")
        .replace("--", "")
    )

    if text in ("", "-", "----", "N/A", "null", "None"):
        return None

    text = (
        text
        .replace("元", "")
        .replace("股", "")
    )

    try:
        return float(text)
    except Exception:
        return None


def row_digest(row):
    raw = json.dumps(
        row,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str
    )

    return sha256_bytes(
        raw.encode("utf-8")
    )


def check_market_quality(market, rows):
    codes = []
    missing_code = 0
    zero_volume = 0
    missing_volume = 0

    ohlc_missing = {
        key: 0
        for key in OHLC_KEYS
    }

    for row in rows:
        code = str(
            find_value_by_keys(row, CODE_KEYS) or ""
        ).strip()

        if not code:
            missing_code += 1
        else:
            codes.append(code)

        volume_value = find_value_by_keys(
            row,
            VOLUME_KEYS
        )

        volume = to_number(volume_value)

        if (
            volume_value is None
            or str(volume_value).strip() == ""
        ):
            missing_volume += 1
        elif volume == 0:
            zero_volume += 1

        for field, aliases in OHLC_KEYS.items():
            value = find_value_by_keys(
                row,
                aliases
            )

            if (
                value is None
                or str(value).strip() in ("", "-", "----")
            ):
                ohlc_missing[field] += 1

    seen = set()
    duplicates = []

    for code in codes:
        if code in seen:
            duplicates.append(code)

        seen.add(code)

    return {
        "market": market,
        "row_count": len(rows),
        "unique_code_count": len(set(codes)),
        "missing_code_rows": missing_code,
        "duplicate_code_count": len(duplicates),
        "duplicate_codes": sorted(set(duplicates)),
        "zero_volume_rows": zero_volume,
        "missing_volume_rows": missing_volume,
        "ohlc_missing_rows": ohlc_missing,
        "note": (
            "零成交量、OHLC 缺漏及重複代號均列入檢核，"
            "不自行改寫官方資料。"
        ),
    }


def to_market_records(market, rows):
    output = []

    for row in rows:
        code = str(
            find_value_by_keys(row, CODE_KEYS) or ""
        ).strip()

        name = str(
            find_value_by_keys(row, NAME_KEYS) or ""
        ).strip()

        record = {
            "market": market,
            "trade_date": TARGET_DATE.isoformat(),
            "instrument_code": code,
            "instrument_name": name,
            "source_row_sha256": row_digest(row),
        }

        for key, value in row.items():
            record[f"raw_{key}"] = (
                ""
                if value is None
                else str(value)
            )

        output.append(record)

    return output


def write_csv(path, records):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    if not records:
        path.write_text(
            "NO_DATA\n",
            encoding="utf-8-sig"
        )
        return

    fields = []

    for row in records:
        for key in row:
            if key not in fields:
                fields.append(key)

    with open(
        path,
        "w",
        encoding="utf-8-sig",
        newline=""
    ) as f:
        writer = csv.DictWriter(
            f,
            fieldnames=fields,
            extrasaction="ignore"
        )

        writer.writeheader()
        writer.writerows(records)


# ============================================================
# 06. 原始證據封存、README、ZIP
# ============================================================

def save_raw_evidence(output_dir, source_items):
    raw_dir = output_dir / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    manifest = []

    for market, item in source_items.items():
        raw = item.get("raw_bytes") or b""

        entry = {
            "market": market,
            "base_url": item.get("base_url"),
            "params": item.get("params"),
            "requested_trade_date": item.get("requested_trade_date"),
            "response_url": item.get("response_url"),
            "http_status": item.get("http_status"),
            "retrieved_at": item.get("retrieved_at"),
            "response_headers": item.get("response_headers", {}),
            "raw_bytes": len(raw),
            "raw_sha256": (
                sha256_bytes(raw)
                if raw
                else None
            ),
            "error": item.get("error"),
        }

        if raw:
            filename = (
                f"{market}_指定日期原始回應_"
                f"{TARGET_DATE:%Y%m%d}.json.gz"
            )

            info = save_gzip(
                raw_dir / filename,
                raw
            )

            entry["raw_archive"] = info

        save_json(
            raw_dir / f"{market}_請求與回應metadata.json",
            entry
        )

        manifest.append(entry)

    save_json(
        raw_dir / "原始回應清單.json",
        manifest
    )

    return manifest


def save_notebook_source(output_dir):
    code_dir = output_dir / "程式碼"
    code_dir.mkdir(parents=True, exist_ok=True)

    script_path = code_dir / "SSO_v138_daily.py"

    try:
        ip = get_ipython()
        history = ip.history_manager.input_hist_raw

        if history:
            source = history[-1]

            if (
                isinstance(source, str)
                and len(source) > 1000
            ):
                script_path.write_text(
                    source,
                    encoding="utf-8"
                )

                log(
                    f"[PASS] notebook 程式碼已保存：{script_path}"
                )

                return str(script_path)

    except Exception as exc:
        log(
            f"[WARN] 無法擷取 notebook 原始碼：{exc}"
        )

    script_path.write_text(
        "# 無法從 notebook 自動擷取完整原始碼；"
        "請以本對話中的 v1.3.8 完整程式碼為準。\n",
        encoding="utf-8",
    )

    return str(script_path)


def create_zip(output_dir, zip_path):
    with zipfile.ZipFile(
        zip_path,
        "w",
        compression=zipfile.ZIP_DEFLATED,
        compresslevel=6
    ) as zf:

        for path in sorted(output_dir.rglob("*")):
            if (
                path.is_file()
                and path.name != "GitHub與main核對結果.json"
            ):
                zf.write(
                    path,
                    path.relative_to(CONTINUATION_ROOT)
                )

    with zipfile.ZipFile(zip_path, "r") as zf:
        bad_member = zf.testzip()

        if bad_member:
            raise RuntimeError(
                f"ZIP CRC 錯誤：{bad_member}"
            )

    return {
        "path": str(zip_path),
        "bytes": zip_path.stat().st_size,
        "sha256": sha256_file(zip_path),
    }


def write_readme(
    output_dir,
    status,
    market_checks,
    quality,
    raw_manifest
):
    lines = [
        f"# {VERSION}｜雙市場官方指定日期行情補充",
        "",
        f"目標資料日期：{TARGET_DATE.isoformat()}",
        f"執行時間：{RUN_TIME.isoformat()}",
        f"狀態：{status}",
        "",
        "## 官方指定日期來源",
        f"TWSE：{TWSE_URL}",
        f"查詢參數：{json_text(twse_params())}",
        f"TPEx：{TPEX_URL}",
        f"查詢參數：{json_text(tpex_params())}",
        "",
        "## 日期核驗結果",
    ]

    for market in ("TWSE", "TPEX"):
        check = market_checks.get(market, {})

        lines.append(
            f"- {market}：passed={check.get('passed')}；"
            f"rows={check.get('row_count')}；"
            f"response_dates={check.get('response_dates')}；"
            f"error={check.get('error')}"
        )

    lines += [
        "",
        "## 原始資料保存原則",
        "- 原始 HTTP 回應以 gzip 保存，另保存 URL、參數、HTTP 狀態、回應標頭及 SHA256。",
        "- 不改寫官方原始欄位、不覆蓋既有資料、不更新正式資料庫。",
        "- 若狀態為 WAITING_DATE_VERIFICATION，本資料夾僅為診斷證據，不得併入正式行情資料。",
        "",
    ]

    if status == "PASS":
        lines += [
            "## 正式行情輸出",
            "- 兩市場均通過指定日期驗證及最低筆數檢核。",
            "- 已產生逐市場與合併標準化 CSV。",
            "- 品質檢查只標示異常，不自行修補成交量或 OHLC。",
            "",
        ]

    (output_dir / "README.md").write_text(
        "\n".join(lines),
        encoding="utf-8"
    )


# ============================================================
# 07. GitHub audit branch
# ============================================================

def gh_request(method, url, token, **kwargs):
    headers = kwargs.pop("headers", {})

    headers.update({
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "SSO-Market-Data-Continuation-v1.3.8",
    })

    response = requests.request(
        method,
        url,
        headers=headers,
        timeout=120,
        **kwargs
    )

    if response.status_code >= 400:
        raise RuntimeError(
            f"GitHub HTTP {response.status_code}: "
            f"{response.text[:1200]}"
        )

    return response


def get_main_sha(token):
    data = gh_request(
        "GET",
        f"https://api.github.com/repos/{REPO}/git/ref/heads/main",
        token
    ).json()

    return data["object"]["sha"]


def github_put_file(
    token,
    branch,
    remote_path,
    content,
    message
):
    url = (
        f"https://api.github.com/repos/{REPO}/contents/"
        f"{quote(remote_path, safe='/')}"
    )

    # Contents API 更新既有檔案時必須附上目前 blob SHA。
    check = requests.get(
        url,
        params={"ref": branch},
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "SSO-Market-Data-Continuation-v1.3.8",
        },
        timeout=120,
    )

    if check.status_code == 200:
        current_sha = check.json().get("sha")

    elif check.status_code == 404:
        current_sha = None

    else:
        raise RuntimeError(
            f"GitHub 檢查既有檔案失敗 HTTP {check.status_code}: "
            f"{check.text[:800]}"
        )

    body = {
        "message": message,
        "content": base64.b64encode(
            content
        ).decode("ascii"),
        "branch": branch,
    }

    if current_sha:
        body["sha"] = current_sha

    response = gh_request(
        "PUT",
        url,
        token,
        json=body
    )

    return response.json()


def github_readback(
    branch,
    remote_path,
    expected_sha256
):
    raw_url = (
        f"https://raw.githubusercontent.com/{REPO}/"
        f"{branch}/{quote(remote_path, safe='/')}"
    )

    response = requests.get(
        raw_url,
        timeout=120
    )

    response.raise_for_status()

    actual = sha256_bytes(response.content)

    if actual != expected_sha256:
        raise RuntimeError(
            f"GitHub readback SHA256 不符：{remote_path}"
        )

    return actual


def upload_github(
    token,
    main_sha,
    zip_path,
    output_dir
):
    branch = (
        "audit/sso-market-data-continuation-v1-"
        f"v138-daily-{TARGET_DATE:%Y%m%d}-{STAMP}"
    )

    gh_request(
        "POST",
        f"https://api.github.com/repos/{REPO}/git/refs",
        token,
        json={
            "ref": f"refs/heads/{branch}",
            "sha": main_sha,
        }
    )

    remote_root = (
        "audit/sso-market-data-continuation-v1/"
        f"{output_dir.name}"
    )

    files = [
        (
            zip_path.name,
            zip_path.read_bytes()
        )
    ]

    for path in sorted(output_dir.rglob("*")):
        if (
            path.is_file()
            and path.name != "GitHub與main核對結果.json"
        ):
            files.append((
                str(
                    path.relative_to(output_dir)
                ).replace("\\", "/"),
                path.read_bytes()
            ))

    uploaded = []

    for rel_path, content in files:
        remote_path = f"{remote_root}/{rel_path}"

        github_put_file(
            token,
            branch,
            remote_path,
            content,
            (
                "SSO v1.3.8 official daily market evidence: "
                f"{Path(rel_path).name}"
            ),
        )

        github_readback(
            branch,
            remote_path,
            sha256_bytes(content)
        )

        uploaded.append({
            "path": remote_path,
            "bytes": len(content),
            "sha256": sha256_bytes(content),
            "readback": "PASS",
        })

    ref = gh_request(
        "GET",
        (
            f"https://api.github.com/repos/{REPO}"
            f"/git/ref/heads/{quote(branch, safe='/')}"
        ),
        token
    ).json()

    return {
        "branch": branch,
        "commit_after_package_upload": ref["object"]["sha"],
        "uploaded_files": uploaded,
        "readback": "PASS",
        "remote_root": remote_root,
    }


def upload_final_record(
    token,
    branch,
    remote_root,
    record_path
):
    content = record_path.read_bytes()
    remote_path = (
        f"{remote_root}/{record_path.name}"
    )

    github_put_file(
        token,
        branch,
        remote_path,
        content,
        "SSO v1.3.8 final GitHub and main verification record",
    )

    github_readback(
        branch,
        remote_path,
        sha256_bytes(content)
    )

    return {
        "path": remote_path,
        "sha256": sha256_bytes(content),
        "readback": "PASS",
    }


# ============================================================
# 08. 主流程
# ============================================================

def main():
    log("=" * 96)

    log(
        f"啟動 {VERSION}｜"
        f"{TARGET_DATE.isoformat()} 雙市場官方指定日期行情補充"
    )

    log(
        "原始資料唯讀；只新增獨立封存；"
        "不修改正式資料庫、舊版本或 main。"
    )

    log(f"[ENDPOINT] TWSE={TWSE_URL}")
    log(
        f"[PARAMS] TWSE="
        f"{json.dumps(twse_params(), ensure_ascii=False)}"
    )

    log(f"[ENDPOINT] TPEX={TPEX_URL}")
    log(
        f"[PARAMS] TPEX="
        f"{json.dumps(tpex_params(), ensure_ascii=False)}"
    )

    mount_drive()

    token = get_github_token()

    main_sha_before = None
    github_ready = False
    github_precheck_error = None

    if token:
        try:
            main_sha_before = get_main_sha(token)

            log(
                f"[GITHUB] main before={main_sha_before}"
            )

            if main_sha_before == EXPECTED_MAIN_SHA:
                github_ready = True
            else:
                github_precheck_error = (
                    f"main SHA 已與既定基準不同：{main_sha_before}；"
                    "為避免分支基準不符，本次略過 GitHub 上傳。"
                )

        except Exception as exc:
            github_precheck_error = (
                f"GitHub main precheck 失敗："
                f"{type(exc).__name__}: {exc}"
            )

    else:
        github_precheck_error = (
            "未取得 GITHUB_TOKEN，略過 GitHub 上傳。"
        )

    session = make_session()
    source_items = {}

    # 兩市場獨立擷取：其中一個失敗，不阻斷另一個。
    source_items["TWSE"] = fetch_source(
        session,
        "TWSE",
        TWSE_URL,
        twse_params()
    )

    time.sleep(1.2)

    source_items["TPEX"] = fetch_source(
        session,
        "TPEX",
        TPEX_URL,
        tpex_params()
    )

    market_checks = {}
    parsed_data = {}

    for market in ("TWSE", "TPEX"):
        check, parsed = validate_market_result(
            source_items[market]
        )

        market_checks[market] = check

        if parsed is not None:
            parsed_data[market] = parsed

        if check["passed"]:
            log(
                f"[DATE PASS] {market} "
                f"requested={check['requested_date']} "
                f"rows={check['row_count']:,} "
                f"basis={check['date_confirmation_basis']}"
            )
        else:
            log(
                f"[DATE FAIL] {market} "
                f"{check.get('error')}"
            )

    overall_pass = all(
        market_checks[m]["passed"]
        for m in ("TWSE", "TPEX")
    )

    status = (
        "PASS"
        if overall_pass
        else "WAITING_DATE_VERIFICATION"
    )

    suffix = (
        "PASS"
        if overall_pass
        else "待補核驗"
    )

    output_name = (
        f"{OUTPUT_BASE_NAME}_{suffix}"
    )

    output_dir = CONTINUATION_ROOT / output_name
    zip_path = CONTINUATION_ROOT / f"{output_name}.zip"

    if output_dir.exists() or zip_path.exists():
        raise FileExistsError(
            f"輸出路徑已存在，避免覆寫而停止：{output_dir}"
        )

    output_dir.mkdir(
        parents=True,
        exist_ok=False
    )

    raw_manifest = save_raw_evidence(
        output_dir,
        source_items
    )

    reports_dir = output_dir / "reports"
    reports_dir.mkdir(
        parents=True,
        exist_ok=True
    )

    quality = []

    if overall_pass:
        normalized_dir = output_dir / "normalized"
        normalized_dir.mkdir(
            parents=True,
            exist_ok=True
        )

        all_records = []

        for market in ("TWSE", "TPEX"):
            rows = parsed_data[market]["rows"]

            q = check_market_quality(
                market,
                rows
            )

            quality.append(q)

            records = to_market_records(
                market,
                rows
            )

            all_records.extend(records)

            write_csv(
                normalized_dir / f"{market}_官方行情.csv",
                records
            )

        write_csv(
            normalized_dir / "雙市場官方行情.csv",
            all_records
        )

        write_csv(
            reports_dir / "資料品質檢查.csv",
            [
                {
                    "市場": q["market"],
                    "資料列數": q["row_count"],
                    "唯一代號數": q["unique_code_count"],
                    "缺少代號列數": q["missing_code_rows"],
                    "重複代號數": q["duplicate_code_count"],
                    "零成交量列數": q["zero_volume_rows"],
                    "缺少成交量列數": q["missing_volume_rows"],
                    "缺開盤價": q["ohlc_missing_rows"]["open"],
                    "缺最高價": q["ohlc_missing_rows"]["high"],
                    "缺最低價": q["ohlc_missing_rows"]["low"],
                    "缺收盤價": q["ohlc_missing_rows"]["close"],
                }
                for q in quality
            ]
        )

    else:
        # 日期不完整或不一致時，只封存原始證據與核驗結果，
        # 不建立目標日期標準化行情 CSV。
        log(
            "[HOLD] 至少一個市場未通過；"
            "不產生正式標準化行情 CSV。"
        )

    summary = {
        "version": VERSION,
        "target_date": TARGET_DATE.isoformat(),
        "run_time_taipei": RUN_TIME.isoformat(),
        "status": status,
        "purpose": "雙市場官方指定日期歷史行情增量補充",
        "sources": market_checks,
        "raw_evidence": raw_manifest,
        "quality": quality,
        "total_rows": (
            sum(
                market_checks[m].get("row_count", 0)
                for m in ("TWSE", "TPEX")
            )
            if overall_pass
            else None
        ),
        "data_handling": {
            "original_api_response_preserved": True,
            "original_values_modified": False,
            "historical_data_overwritten": False,
            "formal_database_updated": False,
            "normalized_csv_created": overall_pass,
            "unverified_data_mixed_into_official_dataset": False,
            "etf_etn_tdr_mixed_security_warning": (
                "保留官方指定日期端點回傳的完整證券範圍；"
                "未以代號自行排除 ETF、ETN、TDR 或其他證券。"
                "普通股範圍應由已核實的官方證券名單另行界定。"
            ),
        },
        "github_expected_main_sha": EXPECTED_MAIN_SHA,
        "github_main_sha_before": main_sha_before,
        "github_precheck_error": github_precheck_error,
    }

    save_json(
        reports_dir / "每日行情摘要.json",
        summary
    )

    save_json(
        reports_dir / "兩市場日期與來源核驗.json",
        {
            "target_date": TARGET_DATE.isoformat(),
            "overall_pass": overall_pass,
            "market_checks": market_checks,
        }
    )

    write_readme(
        output_dir,
        status,
        market_checks,
        quality,
        raw_manifest
    )

    save_notebook_source(output_dir)

    zip_info = create_zip(
        output_dir,
        zip_path
    )

    log(
        f"[ZIP PASS] {zip_path} "
        f"bytes={zip_info['bytes']:,} "
        f"SHA256={zip_info['sha256']}"
    )

    github_result = {
        "attempted": False,
        "success": False,
        "branch": None,
        "error": github_precheck_error,
    }

    remote_root = None

    if token and github_ready:
        try:
            github_result["attempted"] = True

            uploaded = upload_github(
                token,
                main_sha_before,
                zip_path,
                output_dir
            )

            github_result.update({
                "success": True,
                **uploaded,
            })

            remote_root = uploaded["remote_root"]

            log(
                f"[GITHUB PASS] branch={uploaded['branch']}"
            )

            log(
                f"[GITHUB PASS] readback={uploaded['readback']} "
                f"files={len(uploaded['uploaded_files'])}"
            )

        except Exception as exc:
            github_result["error"] = (
                f"{type(exc).__name__}: {exc}"
            )

            log(
                f"[GITHUB FAIL] {github_result['error']}"
            )

    else:
        log(
            f"[GITHUB SKIP] {github_precheck_error}"
        )

    main_sha_after = None
    main_unchanged = None

    if token:
        try:
            main_sha_after = get_main_sha(token)

            main_unchanged = (
                main_sha_before == main_sha_after
                if main_sha_before
                else None
            )

            log(
                f"[GITHUB] main after package upload={main_sha_after}"
            )

        except Exception as exc:
            github_result["main_after_check_error"] = (
                f"{type(exc).__name__}: {exc}"
            )

    final_record = {
        "version": VERSION,
        "data_date": TARGET_DATE.isoformat(),
        "status": status,
        "output_folder": str(output_dir),
        "zip_path": str(zip_path),
        "zip_sha256": zip_info["sha256"],
        "github": github_result,
        "main_sha_before": main_sha_before,
        "main_sha_after_package_upload": main_sha_after,
        "main_unchanged_after_package_upload": main_unchanged,
        "drive_original_data_modified": False,
        "formal_database_modified": False,
    }

    final_record_path = (
        output_dir / "GitHub與main核對結果.json"
    )

    save_json(
        final_record_path,
        final_record
    )

    if github_result.get("success") and remote_root:
        try:
            # 先再次核對 main，再把最終核對紀錄寫入 audit branch。
            main_sha_before_final_record = get_main_sha(token)

            unchanged_before_final_record = (
                main_sha_before_final_record == main_sha_before
                if main_sha_before
                else None
            )

            final_record["main_sha_before_final_record"] = (
                main_sha_before_final_record
            )

            final_record["main_unchanged_before_final_record"] = (
                unchanged_before_final_record
            )

            save_json(
                final_record_path,
                final_record
            )

            record_upload = upload_final_record(
                token,
                github_result["branch"],
                remote_root,
                final_record_path
            )

            github_result["final_record_upload"] = record_upload

            log(
                f"[GITHUB PASS] final record readback="
                f"{record_upload['readback']}"
            )

            # audit branch 上傳不應變動 main；
            # 完成後再核對一次並記錄於 Drive 本機紀錄。
            main_sha_after_final_record = get_main_sha(token)

            final_record["main_sha_after_final_record"] = (
                main_sha_after_final_record
            )

            final_record["main_unchanged_final"] = (
                main_sha_after_final_record == main_sha_before
                if main_sha_before
                else None
            )

            final_record["github"]["final_record_upload"] = (
                record_upload
            )

            final_record["github"]["main_sha_after_final_record"] = (
                main_sha_after_final_record
            )

            final_record["github"]["main_unchanged_final"] = (
                final_record["main_unchanged_final"]
            )

            save_json(
                final_record_path,
                final_record
            )

        except Exception as exc:
            github_result["final_record_error"] = (
                f"{type(exc).__name__}: {exc}"
            )

            final_record["github"] = github_result

            save_json(
                final_record_path,
                final_record
            )

            log(
                "[GITHUB WARN] final record upload/check failed: "
                f"{github_result['final_record_error']}"
            )

    log("=" * 96)

    log(f"[FINAL] status={status}")

    log(
        f"[FINAL] TWSE rows="
        f"{market_checks['TWSE'].get('row_count', 0):,} "
        f"passed={market_checks['TWSE']['passed']}"
    )

    log(
        f"[FINAL] TPEX rows="
        f"{market_checks['TPEX'].get('row_count', 0):,} "
        f"passed={market_checks['TPEX']['passed']}"
    )

    log(f"[FINAL] OUTPUT={output_dir}")
    log(f"[FINAL] ZIP={zip_path}")
    log(f"[FINAL] ZIP SHA256={zip_info['sha256']}")
    log(f"[FINAL] GitHub upload={github_result['success']}")
    log(f"[FINAL] main unchanged={main_unchanged}")

    if overall_pass:
        log(
            "[FINAL] 兩市場指定日期行情通過；"
            "已產生標準化 CSV。"
        )
    else:
        log(
            "[FINAL] 尚未完成雙市場日期核驗；"
            "僅封存診斷原始證據，未產生正式行情 CSV。"
        )

    log(
        "[FINAL] 舊資料、原始資料及正式資料庫未修改。"
    )

    log("=" * 96)


try:
    main()

except Exception as exc:
    log(
        f"[FATAL] {type(exc).__name__}: {exc}"
    )
    traceback.print_exc()
    raise