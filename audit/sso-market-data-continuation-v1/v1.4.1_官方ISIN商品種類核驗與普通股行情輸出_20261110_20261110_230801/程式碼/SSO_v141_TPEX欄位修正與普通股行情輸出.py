# -*- coding: utf-8 -*-
"""
SSO_Market_Data_Continuation_TWSE_TPEX v1.4.1
官方 ISIN 商品種類核驗與普通股行情輸出（單次完整流程）

本版一次完成：
1. 唯讀驗證 v1.3.8 2026-11-10 雙市場官方行情及 ZIP SHA256。
2. 唯讀驗證並解析 v1.3.3 官方來源 ZIP，修正 CSV 前置標題列造成的表頭辨識失敗。
3. 取得 TWSE / TPEx 官方公司基本資料，僅作主檔交叉核對，不單獨視為普通股證明。
4. 取得 TWSE 官方 ISIN 綜合證券名錄，使用 Security Code / Market / Type of security / CFICode 分類。
5. 逐筆分類普通股、ETF、ETN、TDR、其他非普通股、未知及來源衝突。
6. 只將有明確官方普通股證據的資料列輸出為「已確認普通股行情」。
7. 產出逐筆、各類拆分、未匹配、主檔交叉核對、品質報告及來源證據。
8. ZIP 封存、GitHub 獨立 audit branch 上傳、逐檔讀回 SHA256、main 前後 SHA 核對。

安全：原始資料唯讀；不覆蓋舊版、不更新正式資料庫、不重訓、不回測、不選股、不修改 main。
Colab 單一 cell 執行；不使用 argparse，kernel -f 參數不影響。
"""
from __future__ import annotations

import os
import re
import io
import csv
import json
import time
import gzip
import base64
import hashlib
import zipfile
import traceback
from pathlib import Path
from datetime import datetime, date
from zoneinfo import ZoneInfo
from html.parser import HTMLParser
from urllib.parse import quote

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# =============================================================================
# 01. 固定設定
# =============================================================================
VERSION = "v1.4.1"
TARGET_DATE = date(2026, 11, 10)
TZ = ZoneInfo("Asia/Taipei")
RUN_TIME = datetime.now(TZ)
STAMP = RUN_TIME.strftime("%Y%m%d_%H%M%S")

PROJECT_ROOT = Path("/content/drive/MyDrive/看股最速適")
CONTINUATION_ROOT = PROJECT_ROOT / "SSO_Market_Data_Continuation_TWSE_TPEX_v1"
V138_DIRNAME = "v1.3.8_雙市場官方指定日期行情補充_20261110_20261110_223151_PASS"
V138_ZIPNAME = V138_DIRNAME + ".zip"
V138_CSV_REL = "normalized/雙市場官方行情.csv"
V138_ZIP_SHA256 = "97fb3ab0bc6bc6c508f800f68d14c2bd47baa5d963fb2452a40eabda3c62c1c8"

REPO = "CHLINX/sso"
EXPECTED_MAIN_SHA = "aaabc2433fb5df2bb68af06461178305d6bd30d1"

V133_BRANCH = "audit/sso-market-data-continuation-v1-v133-upload-20261109_125725"
V133_ZIP_REL = (
    "audit/sso-market-data-continuation-v1/"
    "v1.3.3_官方普通股範圍與逐日行情交叉驗證_20261109_125127/"
    "v1.3.3_官方普通股範圍與逐日行情交叉驗證_20261109_125127.zip"
)
V133_ZIP_SHA256 = "48fbd3a78edcde0efbb4b39f7dc24197d55047fbac00b3b7f9a41f586d32480d"
V133_RAW_URL = (
    f"https://raw.githubusercontent.com/{REPO}/{V133_BRANCH}/"
    + quote(V133_ZIP_REL, safe="/")
)

TWSE_COMPANY_URL = "https://openapi.twse.com.tw/v1/opendata/t187ap03_L"
TPEX_COMPANY_URL = "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap03_O"
# 官方 ISIN 綜合證券清單，包含 Security Code / Market / Type of security / CFICode。
ISIN_URL = "https://isin.twse.com.tw/isin/e_single_main.jsp"
# TWSE 官方基金基本資料彙總表，作 ETF 補充交叉核對（不是主要分類來源）。
TWSE_FUND_URL = "https://openapi.twse.com.tw/v1/opendata/t187ap47_L"

MIN_MASTER_ROWS = {"TWSE": 500, "TPEX": 300}
MIN_ISIN_ROWS = 1500
MIN_ISIN_MARKET_ROWS = {"TWSE": 800, "TPEX": 650}
MIN_ISIN_MASTER_OVERLAP = {"TWSE": 0.70, "TPEX": 0.70}
TIMEOUT = (25, 150)

# =============================================================================
# 02. 通用工具
# =============================================================================
def log(msg: str) -> None:
    print(f"[{datetime.now(TZ).strftime('%H:%M:%S')}] {msg}", flush=True)


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def save_json(path: Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2, default=str), encoding="utf-8")


def norm_key(value) -> str:
    return re.sub(r"[\s_\-（）()：:./]+", "", str(value or "")).lower()


def normalize_code(value) -> str:
    if value is None:
        return ""
    s = str(value).strip().replace("\ufeff", "")
    if s.endswith(".0") and s[:-2].isdigit():
        s = s[:-2]
    return s.upper()


def first_value(row: dict, aliases) -> str:
    lookup = {norm_key(k): v for k, v in row.items()}
    for alias in aliases:
        k = norm_key(alias)
        if k in lookup and lookup[k] is not None:
            return str(lookup[k]).strip()
    return ""


def make_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=5, connect=5, read=5, status=5,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"], raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=8, pool_maxsize=8)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (compatible; SSO-Market-Data-Continuation/1.4.0)",
        "Accept": "text/html,application/json,text/csv,*/*",
        "Accept-Language": "en-US,en;q=0.9,zh-TW;q=0.8,zh;q=0.7",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    })
    return session


def mount_drive() -> None:
    if not Path("/content/drive/MyDrive").exists():
        from google.colab import drive
        drive.mount("/content/drive")
    if not CONTINUATION_ROOT.exists():
        raise FileNotFoundError(f"找不到行情續補根目錄：{CONTINUATION_ROOT}")


def get_github_token():
    token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
    if token:
        return token
    try:
        from google.colab import userdata
        return userdata.get("GITHUB_TOKEN")
    except Exception:
        return None


def gzip_save(path: Path, raw: bytes):
    path.parent.mkdir(parents=True, exist_ok=True)
    packed = gzip.compress(raw, compresslevel=6, mtime=0)
    path.write_bytes(packed)
    return {
        "path": str(path),
        "raw_bytes": len(raw),
        "gzip_bytes": len(packed),
        "raw_sha256": sha256_bytes(raw),
        "gzip_sha256": sha256_bytes(packed),
    }


def write_csv(path: Path, rows: list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("NO_DATA\n", encoding="utf-8-sig")
        return
    fields = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def json_records(payload):
    if isinstance(payload, list) and all(isinstance(x, dict) for x in payload):
        return payload
    if isinstance(payload, dict):
        for key in ("data", "Data", "result", "results", "records", "aaData"):
            value = payload.get(key)
            if isinstance(value, list) and all(isinstance(x, dict) for x in value):
                return value
    raise ValueError("官方 JSON 回應不是可辨識的物件陣列")

# =============================================================================
# 03. v1.3.8 行情輸入驗證
# =============================================================================
def load_v138_quotes():
    folder = CONTINUATION_ROOT / V138_DIRNAME
    zip_path = CONTINUATION_ROOT / V138_ZIPNAME
    csv_path = folder / V138_CSV_REL
    if not folder.exists() or not csv_path.exists() or not zip_path.exists():
        raise FileNotFoundError("v1.3.8 行情資料夾、CSV 或 ZIP 不完整")
    actual_zip_sha = sha256_file(zip_path)
    if actual_zip_sha != V138_ZIP_SHA256:
        raise ValueError(f"v1.3.8 ZIP SHA256 不符：{actual_zip_sha}")
    with zipfile.ZipFile(zip_path, "r") as zf:
        bad = zf.testzip()
        if bad:
            raise ValueError(f"v1.3.8 ZIP CRC 錯誤：{bad}")
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        quotes = list(csv.DictReader(f))
    required = {"market", "trade_date", "instrument_code", "instrument_name", "source_row_sha256"}
    if not quotes or not required.issubset(quotes[0].keys()):
        raise ValueError("v1.3.8 標準化行情 CSV 缺必要欄位")
    for row in quotes:
        row["market"] = str(row.get("market", "")).strip().upper()
        row["instrument_code"] = normalize_code(row.get("instrument_code"))
        row["instrument_name"] = str(row.get("instrument_name", "")).strip()
    dates = sorted({r.get("trade_date", "") for r in quotes})
    if dates != [TARGET_DATE.isoformat()]:
        raise ValueError(f"v1.3.8 行情日期不是唯一目標日：{dates}")
    invalid_market = sorted({r["market"] for r in quotes} - {"TWSE", "TPEX"})
    if invalid_market:
        raise ValueError(f"v1.3.8 有未知市場標記：{invalid_market}")
    return {
        "quotes": quotes,
        "directory": str(folder),
        "csv_path": str(csv_path),
        "zip_path": str(zip_path),
        "zip_sha256": actual_zip_sha,
        "csv_sha256": sha256_file(csv_path),
    }

# =============================================================================
# 04. v1.3.3 ZIP 歷史公司主檔：尋找實際表頭，不假設第一列就是表頭
# =============================================================================
CODE_ALIASES = [
    "公司代號", "證券代號", "股票代號", "代號",
    "SecuritiesCompanyCode", "SecurityCode", "Security Code",
    "CompanyCode", "Company Code", "Code",
]
SHORT_NAME_ALIASES = [
    "公司簡稱", "證券簡稱", "股票簡稱", "簡稱",
    "CompanyAbbreviation", "Company Abbreviation", "CompanyShortName", "Name",
]
FULL_NAME_ALIASES = [
    "公司名稱", "證券名稱", "股票名稱",
    "CompanyName", "Company Name", "SecurityName", "Security Name",
]


def decode_text(raw: bytes):
    for enc in ("utf-8-sig", "utf-8", "cp950", "big5"):
        try:
            return raw.decode(enc), enc
        except Exception:
            continue
    raise UnicodeDecodeError("unknown", raw, 0, min(1, len(raw)), "無法解碼")


def parse_csv_with_header_scan(raw: bytes):
    text, encoding = decode_text(raw)
    sample = text[:20000]
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",\t;|")
        delimiter = dialect.delimiter
    except Exception:
        delimiter = ","
    matrix = list(csv.reader(io.StringIO(text), delimiter=delimiter))
    header_index = None
    code_col = None
    for i, row in enumerate(matrix[:40]):
        normalized = {norm_key(c): j for j, c in enumerate(row)}
        for alias in CODE_ALIASES:
            key = norm_key(alias)
            if key in normalized:
                header_index = i
                code_col = normalized[key]
                break
        if header_index is not None:
            break
    if header_index is None:
        raise ValueError("前 40 列未找到公司／證券代號表頭")
    headers = [str(x).strip().replace("\ufeff", "") for x in matrix[header_index]]
    records = []
    for row in matrix[header_index + 1:]:
        if not row or not any(str(x).strip() for x in row):
            continue
        if len(row) < len(headers):
            row = row + [""] * (len(headers) - len(row))
        elif len(row) > len(headers):
            row = row[:len(headers)]
        records.append(dict(zip(headers, row)))
    profiles = []
    for row in records:
        code = normalize_code(first_value(row, CODE_ALIASES))
        if not code:
            continue
        profiles.append({
            "instrument_code": code,
            "company_short_name": first_value(row, SHORT_NAME_ALIASES),
            "company_name": first_value(row, FULL_NAME_ALIASES),
        })
    return {
        "rows": records,
        "profiles": profiles,
        "encoding": encoding,
        "delimiter": delimiter,
        "header_row_1based": header_index + 1,
        "header": headers,
        "recognized_codes": len({p["instrument_code"] for p in profiles}),
    }


def load_v133_history(session, out_dir: Path):
    target = out_dir / "sources" / "v1.3.3_官方普通股範圍來源.zip"
    target.parent.mkdir(parents=True, exist_ok=True)
    response = session.get(V133_RAW_URL, timeout=TIMEOUT)
    response.raise_for_status()
    raw = response.content
    actual = sha256_bytes(raw)
    if actual != V133_ZIP_SHA256:
        raise ValueError(f"v1.3.3 官方 ZIP SHA256 不符：{actual}")
    target.write_bytes(raw)
    parsed = []
    errors = []
    with zipfile.ZipFile(io.BytesIO(raw), "r") as zf:
        bad = zf.testzip()
        if bad:
            raise ValueError(f"v1.3.3 ZIP CRC 錯誤：{bad}")
        inventory = zf.namelist()
        members = [n for n in inventory if "公司基本資料" in Path(n).name and n.lower().endswith(".csv")]
        for member in members:
            if "上櫃" in member or "tpex" in member.lower() or "otc" in member.lower():
                market = "TPEX"
            elif "上市" in member or "twse" in member.lower() or "listed" in member.lower():
                market = "TWSE"
            else:
                market = "UNKNOWN"
            data = zf.read(member)
            try:
                result = parse_csv_with_header_scan(data)
                result.update({
                    "member": member,
                    "market": market,
                    "member_sha256": sha256_bytes(data),
                })
                parsed.append(result)
            except Exception as exc:
                errors.append({"member": member, "market": market, "error": f"{type(exc).__name__}: {exc}"})
    save_json(out_dir / "sources" / "v1.3.3_歷史公司主檔解析結果.json", {
        "zip_sha256": actual,
        "member_count": len(inventory),
        "company_master_member_count": len(members),
        "parsed_members": [
            {k: v for k, v in x.items() if k != "rows" and k != "profiles" and k != "header"}
            for x in parsed
        ],
        "errors": errors,
    })
    return {"path": str(target), "sha256": actual, "member_count": len(inventory), "members": parsed, "errors": errors}

# =============================================================================
# 05. 官方公司主檔（僅作交叉參考）
# =============================================================================
def fetch_company_master(session, market, url):
    r = session.get(url, timeout=TIMEOUT)
    r.raise_for_status()
    payload = r.json()
    records = json_records(payload)
    if len(records) < MIN_MASTER_ROWS[market]:
        raise ValueError(f"{market} 公司主檔資料列不足：{len(records)}")
    profiles = []
    for row in records:
        code = normalize_code(first_value(row, CODE_ALIASES))
        if not code:
            continue
        profiles.append({
            "market": market,
            "instrument_code": code,
            "short_name": first_value(row, SHORT_NAME_ALIASES),
            "full_name": first_value(row, FULL_NAME_ALIASES),
            "source": url,
        })
    if len(profiles) < MIN_MASTER_ROWS[market]:
        sample_keys = sorted({str(k) for row in records[:10] for k in row.keys()})
        raise ValueError(
            f"{market} 主檔代號解析不足：{len(profiles)}；"
            f"回應筆數={len(records)}；前10列欄位={sample_keys[:40]}"
        )
    return {
        "market": market,
        "url": url,
        "http_status": r.status_code,
        "retrieved_at": datetime.now(TZ).isoformat(),
        "raw_bytes": r.content,
        "raw_sha256": sha256_bytes(r.content),
        "record_count": len(records),
        "profiles": profiles,
    }

# =============================================================================
# 06. ISIN HTML table parser
# =============================================================================
class HTMLTableParser(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.tables = []
        self.in_table = False
        self.in_row = False
        self.in_cell = False
        self.current_table = []
        self.current_row = []
        self.current_cell = []

    def handle_starttag(self, tag, attrs):
        tag = tag.lower()
        if tag == "table":
            if not self.in_table:
                self.in_table = True
                self.current_table = []
        elif self.in_table and tag == "tr":
            self.in_row = True
            self.current_row = []
        elif self.in_table and self.in_row and tag in ("td", "th"):
            self.in_cell = True
            self.current_cell = []
        elif self.in_table and self.in_row and tag == "br" and self.in_cell:
            self.current_cell.append(" ")

    def handle_data(self, data):
        if self.in_table and self.in_row and self.in_cell:
            self.current_cell.append(data)

    def handle_endtag(self, tag):
        tag = tag.lower()
        if tag in ("td", "th") and self.in_cell:
            value = " ".join("".join(self.current_cell).split())
            self.current_row.append(value)
            self.in_cell = False
        elif tag == "tr" and self.in_row:
            if self.current_row:
                self.current_table.append(self.current_row)
            self.in_row = False
        elif tag == "table" and self.in_table:
            if self.current_table:
                self.tables.append(self.current_table)
            self.in_table = False
            self.current_table = []


def norm_header(s):
    return re.sub(r"[^a-z0-9\u4e00-\u9fff]", "", str(s or "").lower())


def locate_header(columns):
    aliases = {
        "isin": {"isincode", "國際證券辨識號碼", "isin"},
        "code": {"securitycode", "證券代號", "代號", "股票代號"},
        "name": {"securityname", "證券名稱", "名稱"},
        "market": {"market", "市場別", "市場"},
        "type": {"typeofsecurity", "證券種類", "證券類別", "有價證券種類"},
        "cfi": {"cficode", "cfi代碼", "金融工具分類代碼"},
        "listed_date": {"datestocklisted", "上市日期", "上櫃日期", "掛牌日期"},
        "industry": {"industrialgroup", "產業別", "產業類別"},
        "remarks": {"remarks", "備註"},
    }
    normalized = [norm_header(x) for x in columns]
    found = {}
    for field, opts in aliases.items():
        for i, value in enumerate(normalized):
            if value in opts:
                found[field] = i
                break
    if "code" in found and ("type" in found or "cfi" in found) and "market" in found:
        return found
    return None


def normalize_market(value):
    s = re.sub(r"\s+", " ", str(value or "").strip().upper())
    if "TWSE" in s or "TAIWAN STOCK EXCHANGE" in s or s in ("上市", "上市市場"):
        return "TWSE"
    if "TPEX" in s or "TPEx".upper() in s or "TAIPEI EXCHANGE" in s or "OTC" in s or s in ("上櫃", "上櫃市場"):
        return "TPEX"
    return "UNKNOWN"


def decode_isin_html(raw: bytes):
    failures = []
    for encoding in ("utf-8", "utf-8-sig", "cp950", "big5"):
        try:
            return raw.decode(encoding), encoding
        except Exception as exc:
            failures.append(f"{encoding}:{exc}")
    raise ValueError("ISIN HTML 解碼失敗：" + " | ".join(failures))


def parse_isin_html(raw: bytes):
    text, encoding = decode_isin_html(raw)
    parser = HTMLTableParser()
    parser.feed(text)
    selected = None
    header_map = None
    header_row_index = None
    for table in parser.tables:
        for ri, row in enumerate(table[:20]):
            mapping = locate_header(row)
            if mapping:
                selected = table
                header_map = mapping
                header_row_index = ri
                break
        if selected is not None:
            break
    if selected is None:
        raise ValueError(
            f"ISIN 官方頁找不到具備代號、Market、Type/CFI 的表格；HTML bytes={len(raw):,} tables={len(parser.tables)}"
        )

    rows = []
    malformed = 0
    max_cols = max(len(r) for r in selected)
    for row in selected[header_row_index + 1:]:
        if len(row) <= header_map["code"]:
            malformed += 1
            continue
        def cell(field):
            idx = header_map.get(field)
            return row[idx].strip() if idx is not None and idx < len(row) else ""
        code = normalize_code(cell("code"))
        if not code or not re.fullmatch(r"[0-9A-Z]{4,8}", code):
            continue
        market = normalize_market(cell("market"))
        item = {
            "market": market,
            "instrument_code": code,
            "isin_code": cell("isin"),
            "security_name": cell("name"),
            "market_raw": cell("market"),
            "security_type": cell("type"),
            "cfi_code": re.sub(r"\s+", "", cell("cfi")).upper(),
            "industrial_group": cell("industry"),
            "listed_date": cell("listed_date"),
            "remarks": cell("remarks"),
        }
        rows.append(item)
    unique_records = {(x["market"], x["instrument_code"], x["isin_code"], x["security_type"], x["cfi_code"]) for x in rows}
    return {
        "records": rows,
        "encoding": encoding,
        "table_count": len(parser.tables),
        "selected_table_rows": len(selected),
        "header_row_1based_in_table": header_row_index + 1,
        "header_map": header_map,
        "malformed_rows": malformed,
        "unique_security_records": len(unique_records),
        "html_title": (re.search(r"<title[^>]*>(.*?)</title>", text, re.I | re.S).group(1).strip() if re.search(r"<title[^>]*>(.*?)</title>", text, re.I | re.S) else ""),
    }


def fetch_isin_master(session, out_dir: Path):
    log("[GET] TWSE 官方 ISIN 綜合證券名錄")
    response = session.get(ISIN_URL, timeout=TIMEOUT)
    response.raise_for_status()
    raw = response.content
    if not raw:
        raise ValueError("ISIN 官方頁回傳空內容")
    low = raw.lstrip()[:100].lower()
    if low.startswith(b"<!doctype html") is False and low.startswith(b"<html") is False and b"<table" not in low:
        # 只做格式提示；完整 HTML 可能在後段出現 table。
        pass
    parsed = parse_isin_html(raw)
    records = parsed["records"]
    twse_count = sum(1 for r in records if r["market"] == "TWSE")
    tpex_count = sum(1 for r in records if r["market"] == "TPEX")
    if len(records) < MIN_ISIN_ROWS:
        raise ValueError(f"ISIN 名錄解析筆數不足：{len(records)} < {MIN_ISIN_ROWS}")
    if twse_count < MIN_ISIN_MARKET_ROWS["TWSE"] or tpex_count < MIN_ISIN_MARKET_ROWS["TPEX"]:
        raise ValueError(
            f"ISIN 市場筆數不足：TWSE={twse_count}, TPEX={tpex_count}; 視為不完整，不建立普通股確認清單"
        )
    raw_info = gzip_save(out_dir / "raw" / "official_isin" / "TWSE_ISIN_綜合證券名錄.html.gz", raw)
    headers = {k: v for k, v in response.headers.items() if k.lower() in (
        "content-type", "content-length", "date", "last-modified", "etag", "cache-control"
    )}
    meta = {
        "url": response.url,
        "http_status": response.status_code,
        "retrieved_at": datetime.now(TZ).isoformat(),
        "response_headers": headers,
        "raw_bytes": len(raw),
        "raw_sha256": sha256_bytes(raw),
        "archive": raw_info,
        "parse": {k: v for k, v in parsed.items() if k != "records"},
        "market_counts": {"TWSE": twse_count, "TPEX": tpex_count},
        "classification_source": "Official TWSE ISIN List of ISIN Code; Security Code / Market / Type of security / CFICode",
    }
    save_json(out_dir / "raw" / "official_isin" / "ISIN_名錄metadata.json", meta)
    return {**parsed, "meta": meta}

# =============================================================================
# 07. 證券種類分類
# =============================================================================
def classify_security_type(item: dict):
    sec_type = re.sub(r"\s+", " ", str(item.get("security_type", "")).strip()).upper()
    cfi = re.sub(r"\s+", "", str(item.get("cfi_code", "")).strip()).upper()
    name = str(item.get("security_name", "")).strip()

    # 明確產品類別優先，避免任何 CFI / 名稱歧義。
    if "ETF" in sec_type or "EXCHANGE TRADED FUND" in sec_type:
        return "ETF", "EXPLICIT_ISIN_SECURITY_TYPE_ETF", "ISIN Type of security 明確標示 ETF"
    if "ETN" in sec_type or "EXCHANGE TRADED NOTE" in sec_type:
        return "ETN", "EXPLICIT_ISIN_SECURITY_TYPE_ETN", "ISIN Type of security 明確標示 ETN"
    if "TDR" in sec_type or "DEPOSITARY RECEIPT" in sec_type or "DEPOSITORY RECEIPT" in sec_type:
        return "TDR", "EXPLICIT_ISIN_SECURITY_TYPE_TDR", "ISIN Type of security 明確標示存託憑證"
    # ISO 10962 CFI 的 ED 群組代表股票存託憑證；Type 欄位缺漏時亦可辨識。
    if cfi.startswith("ED"):
        return "TDR", "OFFICIAL_ISIN_CFI_ED", f"ISIN CFI={cfi}，ED 為股票存託憑證分類"
    if any(x in sec_type for x in ("WARRANT", "CERTIFICATE", "BOND", "DEBENTURE", "PREFERRED", "PREFERENCE", "RIGHT", "FUTURE", "OPTION")):
        return "其他非普通股", "EXPLICIT_ISIN_NON_ORDINARY_TYPE", f"ISIN Type of security={sec_type}"

    # CFI 前兩碼 ES 表示普通／一般股（Equities / Common or Ordinary Shares）。
    # 僅使用官方 CFI 欄位，不由股票代號或公司名稱推斷。
    if cfi.startswith("ES"):
        return "普通股", "OFFICIAL_ISIN_CFI_ES", f"ISIN CFI={cfi}，前兩碼 ES 為普通／一般股分類"

    # 明確文字分類可作次要確認；泛稱 Stock 不足以證明普通股類別。
    compact = re.sub(r"[^A-Z]", "", sec_type)
    if any(x in compact for x in ("COMMONSTOCK", "ORDINARYSHARE", "ORDINARYSTOCK", "COMMONSHARE")):
        return "普通股", "EXPLICIT_ISIN_COMMON_ORDINARY_TYPE", f"ISIN Type of security 明確標示 {sec_type}"
    if sec_type:
        return "其他非普通股／需再確認", "ISIN_TYPE_NOT_COMMON", f"ISIN Type of security={sec_type}; CFI={cfi}"
    return "ISIN類別缺漏／待確認", "ISIN_TYPE_AND_CFI_MISSING", f"ISIN Type/CFI 欄位缺漏；名稱僅供參考：{name}"


def build_isin_maps(records):
    maps = {}
    for item in records:
        key = (item["market"], item["instrument_code"])
        if item["market"] == "UNKNOWN":
            continue
        maps.setdefault(key, []).append(item)
    return maps


def choose_isin_record(items):
    """同市場同代號如出現多筆相同類別可視為重複證據；若類別/CFI衝突則隔離。"""
    signatures = {
        (classify_security_type(x)[0], x.get("security_type", ""), x.get("cfi_code", ""))
        for x in items
    }
    if len(signatures) > 1:
        return None, "ISIN 同市場同代號有不同證券類別/CFI，隔離不判斷"
    # 相同類型的重複 ISIN 記錄，保留第一筆並在報告列出筆數。
    return items[0], None


def classify_quotes(quotes, isin_records, current_profiles, history_members):
    isin_maps = build_isin_maps(isin_records)
    current_maps = {
        market: {x["instrument_code"]: x for x in current_profiles.get(market, [])}
        for market in ("TWSE", "TPEX")
    }
    historical_maps = {"TWSE": set(), "TPEX": set()}
    for member in history_members:
        if member.get("market") in historical_maps:
            historical_maps[member["market"]].update(p["instrument_code"] for p in member.get("profiles", []))

    output = []
    confirmed = []
    excluded = {"ETF": [], "ETN": [], "TDR": [], "其他非普通股": []}
    unknown = []
    conflicts = []
    duplicate_quote_rows = []
    quote_counts = {}
    for q in quotes:
        k = (q["market"], q["instrument_code"])
        quote_counts[k] = quote_counts.get(k, 0) + 1
    duplicate_isin_rows = []
    for (m, c), items in isin_maps.items():
        if len(items) > 1:
            for item in items:
                duplicate_isin_rows.append({**item, "isin_market_code_count": len(items)})

    for q in quotes:
        market = q["market"]
        code = q["instrument_code"]
        key = (market, code)
        entries = isin_maps.get(key, [])
        current_match = current_maps.get(market, {}).get(code)
        history_match = code in historical_maps.get(market, set())
        isin = None
        conflict_note = None
        if len(entries) == 1:
            isin = entries[0]
        elif len(entries) > 1:
            isin, conflict_note = choose_isin_record(entries)
        quote_dup_count = quote_counts.get(key, 0)
        if quote_dup_count > 1:
            category = "行情同市場代號重複／待確認"
            code_class = "DUPLICATE_QUOTE_KEY_HOLD"
            basis = f"同市場同代號行情列數={quote_dup_count}；為避免重複計入，隔離不納入普通股行情"
        elif conflict_note:
            category = "ISIN來源衝突／待確認"
            code_class = "ISIN_CONFLICT_HOLD"
            basis = conflict_note
        elif isin is None:
            category = "ISIN未匹配／證券種類待確認"
            code_class = "ISIN_NO_MATCH_UNKNOWN"
            basis = "未命中官方 ISIN 名錄；不以公司主檔、代號或名稱猜測普通股/ETF/ETN/TDR"
        else:
            category, code_class, basis = classify_security_type(isin)
        row = {k: ("" if v is None else str(v)) for k, v in q.items()}
        row.update({
            "official_isin_match": bool(isin),
            "isin_match_count": len(entries),
            "quote_market_code_count": quote_dup_count,
            "quote_market_code_duplicate": quote_dup_count > 1,
            "isin_code": isin.get("isin_code", "") if isin else "",
            "isin_security_name": isin.get("security_name", "") if isin else "",
            "isin_market_raw": isin.get("market_raw", "") if isin else "",
            "isin_security_type": isin.get("security_type", "") if isin else "",
            "isin_cfi_code": isin.get("cfi_code", "") if isin else "",
            "isin_industrial_group": isin.get("industrial_group", "") if isin else "",
            "isin_listed_date": isin.get("listed_date", "") if isin else "",
            "isin_remarks": isin.get("remarks", "") if isin else "",
            "security_category": category,
            "classification_code": code_class,
            "classification_basis": basis,
            "current_company_master_match": bool(current_match),
            "current_company_master_name": (current_match.get("short_name") or current_match.get("full_name")) if current_match else "",
            "historical_v133_company_master_match": history_match,
            "ordinary_stock_confirmed": category == "普通股",
            "included_in_confirmed_ordinary_stock_file": category == "普通股",
        })
        output.append(row)
        if category == "普通股":
            confirmed.append(row)
        elif category in excluded:
            excluded[category].append(row)
        elif category == "其他非普通股／需再確認":
            excluded["其他非普通股"].append(row)
        else:
            unknown.append(row)
        if conflict_note or quote_dup_count > 1:
            conflicts.append(row)
        if quote_dup_count > 1:
            duplicate_quote_rows.append(row)
    return {
        "all": output,
        "confirmed_ordinary": confirmed,
        "excluded": excluded,
        "unknown": unknown,
        "conflicts": conflicts,
        "duplicate_quote_rows": duplicate_quote_rows,
        "duplicate_isin_rows": duplicate_isin_rows,
        "current_maps": current_maps,
        "historical_maps": historical_maps,
    }

# =============================================================================
# 08. GitHub audit branch
# =============================================================================
def gh_request(method, url, token, **kwargs):
    headers = kwargs.pop("headers", {})
    headers.update({
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "SSO-Market-Data-Continuation-v1.4.1",
    })
    r = requests.request(method, url, headers=headers, timeout=150, **kwargs)
    if r.status_code >= 400:
        raise RuntimeError(f"GitHub HTTP {r.status_code}: {r.text[:1200]}")
    return r


def get_main_sha(token):
    j = gh_request("GET", f"https://api.github.com/repos/{REPO}/git/ref/heads/main", token).json()
    return j["object"]["sha"]


def upload_one(token, branch, remote_path, content):
    api_url = f"https://api.github.com/repos/{REPO}/contents/{quote(remote_path, safe='/')}"
    body = {
        "message": f"SSO v1.4.1 official ISIN classification: {Path(remote_path).name}",
        "content": base64.b64encode(content).decode("ascii"),
        "branch": branch,
    }
    j = gh_request("PUT", api_url, token, json=body).json()
    raw_url = f"https://raw.githubusercontent.com/{REPO}/{branch}/{quote(remote_path, safe='/')}"
    rr = requests.get(raw_url, timeout=150)
    rr.raise_for_status()
    expected = sha256_bytes(content)
    actual = sha256_bytes(rr.content)
    if actual != expected:
        raise RuntimeError(f"GitHub readback SHA256 不符：{remote_path}")
    return {"path": remote_path, "bytes": len(content), "sha256": expected, "commit": j.get("commit", {}).get("sha"), "readback": "PASS"}


def upload_package(token, main_sha, out_dir, zip_path):
    branch = f"audit/sso-market-data-continuation-v1-v141-isin-{TARGET_DATE:%Y%m%d}-{STAMP}"
    gh_request(
        "POST", f"https://api.github.com/repos/{REPO}/git/refs", token,
        json={"ref": f"refs/heads/{branch}", "sha": main_sha},
    )
    remote_root = f"audit/sso-market-data-continuation-v1/{out_dir.name}"
    items = [(zip_path.name, zip_path.read_bytes())]
    for p in sorted(out_dir.rglob("*")):
        if p.is_file() and p.name != "GitHub與main核對結果.json":
            items.append((str(p.relative_to(out_dir)).replace("\\", "/"), p.read_bytes()))
    uploaded = []
    for rel, content in items:
        uploaded.append(upload_one(token, branch, f"{remote_root}/{rel}", content))
    return branch, remote_root, uploaded

# =============================================================================
# 09. 主流程與報告
# =============================================================================
def main():
    log("=" * 100)
    log(f"啟動 {VERSION}｜{TARGET_DATE.isoformat()} 官方 ISIN 證券種類核驗與普通股行情輸出")
    log("只讀 v1.3.8 / v1.3.3；只新增日期專屬封存；不修改正式資料庫或 main。")
    mount_drive()

    out_name = f"{VERSION}_官方ISIN商品種類核驗與普通股行情輸出_{TARGET_DATE:%Y%m%d}_{STAMP}"
    out_dir = CONTINUATION_ROOT / out_name
    zip_path = CONTINUATION_ROOT / f"{out_name}.zip"
    if out_dir.exists() or zip_path.exists():
        raise FileExistsError(f"輸出路徑已存在，避免覆寫：{out_dir}")
    out_dir.mkdir(parents=True, exist_ok=False)
    for sub in ("raw/official_isin", "raw/company_master", "sources", "normalized", "reports", "程式碼"):
        (out_dir / sub).mkdir(parents=True, exist_ok=True)

    session = make_session()
    inputs = load_v138_quotes()
    quotes = inputs["quotes"]
    log(f"[INPUT PASS] v1.3.8 rows={len(quotes):,} date={TARGET_DATE} ZIP SHA256={inputs['zip_sha256']}")
    (out_dir / "sources" / V138_ZIPNAME).write_bytes(Path(inputs["zip_path"]).read_bytes())

    token = get_github_token()
    main_before = None
    github_ready = False
    github_precheck_error = None
    if token:
        try:
            main_before = get_main_sha(token)
            log(f"[GITHUB] main before={main_before}")
            github_ready = main_before == EXPECTED_MAIN_SHA
            if not github_ready:
                github_precheck_error = f"main SHA 與基準不同：{main_before}；略過上傳。"
        except Exception as exc:
            github_precheck_error = f"main precheck 失敗：{type(exc).__name__}: {exc}"
    else:
        github_precheck_error = "未取得 GITHUB_TOKEN，略過 GitHub 上傳。"

    # 先下載並解析歷史官方公司主檔（不作普通股唯一證明）。
    v133 = load_v133_history(session, out_dir)
    log(f"[V133 PASS] SHA256={v133['sha256']} members={v133['member_count']} parsed_company_master={len(v133['members'])}")
    for e in v133["errors"]:
        log(f"[V133 WARN] {e['member']} {e['error']}")

    # 當前公司主檔分市場請求，僅作交叉比對。
    current_profiles = {"TWSE": [], "TPEX": []}
    current_meta = {}
    current_master_complete = True
    for market, url in (("TWSE", TWSE_COMPANY_URL), ("TPEX", TPEX_COMPANY_URL)):
        try:
            result = fetch_company_master(session, market, url)
            current_profiles[market] = result["profiles"]
            archive = gzip_save(out_dir / "raw" / "company_master" / f"{market}_官方公司基本資料.json.gz", result["raw_bytes"])
            meta = {k: v for k, v in result.items() if k not in ("raw_bytes", "profiles")}
            meta["raw_archive"] = archive
            current_meta[market] = meta
            save_json(out_dir / "raw" / "company_master" / f"{market}_官方公司基本資料_metadata.json", meta)
            log(f"[MASTER PASS] {market} profiles={len(result['profiles']):,} SHA256={result['raw_sha256']}")
        except Exception as exc:
            current_master_complete = False
            current_meta[market] = {"market": market, "url": url, "status": "FAIL", "error": f"{type(exc).__name__}: {exc}"}
            log(f"[MASTER WARN] {market} {current_meta[market]['error']}")

    # 主要商品分類來源：TWSE 官方 ISIN 綜合名錄。
    isin_result = None
    isin_error = None
    try:
        isin_result = fetch_isin_master(session, out_dir)
        log(
            f"[ISIN PASS] rows={len(isin_result['records']):,} "
            f"TWSE={isin_result['meta']['market_counts']['TWSE']:,} "
            f"TPEX={isin_result['meta']['market_counts']['TPEX']:,} "
            f"encoding={isin_result['encoding']} SHA256={isin_result['meta']['raw_sha256']}"
        )
    except Exception as exc:
        isin_error = f"{type(exc).__name__}: {exc}"
        log(f"[ISIN FAIL] {isin_error}")
        save_json(out_dir / "raw" / "official_isin" / "ISIN_擷取失敗.json", {
            "url": ISIN_URL,
            "retrieved_at": datetime.now(TZ).isoformat(),
            "error": isin_error,
        })

    # 若 ISIN 名錄不完整或不可用，不把任何公司主檔匹配結果升級為已確認普通股。
    if isin_result is None:
        isin_records = []
        isin_source_complete = False
    else:
        isin_records = isin_result["records"]
        isin_source_complete = True

    # 官方公司主檔與歷史名單 CSV 列表交叉核對。
    historical_summary = []
    for member in v133["members"]:
        market = member["market"]
        old_codes = {x["instrument_code"] for x in member["profiles"]}
        current_codes = {x["instrument_code"] for x in current_profiles.get(market, [])} if market in current_profiles else set()
        historical_summary.append({
            "member": member["member"],
            "market": market,
            "rows_after_header": len(member["rows"]),
            "header_row_1based": member["header_row_1based"],
            "recognized_codes": len(old_codes),
            "member_sha256": member["member_sha256"],
            "old_codes_missing_from_current_count": len(old_codes - current_codes) if current_codes else None,
            "current_codes_not_in_old_count": len(current_codes - old_codes) if current_codes else None,
            "historical_codes_in_current_sample": sorted(old_codes & current_codes)[:30],
        })
    save_json(out_dir / "reports" / "v1.3.3歷史公司主檔交叉核對.json", historical_summary)

    if isin_source_complete:
        classified = classify_quotes(quotes, isin_records, current_profiles, v133["members"])
    else:
        # 不完整 ISIN 來源時，所有列進 unknown，不以公司主檔填補法律分類。
        fallback = []
        for q in quotes:
            row = {k: ("" if v is None else str(v)) for k, v in q.items()}
            row.update({
                "official_isin_match": False,
                "isin_match_count": 0,
                "security_category": "ISIN來源失敗／證券種類待確認",
                "classification_code": "ISIN_SOURCE_UNAVAILABLE",
                "classification_basis": isin_error or "ISIN 名錄來源不完整",
                "current_company_master_match": q["instrument_code"] in {x["instrument_code"] for x in current_profiles.get(q["market"], [])},
                "historical_v133_company_master_match": False,
                "ordinary_stock_confirmed": False,
                "included_in_confirmed_ordinary_stock_file": False,
            })
            fallback.append(row)
        classified = {
            "all": fallback, "confirmed_ordinary": [],
            "excluded": {"ETF": [], "ETN": [], "TDR": [], "其他非普通股": []},
            "unknown": fallback, "conflicts": [],
            "duplicate_quote_rows": [], "duplicate_isin_rows": [],
        }

    # ISIN 名錄對現有官方公司主檔的市場別覆蓋率，作完整性輔助檢核。
    isin_maps = build_isin_maps(isin_records)
    isin_overlap = {}
    for market in ("TWSE", "TPEX"):
        profile_codes = {x["instrument_code"] for x in current_profiles.get(market, [])}
        matched = {code for (m, code) in isin_maps if m == market and code in profile_codes}
        ratio = len(matched) / len(profile_codes) if profile_codes else 0.0
        isin_overlap[market] = {
            "current_company_master_codes": len(profile_codes),
            "isin_matched_codes": len(matched),
            "overlap_ratio": round(ratio, 6),
            "minimum_required": MIN_ISIN_MASTER_OVERLAP[market],
            "passed": ratio >= MIN_ISIN_MASTER_OVERLAP[market],
        }
    isin_coverage_pass = bool(isin_source_complete and all(x["passed"] for x in isin_overlap.values()))

    # 如果 ISIN 名錄雖通過最低筆數，但對官方公司主檔覆蓋不足，將普通股確認清單降級隔離。
    if not isin_coverage_pass:
        for row in classified["confirmed_ordinary"]:
            row["security_category"] = "ISIN覆蓋不足／普通股暫不確認"
            row["classification_code"] = "ISIN_MASTER_COVERAGE_INSUFFICIENT"
            row["classification_basis"] += "；整體 ISIN 名錄對公司主檔覆蓋率不足，暫不納入正式普通股清單"
            row["ordinary_stock_confirmed"] = False
            row["included_in_confirmed_ordinary_stock_file"] = False
            classified["unknown"].append(row)
        classified["confirmed_ordinary"] = []

    # 匯出完整逐筆與拆分 CSV。
    write_csv(out_dir / "normalized" / "已確認普通股行情.csv", classified["confirmed_ordinary"])
    write_csv(out_dir / "reports" / "逐筆官方ISIN對照與證券分類.csv", classified["all"])
    write_csv(out_dir / "reports" / "ETF行情.csv", classified["excluded"]["ETF"])
    write_csv(out_dir / "reports" / "ETN行情.csv", classified["excluded"]["ETN"])
    write_csv(out_dir / "reports" / "TDR行情.csv", classified["excluded"]["TDR"])
    write_csv(out_dir / "reports" / "其他非普通股行情.csv", classified["excluded"]["其他非普通股"])
    write_csv(out_dir / "reports" / "ISIN未匹配與待確認.csv", classified["unknown"])
    write_csv(out_dir / "reports" / "ISIN來源衝突與隔離列.csv", classified["conflicts"])
    write_csv(out_dir / "reports" / "行情同市場代號重複列.csv", classified["duplicate_quote_rows"])
    write_csv(out_dir / "reports" / "ISIN名錄同市場代號重複列.csv", classified["duplicate_isin_rows"])

    # 所有 ISIN 官方列，以便日後可重建分類、檢查同代號重複與類別歧異。
    write_csv(out_dir / "reports" / "官方ISIN名錄完整解析.csv", isin_records)

    # 行情品質不修補，只統計缺漏及零成交量。
    def is_empty_value(v):
        return str(v or "").strip() in ("", "-", "--", "----", "N/A", "None", "null")
    volume_keys = [k for k in quotes[0].keys() if "成交股數" in k or "成交量" in k]
    ohlc_names = {
        "open": [k for k in quotes[0].keys() if "開盤價" in k or "開盤 " in k or "開盤" == k],
        "high": [k for k in quotes[0].keys() if "最高價" in k or "最高 " in k or "最高" == k],
        "low": [k for k in quotes[0].keys() if "最低價" in k or "最低 " in k or "最低" == k],
        "close": [k for k in quotes[0].keys() if "收盤價" in k or "收盤 " in k or "收盤" == k],
    }
    quality = {
        "quote_rows": len(quotes),
        "zero_volume_rows": sum(1 for q in quotes if any(str(q.get(k, "")).strip().replace(",", "") in ("0", "0.0", "0.00") for k in volume_keys)),
        "missing_volume_rows": sum(1 for q in quotes if not any(not is_empty_value(q.get(k)) for k in volume_keys)),
        "ohlc_missing_rows": {
            field: sum(1 for q in quotes if not any(not is_empty_value(q.get(k)) for k in keys))
            for field, keys in ohlc_names.items()
        },
        "handling": "不刪除零成交量列、不補值、不修正 OHLC；原始值完整保留。",
    }
    save_json(out_dir / "reports" / "行情品質檢查.json", quality)

    category_counts = {}
    market_counts = {}
    cfi_counts = {}
    for row in classified["all"]:
        cat = row.get("security_category", "")
        category_counts[cat] = category_counts.get(cat, 0) + 1
        market = row.get("market", "")
        market_counts.setdefault(market, {})
        market_counts[market][cat] = market_counts[market].get(cat, 0) + 1
        cfi = row.get("isin_cfi_code", "") or "(未命中)"
        cfi_counts[cfi] = cfi_counts.get(cfi, 0) + 1

    summary_status = (
        "PASS_CONFIRMED_ORDINARY_STOCKS"
        if isin_source_complete and isin_coverage_pass and classified["confirmed_ordinary"]
        else "PARTIAL_ISIN_COVERAGE_OR_CLASSIFICATION"
    )
    summary = {
        "version": VERSION,
        "target_trade_date": TARGET_DATE.isoformat(),
        "run_time_taipei": RUN_TIME.isoformat(),
        "status": summary_status,
        "input": {
            "v138_directory": inputs["directory"],
            "v138_zip_sha256": inputs["zip_sha256"],
            "v138_csv_sha256": inputs["csv_sha256"],
            "quote_rows": len(quotes),
            "quote_dates": sorted({q["trade_date"] for q in quotes}),
            "v133_zip_sha256": v133["sha256"],
            "v133_zip_members": v133["member_count"],
            "v133_company_master_members_parsed": len(v133["members"]),
            "v133_parse_errors": v133["errors"],
        },
        "current_company_masters": current_meta,
        "isin_source": (isin_result["meta"] if isin_result else {"url": ISIN_URL, "error": isin_error}),
        "isin_coverage": isin_overlap,
        "isin_coverage_pass": isin_coverage_pass,
        "counts": {
            "all_quote_rows": len(classified["all"]),
            "confirmed_ordinary_stock_rows": len(classified["confirmed_ordinary"]),
            "etf_rows": len(classified["excluded"]["ETF"]),
            "etn_rows": len(classified["excluded"]["ETN"]),
            "tdr_rows": len(classified["excluded"]["TDR"]),
            "other_non_ordinary_rows": len(classified["excluded"]["其他非普通股"]),
            "unknown_or_conflict_rows": len(classified["unknown"]),
            "isin_conflict_or_isolated_rows": len(classified["conflicts"]),
            "duplicate_quote_rows": len(classified["duplicate_quote_rows"]),
            "duplicate_isin_rows": len(classified["duplicate_isin_rows"]),
            "category_counts": category_counts,
            "market_category_counts": market_counts,
            "cfi_counts": cfi_counts,
        },
        "classification_policy": {
            "primary_source": "TWSE official ISIN comprehensive list",
            "official_fields": ["Security Code", "Market", "Type of security", "CFICode"],
            "ordinary_stock_rule": "CFI 前兩碼 ES（Equities / Common or Ordinary Shares），或 ISIN Type of security 明確標示 Common Stock / Ordinary Share；不得以公司主檔命中、代號或名稱單獨確認普通股。",
            "company_master_role": "僅作交叉參考，不單獨作普通股法律分類依據。",
            "unmatched_rule": "沒有確切 ISIN 記錄、欄位缺漏、同代號記錄衝突或整體名錄覆蓋不足者，不納入已確認普通股行情。",
        },
        "quality": quality,
        "non_mutation": {
            "v138_original_modified": False,
            "v133_original_zip_modified": False,
            "formal_database_modified": False,
            "main_branch_modified": False,
            "training_or_backtest_performed": False,
        },
    }
    save_json(out_dir / "reports" / "官方ISIN普通股分類摘要.json", summary)

    readme = f"""# {VERSION}｜官方 ISIN 商品種類核驗與普通股行情輸出

目標行情日期：{TARGET_DATE.isoformat()}
執行時間：{RUN_TIME.isoformat()}
狀態：{summary_status}

## 輸入來源
- v1.3.8 雙市場行情 ZIP：SHA256 `{inputs['zip_sha256']}`
- v1.3.3 官方歷史主檔 ZIP：SHA256 `{v133['sha256']}`
- TWSE 官方 ISIN 綜合證券名錄：`{ISIN_URL}`
- TWSE 官方上市公司基本資料：`{TWSE_COMPANY_URL}`
- TPEx 官方上櫃公司基本資料：`{TPEX_COMPANY_URL}`

## 分類結果
- 原始行情列：{len(quotes):,}
- 已確認普通股：{len(classified['confirmed_ordinary']):,}
- ETF：{len(classified['excluded']['ETF']):,}
- ETN：{len(classified['excluded']['ETN']):,}
- TDR：{len(classified['excluded']['TDR']):,}
- 其他非普通股：{len(classified['excluded']['其他非普通股']):,}
- 未匹配／待確認：{len(classified['unknown']):,}
- ISIN 衝突或隔離列：{len(classified['conflicts']):,}
- 行情重複代號列：{len(classified['duplicate_quote_rows']):,}
- ISIN 名錄重複代號列：{len(classified['duplicate_isin_rows']):,}

## 分類原則
只以官方 ISIN 名錄的 Market、Type of security、CFICode 分類。CFI 前兩碼 ES 代表普通／一般股；其他商品類別依 Type of security 明確排除。公司主檔配對僅供交叉參考，不單獨作普通股證明。資料來源不足或衝突時，保留待確認，不猜測。

## 資料安全
原始 v1.3.8 / v1.3.3 唯讀；不修改正式資料庫、不重訓、不回測、不選股、不修改 main。零成交量及 OHLC 缺漏均保留原值，不自行補值。
"""
    (out_dir / "README.md").write_text(readme, encoding="utf-8")

    # 保存目前 Colab cell 原始碼，供後續稽核。
    try:
        ip = get_ipython()
        hist = ip.history_manager.input_hist_raw
        if hist and isinstance(hist[-1], str) and len(hist[-1]) > 10000:
            (out_dir / "程式碼" / "SSO_v141_TPEX欄位修正與普通股行情輸出.py").write_text(hist[-1], encoding="utf-8")
        else:
            (out_dir / "程式碼" / "程式碼保存狀態.txt").write_text("Notebook history 未取得完整 cell 原始碼；以本次交付完整程式為準。\n", encoding="utf-8")
    except Exception as exc:
        (out_dir / "程式碼" / "程式碼保存狀態.txt").write_text(f"Notebook history 擷取失敗：{type(exc).__name__}: {exc}\n", encoding="utf-8")

    # ZIP 封存。
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
        for p in sorted(out_dir.rglob("*")):
            if p.is_file() and p.name != "GitHub與main核對結果.json":
                zf.write(p, p.relative_to(CONTINUATION_ROOT))
    with zipfile.ZipFile(zip_path, "r") as zf:
        bad = zf.testzip()
        if bad:
            raise RuntimeError(f"ZIP CRC 錯誤：{bad}")
    zip_sha = sha256_file(zip_path)
    log(f"[ZIP PASS] {zip_path} bytes={zip_path.stat().st_size:,} SHA256={zip_sha}")

    # GitHub audit branch 上傳。
    github = {"attempted": False, "success": False, "error": github_precheck_error}
    main_after = None
    main_unchanged = None
    remote_root = None
    if token and github_ready:
        try:
            github["attempted"] = True
            branch, remote_root, uploaded = upload_package(token, main_before, out_dir, zip_path)
            github.update({"success": True, "branch": branch, "remote_root": remote_root, "uploaded_files": uploaded})
            log(f"[GITHUB PASS] branch={branch} files={len(uploaded)} readback=PASS")
        except Exception as exc:
            github["error"] = f"{type(exc).__name__}: {exc}"
            log(f"[GITHUB FAIL] {github['error']}")

    if token:
        try:
            main_after = get_main_sha(token)
            main_unchanged = (main_before == main_after) if main_before else None
            log(f"[GITHUB] main after package={main_after} unchanged={main_unchanged}")
        except Exception as exc:
            github["main_after_error"] = f"{type(exc).__name__}: {exc}"

    final = {
        "version": VERSION,
        "status": summary_status,
        "output_dir": str(out_dir),
        "zip_path": str(zip_path),
        "zip_sha256": zip_sha,
        "github": github,
        "main_sha_before": main_before,
        "main_sha_after_package": main_after,
        "main_unchanged": main_unchanged,
        "original_sources_modified": False,
        "formal_database_modified": False,
    }
    final_path = out_dir / "GitHub與main核對結果.json"
    save_json(final_path, final)

    # 上傳最終核對紀錄，再次回讀及確認 main。
    if github.get("success") and remote_root:
        try:
            main_before_final = get_main_sha(token)
            final["main_sha_before_final_record"] = main_before_final
            final["main_unchanged_before_final_record"] = (main_before_final == main_before) if main_before else None
            save_json(final_path, final)
            remote_path = f"{remote_root}/GitHub與main核對結果.json"
            final_upload = upload_one(token, github["branch"], remote_path, final_path.read_bytes())
            final["final_record_upload"] = final_upload
            main_after_final = get_main_sha(token)
            final["main_sha_after_final_record"] = main_after_final
            final["main_unchanged_final"] = (main_after_final == main_before) if main_before else None
            save_json(final_path, final)
            log(f"[GITHUB PASS] final record readback=PASS main unchanged final={final['main_unchanged_final']}")
        except Exception as exc:
            final["final_record_error"] = f"{type(exc).__name__}: {exc}"
            save_json(final_path, final)
            log(f"[GITHUB WARN] final record upload/check failed: {final['final_record_error']}")

    log("=" * 100)
    log(f"[FINAL] status={summary_status}")
    log(f"[FINAL] quote rows={len(quotes):,}")
    log(f"[FINAL] ISIN source complete={isin_source_complete} coverage_pass={isin_coverage_pass}")
    log(f"[FINAL] confirmed ordinary stocks={len(classified['confirmed_ordinary']):,}")
    log(f"[FINAL] ETF={len(classified['excluded']['ETF']):,} ETN={len(classified['excluded']['ETN']):,} TDR={len(classified['excluded']['TDR']):,} other_non_ordinary={len(classified['excluded']['其他非普通股']):,}")
    log(f"[FINAL] unknown_or_conflict={len(classified['unknown']):,} isolated={len(classified['conflicts']):,} quote_duplicates={len(classified['duplicate_quote_rows']):,} isin_duplicates={len(classified['duplicate_isin_rows']):,}")
    log(f"[FINAL] OUTPUT={out_dir}")
    log(f"[FINAL] ZIP={zip_path}")
    log(f"[FINAL] ZIP SHA256={zip_sha}")
    log(f"[FINAL] GitHub upload={github.get('success')} main unchanged={main_unchanged}")
    log("[FINAL] 原始資料、既有版本及正式資料庫未修改。")
    log("=" * 100)


# =============================================================================
# 10. 離線自我測試（實際執行前先測核心分類邏輯）
# =============================================================================
def self_test():
    fixtures = [
        {"security_type": "ETF", "cfi_code": "CEOJEU", "security_name": "TEST ETF"},
        {"security_type": "ETN", "cfi_code": "", "security_name": "TEST ETN"},
        {"security_type": "Depositary Receipt", "cfi_code": "ESVUFR", "security_name": "TEST DR"},
        {"security_type": "Stock", "cfi_code": "EDXXXX", "security_name": "TEST CFI DR"},
        {"security_type": "Stock", "cfi_code": "ESVUFR", "security_name": "TEST COMMON"},
        {"security_type": "Stock", "cfi_code": "", "security_name": "TEST UNKNOWN CLASS"},
        {"security_type": "Bond", "cfi_code": "DBFTFR", "security_name": "TEST BOND"},
    ]
    got = [classify_security_type(x)[0] for x in fixtures]
    expected = ["ETF", "ETN", "TDR", "TDR", "普通股", "其他非普通股／需再確認", "其他非普通股"]
    if got != expected:
        raise AssertionError(f"分類自我測試失敗：{got} != {expected}")
    sample_csv = "資料日期,20261109\n\n公司代號,公司名稱,公司簡稱\n1101,台泥,台泥\n2330,台積電,台積電\n".encode("utf-8-sig")
    parsed = parse_csv_with_header_scan(sample_csv)
    if parsed["recognized_codes"] != 2:
        raise AssertionError("v1.3.3 前置標題 CSV 表頭掃描自我測試失敗")

    # v1.4.0 實際失敗點：TPEx OpenAPI 使用英文欄名，必須辨識複數 SecuritiesCompanyCode。
    tpex_fixture = {
        "SecuritiesCompanyCode": "6488",
        "CompanyName": "環球晶圓股份有限公司",
        "CompanyAbbreviation": "環球晶",
    }
    tpex_code = normalize_code(first_value(tpex_fixture, CODE_ALIASES))
    tpex_short = first_value(tpex_fixture, SHORT_NAME_ALIASES)
    tpex_full = first_value(tpex_fixture, FULL_NAME_ALIASES)
    if (tpex_code, tpex_short, tpex_full) != ("6488", "環球晶", "環球晶圓股份有限公司"):
        raise AssertionError(
            f"TPEx 英文欄位對應自我測試失敗：{tpex_code!r}, {tpex_short!r}, {tpex_full!r}"
        )
    log("[SELF-TEST PASS] ISIN分類、歷史CSV表頭掃描及TPEx英文主檔欄位")


try:
    self_test()
    main()
except Exception as exc:
    log(f"[FATAL] {type(exc).__name__}: {exc}")
    traceback.print_exc()
    raise