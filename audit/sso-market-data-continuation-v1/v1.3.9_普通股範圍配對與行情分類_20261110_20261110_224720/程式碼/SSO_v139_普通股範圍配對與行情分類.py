# -*- coding: utf-8 -*-
"""
SSO_Market_Data_Continuation_TWSE_TPEX v1.3.9
2026-11-10 官方公司主檔配對、商品分類及普通股候選行情輸出

單次執行內容：
1. 唯讀 v1.3.8 的 2026-11-10 雙市場官方行情與 ZIP。
2. 下載並 SHA256 驗證 v1.3.3 官方普通股範圍來源 ZIP。
3. 讀取 v1.3.3 ZIP 內所有「公司基本資料」CSV 作歷史主檔交叉參考。
4. 讀取 TWSE、TPEx 官方當前公司基本資料 OpenAPI。
5. 嘗試讀取 TWSE/TPEx 官方 ETF/ETN/TDR 商品頁或 OpenAPI 目錄；只有官方名錄可明確配對時才分類為商品。
6. 逐筆配對 2,409 筆行情，輸出公司主檔匹配候選、明確商品分類、未匹配待確認、重複代號與品質報告。
7. ZIP 封存並上傳 GitHub 獨立 audit branch，回讀 SHA256 並核對 main 前後 SHA。

安全：Drive 原始資料唯讀；不更新正式資料庫；不重訓、不回測、不選股；不修改 main。

Colab：將本完整程式放在單一 cell 執行；可忽略 notebook 的 -f kernel.json 參數。
"""
from __future__ import annotations

import os
import re
import io
import csv
import json
import time
import base64
import gzip
import hashlib
import zipfile
import traceback
import html
from html.parser import HTMLParser
from pathlib import Path
from datetime import datetime, date
from zoneinfo import ZoneInfo
from urllib.parse import quote

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# -----------------------------------------------------------------------------
# 01. 固定來源與設定
# -----------------------------------------------------------------------------
VERSION = "v1.3.9"
TARGET_DATE = date(2026, 11, 10)
TZ = ZoneInfo("Asia/Taipei")
RUN_TIME = datetime.now(TZ)
STAMP = RUN_TIME.strftime("%Y%m%d_%H%M%S")

PROJECT_ROOT = Path("/content/drive/MyDrive/看股最速適")
CONTINUATION_ROOT = PROJECT_ROOT / "SSO_Market_Data_Continuation_TWSE_TPEX_v1"
V138_DIRNAME = "v1.3.8_雙市場官方指定日期行情補充_20261110_20261110_223151_PASS"
V138_ZIPNAME = V138_DIRNAME + ".zip"

REPO = "CHLINX/sso"
EXPECTED_MAIN_SHA = "aaabc2433fb5df2bb68af06461178305d6bd30d1"

V138_BRANCH = "audit/sso-market-data-continuation-v1-v138-daily-20261110-20261110_223151"
V138_NORMALIZED_REL = "normalized/雙市場官方行情.csv"
V138_ZIP_SHA256 = "97fb3ab0bc6bc6c508f800f68d14c2bd47baa5d963fb2452a40eabda3c62c1c8"

V133_BRANCH = "audit/sso-market-data-continuation-v1-v133-upload-20261109_125725"
V133_ZIP_REL = (
    "audit/sso-market-data-continuation-v1/"
    "v1.3.3_官方普通股範圍與逐日行情交叉驗證_20261109_125127/"
    "v1.3.3_官方普通股範圍與逐日行情交叉驗證_20261109_125127.zip"
)
V133_ZIP_SHA256 = "48fbd3a78edcde0efbb4b39f7dc24197d55047fbac00b3b7f9a41f586d32480d"
V133_RAW_URL = (
    "https://raw.githubusercontent.com/CHLINX/sso/"
    + V133_BRANCH + "/" + quote(V133_ZIP_REL, safe="/")
)

TWSE_COMPANY_URL = "https://openapi.twse.com.tw/v1/opendata/t187ap03_L"
TPEX_COMPANY_URL = "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap03_O"
TWSE_ETF_PAGE = "https://www.twse.com.tw/zh/products/securities/etf/products/list.html"
TPEX_ETF_PAGE = "https://info.tpex.org.tw/ETF/zh/filter.html"
TWSE_ETN_PAGE = "https://www.twse.com.tw/zh/products/securities/etn/products/list.html"
TWSE_TDR_PAGE = "https://www.twse.com.tw/zh/products/securities/tdr.html"
TPEX_SWAGGER_URL = "https://www.tpex.org.tw/openapi/swagger.json"

MIN_MASTER_ROWS = {"TWSE": 500, "TPEX": 300}
TIMEOUT = (20, 120)

# -----------------------------------------------------------------------------
# 02. 通用工具
# -----------------------------------------------------------------------------
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
    path.write_text(
        json.dumps(obj, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )


def safe_name(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", s)


def mount_drive() -> None:
    if not Path("/content/drive/MyDrive").exists():
        from google.colab import drive
        drive.mount("/content/drive")
    if not CONTINUATION_ROOT.exists():
        raise FileNotFoundError(f"找不到行情續補根目錄：{CONTINUATION_ROOT}")


def get_token():
    token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
    if token:
        return token
    try:
        from google.colab import userdata
        return userdata.get("GITHUB_TOKEN")
    except Exception:
        return None


def make_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=4, connect=4, read=4, status=4, backoff_factor=1.2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"], raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=8, pool_maxsize=8)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (compatible; SSO-Market-Data-Continuation/1.3.9)",
        "Accept": "application/json,text/csv,text/html,*/*",
        "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
        "Cache-Control": "no-cache",
    })
    return s


def norm_key(x) -> str:
    return re.sub(r"[\s_\-（）()：:./]+", "", str(x or "")).lower()


def normalize_code(x) -> str:
    if x is None:
        return ""
    s = str(x).strip().replace("\ufeff", "")
    if s.endswith(".0") and s[:-2].isdigit():
        s = s[:-2]
    return s.upper()


def first_value(row: dict, aliases) -> str:
    lookup = {norm_key(k): v for k, v in row.items()}
    for a in aliases:
        k = norm_key(a)
        if k in lookup and lookup[k] is not None:
            return str(lookup[k]).strip()
    return ""


def records_from_payload(payload):
    if isinstance(payload, list) and all(isinstance(x, dict) for x in payload):
        return payload
    if isinstance(payload, dict):
        for key in ("data", "Data", "result", "results", "records", "aaData"):
            v = payload.get(key)
            if isinstance(v, list) and all(isinstance(x, dict) for x in v):
                return v
    raise ValueError("回應不是可辨識的 JSON 物件陣列")


def decode_csv_bytes(raw: bytes):
    last = None
    for enc in ("utf-8-sig", "big5", "cp950", "utf-8"):
        try:
            text = raw.decode(enc)
            sample = text[:10000]
            try:
                dialect = csv.Sniffer().sniff(sample, delimiters=",\t;|")
                delim = dialect.delimiter
            except Exception:
                delim = ","
            rows = list(csv.DictReader(io.StringIO(text), delimiter=delim))
            if rows and any(str(k).strip() for k in (rows[0].keys() or [])):
                return rows, enc, delim
            last = f"{enc}: 沒有可辨識資料列"
        except Exception as exc:
            last = f"{enc}: {exc}"
    raise ValueError(f"CSV 解碼失敗：{last}")


def profile_records(market: str, records: list, source: str):
    code_aliases = ["公司代號", "證券代號", "股票代號", "SecuritiesCompanyCode", "Code"]
    short_aliases = ["公司簡稱", "證券簡稱", "股票簡稱", "CompanyAbbreviation", "Name"]
    name_aliases = ["公司名稱", "CompanyName", "證券名稱"]
    out = []
    for r in records:
        code = normalize_code(first_value(r, code_aliases))
        if not code:
            continue
        out.append({
            "market": market,
            "instrument_code": code,
            "company_short_name": first_value(r, short_aliases),
            "company_name": first_value(r, name_aliases),
            "source": source,
            "raw": r,
        })
    if not out:
        raise ValueError(f"{market} 公司主檔無法辨識公司代號欄位")
    return out

# -----------------------------------------------------------------------------
# 03. HTML table parser：只從官方商品頁辨識明列商品，不猜測未匹配類型
# -----------------------------------------------------------------------------
class TableParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.tables = []
        self.in_table = False
        self.in_row = False
        self.in_cell = False
        self.row = []
        self.cell = []
        self.table = []

    def handle_starttag(self, tag, attrs):
        tag = tag.lower()
        if tag == "table":
            self.in_table = True
            self.table = []
        elif self.in_table and tag == "tr":
            self.in_row = True
            self.row = []
        elif self.in_table and self.in_row and tag in ("td", "th"):
            self.in_cell = True
            self.cell = []

    def handle_data(self, data):
        if self.in_table and self.in_row and self.in_cell:
            self.cell.append(data)

    def handle_endtag(self, tag):
        tag = tag.lower()
        if tag in ("td", "th") and self.in_cell:
            self.row.append(" ".join(" ".join(self.cell).split()))
            self.in_cell = False
        elif tag == "tr" and self.in_row:
            if self.row:
                self.table.append(self.row)
            self.in_row = False
        elif tag == "table" and self.in_table:
            if self.table:
                self.tables.append(self.table)
            self.in_table = False


def scrape_product_page(session, market, product_type, url):
    result = {"market": market, "type": product_type, "url": url, "status": "UNAVAILABLE", "codes": [], "error": None}
    try:
        r = session.get(url, timeout=TIMEOUT)
        result["http_status"] = r.status_code
        if r.status_code >= 400:
            result["error"] = f"HTTP {r.status_code}"
            return result
        parser = TableParser()
        parser.feed(r.text)
        found = {}
        for table in parser.tables:
            for row in table:
                # 官方商品頁偶爾把新臺幣/外幣代號放在同一格，
                # 因此搜尋整個欄位中的完整證券代號，而不是要求儲存格完全相等。
                for i, cell in enumerate(row):
                    cell_text = cell.strip()
                    # 避免將上市日期中的 2003、0630 等片段誤認為證券代號。
                    if re.search(r"\d{4}[./-]\d{1,2}[./-]\d{1,2}", cell_text):
                        continue
                    codes = re.findall(r"(?<![A-Za-z0-9])\d{4,6}[A-Za-z]?(?![A-Za-z0-9])", cell_text)
                    name = row[i + 1].strip() if i + 1 < len(row) else ""
                    for raw_code in codes:
                        code = normalize_code(raw_code)
                        if code:
                            found[code] = name
        result["codes"] = [{"code": k, "name": v} for k, v in sorted(found.items())]
        result["status"] = "PASS" if found else "FETCHED_NO_PARSEABLE_ROWS"
        if not found:
            result["error"] = "網頁取得成功，但未解析到可核對的商品代號表格；不以空集合宣告沒有該商品。"
    except Exception as exc:
        result["error"] = f"{type(exc).__name__}: {exc}"
    return result


def scan_tpex_swagger_for_products(session):
    """檢查官方 OpenAPI 是否有免參數 ETF/ETN/TDR 清單；僅存疑似清單端點與成功解析資料。"""
    audit = {"url": TPEX_SWAGGER_URL, "status": "UNAVAILABLE", "discovered": [], "error": None}
    catalogs = []
    try:
        r = session.get(TPEX_SWAGGER_URL, timeout=TIMEOUT)
        audit["http_status"] = r.status_code
        r.raise_for_status()
        spec = r.json()
        paths = spec.get("paths", {}) if isinstance(spec, dict) else {}
        for path, methods in paths.items():
            p = str(path).lower()
            if not any(t in p for t in ("etf", "etn", "tdr")):
                continue
            getop = methods.get("get") if isinstance(methods, dict) else None
            if not isinstance(getop, dict):
                continue
            required = []
            for param in getop.get("parameters", []) or []:
                if param.get("required"):
                    required.append(param.get("name"))
            audit["discovered"].append({"path": path, "required_params": required})
            if required:
                continue
            endpoint = "https://www.tpex.org.tw/openapi/v1/" + str(path).strip("/").split("/")[-1]
            try:
                rr = session.get(endpoint, timeout=TIMEOUT)
                rr.raise_for_status()
                payload = rr.json()
                rows = records_from_payload(payload)
                # 只有至少找到代號欄位的列表才認成官方商品名錄。
                codes = []
                for row in rows:
                    code = normalize_code(first_value(row, ["證券代號", "代號", "SecuritiesCompanyCode", "Code", "stock_code"]))
                    if code:
                        codes.append({"code": code, "name": first_value(row, ["證券名稱", "名稱", "ETF名稱", "ETN名稱", "Name"])})
                if codes:
                    label = "ETF" if "etf" in p else ("ETN" if "etn" in p else "TDR")
                    catalogs.append({"market": "TPEX", "type": label, "url": endpoint, "status": "PASS", "codes": codes})
            except Exception as exc:
                audit.setdefault("endpoint_errors", []).append({"url": endpoint, "error": f"{type(exc).__name__}: {exc}"})
        audit["status"] = "PASS" if audit["discovered"] else "NO_PRODUCT_ENDPOINT_DISCOVERED"
    except Exception as exc:
        audit["error"] = f"{type(exc).__name__}: {exc}"
    return audit, catalogs

# -----------------------------------------------------------------------------
# 04. 官方主檔與歷史範圍下載
# -----------------------------------------------------------------------------
def fetch_json_records(session, market, url):
    r = session.get(url, timeout=TIMEOUT)
    r.raise_for_status()
    payload = r.json()
    records = records_from_payload(payload)
    if len(records) < MIN_MASTER_ROWS[market]:
        raise ValueError(f"{market} 公司主檔僅 {len(records)} 筆，低於門檻 {MIN_MASTER_ROWS[market]}")
    normalized = profile_records(market, records, url)
    return {
        "market": market,
        "url": url,
        "http_status": r.status_code,
        "retrieved_at": datetime.now(TZ).isoformat(),
        "raw_bytes": r.content,
        "raw_sha256": sha256_bytes(r.content),
        "record_count": len(records),
        "recognized_code_count": len(normalized),
        "profiles": normalized,
        "payload": payload,
    }


def download_v133_zip(session, output_dir: Path):
    local = output_dir / "sources" / "v1.3.3_官方普通股範圍來源.zip"
    local.parent.mkdir(parents=True, exist_ok=True)
    r = session.get(V133_RAW_URL, timeout=TIMEOUT)
    r.raise_for_status()
    data = r.content
    actual = sha256_bytes(data)
    if actual != V133_ZIP_SHA256:
        raise ValueError(f"v1.3.3 ZIP SHA256 不符：{actual} != {V133_ZIP_SHA256}")
    local.write_bytes(data)
    with zipfile.ZipFile(io.BytesIO(data), "r") as zf:
        bad = zf.testzip()
        if bad:
            raise ValueError(f"v1.3.3 ZIP CRC 錯誤：{bad}")
        inventory = zf.namelist()
        master_members = [
            n for n in inventory
            if "公司基本資料" in Path(n).name and n.lower().endswith(".csv")
        ]
        parsed = []
        errors = []
        for member in master_members:
            try:
                rows, encoding, delimiter = decode_csv_bytes(zf.read(member))
                # 市場由檔名判別；若檔名不足以判斷，只標成 UNKNOWN，不亂分市場。
                low = member.lower()
                if "上櫃" in member or "tpex" in low or "otc" in low:
                    market = "TPEX"
                elif "上市" in member or "twse" in low or "listed" in low:
                    market = "TWSE"
                else:
                    market = "UNKNOWN"
                try:
                    prof = profile_records(market, rows, f"v1.3.3 ZIP::{member}")
                except Exception:
                    prof = []
                    errors.append({"member": member, "error": "欄位未能轉換成公司代號主檔"})
                parsed.append({
                    "member": member,
                    "market": market,
                    "rows": rows,
                    "encoding": encoding,
                    "delimiter": delimiter,
                    "profile_count": len(prof),
                    "profiles": prof,
                    "member_sha256": sha256_bytes(zf.read(member)),
                })
            except Exception as exc:
                errors.append({"member": member, "error": f"{type(exc).__name__}: {exc}"})
    return {
        "path": str(local),
        "bytes": len(data),
        "sha256": actual,
        "member_count": len(inventory),
        "inventory": inventory,
        "master_members": parsed,
        "parse_errors": errors,
    }


def parse_quotes(path: Path):
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))
    required = {"market", "trade_date", "instrument_code", "instrument_name", "source_row_sha256"}
    if not rows or not required.issubset(rows[0].keys()):
        raise ValueError(f"行情 CSV 欄位不符或無資料：{path}")
    for r in rows:
        r["market"] = str(r.get("market", "")).strip().upper()
        r["instrument_code"] = normalize_code(r.get("instrument_code"))
        r["instrument_name"] = str(r.get("instrument_name", "")).strip()
    return rows

# -----------------------------------------------------------------------------
# 05. 配對、分類、品質檢查
# -----------------------------------------------------------------------------
def write_csv(path: Path, rows: list):
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("NO_DATA\n", encoding="utf-8-sig")
        return
    fields = []
    for row in rows:
        for k in row:
            if k not in fields:
                fields.append(k)
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


def flatten_quote_row(row):
    return {k: ("" if v is None else str(v)) for k, v in row.items()}


def company_master_csv_rows(profiles):
    out = []
    for p in profiles:
        raw = p["raw"]
        out.append({
            "market": p["market"],
            "instrument_code": p["instrument_code"],
            "company_short_name": p["company_short_name"],
            "company_name": p["company_name"],
            "source": p["source"],
            "profile_date": first_value(raw, ["出表日期", "資料日期", "更新日期"]),
            "ordinary_share_par_value": first_value(raw, ["普通股每股面額"]),
            "issued_ordinary_shares_or_tdr_underlying": first_value(raw, ["已發行普通股數或TDR原股發行股數"]),
            "raw_profile_json": json.dumps(raw, ensure_ascii=False, default=str),
        })
    return out


def run_classification(quotes, profiles_by_market, product_catalogs):
    master_maps = {
        market: {p["instrument_code"]: p for p in profiles_by_market.get(market, [])}
        for market in ("TWSE", "TPEX")
    }
    product_map = {}
    for cat in product_catalogs:
        for item in cat.get("codes", []):
            code = normalize_code(item.get("code"))
            if code:
                product_map[(cat.get("market", ""), code)] = {
                    "type": cat.get("type", "商品"),
                    "name": item.get("name", ""),
                    "source": cat.get("url", ""),
                }

    counts_by_key = {}
    for q in quotes:
        k = (q["market"], q["instrument_code"])
        counts_by_key[k] = counts_by_key.get(k, 0) + 1

    all_rows = []
    candidate_rows = []
    unmatched_rows = []
    product_rows = []
    duplicate_rows = []

    for q in quotes:
        market = q["market"]
        code = q["instrument_code"]
        key = (market, code)
        duplicate_count = counts_by_key.get(key, 0)
        profile = master_maps.get(market, {}).get(code)
        product = product_map.get(key)
        name = q.get("instrument_name", "")

        # TDR 字樣或代號僅為風險提示，除非進入官方商品名錄，否則不直接宣稱確定 TDR。
        tdr_suspect = bool(re.search(r"(?:-DR|\bDR\b|存託憑證)", name, re.I) or re.match(r"^91\d{2,4}$", code))

        if product:
            category = product["type"]
            classification = "OFFICIAL_PRODUCT_CATALOG_MATCH"
            confidence = "官方商品名錄明確配對"
            included_candidate = False
            matched_source = product["source"]
            catalog_name = product["name"]
        elif tdr_suspect:
            category = "TDR候選／待名錄確認"
            classification = "TDR_CANDIDATE_UNCONFIRMED"
            confidence = "僅依代號／名稱提示，非官方商品名錄確證"
            included_candidate = False
            matched_source = ""
            catalog_name = ""
        elif profile:
            category = "公司主檔匹配個股候選"
            classification = "OFFICIAL_COMPANY_MASTER_MATCH"
            confidence = "官方公司基本資料代號匹配；尚非逐項普通股商品代碼證明"
            included_candidate = True
            matched_source = profile["source"]
            catalog_name = profile.get("company_short_name", "") or profile.get("company_name", "")
        else:
            category = "未匹配／商品種類待確認"
            classification = "UNMATCHED_SECURITY_TYPE_UNKNOWN"
            confidence = "未命中目前公司主檔或可解析官方商品名錄；不推測 ETF／ETN／TDR"
            included_candidate = False
            matched_source = ""
            catalog_name = ""

        out = flatten_quote_row(q)
        out.update({
            "classification": classification,
            "security_category": category,
            "company_master_match": bool(profile),
            "company_master_name": (profile.get("company_short_name") or profile.get("company_name")) if profile else "",
            "official_product_catalog_match": bool(product),
            "official_product_catalog_name": catalog_name,
            "classification_basis": confidence,
            "classification_source": matched_source,
            "duplicate_market_code_count": duplicate_count,
            "duplicate_market_code_flag": duplicate_count > 1,
            "included_in_company_master_candidate_file": included_candidate,
        })
        all_rows.append(out)
        if included_candidate:
            candidate_rows.append(out)
        if classification == "UNMATCHED_SECURITY_TYPE_UNKNOWN":
            unmatched_rows.append(out)
        if product or tdr_suspect:
            product_rows.append(out)
        if duplicate_count > 1:
            duplicate_rows.append(out)

    return {
        "all": all_rows,
        "candidate": candidate_rows,
        "unmatched": unmatched_rows,
        "product_or_tdr_candidates": product_rows,
        "duplicates": duplicate_rows,
        "master_maps": master_maps,
    }

# -----------------------------------------------------------------------------
# 06. GitHub 上傳
# -----------------------------------------------------------------------------
def gh_request(method, url, token, **kwargs):
    headers = kwargs.pop("headers", {})
    headers.update({
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "SSO-Market-Data-Continuation-v1.3.9",
    })
    r = requests.request(method, url, headers=headers, timeout=120, **kwargs)
    if r.status_code >= 400:
        raise RuntimeError(f"GitHub HTTP {r.status_code}: {r.text[:1500]}")
    return r


def get_main_sha(token):
    j = gh_request("GET", f"https://api.github.com/repos/{REPO}/git/ref/heads/main", token).json()
    return j["object"]["sha"]


def upload_one(token, branch, remote_path, content):
    api_url = f"https://api.github.com/repos/{REPO}/contents/{quote(remote_path, safe='/')}"
    body = {
        "message": f"SSO v1.3.9 company universe audit: {Path(remote_path).name}",
        "content": base64.b64encode(content).decode("ascii"),
        "branch": branch,
    }
    j = gh_request("PUT", api_url, token, json=body).json()
    raw_url = (
        f"https://raw.githubusercontent.com/{REPO}/"
        f"{branch}/{quote(remote_path, safe='/')}"
    )
    rr = requests.get(raw_url, timeout=120)
    rr.raise_for_status()
    actual = sha256_bytes(rr.content)
    expected = sha256_bytes(content)
    if actual != expected:
        raise RuntimeError(f"GitHub readback SHA256 mismatch: {remote_path}")
    return {
        "path": remote_path,
        "bytes": len(content),
        "sha256": expected,
        "commit": j.get("commit", {}).get("sha"),
        "readback": "PASS",
    }


def upload_package(token, main_sha, out_dir, zip_path):
    branch = f"audit/sso-market-data-continuation-v1-v139-universe-{TARGET_DATE:%Y%m%d}-{STAMP}"
    gh_request(
        "POST",
        f"https://api.github.com/repos/{REPO}/git/refs",
        token,
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

# -----------------------------------------------------------------------------
# 07. 主流程
# -----------------------------------------------------------------------------
def main():
    log("=" * 100)
    log(f"啟動 {VERSION}｜2026-11-10 官方公司主檔配對與個股行情分類")
    log("原始資料唯讀；單次完成範圍配對、分類、報告、ZIP、GitHub audit 上傳與 main 核對。")
    mount_drive()

    v138_dir = CONTINUATION_ROOT / V138_DIRNAME
    v138_zip = CONTINUATION_ROOT / V138_ZIPNAME
    quote_path = v138_dir / V138_NORMALIZED_REL
    if not quote_path.exists():
        raise FileNotFoundError(f"找不到 v1.3.8 雙市場行情：{quote_path}")
    if not v138_zip.exists():
        raise FileNotFoundError(f"找不到 v1.3.8 ZIP：{v138_zip}")
    v138_sha = sha256_file(v138_zip)
    if v138_sha != V138_ZIP_SHA256:
        raise ValueError(f"v1.3.8 ZIP SHA256 不符：{v138_sha}")
    with zipfile.ZipFile(v138_zip, "r") as zf:
        bad = zf.testzip()
        if bad:
            raise ValueError(f"v1.3.8 ZIP CRC 錯誤：{bad}")

    token = get_token()
    main_before = None
    github_ready = False
    github_precheck_error = None
    if token:
        try:
            main_before = get_main_sha(token)
            log(f"[GITHUB] main before={main_before}")
            if main_before == EXPECTED_MAIN_SHA:
                github_ready = True
            else:
                github_precheck_error = "main SHA 與既定基準不同；為避免使用錯誤基準，本次略過 GitHub 上傳。"
        except Exception as exc:
            github_precheck_error = f"main precheck error: {type(exc).__name__}: {exc}"
    else:
        github_precheck_error = "未取得 GITHUB_TOKEN，略過 GitHub 上傳。"

    session = make_session()
    out_name = f"{VERSION}_普通股範圍配對與行情分類_{TARGET_DATE:%Y%m%d}_{STAMP}"
    out_dir = CONTINUATION_ROOT / out_name
    zip_path = CONTINUATION_ROOT / f"{out_name}.zip"
    if out_dir.exists() or zip_path.exists():
        raise FileExistsError(f"輸出路徑已存在，避免覆寫：{out_dir}")
    out_dir.mkdir(parents=True, exist_ok=False)
    (out_dir / "raw" / "company_master").mkdir(parents=True, exist_ok=True)
    (out_dir / "reports").mkdir(parents=True, exist_ok=True)
    (out_dir / "normalized").mkdir(parents=True, exist_ok=True)

    # 先複製原始證據 ZIP 到新封存資料夾，不改原檔。
    source_dir = out_dir / "sources"
    source_dir.mkdir(parents=True, exist_ok=True)
    v138_copy = source_dir / V138_ZIPNAME
    v138_copy.write_bytes(v138_zip.read_bytes())

    quotes = parse_quotes(quote_path)
    wrong_dates = sorted({q.get("trade_date", "") for q in quotes if q.get("trade_date") != TARGET_DATE.isoformat()})
    if wrong_dates:
        raise ValueError(f"v1.3.8 行情出現非目標日期：{wrong_dates}")
    log(f"[INPUT PASS] v1.3.8 rows={len(quotes):,} date={TARGET_DATE}")

    # 下載並驗證 v1.3.3 歷史官方普通股範圍 ZIP。
    v133 = download_v133_zip(session, out_dir)
    log(f"[V133 PASS] ZIP SHA256={v133['sha256']} members={v133['member_count']} company_master_members={len(v133['master_members'])}")

    # 官方當前公司主檔，雙市場分別請求，不因單邊失敗而略過另一邊。
    current_profiles = {"TWSE": [], "TPEX": []}
    profile_results = {}
    for market, url in (("TWSE", TWSE_COMPANY_URL), ("TPEX", TPEX_COMPANY_URL)):
        try:
            res = fetch_json_records(session, market, url)
            profile_results[market] = {
                k: v for k, v in res.items()
                if k not in ("raw_bytes", "payload", "profiles")
            }
            current_profiles[market] = res["profiles"]
            gz = gzip.compress(res["raw_bytes"], compresslevel=6, mtime=0)
            raw_path = out_dir / "raw" / "company_master" / f"{market}_官方公司基本資料.json.gz"
            raw_path.write_bytes(gz)
            save_json(out_dir / "raw" / "company_master" / f"{market}_官方公司基本資料_metadata.json", {
                **profile_results[market],
                "gzip_sha256": sha256_bytes(gz),
                "original_sha256": res["raw_sha256"],
                "gzip_bytes": len(gz),
            })
            log(f"[MASTER PASS] {market} profiles={len(current_profiles[market]):,} sha256={res['raw_sha256']}")
        except Exception as exc:
            profile_results[market] = {"market": market, "url": url, "status": "FAIL", "error": f"{type(exc).__name__}: {exc}"}
            log(f"[MASTER FAIL] {market} {profile_results[market]['error']}")

    current_master_pass = all(len(current_profiles[m]) >= MIN_MASTER_ROWS[m] for m in ("TWSE", "TPEX"))

    # 官方商品名錄嘗試抓取。成功且解析到代號才作明確分類；否則保留待確認。
    product_catalogs = []
    product_attempts = []
    for market, ptype, url in (
        ("TWSE", "ETF", TWSE_ETF_PAGE),
        ("TPEX", "ETF", TPEX_ETF_PAGE),
        ("TWSE", "ETN", TWSE_ETN_PAGE),
        ("TWSE", "TDR", TWSE_TDR_PAGE),
    ):
        res = scrape_product_page(session, market, ptype, url)
        product_attempts.append(res)
        if res.get("status") == "PASS":
            product_catalogs.append(res)
        log(f"[PRODUCT] {market} {ptype} status={res['status']} codes={len(res.get('codes', []))}")

    swagger_audit, swagger_catalogs = scan_tpex_swagger_for_products(session)
    product_attempts.append(swagger_audit)
    product_catalogs.extend(swagger_catalogs)
    log(f"[TPEX SWAGGER] {swagger_audit.get('status')} product_catalogs={len(swagger_catalogs)}")

    # 歷史 v1.3.3 公司主檔交叉參考，不覆蓋或取代當前官方主檔。
    old_master_summary = []
    for item in v133["master_members"]:
        old_codes = sorted({p["instrument_code"] for p in item.get("profiles", [])})
        market = item.get("market", "UNKNOWN")
        current_codes = {p["instrument_code"] for p in current_profiles.get(market, [])} if market in current_profiles else set()
        old_master_summary.append({
            "member": item["member"],
            "market": market,
            "rows": len(item.get("rows", [])),
            "recognized_codes": len(old_codes),
            "member_sha256": item["member_sha256"],
            "encoding": item["encoding"],
            "old_codes_missing_from_current": sorted(set(old_codes) - current_codes) if current_codes else [],
            "current_codes_not_in_old": sorted(current_codes - set(old_codes)) if current_codes else [],
        })

    profiles_by_market = current_profiles
    classified = run_classification(quotes, profiles_by_market, product_catalogs)

    # 產出各類資料。
    write_csv(out_dir / "normalized" / "公司主檔匹配個股候選行情.csv", classified["candidate"])
    write_csv(out_dir / "reports" / "逐筆證券分類與主檔配對.csv", classified["all"])
    write_csv(out_dir / "reports" / "未匹配商品種類待確認.csv", classified["unmatched"])
    write_csv(out_dir / "reports" / "官方商品名錄配對與TDR候選.csv", classified["product_or_tdr_candidates"])
    write_csv(out_dir / "reports" / "重複代號檢查.csv", classified["duplicates"])
    write_csv(out_dir / "reports" / "官方當前公司主檔.csv", company_master_csv_rows(current_profiles["TWSE"] + current_profiles["TPEX"]))

    # 市場別/分類統計。
    category_counts = {}
    market_counts = {}
    for r in classified["all"]:
        category_counts[r["security_category"]] = category_counts.get(r["security_category"], 0) + 1
        market_counts.setdefault(r["market"], {})
        market_counts[r["market"]][r["security_category"]] = market_counts[r["market"]].get(r["security_category"], 0) + 1

    # 每市場重複代號組數
    duplicate_groups = {}
    for r in classified["duplicates"]:
        k = (r["market"], r["instrument_code"])
        duplicate_groups[k] = duplicate_groups.get(k, 0) + 1

    historical_date = None
    try:
        with open(out_dir / "raw" / "company_master" / "TWSE_官方公司基本資料_metadata.json", encoding="utf-8") as f:
            md = json.load(f)
        historical_date = md.get("retrieved_at")
    except Exception:
        pass

    summary = {
        "version": VERSION,
        "target_trade_date": TARGET_DATE.isoformat(),
        "run_time_taipei": RUN_TIME.isoformat(),
        "status": (
            "PASS_WITH_PRODUCT_CLASSIFICATION_LIMITS"
            if current_master_pass
            else "PARTIAL_MASTER_SOURCE_FAILURE"
        ),
        "input": {
            "v138_directory": str(v138_dir),
            "v138_zip_sha256": v138_sha,
            "v138_normalized_csv": str(quote_path),
            "quote_rows": len(quotes),
            "quote_dates": sorted({q.get("trade_date") for q in quotes}),
            "v133_zip_sha256": v133["sha256"],
            "v133_zip_members": v133["member_count"],
            "v133_company_master_members": len(v133["master_members"]),
        },
        "current_official_company_masters": profile_results,
        "current_master_complete": current_master_pass,
        "historical_v133_company_master_crosscheck": old_master_summary,
        "product_catalog_attempts": product_attempts,
        "product_catalog_successful_sources": [
            {"market": c.get("market"), "type": c.get("type"), "url": c.get("url"), "code_count": len(c.get("codes", []))}
            for c in product_catalogs
        ],
        "classification_basis": {
            "company_master_match": "當日行情的市場別+代號命中對應市場官方公司基本資料。此層級標為個股候選，不宣稱完全等於逐商品普通股法律分類。",
            "etf_etn_tdr": "只有配對可解析的官方商品名錄才標成明確商品；若官方商品名錄未取得，未匹配項目保持待確認。",
            "tdr_candidate": "代號91開頭或名稱含DR/存託憑證僅列為候選提示，不作官方確證。",
            "unmatched": "未命中公司主檔與已取得商品名錄，不能推測為ETF、ETN、TDR或普通股。",
        },
        "counts": {
            "all_quote_rows": len(classified["all"]),
            "company_master_matched_individual_stock_candidates": len(classified["candidate"]),
            "unmatched_unknown_security_type": len(classified["unmatched"]),
            "official_product_or_tdr_candidate_rows": len(classified["product_or_tdr_candidates"]),
            "duplicate_rows": len(classified["duplicates"]),
            "duplicate_market_code_groups": len(duplicate_groups),
            "category_counts": category_counts,
            "market_category_counts": market_counts,
        },
        "data_quality_observation": {
            "zero_volume_rows": sum(1 for r in quotes if any(str(v).strip() in ("0", "0.0", "0.00") for k, v in r.items() if "成交股數" in k or "成交量" in k)),
            "treatment": "不刪除零成交量列、不補值、不修改OHLC；全部保留並提供逐筆分類報告。",
        },
        "non_mutation": {
            "original_v138_files_modified": False,
            "original_v133_zip_modified": False,
            "formal_database_modified": False,
            "main_branch_modified": False,
            "training_or_backtest_performed": False,
        },
    }
    save_json(out_dir / "reports" / "普通股範圍主檔交叉比對摘要.json", summary)

    readme = f"""# {VERSION}｜2026-11-10 普通股範圍配對與行情分類

目標行情日期：{TARGET_DATE.isoformat()}
執行時間：{RUN_TIME.isoformat()}
狀態：{summary['status']}

## 輸入來源

- v1.3.8 雙市場行情 ZIP：SHA256 `{v138_sha}`
- v1.3.3 官方普通股範圍來源 ZIP：SHA256 `{v133['sha256']}`
- TWSE 官方上市公司基本資料：`{TWSE_COMPANY_URL}`
- TPEx 官方上櫃公司基本資料：`{TPEX_COMPANY_URL}`

## 行情配對結果

- 雙市場行情列數：{len(classified['all']):,}
- 官方公司主檔匹配個股候選：{len(classified['candidate']):,}
- 官方商品名錄配對或 TDR 候選：{len(classified['product_or_tdr_candidates']):,}
- 未匹配、商品種類待確認：{len(classified['unmatched']):,}
- 重複代號列：{len(classified['duplicates']):,}

## 分類原則

公司主檔匹配僅代表行情代號命中該市場官方公司基本資料，列為「個股候選」，不過度宣稱逐商品普通股法律分類。只有取得並解析官方 ETF/ETN/TDR 商品名錄且代號吻合，才標成明確商品類別。代號或名稱的 TDR 提示只列為候選，未取得名錄時不宣稱確證。未匹配資料不猜測商品種類。

## 主要輸出

- `normalized/公司主檔匹配個股候選行情.csv`
- `reports/逐筆證券分類與主檔配對.csv`
- `reports/未匹配商品種類待確認.csv`
- `reports/官方商品名錄配對與TDR候選.csv`
- `reports/重複代號檢查.csv`
- `reports/官方當前公司主檔.csv`
- `reports/普通股範圍主檔交叉比對摘要.json`

## 資料安全

原始資料唯讀；本版只建立新封存，不修改 v1.3.8、v1.3.3、正式資料庫或 main；不重訓、不回測、不選股。零成交量與缺OHLC資料不刪除、不補值。
"""
    (out_dir / "README.md").write_text(readme, encoding="utf-8")

    # 保存目前 cell 程式碼（Colab 優先）。
    source_path = out_dir / "程式碼" / "SSO_v139_普通股範圍配對與行情分類.py"
    source_path.parent.mkdir(parents=True, exist_ok=True)
    source_saved = False
    try:
        ip = get_ipython()
        hist = ip.history_manager.input_hist_raw
        if hist and isinstance(hist[-1], str) and len(hist[-1]) > 10000:
            source_path.write_text(hist[-1], encoding="utf-8")
            source_saved = True
    except Exception as exc:
        log(f"[WARN] 程式碼自動封存失敗：{exc}")
    if not source_saved:
        source_path.write_text(
            "# Notebook history 無法取得完整 cell source；請以本次提供的完整程式碼為準。\n",
            encoding="utf-8",
        )

    # ZIP package
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
        for p in sorted(out_dir.rglob("*")):
            if p.is_file():
                zf.write(p, p.relative_to(CONTINUATION_ROOT))
    with zipfile.ZipFile(zip_path, "r") as zf:
        bad = zf.testzip()
        if bad:
            raise RuntimeError(f"ZIP CRC 錯誤：{bad}")
    zip_sha = sha256_file(zip_path)
    log(f"[ZIP PASS] {zip_path} bytes={zip_path.stat().st_size:,} SHA256={zip_sha}")

    github = {"attempted": False, "success": False, "error": github_precheck_error}
    main_after = None
    main_unchanged = None
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
        "status": summary["status"],
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

    # 將核對結果新增至 audit branch 並讀回；main 再核對一次。
    if github.get("success"):
        try:
            remote_path = f"{github['remote_root']}/GitHub與main核對結果.json"
            content = final_path.read_bytes()
            upload_one(token, github["branch"], remote_path, content)
            final["final_record_readback"] = "PASS"
            final["main_sha_after_final_record_upload"] = get_main_sha(token)
            final["main_unchanged_final"] = (final["main_sha_after_final_record_upload"] == main_before)
            save_json(final_path, final)
            log(f"[GITHUB PASS] final record readback=PASS main unchanged final={final['main_unchanged_final']}")
        except Exception as exc:
            final["final_record_error"] = f"{type(exc).__name__}: {exc}"
            save_json(final_path, final)
            log(f"[GITHUB WARN] final record upload/check failed: {final['final_record_error']}")

    # 本機最終摘要追加 GitHub 狀態，保持封存目錄自足；ZIP 先前已包含完整研究報告。
    summary["github"] = github
    summary["github_main_sha_before"] = main_before
    summary["github_main_sha_after_package"] = main_after
    summary["github_main_unchanged"] = main_unchanged
    save_json(out_dir / "reports" / "普通股範圍主檔交叉比對摘要.json", summary)

    log("=" * 100)
    log(f"[FINAL] status={summary['status']}")
    log(f"[FINAL] quote rows={len(classified['all']):,}")
    log(f"[FINAL] company-master matched candidates={len(classified['candidate']):,}")
    log(f"[FINAL] official product/TDR candidate rows={len(classified['product_or_tdr_candidates']):,}")
    log(f"[FINAL] unknown unmatched rows={len(classified['unmatched']):,}")
    log(f"[FINAL] duplicate rows={len(classified['duplicates']):,}")
    log(f"[FINAL] output={out_dir}")
    log(f"[FINAL] zip={zip_path}")
    log(f"[FINAL] zip sha256={zip_sha}")
    log(f"[FINAL] github upload={github.get('success')} main unchanged={main_unchanged}")
    log("[FINAL] 原始資料、既有版本及正式資料庫均未修改。")
    log("=" * 100)


try:
    main()
except Exception as exc:
    log(f"[FATAL] {type(exc).__name__}: {exc}")
    traceback.print_exc()
    raise