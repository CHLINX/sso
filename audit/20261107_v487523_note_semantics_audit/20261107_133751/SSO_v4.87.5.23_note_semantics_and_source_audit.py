# -*- coding: utf-8 -*-
"""看股最速適 v4.87.5.23｜九日雙市場單一來源校正與備註稽核

本版沿用 v4.87.5.22 的單一來源／自然鍵選擇流程，重新讀取 v4.87.5.19
及其封存的 v4.87.5.16 來源，不重新呼叫行情 API、不修改原始資料、不合併歷史主表。
將漲跌方向符號（+/-/X）與文字備註分流，重新輸出逐筆表及 18 組覆蓋稽核。
"""
from __future__ import annotations

import base64
import csv
import datetime as dt
import hashlib
import gzip
import inspect
import io
import json
import linecache
import os
import re
import shutil
import sys
import time
import tempfile
import traceback
import zipfile
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Sequence, Tuple

VERSION = "v4.87.5.23"
REPO = "CHLINX/sso"
ROOT = Path("/content/drive/MyDrive/看股最速適")
PREVIOUS_PREFIX = "看股最速適_v4.87.5.19_官方註記欄位補正與TWSE補抓_"
PREVIOUS20_PREFIX = "看股最速適_v4.87.5.20_九日雙市場備註欄位統一與程式封存_"
SCRIPT_BASENAME = "SSO_v4.87.5.23_note_semantics_and_source_audit.py"
TRADE_DATES = [
    "2026-10-27", "2026-10-28", "2026-10-29", "2026-10-30",
    "2026-11-02", "2026-11-03", "2026-11-04", "2026-11-05", "2026-11-06",
]
EXPECTED_COUNTS = {
    ("TWSE", "2026-10-27"): 1386, ("TWSE", "2026-10-28"): 1386,
    ("TWSE", "2026-10-29"): 1388, ("TWSE", "2026-10-30"): 1389,
    ("TWSE", "2026-11-02"): 1389, ("TWSE", "2026-11-03"): 1390,
    ("TWSE", "2026-11-04"): 1390, ("TWSE", "2026-11-05"): 1389,
    ("TWSE", "2026-11-06"): 1389,
    ("TPEX", "2026-10-27"): 1020, ("TPEX", "2026-10-28"): 1020,
    ("TPEX", "2026-10-29"): 1017, ("TPEX", "2026-10-30"): 1018,
    ("TPEX", "2026-11-02"): 1018, ("TPEX", "2026-11-03"): 1018,
    ("TPEX", "2026-11-04"): 1018, ("TPEX", "2026-11-05"): 1017,
    ("TPEX", "2026-11-06"): 1017,
}
HOLIDAYS = ["2026-10-23", "2026-10-26"]
OUTPUT_COLUMNS = [
    "market", "date", "code", "name", "open", "high", "low", "close",
    "change", "change_note", "change_sign", "change_sign_field",
    "suspension_note", "limit_touch_note", "official_note",
    "source_note_fields", "source_descriptive_note_fields", "note_status", "備註", "volume_shares", "trading_value",
    "trade_count", "ohlc_valid", "positive_volume", "source_member", "source_format",
]

# Header aliases are deliberately conservative. Numeric TPEx 漲跌 is a price difference,
# not a note; it is only mapped to change_note when the cell is a symbol (+/-/X).
ALIASES = {
    "market": ["market", "market_type", "市場", "市場別", "市場類別"],
    "date": ["date", "trade_date", "日期", "資料日期", "交易日期", "交易日", "日 期"],
    "code": ["code", "symbol", "stock_id", "securitiescompanycode", "證券代號", "證券代碼", "股票代號", "代號"],
    "name": ["name", "company_name", "證券名稱", "股票名稱", "名稱"],
    "open": ["open", "開盤", "開盤價"],
    "high": ["high", "最高", "最高價"],
    "low": ["low", "最低", "最低價"],
    "close": ["close", "收盤", "收盤價"],
    "change": ["change", "price_change", "漲跌價差", "漲跌價", "漲跌"],
    "volume": ["volume_shares", "tradingshares", "成交股數", "成交量", "成交股數(股)", "成交量(股)", "成交仟股"],
    "amount": ["trading_value", "turnover_value", "成交金額", "成交金額(元)", "成交仟元"],
    "trades": ["trade_count", "tradecount", "成交筆數", "筆數"],
    "change_note": ["change_note", "price_change_sign", "漲跌註記", "漲跌符號", "漲跌方向", "漲跌(+/-)", "漲跌(＋/－)", "漲跌(+／-)", "漲跌註記(＋/－)", "漲跌記號"],
    "suspension_note": ["suspension_note", "暫停交易註記", "暫停交易說明", "暫停交易"],
    "limit_touch_note": ["limit_touch_note", "牛熊證觸及限制價格註記", "觸及限制價格註記"],
    "official_note": ["official_note", "source_note", "備註", "附註", "說明", "特殊處理註記"],
}

DIRECTION_SIGN_VALUES = {"+", "-", "X", "＋", "－"}
DIRECTION_SIGN_HEADERS_RAW = (
    "price_change_sign", "漲跌符號", "漲跌方向", "漲跌(+/-)", "漲跌(＋/－)",
    "漲跌(+／-)", "漲跌(＋／－)", "漲跌記號", "漲跌註記(＋/－)",
)


def is_direction_sign_header(value: Any) -> bool:
    normalized = norm_header(value)
    return any(normalized == norm_header(name) for name in DIRECTION_SIGN_HEADERS_RAW)


def is_direction_sign_value(value: Any) -> bool:
    return clean_text(value) in DIRECTION_SIGN_VALUES


def log(message: str) -> None:
    print(f"[{dt.datetime.now().strftime('%H:%M:%S')}] {message}", flush=True)


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2, default=str), encoding="utf-8")


def write_csv(path: Path, rows: Sequence[Dict[str, Any]], columns: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(columns), extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def norm_header(value: Any) -> str:
    s = str(value or "").strip().lower()
    s = re.sub(r"<[^>]+>", "", s)
    s = s.replace("　", "").replace(" ", "").replace("\ufeff", "")
    s = s.replace("／", "/").replace("－", "-").replace("＋", "+")
    return s


def find_key(row: Dict[str, Any], aliases: Iterable[str]) -> Optional[str]:
    normalized = {norm_header(k): k for k in row.keys()}
    for alias in aliases:
        a = norm_header(alias)
        if a in normalized:
            return normalized[a]
    for alias in aliases:
        a = norm_header(alias)
        for k_norm, original in normalized.items():
            if a and a in k_norm:
                return original
    return None


def get_value(row: Dict[str, Any], field: str) -> Any:
    key = find_key(row, ALIASES[field])
    return row.get(key) if key is not None else None


def parse_date(value: Any, path_hint: str = "") -> Optional[str]:
    candidates = []
    if value is not None:
        candidates.append(str(value).strip())
    candidates.append(path_hint)
    for raw in candidates:
        s = str(raw or "").strip()
        m = re.search(r"(?<!\d)(20\d{2})[-/]?(\d{2})[-/]?(\d{2})(?!\d)", s)
        if m:
            y, mo, d = map(int, m.groups())
            try:
                return dt.date(y, mo, d).isoformat()
            except ValueError:
                pass
        m = re.search(r"(?<!\d)(\d{3})[/.-]?(\d{2})[/.-]?(\d{2})(?!\d)", s)
        if m:
            y, mo, d = map(int, m.groups())
            try:
                return dt.date(y + 1911, mo, d).isoformat()
            except ValueError:
                pass
    return None


def infer_market(value: Any, path_hint: str) -> Optional[str]:
    text = (str(value or "") + " " + path_hint).upper()
    if "TWSE" in text or "上市" in text:
        return "TWSE"
    if "TPEX" in text or "TPEx" in text or "上櫃" in text or "櫃買" in text:
        return "TPEX"
    if "OTC" in text:
        return "TPEX"
    return None


def parse_num(value: Any) -> Optional[float]:
    if value is None:
        return None
    s = str(value).strip().replace(",", "").replace("　", "")
    if not s or s in {"--", "-", "X", "N/A", "nan", "None", "null"}:
        return None
    s = s.replace("元", "").replace("股", "").replace("仟", "").strip()
    try:
        x = float(s)
        if x != x or x in (float("inf"), float("-inf")):
            return None
        return x
    except Exception:
        return None


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    s = str(value).strip()
    return "" if s.lower() in {"none", "nan", "null"} else s


def note_value(row: Dict[str, Any], aliases: Iterable[str]) -> Tuple[str, Optional[str]]:
    k = find_key(row, aliases)
    return (clean_text(row.get(k)), k) if k is not None else ("", None)


def normalize_record(source_row: Dict[str, Any], market_hint: Optional[str], path_hint: str,
                     source_member: str, source_format: str) -> Optional[Dict[str, Any]]:
    market = infer_market(get_value(source_row, "market"), path_hint) or market_hint
    date = parse_date(get_value(source_row, "date"), path_hint)
    code = clean_text(get_value(source_row, "code"))
    if market not in {"TWSE", "TPEX"} or date not in TRADE_DATES or not code:
        return None
    normalized_marker = (find_key(source_row, ["source_note_fields"]) is not None
                         or find_key(source_row, ["note_status"]) is not None)
    raw_notes: Dict[str, Tuple[str, Optional[str]]] = {
        "change_note": note_value(source_row, ALIASES["change_note"]),
        "suspension_note": note_value(source_row, ALIASES["suspension_note"]),
        "limit_touch_note": note_value(source_row, ALIASES["limit_touch_note"]),
        # In normalized rows 備註 is generated display text; only explicit source_note fields count.
        "official_note": note_value(source_row, ["official_note", "source_note"] if normalized_marker else ALIASES["official_note"]),
    }
    if not raw_notes["official_note"][1] and not normalized_marker:
        # Only an exact 註記 header is accepted here; do not substring-match 漲跌註記.
        exact_note_key = next((k for k in source_row if norm_header(k) == norm_header("註記")), None)
        if exact_note_key is not None:
            raw_notes["official_note"] = (clean_text(source_row.get(exact_note_key)), exact_note_key)

    change_note_value, change_note_key = raw_notes["change_note"]
    # A dedicated +/-/X column is a direction marker, not explanatory 備註.
    change_sign = change_note_value if is_direction_sign_value(change_note_value) else ""
    change_sign_field = change_note_key if change_sign else ""
    if not change_note_key:
        plain_change_key = find_key(source_row, ["漲跌"])
        plain_change_value = clean_text(source_row.get(plain_change_key)) if plain_change_key else ""
        if is_direction_sign_value(plain_change_value):
            change_note_value, change_note_key = plain_change_value, plain_change_key
            change_sign, change_sign_field = plain_change_value, plain_change_key
            raw_notes["change_note"] = (plain_change_value, plain_change_key)

    declared_source_fields = clean_text(source_row.get("source_note_fields"))
    if normalized_marker:
        present_fields = [x.strip() for x in declared_source_fields.split("；") if x.strip()]
    else:
        present_fields = [v[1] for v in raw_notes.values() if v[1] is not None]
    # Exclude pure direction-sign headers from narrative-note availability.
    descriptive_fields = [
        field for field in present_fields
        if not is_direction_sign_header(field)
        and not (change_note_key and norm_header(field) == norm_header(change_note_key)
                 and is_direction_sign_value(change_note_value))
    ]

    def valid_text_note(value: str) -> bool:
        return bool(value) and value not in {"--", "-", "X", "N/A", "無", "無註記"}

    narrative_parts = []
    # Some sources use 漲跌註記 for descriptive event text; keep it only when it is not a sign.
    if change_note_key and not is_direction_sign_header(change_note_key) and not is_direction_sign_value(change_note_value):
        if valid_text_note(change_note_value):
            narrative_parts.append(f"漲跌註記：{change_note_value}")
    for field, label in (("suspension_note", "暫停交易註記"),
                         ("limit_touch_note", "牛熊證觸及限制價格註記"),
                         ("official_note", "官方備註／註記")):
        value = raw_notes[field][0]
        if valid_text_note(value):
            narrative_parts.append(f"{label}：{value}")

    unverified_csv_source = source_format.upper() in {"CSV", "CSV.GZ"} and not normalized_marker
    if narrative_parts and unverified_csv_source:
        note_status = "SOURCE_NOTE_PRESERVED_UNVERIFIED"
        note = "；".join(narrative_parts) + "；來源為封存 CSV，未能由原始 JSON 證實欄位對應"
    elif narrative_parts:
        note_status = "OFFICIAL_DESCRIPTIVE_NOTE_PRESENT"
        note = "；".join(narrative_parts)
    elif descriptive_fields and unverified_csv_source:
        note_status = "SOURCE_NOTE_PRESERVED_UNVERIFIED"
        note = "來源 CSV 有文字備註欄位，但無法由原始 JSON 證實欄位對應"
    elif descriptive_fields:
        note_status = "OFFICIAL_NOTE_FIELDS_EMPTY"
        note = "官方文字備註欄位存在，但本筆沒有非空文字註記"
    else:
        note_status = "OFFICIAL_NOTE_FIELD_NOT_RETURNED"
        note = "官方原始回應未提供可辨識的文字備註欄位"

    o, h, l, c = (parse_num(get_value(source_row, k)) for k in ("open", "high", "low", "close"))
    vol = parse_num(get_value(source_row, "volume"))
    # TPEx dailyQuotes uses shares; older per-security tradingStock uses thousand shares.
    # The raw source unit is preserved separately; do not multiply unless the source schema says 仟股.
    if vol is not None and ("成交仟股" in " ".join(map(str, source_row.keys()))):
        vol *= 1000
    amount = parse_num(get_value(source_row, "amount"))
    if amount is not None and ("成交仟元" in " ".join(map(str, source_row.keys()))):
        amount *= 1000
    change = parse_num(get_value(source_row, "change"))
    ohlc_valid = int(all(x is not None and x > 0 for x in (o, h, l, c)) and l <= min(o, c) <= max(o, c) <= h)
    return {
        "market": market, "date": date, "code": code,
        "name": clean_text(get_value(source_row, "name")),
        "open": o, "high": h, "low": l, "close": c, "change": change,
        "change_note": raw_notes["change_note"][0],
        "change_sign": change_sign,
        "change_sign_field": change_sign_field,
        "suspension_note": raw_notes["suspension_note"][0],
        "limit_touch_note": raw_notes["limit_touch_note"][0],
        "official_note": raw_notes["official_note"][0],
        "source_note_fields": "；".join(str(x) for x in present_fields),
        "source_descriptive_note_fields": "；".join(str(x) for x in descriptive_fields),
        "note_status": note_status, "備註": note,
        "volume_shares": vol, "trading_value": amount,
        "trade_count": parse_num(get_value(source_row, "trades")),
        "ohlc_valid": ohlc_valid,
        "positive_volume": int(vol is not None and vol > 0),
        "source_member": source_member, "source_format": source_format,
    }


def decode_bytes(data: bytes) -> str:
    for enc in ("utf-8-sig", "utf-8", "cp950", "big5"):
        try:
            return data.decode(enc)
        except UnicodeDecodeError:
            continue
    return data.decode("utf-8", errors="replace")


def read_csv_records(data: bytes, path_hint: str, market_hint: Optional[str], source_member: str) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    text = decode_bytes(data)
    sample = text[:8192]
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",\t;")
    except Exception:
        dialect = csv.excel
    reader = csv.DictReader(io.StringIO(text), dialect=dialect)
    if not reader.fieldnames:
        return [], {"member": source_member, "format": "CSV", "rows": 0, "reason": "NO_HEADER"}
    normalized_headers = [norm_header(x) for x in reader.fieldnames]
    has_code = any(any(norm_header(a) == h or norm_header(a) in h for a in ALIASES["code"]) for h in normalized_headers)
    has_ohlc = all(any(norm_header(a) == h or norm_header(a) in h for a in ALIASES[k] for h in normalized_headers) for k in ("open", "high", "low", "close"))
    if not has_code or not has_ohlc:
        return [], {"member": source_member, "format": "CSV", "rows": 0, "reason": "NO_CODE_OR_OHLC", "headers": reader.fieldnames[:60]}
    out = []
    for row in reader:
        record = normalize_record(row, market_hint, path_hint, source_member, "CSV")
        if record:
            out.append(record)
    return out, {"member": source_member, "format": "CSV", "rows": len(out), "headers": reader.fieldnames[:80], "reason": "PARSED"}


def json_dict_rows(obj: Any) -> Iterator[Tuple[List[str], Dict[str, Any]]]:
    """Yield row dictionaries from array-of-object and fields/data JSON table formats."""
    if isinstance(obj, dict):
        fields = obj.get("fields") or obj.get("Fields") or obj.get("field")
        data = obj.get("data") or obj.get("Data") or obj.get("aaData")
        if isinstance(fields, list) and isinstance(data, list):
            headers = [str(x) for x in fields]
            for item in data:
                if isinstance(item, (list, tuple)):
                    yield headers, {headers[i]: item[i] for i in range(min(len(headers), len(item)))}
                elif isinstance(item, dict):
                    yield headers, item
        for value in obj.values():
            if isinstance(value, (dict, list)):
                yield from json_dict_rows(value)
    elif isinstance(obj, list):
        if obj and all(isinstance(x, dict) for x in obj):
            # A tables[] list contains table objects with fields/data, not security rows.
            if any(("fields" in x or "Fields" in x) and ("data" in x or "Data" in x) for x in obj):
                for value in obj:
                    yield from json_dict_rows(value)
            else:
                for row in obj:
                    yield list(row.keys()), row
        else:
            for value in obj:
                if isinstance(value, (dict, list)):
                    yield from json_dict_rows(value)


def read_json_records(data: bytes, path_hint: str, market_hint: Optional[str], source_member: str) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    try:
        obj = json.loads(decode_bytes(data))
    except Exception as e:
        return [], {"member": source_member, "format": "JSON", "rows": 0, "reason": f"JSON_PARSE_ERROR:{type(e).__name__}"}
    out = []
    for _, row in json_dict_rows(obj):
        record = normalize_record(row, market_hint, path_hint, source_member, "JSON")
        if record:
            out.append(record)
    return out, {"member": source_member, "format": "JSON", "rows": len(out), "reason": "PARSED" if out else "NO_MATCHING_ROWS"}


def scan_zip(zf: zipfile.ZipFile, label: str) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    records: List[Dict[str, Any]] = []
    inventory: List[Dict[str, Any]] = []
    members = [m for m in zf.infolist() if not m.is_dir()]
    for index, member in enumerate(members, 1):
        name = member.filename
        ext = Path(name).suffix.lower()
        market_hint = infer_market(None, name)
        try:
            if name.lower().endswith(".csv.gz"):
                data = gzip.decompress(zf.read(member))
                rows, meta = read_csv_records(data, name, market_hint, f"{label}!{name}")
                meta["format"] = "CSV.GZ"
            elif name.lower().endswith(".json.gz"):
                data = gzip.decompress(zf.read(member))
                rows, meta = read_json_records(data, name, market_hint, f"{label}!{name}")
                meta["format"] = "JSON.GZ"
            elif ext == ".csv":
                data = zf.read(member)
                rows, meta = read_csv_records(data, name, market_hint, f"{label}!{name}")
            elif ext == ".json":
                data = zf.read(member)
                rows, meta = read_json_records(data, name, market_hint, f"{label}!{name}")
            elif ext == ".zip":
                # Nested zip archives are scanned one level deep only to avoid accidental recursion loops.
                data = zf.read(member)
                with zipfile.ZipFile(io.BytesIO(data), "r") as nested:
                    rows, nested_meta = scan_zip(nested, f"{label}!{name}")
                meta = {"member": f"{label}!{name}", "format": "NESTED_ZIP", "rows": len(rows), "reason": "SCANNED", "nested_files": len(nested_meta)}
            else:
                continue
            records.extend(rows)
            if meta.get("rows", 0) or meta.get("reason") not in ("NO_CODE_OR_OHLC", "NO_MATCHING_ROWS"):
                inventory.append({"archive": label, **meta, "bytes": member.file_size})
        except Exception as e:
            inventory.append({"archive": label, "member": name, "format": ext, "rows": 0,
                              "reason": f"READ_ERROR:{type(e).__name__}:{e}", "bytes": member.file_size})
        if index % 100 == 0:
            log(f"封存檔掃描進度：{label} {index}/{len(members)}")
    return records, inventory


def discover_v419() -> Tuple[Path, Path]:
    candidates = [p for p in ROOT.glob(PREVIOUS_PREFIX + "*") if p.is_dir()]
    if not candidates:
        raise FileNotFoundError(f"找不到 v4.87.5.19 成果目錄：{ROOT}")
    candidates.sort(key=lambda p: p.name, reverse=True)
    folder = candidates[0]
    zips = sorted(folder.glob("SSO_v4.87.5.19_*.zip"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not zips:
        raise FileNotFoundError(f"v4.87.5.19 成果目錄沒有 ZIP：{folder}")
    return folder, zips[0]


def collect_v519_retry_rows(folder: Path) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    candidates = list(folder.glob("*補抓行情.csv"))
    rows: List[Dict[str, Any]] = []
    inventory: List[Dict[str, Any]] = []
    for path in candidates:
        parsed, meta = read_csv_records(path.read_bytes(), path.name, "TWSE", str(path))
        rows.extend(parsed)
        inventory.append({"archive": "v4.87.5.19 Drive folder", **meta, "bytes": path.stat().st_size})
    # If the v4.87.5.19 CSV carries newer note fields already parsed into normalized columns,
    # normalize_record preserves them and will set an explicit note status.
    return rows, inventory


def scan_v519_raw_json(zf: zipfile.ZipFile, members: Sequence[str]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    records: List[Dict[str, Any]] = []
    inventory: List[Dict[str, Any]] = []
    for name in members:
        lower = name.lower()
        if not (lower.endswith(".json") or lower.endswith(".json.gz")):
            continue
        if "raw_json" not in lower and "twse_2026" not in lower and "tpex_2026" not in lower:
            continue
        try:
            data = zf.read(name)
            if lower.endswith(".json.gz"):
                data = gzip.decompress(data)
            market_hint = infer_market(None, name)
            rows, meta = read_json_records(data, name, market_hint, f"v4.87.5.19!{name}")
            records.extend(rows)
            inventory.append({"archive": "v4.87.5.19 raw JSON", **meta, "bytes": len(data)})
        except Exception as exc:
            inventory.append({"archive": "v4.87.5.19 raw JSON", "member": name, "format": "JSON",
                              "rows": 0, "reason": f"READ_ERROR:{type(exc).__name__}:{exc}"})
    return records, inventory


def record_quality(row: Dict[str, Any]) -> int:
    score = 0
    for key in ("open", "high", "low", "close", "volume_shares", "name"):
        if row.get(key) not in (None, ""):
            score += 2
    if row.get("note_status") == "OFFICIAL_DESCRIPTIVE_NOTE_PRESENT":
        score += 20
    if row.get("source_format") == "JSON":
        score += 25
    if "raw_json" in str(row.get("source_member")).lower():
        score += 25
    if "v4.87.5.19" in str(row.get("source_member")):
        score += 5
    return score


def dedupe_records(records: Sequence[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    chosen: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
    collisions: List[Dict[str, Any]] = []
    for row in records:
        key = (row["market"], row["date"], row["code"])
        old = chosen.get(key)
        if old is None:
            chosen[key] = row
        else:
            if record_quality(row) > record_quality(old):
                collisions.append({"market": key[0], "date": key[1], "code": key[2],
                                   "selected_source": row["source_member"], "discarded_source": old["source_member"],
                                   "reason": "HIGHER_SOURCE_OR_NOTE_QUALITY"})
                chosen[key] = row
            else:
                collisions.append({"market": key[0], "date": key[1], "code": key[2],
                                   "selected_source": old["source_member"], "discarded_source": row["source_member"],
                                   "reason": "LOWER_SOURCE_OR_NOTE_QUALITY"})
    result = list(chosen.values())
    result.sort(key=lambda r: (r["date"], r["market"], r["code"]))
    return result, collisions


def note_status_label(status: str) -> str:
    return {
        "OFFICIAL_DESCRIPTIVE_NOTE_PRESENT": "官方有提供文字備註",
        "OFFICIAL_NOTE_FIELDS_EMPTY": "官方有文字備註欄位但本筆空值",
        "OFFICIAL_NOTE_FIELD_NOT_RETURNED": "官方來源未提供可辨識文字備註欄位",
        "SOURCE_NOTE_PRESERVED_UNVERIFIED": "保留既有文字備註但來源欄位待核",
    }.get(status, "狀態未分類")


def build_audits(records: Sequence[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    day_rows = []
    coverage = []
    sign_rows = []
    status_names = ("OFFICIAL_DESCRIPTIVE_NOTE_PRESENT", "OFFICIAL_NOTE_FIELDS_EMPTY",
                    "OFFICIAL_NOTE_FIELD_NOT_RETURNED", "SOURCE_NOTE_PRESERVED_UNVERIFIED")
    for date in TRADE_DATES:
        for market in ("TWSE", "TPEX"):
            group = [r for r in records if r["date"] == date and r["market"] == market]
            counts: Dict[str, int] = {}
            for r in group:
                counts[r["note_status"]] = counts.get(r["note_status"], 0) + 1
            sign_counts: Dict[str, int] = {}
            for r in group:
                sign = r.get("change_sign") or "(無可辨識符號)"
                sign_counts[sign] = sign_counts.get(sign, 0) + 1
            expected = EXPECTED_COUNTS[(market, date)]
            direction_rows = sum(1 for r in group if r.get("change_sign"))
            text_note_rows = counts.get("OFFICIAL_DESCRIPTIVE_NOTE_PRESENT", 0)
            day_rows.append({
                "date": date, "market": market, "expected_rows": expected, "actual_unique_rows": len(group),
                "row_count_pass": len(group) == expected,
                "rows_with_descriptive_note": text_note_rows,
                "rows_note_fields_exist_but_empty": counts.get("OFFICIAL_NOTE_FIELDS_EMPTY", 0),
                "rows_note_field_not_returned": counts.get("OFFICIAL_NOTE_FIELD_NOT_RETURNED", 0),
                "rows_note_preserved_unverified": counts.get("SOURCE_NOTE_PRESERVED_UNVERIFIED", 0),
                "rows_with_direction_sign": direction_rows,
                "note_coverage_definition": "只計官方文字說明／註記；漲跌+/-/X符號另欄保存，不算文字備註",
            })
            all_fields = sorted({x for r in group for x in str(r.get("source_note_fields") or "").split("；") if x})
            desc_fields = sorted({x for r in group for x in str(r.get("source_descriptive_note_fields") or "").split("；") if x})
            coverage.append({
                "date": date, "market": market, "rows": len(group),
                "source_note_fields_observed": "；".join(all_fields),
                "source_descriptive_note_fields_observed": "；".join(desc_fields),
                "official_descriptive_note_rows": text_note_rows,
                "official_note_fields_empty_rows": counts.get("OFFICIAL_NOTE_FIELDS_EMPTY", 0),
                "no_descriptive_note_field_rows": counts.get("OFFICIAL_NOTE_FIELD_NOT_RETURNED", 0),
                "unverified_preserved_note_rows": counts.get("SOURCE_NOTE_PRESERVED_UNVERIFIED", 0),
                "direction_sign_rows": direction_rows,
            })
            for sign, sign_count in sorted(sign_counts.items()):
                sign_rows.append({"date": date, "market": market, "direction_sign": sign,
                                  "rows": sign_count, "rows_total": len(group)})
    return day_rows, coverage, sign_rows


def resolve_script_source() -> Optional[Path]:
    candidates: List[Path] = []
    module_file = globals().get("__file__")
    if isinstance(module_file, str) and module_file.strip():
        candidates.append(Path(module_file).expanduser())
    try:
        source_file = inspect.getsourcefile(main)
        if source_file:
            candidates.append(Path(source_file).expanduser())
    except Exception:
        pass
    code_file = getattr(getattr(main, "__code__", None), "co_filename", None)
    if isinstance(code_file, str) and code_file and not code_file.startswith("<"):
        candidates.append(Path(code_file).expanduser())
    candidates.extend([Path("/content") / SCRIPT_BASENAME, Path.cwd() / SCRIPT_BASENAME,
                       Path("/mnt/data") / SCRIPT_BASENAME])
    seen = set()
    for candidate in candidates:
        try:
            resolved = candidate.resolve()
            if resolved in seen:
                continue
            seen.add(resolved)
            if resolved.is_file() and resolved.suffix.lower() == ".py":
                return resolved
        except Exception:
            continue
    # IPython's linecache may still hold an executed source file even if cwd differs.
    if isinstance(code_file, str) and code_file in linecache.cache:
        lines = linecache.getlines(code_file)
        if lines:
            fallback = Path.cwd() / SCRIPT_BASENAME
            fallback.write_text("".join(lines), encoding="utf-8")
            return fallback.resolve()
    return None


def get_github_token() -> Optional[str]:
    token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
    if token:
        return token.strip()
    try:
        from google.colab import userdata  # type: ignore
        token = userdata.get("GITHUB_TOKEN")
        if token:
            return str(token).strip()
    except Exception:
        pass
    return None


def github_json(session: Any, method: str, url: str, **kwargs: Any) -> Any:
    response = session.request(method, url, timeout=90, **kwargs)
    if not response.ok:
        raise RuntimeError(f"GitHub API {method} {url} HTTP {response.status_code}: {response.text[:1000]}")
    if response.status_code == 204 or not response.text:
        return {}
    return response.json()


def github_upload(zip_path: Path, summary_path: Path, script_path: Path, timestamp: str) -> Dict[str, Any]:
    import requests
    token = get_github_token()
    if not token:
        raise RuntimeError("找不到 GITHUB_TOKEN；Drive 成果仍保留，GitHub 未上傳")
    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json",
                            "X-GitHub-Api-Version": "2022-11-28", "User-Agent": "SSO-v4.87.5.23-audit"})
    api = f"https://api.github.com/repos/{REPO}"
    main_before = github_json(session, "GET", api + "/branches/main")["commit"]["sha"]
    commit_info = github_json(session, "GET", api + f"/git/commits/{main_before}")
    base_tree = commit_info["tree"]["sha"]
    branch = f"audit/v487523-note-semantics-{timestamp}"
    folder = f"audit/20261107_v487523_note_semantics_audit/{timestamp}"
    files = [(f"{folder}/{zip_path.name}", zip_path),
             (f"{folder}/{summary_path.name}", summary_path),
             (f"{folder}/{script_path.name}", script_path)]
    tree_entries = []
    for repo_path, local in files:
        blob = github_json(session, "POST", api + "/git/blobs", json={
            "content": base64.b64encode(local.read_bytes()).decode("ascii"), "encoding": "base64"})
        tree_entries.append({"path": repo_path, "mode": "100644", "type": "blob", "sha": blob["sha"]})
    tree = github_json(session, "POST", api + "/git/trees", json={"base_tree": base_tree, "tree": tree_entries})
    commit = github_json(session, "POST", api + "/git/commits", json={
        "message": "audit: select exact official source groups for nine-session quote normalization",
        "tree": tree["sha"], "parents": [main_before]})
    github_json(session, "POST", api + "/git/refs", json={"ref": f"refs/heads/{branch}", "sha": commit["sha"]})
    file_checks = []
    for repo_path, local in files:
        url = f"https://raw.githubusercontent.com/{REPO}/{branch}/{repo_path}"
        response = session.get(url, timeout=120)
        if not response.ok:
            raise RuntimeError(f"GitHub 遠端檔案下載核對失敗：{repo_path} HTTP {response.status_code}")
        local_sha = sha256_file(local)
        remote_sha = sha256_bytes(response.content)
        file_checks.append({"path": repo_path, "local_sha256": local_sha, "remote_sha256": remote_sha,
                            "match": local_sha == remote_sha})
        if local_sha != remote_sha:
            raise RuntimeError(f"GitHub 遠端檔案 SHA256 不一致：{repo_path}")
    main_after = github_json(session, "GET", api + "/branches/main")["commit"]["sha"]
    if main_before != main_after:
        raise RuntimeError(f"GitHub main 前後 SHA 不一致：{main_before} != {main_after}")
    return {"status": "PASS", "branch": branch, "commit": commit["sha"],
            "main_before": main_before, "main_after": main_after, "main_unchanged": True,
            "files": file_checks}


def discover_v420(root: Path = ROOT) -> Tuple[Path, Path]:
    """Locate the actual v4.87.5.20 output using its recorded folder/file names.

    The failed v4.87.5.21 build used an invented folder prefix. Search only
    immediate SSO root children for v4.87.5.20 output folders, then require the
    exact row-level CSV. Never substitute a different version or source file.
    """
    if not root.exists():
        raise FileNotFoundError(f"看股最速適 Drive 根目錄不存在或尚未掛載：{root}")
    candidates = [p for p in root.iterdir()
                  if p.is_dir() and p.name.startswith(PREVIOUS20_PREFIX)]
    valid = []
    rejected = []
    for folder in candidates:
        csv_path = folder / "九日雙市場行情_備註欄位統一.csv"
        if csv_path.is_file() and csv_path.stat().st_size > 0:
            valid.append((folder, csv_path))
        else:
            rejected.append(folder.name)
    if not valid:
        # Diagnostic-only fallback: report matching v4.87.5.20 folders. Do not
        # recursively pick unrelated files or silently fall back to v4.87.5.19.
        all_v420 = [p.name for p in root.iterdir()
                    if p.is_dir() and p.name.startswith("看股最速適_v4.87.5.20_")]
        raise FileNotFoundError(
            "找不到 v4.87.5.20 逐筆表『九日雙市場行情_備註欄位統一.csv』。"
            f"\nDrive 根目錄：{root}"
            f"\n符合本版預期資料夾前綴：{[p.name for p in candidates]}"
            f"\n其他 v4.87.5.20 資料夾：{all_v420}"
            f"\n資料夾存在但缺少逐筆表：{rejected}"
        )
    valid.sort(key=lambda item: item[0].name)
    # Folder suffixes use YYYYMMDD_HHMMSS, so the final lexical item is newest.
    folder, csv_path = valid[-1]
    # Validate expected columns before allowing the file into source comparison.
    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        header = next(reader, [])
    required = {"market", "date", "code", "source_member", "note_status"}
    missing = sorted(required - set(header))
    if missing:
        raise RuntimeError(f"v4.87.5.20 逐筆表缺少必要欄位：{missing}；檔案：{csv_path}")
    return folder, csv_path


def source_rank(source_member: str, rows: Sequence[Dict[str, Any]]) -> Tuple[int, int, int, str]:
    name = source_member.lower()
    is_json = int(any(r.get("source_format") == "JSON" for r in rows))
    raw_json = int("raw_json" in name or "rawjson" in name)
    official_name = int(any(token in name for token in ("mi_index", "dailyquotes", "tradingstock", "official")))
    has_note = sum(1 for r in rows if r.get("note_status") == "OFFICIAL_DESCRIPTIVE_NOTE_PRESENT")
    # Priority: original JSON first, then an explicitly named raw response/official table,
    # then richer note coverage. Lexical tie-break is deterministic.
    return (is_json * 100 + raw_json * 50 + official_name * 20, has_note, len(rows), source_member)


def select_authoritative_source_groups(records: Sequence[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Choose one complete per-day source file per market; never union partial sources blindly."""
    from collections import defaultdict
    groups: Dict[Tuple[str, str, str], Dict[Tuple[str, str, str], Dict[str, Any]]] = defaultdict(dict)
    for row in records:
        market, date = row.get("market"), row.get("date")
        source = str(row.get("source_member") or "")
        key = (market, date, str(row.get("code") or ""))
        if market not in {"TWSE", "TPEX"} or date not in TRADE_DATES or not key[2] or not source:
            continue
        old = groups[(market, date, source)].get(key)
        if old is None or record_quality(row) > record_quality(old):
            groups[(market, date, source)][key] = row

    selected: List[Dict[str, Any]] = []
    candidate_audit: List[Dict[str, Any]] = []
    key_audit: List[Dict[str, Any]] = []
    for date in TRADE_DATES:
        for market in ("TWSE", "TPEX"):
            expected = EXPECTED_COUNTS[(market, date)]
            candidates = []
            for (m, d, source), keymap in groups.items():
                if m == market and d == date:
                    rows = list(keymap.values())
                    candidates.append((source, keymap, rows, source_rank(source, rows)))
            exact = [c for c in candidates if len(c[1]) == expected]
            pool = exact if exact else candidates
            chosen = max(pool, key=lambda c: (c[3][0], c[3][1], -abs(len(c[1]) - expected), c[3][2], c[0])) if pool else None
            if chosen:
                selected.extend(chosen[2])
                chosen_keys = set(chosen[1].keys())
                for source, keymap, rows, rank in exact:
                    key_same = set(keymap.keys()) == chosen_keys
                    key_audit.append({
                        "date": date, "market": market, "selected_source": chosen[0],
                        "candidate_source": source, "candidate_unique_rows": len(keymap),
                        "same_natural_key_set_as_selected": key_same,
                        "selected_source_sha_scope": "source_member_and_market_date_group",
                    })
            else:
                chosen_keys = set()
            if exact:
                exact_keysets = [set(c[1].keys()) for c in exact]
                keyset_conflict = any(k != exact_keysets[0] for k in exact_keysets[1:])
            else:
                keyset_conflict = False
            candidate_audit.append({
                "date": date, "market": market, "expected_rows": expected,
                "candidate_source_groups": len(candidates), "exact_count_source_groups": len(exact),
                "selected_source": chosen[0] if chosen else "",
                "selected_unique_rows": len(chosen[1]) if chosen else 0,
                "selected_count_exact": bool(chosen and len(chosen[1]) == expected),
                "exact_source_keyset_conflict": keyset_conflict,
                "selected_source_format": ",".join(sorted({str(r.get("source_format")) for r in chosen[2]})) if chosen else "",
                "decision": "EXACT_SOURCE_SELECTED" if chosen and len(chosen[1]) == expected and not keyset_conflict
                           else "EXACT_COUNT_BUT_KEYSET_CONFLICT" if chosen and len(chosen[1]) == expected
                           else "NO_EXACT_SOURCE_GROUP" if chosen else "NO_CANDIDATE_SOURCE",
            })
            # Include all source candidates, not just the selected one, for transparent review.
            for source, keymap, rows, rank in sorted(candidates, key=lambda c: c[3], reverse=True):
                candidate_audit.append({
                    "date": date, "market": market, "expected_rows": expected,
                    "candidate_source_groups": len(candidates), "exact_count_source_groups": len(exact),
                    "selected_source": chosen[0] if chosen else "",
                    "selected_unique_rows": len(chosen[1]) if chosen else 0,
                    "selected_count_exact": bool(chosen and len(chosen[1]) == expected),
                    "exact_source_keyset_conflict": keyset_conflict,
                    "candidate_source": source, "candidate_unique_rows": len(keymap),
                    "candidate_format": ",".join(sorted({str(r.get("source_format")) for r in rows})),
                    "candidate_has_raw_json": "raw_json" in source.lower(),
                    "candidate_descriptive_note_rows": sum(1 for r in rows if r.get("note_status") == "OFFICIAL_DESCRIPTIVE_NOTE_PRESENT"),
                    "candidate_is_selected": bool(chosen and source == chosen[0]),
                    "decision": "CANDIDATE_DETAIL",
                })
    selected.sort(key=lambda r: (r["date"], r["market"], r["code"]))
    return selected, candidate_audit, key_audit


def load_previous_v420_keys(path: Path) -> Tuple[set, int, Dict[str, int]]:
    keys = set()
    counts: Dict[str, int] = {}
    total = 0
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            total += 1
            market, date, code = row.get("market"), row.get("date"), row.get("code")
            if market and date and code:
                keys.add((market, date, code))
                counts[market] = counts.get(market, 0) + 1
    return keys, total, counts


def compare_previous_rows(previous_keys: set, selected: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    selected_keys = {(r["market"], r["date"], str(r["code"])) for r in selected}
    rows = []
    for key in sorted(previous_keys - selected_keys):
        rows.append({"market": key[0], "date": key[1], "code": key[2],
                     "status": "PREVIOUS_V487520_ROW_NOT_IN_SELECTED_EXACT_SOURCE_GROUP"})
    for key in sorted(selected_keys - previous_keys):
        rows.append({"market": key[0], "date": key[1], "code": key[2],
                     "status": "SELECTED_SOURCE_KEY_NOT_IN_PREVIOUS_V487520_OUTPUT"})
    return rows


def self_test() -> None:
    sample = {
        "證券代號": "2330", "證券名稱": "台積電", "開盤價": "100", "最高價": "102",
        "最低價": "99", "收盤價": "101", "成交股數": "100000", "漲跌(+/-)": "+",
        "暫停交易註記": "", "牛熊證觸及限制價格註記": "", "備註": "盤後補充註記",
        "交易日期": "2026-10-29",
    }
    row = normalize_record(sample, "TWSE", "raw_json/TWSE_2026-10-29.json", "SELFTEST.raw.json", "JSON")
    assert row is not None and row["date"] == "2026-10-29" and row["change_sign"] == "+"
    assert row["change_sign_field"] == "漲跌(+/-)"
    assert row["official_note"] == "盤後補充註記"
    assert row["note_status"] == "OFFICIAL_DESCRIPTIVE_NOTE_PRESENT"
    assert "官方備註／註記：盤後補充註記" in row["備註"]
    assert "漲跌註記：+" not in row["備註"]

    # Sign-only rows must not be misreported as having descriptive official notes.
    sign_only = {
        "證券代號": "2330", "證券名稱": "台積電", "開盤價": "100", "最高價": "102",
        "最低價": "99", "收盤價": "101", "成交股數": "100000", "漲跌(+/-)": "-",
        "交易日期": "2026-10-29",
    }
    sign_row = normalize_record(sign_only, "TWSE", "raw_json/TWSE_2026-10-29.json", "SELFTEST.sign.json", "JSON")
    assert sign_row is not None and sign_row["change_sign"] == "-"
    assert sign_row["note_status"] == "OFFICIAL_NOTE_FIELD_NOT_RETURNED"
    assert "漲跌" not in sign_row["備註"]
    sign_row_fullwidth = dict(sign_only)
    sign_row_fullwidth["漲跌(+/-)"] = "＋"
    fullwidth_row = normalize_record(sign_row_fullwidth, "TWSE", "raw_json/TWSE_2026-10-29.json", "SELFTEST.fullwidth.json", "JSON")
    assert fullwidth_row is not None and fullwidth_row["change_sign"] == "＋"

    # A present but empty explicit 備註 field is not the same as a missing field.
    blank_note = dict(sign_only); blank_note["備註"] = ""
    blank_row = normalize_record(blank_note, "TWSE", "raw_json/TWSE_2026-10-29.json", "SELFTEST.blank.json", "JSON")
    assert blank_row is not None and blank_row["note_status"] == "OFFICIAL_NOTE_FIELDS_EMPTY"
    assert "欄位存在" in blank_row["備註"]

    # A descriptive change annotation must remain a textual note.
    descriptive_change = dict(sign_only)
    descriptive_change.pop("漲跌(+/-)")
    descriptive_change["漲跌註記"] = "除權息"
    text_row = normalize_record(descriptive_change, "TWSE", "raw_json/TWSE_2026-10-29.json", "SELFTEST.text.json", "JSON")
    assert text_row is not None and text_row["note_status"] == "OFFICIAL_DESCRIPTIVE_NOTE_PRESENT"
    assert "漲跌註記：除權息" in text_row["備註"]

    tpex = {"代號": "3105", "名稱": "穩懋", "開盤": "100", "最高": "102", "最低": "99",
            "收盤": "101", "成交股數": "3000", "漲跌": "1.00", "交易日期": "2026-10-29"}
    row2 = normalize_record(tpex, "TPEX", "raw_json/TPEX_2026-10-29.json", "SELFTEST.tpex.json", "JSON")
    assert row2 is not None and row2["change_sign"] == "" and row2["change"] == 1.0
    assert row2["note_status"] == "OFFICIAL_NOTE_FIELD_NOT_RETURNED"

    # One exact source is selected; a second source with extra rows is never unioned into the output.
    base = dict(row); base.update({"market": "TWSE", "date": "2026-10-29", "code": "2330", "source_member": "raw_json/one.json", "source_format": "JSON"})
    exact2 = dict(base); exact2.update({"code": "2317"})
    extra = dict(base); extra.update({"code": "9999", "source_member": "derived/aggregate.csv", "source_format": "CSV"})
    test_key = ("TWSE", "2026-10-29")
    original_expected = EXPECTED_COUNTS[test_key]
    EXPECTED_COUNTS[test_key] = 2
    try:
        selected, audit, keyaudit = select_authoritative_source_groups([base, exact2, extra])
        summary = next(x for x in audit if x.get("decision") != "CANDIDATE_DETAIL" and x.get("date") == "2026-10-29" and x.get("market") == "TWSE")
        assert len(selected) == 2 and summary["selected_count_exact"]
        assert summary["selected_source"] == "raw_json/one.json"
    finally:
        EXPECTED_COUNTS[test_key] = original_expected

    assert parse_date("115/10/29") == "2026-10-29"
    with tempfile.TemporaryDirectory() as td:
        fake_root = Path(td)
        fake_folder = fake_root / "看股最速適_v4.87.5.20_九日雙市場備註欄位統一與程式封存_20261107_084621"
        fake_folder.mkdir()
        fake_csv = fake_folder / "九日雙市場行情_備註欄位統一.csv"
        fake_csv.write_text("market,date,code,source_member,note_status\n", encoding="utf-8-sig")
        found_folder, found_csv = discover_v420(fake_root)
        assert found_folder == fake_folder and found_csv == fake_csv
    assert isinstance(getattr(main, "__code__", None).co_filename, str)


def main() -> None:
    print("=" * 116, flush=True)
    log(f"看股最速適 {VERSION}｜九日雙市場行情單一來源選擇、文字備註與漲跌符號分流核驗")
    log("針對 v4.87.5.20 超額列數根因重建來源組；每個市場／日期只選一個完整來源檔，不把多份來源跨檔聯集")
    log("Drive 原始來源唯讀；不重新下載行情；不合併歷史主表；不執行 29 因子矩陣／模型／回測／選股")
    self_test()

    previous20_folder, previous20_csv = discover_v420()
    previous19_folder, previous19_zip = discover_v419()
    log(f"v4.87.5.20 前版逐筆表：{previous20_csv}")
    log(f"v4.87.5.19 封存 ZIP：{previous19_zip}｜SHA256={sha256_file(previous19_zip)}")

    # Scan the v4.87.5.16 ZIP nested in the verified v4.87.5.19 container.
    with zipfile.ZipFile(previous19_zip, "r") as outer:
        bad = outer.testzip()
        if bad:
            raise RuntimeError(f"v4.87.5.19 ZIP CRC 失敗：{bad}")
        outer_names = outer.namelist()
        nested_name = next((n for n in outer_names if n.lower().endswith(".zip") and "v4.87.5.16" in n), None)
        if not nested_name:
            nested_name = next((n for n in outer_names if n.lower().endswith(".zip") and "previous_evidence" in n.lower()), None)
        if not nested_name:
            raise RuntimeError("v4.87.5.19 ZIP 內找不到 v4.87.5.16 前版行情 ZIP")
        nested_bytes = outer.read(nested_name)
        retry_csv_rows, retry_csv_inventory = collect_v519_retry_rows(previous19_folder)
        if not retry_csv_rows:
            for name in outer_names:
                if name.endswith(".csv") and "補抓行情" in name:
                    parsed, meta = read_csv_records(outer.read(name), name, "TWSE", f"v4.87.5.19!{name}")
                    retry_csv_rows.extend(parsed)
                    retry_csv_inventory.append({"archive": "v4.87.5.19 ZIP", **meta})
        retry_json_rows, retry_json_inventory = scan_v519_raw_json(outer, outer_names)
    log(f"v4.87.5.19 補抓 CSV 候選列：{len(retry_csv_rows):,}；原始 JSON 候選列：{len(retry_json_rows):,}")

    with zipfile.ZipFile(io.BytesIO(nested_bytes), "r") as old_zip:
        old_records, old_inventory = scan_zip(old_zip, "v4.87.5.16")
    log(f"v4.87.5.16 舊封存候選列：{len(old_records):,}；候選來源檔：{len(old_inventory):,}")

    # Combine candidates only for selection diagnostics; output takes exactly one full source per market/date.
    all_candidates = old_records + retry_csv_rows + retry_json_rows
    log(f"全部來源候選列（尚未去重）：{len(all_candidates):,}")
    selected, candidate_audit, key_audit = select_authoritative_source_groups(all_candidates)
    day_audit, note_coverage, sign_audit = build_audits(selected)
    expected_total = sum(EXPECTED_COUNTS.values())
    day_pass = all(r["actual_unique_rows"] == r["expected_rows"] for r in day_audit)
    source_decisions_pass = all(r.get("decision") == "EXACT_SOURCE_SELECTED" for r in candidate_audit if r.get("decision") != "CANDIDATE_DETAIL")
    keysets_pass = not any(r.get("exact_source_keyset_conflict") for r in candidate_audit if r.get("decision") != "CANDIDATE_DETAIL")
    all_pass = len(selected) == expected_total and day_pass and source_decisions_pass and keysets_pass
    log(f"單一來源選擇後：{len(selected):,}/{expected_total:,}；18組筆數={'PASS' if day_pass else 'PARTIAL'}；來源鍵集合={'PASS' if keysets_pass else 'CONFLICT'}")

    previous_keys, previous_total, previous_market_counts = load_previous_v420_keys(previous20_csv)
    delta_rows = compare_previous_rows(previous_keys, selected)
    log(f"v4.87.5.20 舊表列數：{previous_total:,}；本版來源選擇差異鍵：{len(delta_rows):,}")

    timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    out = ROOT / f"看股最速適_v4.87.5.23_九日雙市場單一來源校正與備註稽核_{timestamp}"
    out.mkdir(parents=True, exist_ok=False)
    write_csv(out / "21_來源組合精確筆數選擇稽核.csv", candidate_audit,
              ["date", "market", "expected_rows", "candidate_source_groups", "exact_count_source_groups",
               "selected_source", "selected_unique_rows", "selected_count_exact", "exact_source_keyset_conflict",
               "selected_source_format", "candidate_source", "candidate_unique_rows", "candidate_format",
               "candidate_has_raw_json", "candidate_descriptive_note_rows", "candidate_is_selected", "decision"])
    write_csv(out / "22_精確筆數來源自然鍵集合比較.csv", key_audit,
              ["date", "market", "selected_source", "candidate_source", "candidate_unique_rows",
               "same_natural_key_set_as_selected", "selected_source_sha_scope"])
    write_csv(out / "23_九日雙市場行情_單一來源校正版.csv", selected, OUTPUT_COLUMNS)
    write_csv(out / "24_18組市場日期與資料筆數核對.csv", day_audit,
              ["date", "market", "expected_rows", "actual_unique_rows", "row_count_pass", "rows_with_official_note",
               "rows_note_fields_exist_but_empty", "rows_note_field_not_returned", "rows_note_preserved_unverified", "note_coverage_definition"])
    write_csv(out / "25_18組官方文字備註欄位覆蓋統計.csv", note_coverage,
              ["date", "market", "rows", "source_note_fields_observed", "source_descriptive_note_fields_observed",
               "official_descriptive_note_rows", "official_note_fields_empty_rows", "no_descriptive_note_field_rows",
               "unverified_preserved_note_rows", "direction_sign_rows"])
    write_csv(out / "28_18組漲跌方向符號分布.csv", sign_audit,
              ["date", "market", "direction_sign", "rows", "rows_total"])
    write_csv(out / "26_v487520修正前後自然鍵差異.csv", delta_rows,
              ["market", "date", "code", "status"])
    write_csv(out / "27_封存來源檔案盤點.csv", old_inventory + retry_csv_inventory + retry_json_inventory,
              ["archive", "member", "format", "rows", "reason", "bytes", "headers", "nested_files"])

    statuses = {status: sum(1 for r in selected if r.get("note_status") == status)
                for status in ("OFFICIAL_DESCRIPTIVE_NOTE_PRESENT", "OFFICIAL_NOTE_FIELDS_EMPTY",
                               "OFFICIAL_NOTE_FIELD_NOT_RETURNED", "SOURCE_NOTE_PRESERVED_UNVERIFIED")}
    direction_sign_counts = {sign: sum(1 for r in selected if r.get("change_sign") == sign)
                             for sign in sorted(DIRECTION_SIGN_VALUES)}
    summary_lines = [
        f"看股最速適 {VERSION}｜九日雙市場行情單一來源選擇、文字備註與漲跌符號分流核驗",
        f"執行時間：{dt.datetime.now().isoformat(timespec='seconds')}",
        f"前版 v4.87.5.20 逐筆表：{previous20_csv}",
        f"v4.87.5.20 原表列數：{previous_total:,}；本版單一來源選定列數：{len(selected):,}；預期：{expected_total:,}",
        f"舊封存候選列：{len(old_records):,}；v4.87.5.19 CSV 候選列：{len(retry_csv_rows):,}；v4.87.5.19 原始 JSON 候選列：{len(retry_json_rows):,}",
        f"18 組市場日期筆數 PASS：{day_pass}；每組選定來源筆數精確：{source_decisions_pass}；同筆數來源鍵集合無衝突：{keysets_pass}",
        f"整體狀態：{'PASS' if all_pass else 'PARTIAL/FAILED'}；不會以任意截斷補足筆數。",
        f"官方文字備註狀態（不含漲跌符號）：{json.dumps(statuses, ensure_ascii=False)}",
        f"漲跌方向符號筆數：{json.dumps(direction_sign_counts, ensure_ascii=False)}",
        "選擇原則：每一市場／日期只採一個來源檔案的完整自然鍵集合；原始 JSON 優先；不將不同來源的非重疊資料任意聯集。",
        "若同筆數來源的自然鍵集合互相衝突，或無來源剛好符合官方預期列數，保留衝突報告並標示未通過。",
        "v4.87.5.20 的超額列不直接刪除原始資料；本版僅另產生校正版候選與逐筆差異表。",
        "Drive 原始來源唯讀；不重新下載 API；不合併歷史主表；不輸出 29 因子矩陣；不執行模型、回測或選股。",
    ]
    summary_path = out / "執行摘要.txt"
    summary_path.write_text("\n".join(summary_lines) + "\n", encoding="utf-8")
    manifest = {
        "version": VERSION, "timestamp": timestamp, "previous_v420_folder": str(previous20_folder),
        "previous_v420_csv": str(previous20_csv), "previous_v420_rows": previous_total,
        "old_archive_candidate_rows": len(old_records), "retry_csv_candidate_rows": len(retry_csv_rows),
        "retry_raw_json_candidate_rows": len(retry_json_rows), "selected_rows": len(selected),
        "expected_rows": expected_total, "day_counts_pass": day_pass,
        "source_decisions_pass": source_decisions_pass, "keysets_pass": keysets_pass,
        "overall_pass": all_pass, "descriptive_note_status_counts": statuses,
        "direction_sign_counts": direction_sign_counts,
        "drive_source_read_only": True, "new_api_calls": False, "historical_master_merged": False,
        "model_backtest_stock_selection_run": False,
    }
    write_json(out / "執行中繼資料.json", manifest)

    script_path = resolve_script_source()
    if script_path is None:
        raise RuntimeError("無法定位目前執行中的 .py；請以下載的實體檔案 %run 執行，本次停止 ZIP/GitHub 發布以免遺漏完整程式。")
    script_copy = out / SCRIPT_BASENAME
    shutil.copy2(script_path, script_copy)
    script_sha = sha256_file(script_copy)
    if script_sha != sha256_file(script_path):
        raise RuntimeError("完整程式複製前後 SHA256 不一致")
    manifest["script_archive_name"] = script_copy.name
    manifest["script_size_bytes"] = script_copy.stat().st_size
    manifest["script_sha256"] = script_sha
    write_json(out / "執行中繼資料.json", manifest)
    log(f"完整程式封存 PASS：{script_copy.name}｜{script_copy.stat().st_size:,} bytes｜SHA256={script_sha}")

    zip_path = out / f"SSO_v4.87.5.23_note_semantics_audit_{timestamp}.zip"
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
        for p in sorted(out.rglob("*")):
            if p.is_file() and p != zip_path:
                zf.write(p, p.relative_to(out).as_posix())
    with zipfile.ZipFile(zip_path, "r") as zf:
        bad = zf.testzip()
        if bad:
            raise RuntimeError(f"新版 ZIP CRC 失敗：{bad}")
    log(f"ZIP CRC PASS：{zip_path.name}｜{zip_path.stat().st_size:,} bytes｜SHA256={sha256_file(zip_path)}")

    try:
        gh = github_upload(zip_path, summary_path, script_copy, timestamp)
        write_json(out / "GitHub上傳核驗.json", gh)
        log(f"GitHub audit branch PASS：{gh['branch']}｜commit={gh['commit']}")
        log("GitHub ZIP／摘要／完整程式遠端 SHA256 全數 PASS")
        log(f"GitHub main 前後一致：{gh['main_before']}")
    except Exception as exc:
        gh = {"status": "FAILED", "error": f"{type(exc).__name__}: {exc}"}
        write_json(out / "GitHub上傳核驗.json", gh)
        log(f"GitHub 上傳 FAILED：{gh['error']}")

    log(f"最終校正版行情列數：{len(selected):,}/{expected_total:,}｜整體={ 'PASS' if all_pass else 'PARTIAL/FAILED' }")
    log(f"v4.87.5.20 修正前後自然鍵差異：{len(delta_rows):,}")
    log(f"逐筆表：{out / '23_九日雙市場行情_單一來源校正版.csv'}")
    log(f"來源選擇稽核：{out / '21_來源組合精確筆數選擇稽核.csv'}")
    log(f"文字備註覆蓋統計：{out / '25_18組官方文字備註欄位覆蓋統計.csv'}")
    log(f"漲跌方向符號統計：{out / '28_18組漲跌方向符號分布.csv'}")
    log(f"程式：{script_copy}")
    log(f"ZIP：{zip_path}")
    log(f"GitHub：{gh.get('status')}｜{gh.get('branch', gh.get('error', ''))}")
    log("原始資料唯讀；新校正版獨立保存；未合併歷史主表；29 因子矩陣／模型／回測／選股均未執行")
    print("=" * 116, flush=True)


if __name__ == "__main__":
    print("=" * 112, flush=True)
    log(f"Colab 執行入口已啟動：{VERSION}")
    try:
        main()
    except KeyboardInterrupt:
        log("使用者中斷；已完成的獨立輸出保留，來源檔未修改。")
        raise
    except Exception as exc:
        log(f"程式執行失敗：{type(exc).__name__}: {exc}")
        traceback.print_exc()
        raise
