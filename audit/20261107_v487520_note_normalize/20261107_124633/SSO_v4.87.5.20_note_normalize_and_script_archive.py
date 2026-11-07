# -*- coding: utf-8 -*-
"""看股最速適 v4.87.5.20｜九日雙市場備註欄位統一與程式封存

本版只讀取 v4.87.5.19 成果 ZIP 及其中封存的 v4.87.5.16 ZIP，
不重新呼叫行情 API、不修改原始資料、不合併至歷史主表。
從舊封存的標準化 CSV／原始 JSON 重新擷取可辨識的官方註記，
輸出固定欄位的九交易日雙市場逐筆表與註記欄位覆蓋稽核。
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
import traceback
import zipfile
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Sequence, Tuple

VERSION = "v4.87.5.20"
REPO = "CHLINX/sso"
ROOT = Path("/content/drive/MyDrive/看股最速適")
PREVIOUS_PREFIX = "看股最速適_v4.87.5.19_官方註記欄位補正與TWSE補抓_"
SCRIPT_BASENAME = "SSO_v4.87.5.20_note_normalize_and_script_archive.py"
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
    "change", "change_note", "suspension_note", "limit_touch_note", "official_note",
    "source_note_fields", "note_status", "備註", "volume_shares", "trading_value",
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
        # On normalized rows 備註 is already a derived presentation column. Only use the
        # explicit official_note/source_note payload fields to avoid reclassifying status text.
        "official_note": note_value(source_row, ["official_note", "source_note"] if normalized_marker else ALIASES["official_note"]),
    }
    if not raw_notes["official_note"][1] and not normalized_marker:
        # A generic source column literally named 註記 is valid, but do not let this
        # short alias accidentally match longer fields such as 漲跌註記.
        exact_note_key = next((k for k in source_row if norm_header(k) == norm_header("註記")), None)
        if exact_note_key is not None:
            raw_notes["official_note"] = (clean_text(source_row.get(exact_note_key)), exact_note_key)
    # Some TWSE tables name the separate sign column simply 漲跌(+/-); it is mapped above.
    # A plain 漲跌 column is used as a sign only when the value is a symbol, never a numeric delta.
    if not raw_notes["change_note"][1]:
        k = find_key(source_row, ["漲跌"])
        v = clean_text(source_row.get(k)) if k else ""
        if v in {"+", "-", "X", "＋", "－"}:
            raw_notes["change_note"] = (v, k)
    declared_source_fields = clean_text(source_row.get("source_note_fields"))
    if normalized_marker:
        present_fields = [x.strip() for x in declared_source_fields.split("；") if x.strip()]
    else:
        present_fields = [v[1] for v in raw_notes.values() if v[1] is not None]
    def valid_note(field: str, value: str) -> bool:
        if not value or value in {"--", "N/A", "無", "無註記"}:
            return False
        # In the official TWSE change-sign field, '-' is a real downward sign, not a blank marker.
        if field == "change_note" and value in {"+", "-", "X", "＋", "－"}:
            return True
        return value != "-"
    parts = []
    for field, label in (("change_note", "漲跌註記"), ("suspension_note", "暫停交易註記"),
                         ("limit_touch_note", "牛熊證觸及限制價格註記"), ("official_note", "官方備註／註記")):
        value = raw_notes[field][0]
        if valid_note(field, value):
            parts.append(f"{label}：{value}")
    previous_status = clean_text(source_row.get("note_status"))
    previous_note = clean_text(source_row.get("備註") or source_row.get("note"))
    unverified_csv_source = (source_format.upper() in {"CSV", "CSV.GZ"} and not normalized_marker)
    if parts and unverified_csv_source:
        note_status = "SOURCE_NOTE_PRESERVED_UNVERIFIED"
        note = "；".join(parts) + "；來源為封存 CSV，未能由原始 JSON 證實欄位對應"
    elif parts:
        note_status = "OFFICIAL_NOTE_PRESENT"
        note = "；".join(parts)
    elif previous_status in {"OFFICIAL_NOTE_PRESENT", "OFFICIAL_NOTE_FIELDS_EMPTY", "OFFICIAL_NOTE_FIELD_NOT_RETURNED", "SOURCE_NOTE_PRESERVED_UNVERIFIED"}:
        # A normalized row may already contain derived 備註 text and blank component columns.
        # Preserve its explicit source status rather than counting helper columns as source fields.
        note_status = previous_status
        note = previous_note or "官方原始回應未提供可辨識註記"
    elif present_fields and unverified_csv_source:
        note_status = "SOURCE_NOTE_PRESERVED_UNVERIFIED"
        note = "來源 CSV 有備註／註記欄位，但無法由原始 JSON 證實欄位對應"
    elif present_fields:
        note_status = "OFFICIAL_NOTE_FIELDS_EMPTY"
        note = "官方註記欄位存在，但本筆沒有非空註記值"
    else:
        note_status = "OFFICIAL_NOTE_FIELD_NOT_RETURNED"
        note = "官方原始回應未提供可辨識的備註／註記欄位"
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
        "suspension_note": raw_notes["suspension_note"][0],
        "limit_touch_note": raw_notes["limit_touch_note"][0],
        "official_note": raw_notes["official_note"][0],
        "source_note_fields": "；".join(str(x) for x in present_fields),
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
    if row.get("note_status") == "OFFICIAL_NOTE_PRESENT":
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
        "OFFICIAL_NOTE_PRESENT": "官方有提供註記值",
        "OFFICIAL_NOTE_FIELDS_EMPTY": "官方有註記欄位但本筆空值",
        "OFFICIAL_NOTE_FIELD_NOT_RETURNED": "來源未提供可辨識註記欄位",
        "SOURCE_NOTE_PRESERVED_UNVERIFIED": "保留既有備註但來源欄位待核",
    }.get(status, "狀態未分類")


def build_audits(records: Sequence[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    day_rows = []
    coverage = []
    for date in TRADE_DATES:
        for market in ("TWSE", "TPEX"):
            group = [r for r in records if r["date"] == date and r["market"] == market]
            counts: Dict[str, int] = {}
            for r in group:
                counts[r["note_status"]] = counts.get(r["note_status"], 0) + 1
            expected = EXPECTED_COUNTS[(market, date)]
            day_rows.append({
                "date": date, "market": market, "expected_rows": expected, "actual_unique_rows": len(group),
                "row_count_pass": len(group) == expected,
                "rows_with_official_note": counts.get("OFFICIAL_NOTE_PRESENT", 0),
                "rows_note_fields_exist_but_empty": counts.get("OFFICIAL_NOTE_FIELDS_EMPTY", 0),
                "rows_note_field_not_returned": counts.get("OFFICIAL_NOTE_FIELD_NOT_RETURNED", 0),
                "rows_note_preserved_unverified": counts.get("SOURCE_NOTE_PRESERVED_UNVERIFIED", 0),
                "note_coverage_definition": "只計來源欄位實際提供且非空的註記；狀態文字不算官方註記",
            })
            all_fields = sorted({x for r in group for x in str(r.get("source_note_fields") or "").split("；") if x})
            coverage.append({
                "date": date, "market": market, "rows": len(group),
                "source_note_fields_observed": "；".join(all_fields),
                "official_note_value_rows": counts.get("OFFICIAL_NOTE_PRESENT", 0),
                "official_note_fields_empty_rows": counts.get("OFFICIAL_NOTE_FIELDS_EMPTY", 0),
                "no_note_field_rows": counts.get("OFFICIAL_NOTE_FIELD_NOT_RETURNED", 0),
                "unverified_preserved_note_rows": counts.get("SOURCE_NOTE_PRESERVED_UNVERIFIED", 0),
            })
    return day_rows, coverage


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
                            "X-GitHub-Api-Version": "2022-11-28", "User-Agent": "SSO-v4.87.5.20-audit"})
    api = f"https://api.github.com/repos/{REPO}"
    main_before = github_json(session, "GET", api + "/branches/main")["commit"]["sha"]
    commit_info = github_json(session, "GET", api + f"/git/commits/{main_before}")
    base_tree = commit_info["tree"]["sha"]
    branch = f"audit/v487520-note-normalize-{timestamp}"
    folder = f"audit/20261107_v487520_note_normalize/{timestamp}"
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
        "message": "audit: normalize official note columns across nine TWSE/TPEx sessions",
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


def self_test() -> None:
    sample = {
        "證券代號": "2330", "證券名稱": "台積電", "開盤價": "100", "最高價": "102",
        "最低價": "99", "收盤價": "101", "成交股數": "100000", "漲跌(+/-)": "+",
        "暫停交易註記": "", "牛熊證觸及限制價格註記": "", "備註": "盤後補充註記",
    }
    row = normalize_record(sample, "TWSE", "2026-10-29", "SELFTEST", "DICT")
    assert row is not None
    assert row["date"] == "2026-10-29" and row["change_note"] == "+"
    assert row["official_note"] == "盤後補充註記"
    assert row["note_status"] == "OFFICIAL_NOTE_PRESENT"
    assert "漲跌註記：+" in row["備註"] and "官方備註／註記：盤後補充註記" in row["備註"]
    sample_down = dict(sample)
    sample_down["漲跌(+/-)"] = "-"
    down_row = normalize_record(sample_down, "TWSE", "2026-10-29", "SELFTEST", "DICT")
    assert down_row is not None and down_row["change_note"] == "-" and "漲跌註記：-" in down_row["備註"]
    # TPEx numeric 漲跌 must not be mistaken as an explanatory note.
    tpex = {"代號": "3105", "名稱": "穩懋", "開盤": "100", "最高": "102", "最低": "99",
            "收盤": "101", "成交股數": "3000", "漲跌": "1.00"}
    row2 = normalize_record(tpex, "TPEX", "2026-10-29", "SELFTEST", "DICT")
    assert row2 is not None and row2["change_note"] == "" and row2["change"] == 1.0
    assert row2["note_status"] == "OFFICIAL_NOTE_FIELD_NOT_RETURNED"
    prior_normalized = {"market": "TWSE", "date": "2026-10-29", "code": "2330", "open": 100,
                        "high": 102, "low": 99, "close": 101, "change_note": "",
                        "suspension_note": "", "limit_touch_note": "", "official_note": "",
                        "note_status": "OFFICIAL_NOTE_FIELD_NOT_RETURNED",
                        "備註": "官方原始回應未提供可辨識的備註／註記欄位"}
    row3 = normalize_record(prior_normalized, "TWSE", "2026-10-29", "SELFTEST", "CSV")
    assert row3 is not None and row3["note_status"] == "OFFICIAL_NOTE_FIELD_NOT_RETURNED"
    normalized_tpex = {"market": "TPEX", "date": "2026-10-29", "code": "3105", "open": 100,
                       "high": 102, "low": 99, "close": 101, "change": 1.0,
                       "change_note": "", "suspension_note": "", "limit_touch_note": "",
                       "official_note": "", "source_note_fields": "", "note_status": "OFFICIAL_NOTE_FIELD_NOT_RETURNED",
                       "備註": "官方原始回應未提供可辨識的備註／註記欄位"}
    row4 = normalize_record(normalized_tpex, "TPEX", "2026-10-29", "SELFTEST", "CSV")
    assert row4 is not None and row4["note_status"] == "OFFICIAL_NOTE_FIELD_NOT_RETURNED"
    # ROC date handling.
    assert parse_date("115/10/29") == "2026-10-29"
    tpex_legacy = {"代號": "3105", "名稱": "穩懋", "開盤": "100", "最高": "102", "最低": "99",
                   "收盤": "101", "成交仟股": "2,500", "成交仟元": "250,000"}
    tpex_legacy_row = normalize_record(tpex_legacy, "TPEX", "2026-10-29", "raw.json", "JSON")
    assert tpex_legacy_row is not None and tpex_legacy_row["volume_shares"] == 2500000
    assert tpex_legacy_row["trading_value"] == 250000000
    # Raw JSON should be authoritative and recognize the exact TWSE sign field.
    raw_payload = {"tables": [{"title": "每日收盤行情",
        "fields": ["證券代號", "證券名稱", "開盤價", "最高價", "最低價", "收盤價", "漲跌(+/-)", "暫停交易註記"],
        "data": [["2330", "台積電", "100", "102", "99", "101", "-", ""]]}]}
    raw_rows, _ = read_json_records(json.dumps(raw_payload, ensure_ascii=False).encode("utf-8"),
                                    "raw_json/TWSE_2026-10-29.json", "TWSE", "SELFTEST.raw.json")
    assert raw_rows and raw_rows[0]["note_status"] == "OFFICIAL_NOTE_PRESENT"
    assert raw_rows[0]["change_note"] == "-" and raw_rows[0]["source_note_fields"] == "漲跌(+/-)；暫停交易註記"
    csv_sample = {"market": "TWSE", "date": "2026-10-29", "code": "2330", "name": "台積電",
                  "open": 100, "high": 102, "low": 99, "close": 101,
                  "change_note": "", "note_status": "OFFICIAL_NOTE_FIELD_NOT_RETURNED",
                  "source_note_fields": "", "備註": "官方原始回應未提供可辨識的備註／註記欄位"}
    csv_row = normalize_record(csv_sample, "TWSE", "2026-10-29", "SELFTEST.csv", "CSV")
    merged, _ = dedupe_records([csv_row, raw_rows[0]])
    assert merged[0]["source_format"] == "JSON" and merged[0]["note_status"] == "OFFICIAL_NOTE_PRESENT"
    # Source path resolver must not require a direct __file__ global.
    assert isinstance(getattr(main, "__code__", None).co_filename, str)


def main() -> None:
    print("=" * 112, flush=True)
    log(f"看股最速適 {VERSION}｜九日雙市場官方註記欄位統一與完整程式封存")
    log("只讀取 v4.87.5.19 與其封存 v4.87.5.16；不重新下載行情、不修改原始資料、不合併歷史主表")
    self_test()
    previous_folder, latest_zip = discover_v419()
    with zipfile.ZipFile(latest_zip, "r") as zf:
        bad = zf.testzip()
        if bad:
            raise RuntimeError(f"v4.87.5.19 ZIP CRC 失敗：{bad}")
        outer_names = zf.namelist()
        inner_zip_name = next((n for n in outer_names if n.lower().endswith(".zip") and "v4.87.5.16" in n), None)
        if not inner_zip_name:
            # Some builds use a generic evidence filename; discover one nested ZIP besides the output itself.
            inner_zip_name = next((n for n in outer_names if n.lower().endswith(".zip") and "previous_evidence" in n.lower()), None)
        if not inner_zip_name:
            raise RuntimeError("v4.87.5.19 ZIP 內找不到 v4.87.5.16 前版行情封存 ZIP")
        inner_bytes = zf.read(inner_zip_name)
        retry_rows, retry_inventory = collect_v519_retry_rows(previous_folder)
        if not retry_rows:
            # Read the retry CSV from the archive if it was not retained as a loose file.
            for name in outer_names:
                if name.endswith(".csv") and "補抓行情" in name:
                    parsed, meta = read_csv_records(zf.read(name), name, "TWSE", f"v4.87.5.19!{name}")
                    retry_rows.extend(parsed)
                    retry_inventory.append({"archive": "v4.87.5.19 ZIP", **meta})
        retry_csv_candidate_rows = len(retry_rows)
        retry_raw_rows, retry_raw_inventory = scan_v519_raw_json(zf, outer_names)
        retry_rows.extend(retry_raw_rows)
        retry_inventory.extend(retry_raw_inventory)
        log(f"v4.87.5.19 ZIP SHA256：{sha256_file(latest_zip)}")
        log(f"v4.87.5.19 CSV候選列：{retry_csv_candidate_rows:,}；原始 JSON 重新解析候選列：{len(retry_raw_rows):,}")
        log(f"已定位前版封存：{inner_zip_name}｜{len(inner_bytes):,} bytes")
    with zipfile.ZipFile(io.BytesIO(inner_bytes), "r") as old_zip:
        old_records, old_inventory = scan_zip(old_zip, "v4.87.5.16")
    log(f"前版封存可解析行情候選列：{len(old_records):,}；本版補抓檔解析列：{len(retry_rows):,}")
    records, collisions = dedupe_records(old_records + retry_rows)
    log(f"自然鍵去重後：{len(records):,} 筆；來源重複候選：{len(collisions):,} 組")

    timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    out = ROOT / f"看股最速適_v4.87.5.20_九日雙市場備註欄位統一與程式封存_{timestamp}"
    out.mkdir(parents=True, exist_ok=False)
    inventory_rows = old_inventory + retry_inventory
    write_csv(out / "封存檔行情來源盤點.csv", inventory_rows,
              ["archive", "member", "format", "rows", "reason", "bytes", "headers", "nested_files"])
    write_csv(out / "九日雙市場行情_備註欄位統一.csv", records, OUTPUT_COLUMNS)
    write_csv(out / "來源候選重複自然鍵處理.csv", collisions,
              ["market", "date", "code", "selected_source", "discarded_source", "reason"])
    day_audit, note_coverage = build_audits(records)
    write_csv(out / "18組市場日期與資料筆數核對.csv", day_audit,
              ["date", "market", "expected_rows", "actual_unique_rows", "row_count_pass", "rows_with_official_note",
               "rows_note_fields_exist_but_empty", "rows_note_field_not_returned", "rows_note_preserved_unverified", "note_coverage_definition"])
    write_csv(out / "18組官方備註欄位覆蓋統計.csv", note_coverage,
              ["date", "market", "rows", "source_note_fields_observed", "official_note_value_rows",
               "official_note_fields_empty_rows", "no_note_field_rows", "unverified_preserved_note_rows"])

    expected_total = sum(EXPECTED_COUNTS.values())
    market_pairs_pass = len(records) == expected_total and all(r["row_count_pass"] for r in day_audit)
    summary_lines = [
        f"看股最速適 {VERSION}｜九日雙市場官方註記欄位統一與完整程式封存",
        f"執行時間：{dt.datetime.now().isoformat(timespec='seconds')}",
        f"承接 v4.87.5.19：{latest_zip}",
        f"承接 ZIP SHA256：{sha256_file(latest_zip)}",
        f"前版 v4.87.5.16 封存內檔：{inner_zip_name}",
        f"前版 ZIP SHA256：{sha256_bytes(inner_bytes)}",
        f"舊封存解析候選列：{len(old_records):,}；v4.87.5.19 CSV候選列：{retry_csv_candidate_rows:,}；v4.87.5.19原始JSON候選列：{len(retry_raw_rows):,}；自然鍵去重後：{len(records):,}",
        f"預期逐筆總數：{expected_total:,}；18組市場日期與筆數全數相符：{market_pairs_pass}",
        "備註處理：依各來源原始欄位名稱重新擷取；官方有值、官方欄位空值、來源未提供欄位分別標示。狀態文字不冒充官方註記。",
        "TWSE 專用欄位：漲跌註記（含漲跌(+/-)符號）、暫停交易註記、牛熊證觸及限制價格註記及一般備註。",
        "TPEx 數值型漲跌僅視為價格差，不會誤列為文字備註；若官方來源沒有註記欄位，明確標示來源未提供。",
        "本版不重新下載 API、不修改舊檔、不合併歷史行情主表、不訓練模型、不回測、不選股。",
    ]
    (out / "執行摘要.txt").write_text("\n".join(summary_lines) + "\n", encoding="utf-8")
    manifest = {
        "version": VERSION, "timestamp": timestamp,
        "v419_folder": str(previous_folder), "v419_zip": str(latest_zip), "v419_zip_sha256": sha256_file(latest_zip),
        "v416_inner_zip": inner_zip_name, "v416_inner_zip_sha256": sha256_bytes(inner_bytes),
        "old_archive_candidate_rows": len(old_records),
        "v419_retry_csv_candidate_rows": retry_csv_candidate_rows,
        "v419_retry_raw_json_candidate_rows": len(retry_raw_rows),
        "v419_retry_total_source_candidate_rows": len(retry_rows),
        "deduplicated_rows": len(records), "expected_rows": expected_total,
        "market_day_pairs_pass": market_pairs_pass,
        "all_18_pairs_match_expected": all(r["row_count_pass"] for r in day_audit),
        "official_note_status_counts": {
            status: sum(1 for r in records if r["note_status"] == status)
            for status in ("OFFICIAL_NOTE_PRESENT", "OFFICIAL_NOTE_FIELDS_EMPTY", "OFFICIAL_NOTE_FIELD_NOT_RETURNED", "SOURCE_NOTE_PRESERVED_UNVERIFIED")
        },
        "source_drive_read_only": True, "new_quotes_downloaded": False,
        "merged_into_historical_master": False, "model_backtest_stock_selection_run": False,
    }
    write_json(out / "執行中繼資料.json", manifest)

    script_path = resolve_script_source()
    if script_path is None:
        raise RuntimeError("無法定位目前執行的 Python 原始檔；為確保完整程式封存，本版停止 ZIP/GitHub 發布。請以 %run /content/SSO_v4.87.5.20_note_normalize_and_script_archive.py 執行下載後的實體檔案。")
    script_copy = out / SCRIPT_BASENAME
    shutil.copy2(script_path, script_copy)
    if sha256_file(script_path) != sha256_file(script_copy):
        raise RuntimeError("完整 Python 程式複製前後 SHA256 不一致")
    log(f"完整程式封存 PASS：{script_copy.name}｜{script_copy.stat().st_size:,} bytes｜SHA256={sha256_file(script_copy)}")

    zip_path = out / f"SSO_v4.87.5.20_note_normalize_{timestamp}.zip"
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
        for p in sorted(out.rglob("*")):
            if p.is_file() and p != zip_path:
                zf.write(p, p.relative_to(out).as_posix())
    with zipfile.ZipFile(zip_path, "r") as zf:
        bad = zf.testzip()
        if bad:
            raise RuntimeError(f"新版 ZIP CRC 失敗：{bad}")
    log(f"新版 ZIP CRC PASS：{zip_path.name}｜{zip_path.stat().st_size:,} bytes｜SHA256={sha256_file(zip_path)}")

    github_result: Dict[str, Any]
    try:
        github_result = github_upload(zip_path, out / "執行摘要.txt", script_copy, timestamp)
        log(f"GitHub audit branch PASS：{github_result['branch']}｜commit={github_result['commit']}")
        log("GitHub ZIP／摘要／完整程式遠端 SHA256 全數 PASS")
        log(f"GitHub main 前後一致：{github_result['main_before']}")
    except Exception as exc:
        github_result = {"status": "FAILED", "error": f"{type(exc).__name__}: {exc}"}
        log(f"GitHub 上傳 FAILED：{github_result['error']}")
    write_json(out / "GitHub上傳核驗.json", github_result)

    # Final summary is explicit about rows and note coverage; zero official note values are not a pass.
    statuses = manifest["official_note_status_counts"]
    log(f"資料列筆數核對：{len(records):,}/{expected_total:,}｜18組日期筆數={ 'PASS' if market_pairs_pass else 'PARTIAL/FAILED' }")
    log("官方註記狀態：" + "；".join(f"{k}={v:,}" for k, v in statuses.items()))
    log(f"完整逐筆表：{out / '九日雙市場行情_備註欄位統一.csv'}")
    log(f"備註覆蓋統計：{out / '18組官方備註欄位覆蓋統計.csv'}")
    log(f"完整程式：{script_copy}")
    log(f"ZIP：{zip_path}")
    log(f"GitHub：{github_result.get('status')}｜{github_result.get('branch', github_result.get('error', ''))}")
    log("Drive 原始來源唯讀；未重新下載行情；未合併歷史主表；29因子矩陣／模型／回測／選股均未執行")
    print("=" * 112, flush=True)


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
