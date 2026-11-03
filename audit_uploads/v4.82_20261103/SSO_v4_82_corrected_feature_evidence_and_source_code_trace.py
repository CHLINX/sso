#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""看股最速適 v4.82｜修正候選證據分級、同鍵跨版本比較與原始程式追查

本版承接 v4.81 的實際報表，修正「只有欄名、零資料列卻判為找到候選」的問題。
只讀 Drive 歷史封存與程式／Notebook；不修改來源、不修改 GitHub main、不重訓、不回測。

主要工作：
1. 核驗 v2.50／v2.51 的 24 項欄位表頭，將 0 列表頭與有值資料明確分開。
2. 重新掃描 v4.66 MA20／MA60 長格式差異層，檢查重算值、原始值、差額的逐列算術關係。
3. 以訊號日期、股票代號、市場及持有期間為鍵，比較 v2.50 與兩份 v2.51 的 factor／來源20根波動因子。
4. 唯讀搜尋 Google Drive 專案及 Colab Notebooks 中的 .py／.ipynb，尋找24項特徵原始程式與排名規則。
5. 修正 ZIP 重複加入 Python 的問題，輸出報表並上傳新的 GitHub audit 分支。

Colab 執行：
!python /content/SSO_v4_82_corrected_feature_evidence_and_source_code_trace.py

GitHub Token：Colab Secrets 設定 GITHUB_TOKEN，需 CHLINX/sso Contents Read and write。
"""
from __future__ import annotations

import csv
import gzip
import hashlib
import io
import json
import math
import os
import re
import shutil
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo

VERSION = "v4.82"
REPO = "CHLINX/sso"
TZ = ZoneInfo("Asia/Taipei")
DRIVE_ROOT = Path("/content/drive/MyDrive/看股最速適")
NOTEBOOK_ROOT = Path("/content/drive/MyDrive/Colab Notebooks")
SCRIPT_FILENAME = "SSO_v4_82_corrected_feature_evidence_and_source_code_trace.py"
OUTPUT_PREFIX = "看股最速適_24項特徵證據修正與原始程式追查_v4.82_"
V479_PREFIX = "看股最速適_24項原始特徵程式與歷史數值來源追查_v4.79_"

FEATURES = [
    "ret5_rank", "ret20_rank", "ret60_rank", "dist_ma20_rank", "dist_ma60_rank",
    "range_pos20_rank", "lowvol20_rank", "source_factor_rank", "ret5_abs_scaled",
    "ret20_abs_scaled", "ret60_abs_scaled", "dist_ma20_abs_scaled", "dist_ma60_abs_scaled",
    "market_median_ret5", "market_median_ret20", "market_median_ret60",
    "market_breadth_pos_ret5", "market_breadth_pos_ret20", "market_breadth_pos_ret60",
    "market_breadth_above_ma20", "market_breadth_above_ma60",
    "ret5_rank_x_market_breadth", "ret20_rank_x_market_breadth", "ret60_rank_x_market_breadth",
]
RAW_ALIASES = {
    "dist_ma20_rank": ["收盤相對MA20", "收盤距離MA20", "距離MA20"],
    "dist_ma60_rank": ["收盤相對MA60", "收盤距離MA60", "距離MA60"],
    "source_factor_rank": ["來源20根波動因子", "factor"],
}
FEATURE_EXACT_FILE_TOKEN = "最新達標獲利候選最多10檔"
ARCHIVE_KEYWORDS = ("v4_66_", "SSO_v2.50_", "SSO_v2.51_")
TARGET_CSV_TOKENS = (
    "03_既有樣本特徵重算差異層.csv.gz",
    "02_新增星期六樣本影子OHLC與已驗證特徵及標籤.csv",
    "最新全市場候選持續評估",
    "歷史樣本外每日10檔候選",
    "最新10檔獲利可能性排行",
    "最新達標獲利候選最多10檔",
)
CODE_TERMS = (
    "ret5_rank", "ret20_rank", "ret60_rank", "dist_ma20_rank", "dist_ma60_rank",
    "range_pos20_rank", "lowvol20_rank", "source_factor_rank", "ret5_abs_scaled",
    "market_breadth", "RANK_FEATURES", "rankdata", "percentile", "quantile",
    "zscore", "abs_scaled", "rank_method", "tie_method", "feature_names",
)


def now_taipei() -> datetime:
    return datetime.now(TZ)


def log(message: str) -> None:
    print(f"[{now_taipei().strftime('%H:%M:%S')}] {message}", flush=True)


def sha256_file(path: Path, chunk_size: int = 4 * 1024 * 1024) -> str:
    h = hashlib.sha256()
    total = 0
    with path.open("rb") as f:
        while True:
            b = f.read(chunk_size)
            if not b:
                break
            h.update(b)
            total += len(b)
            if total and total % (256 * 1024 * 1024) < chunk_size:
                log(f"SHA-256進度：{path.name}；{total:,} bytes")
    return h.hexdigest()


def mount_drive() -> None:
    if not DRIVE_ROOT.exists():
        from google.colab import drive  # type: ignore
        drive.mount("/content/drive", force_remount=False)
    if not DRIVE_ROOT.exists():
        raise FileNotFoundError(f"找不到專案資料夾：{DRIVE_ROOT}")
    log(f"Google Drive 已可讀取：{DRIVE_ROOT}")


def find_latest_folder(prefix: str, required: str) -> Path:
    found = sorted((p for p in DRIVE_ROOT.glob(prefix + "*") if p.is_dir() and (p / required).is_file()),
                   key=lambda p: p.stat().st_mtime, reverse=True)
    if not found:
        raise FileNotFoundError(f"找不到 {prefix}*／{required}")
    return found[0]


def load_v479_manifest() -> Tuple[Path, Dict[str, Any]]:
    folder = find_latest_folder(V479_PREFIX, "v4.79_manifest.json")
    data = json.loads((folder / "v4.79_manifest.json").read_text(encoding="utf-8"))
    if data.get("version") != "v4.79" or not isinstance(data.get("archive_summary"), list):
        raise RuntimeError("v4.79 manifest 格式或版本不符")
    log(f"沿用 v4.79 清冊：{folder}")
    return folder, data


def locate_archive(rec: Dict[str, Any]) -> Optional[Path]:
    filename = str(rec.get("filename") or "")
    rel = str(rec.get("relative_path") or filename)
    expected_size = int(rec.get("size_bytes") or -1)
    for p in (DRIVE_ROOT / rel, Path("/content/drive/MyDrive") / rel,
              NOTEBOOK_ROOT / rel, DRIVE_ROOT / filename, NOTEBOOK_ROOT / filename):
        try:
            if p.is_file() and p.stat().st_size == expected_size:
                return p
        except OSError:
            pass
    return None


def choose_archives(manifest: Dict[str, Any]) -> List[Dict[str, Any]]:
    chosen, seen = [], set()
    for rec in manifest["archive_summary"]:
        name = str(rec.get("filename") or rec.get("relative_path") or "")
        if any(k.lower() in name.lower() for k in ARCHIVE_KEYWORDS):
            key = str(rec.get("sha256") or name)
            if key not in seen:
                chosen.append(rec)
                seen.add(key)
    if not any("v4_66_" in str(x.get("filename", "")) for x in chosen):
        raise FileNotFoundError("v4.79 清冊未找到 v4.66 核心封存")
    log(f"指定封存命中 {len(chosen)} 份：" + " | ".join(str(x.get("filename", "")) for x in chosen))
    return chosen


def verify_archive(path: Path, rec: Dict[str, Any]) -> Dict[str, Any]:
    size = path.stat().st_size
    actual_sha = sha256_file(path)
    expected_sha = str(rec.get("sha256") or "")
    expected_size = int(rec.get("size_bytes") or -1)
    result = {"filename": path.name, "size_bytes": size, "expected_size": expected_size,
              "sha256": actual_sha, "expected_sha256": expected_sha,
              "size_match": size == expected_size,
              "sha_match": (not expected_sha or actual_sha == expected_sha),
              "crc": "NOT_CHECKED", "status": "NOT_CHECKED"}
    if size != expected_size or (expected_sha and actual_sha != expected_sha):
        result["status"] = "SOURCE_CHANGED_SKIP"
        return result
    with zipfile.ZipFile(path, "r") as zf:
        bad = zf.testzip()
        result["crc"] = "PASS" if bad is None else f"FAIL:{bad}"
        result["members"] = len([x for x in zf.infolist() if not x.is_dir()])
    result["status"] = "PASS" if result["crc"] == "PASS" else "CRC_FAIL"
    return result


def zip_csv_reader(zf: zipfile.ZipFile, member: str):
    raw = zf.open(member, "r")
    if member.lower().endswith(".gz"):
        binary = gzip.GzipFile(fileobj=raw, mode="rb")
    else:
        binary = raw
    text = io.TextIOWrapper(binary, encoding="utf-8-sig", errors="replace", newline="")
    return raw, binary, text


def parse_float(value: Any) -> Optional[float]:
    try:
        x = float(str(value).strip())
        return x if math.isfinite(x) else None
    except Exception:
        return None


def norm_key(value: Any) -> str:
    return str(value or "").strip()


def get_archive_info(path: Path) -> zipfile.ZipFile:
    return zipfile.ZipFile(path, "r")


def find_member(zf: zipfile.ZipFile, token: str) -> Optional[str]:
    matches = [n for n in zf.namelist() if not n.endswith("/") and n.split("/")[-1].startswith(token)]
    if not matches:
        matches = [n for n in zf.namelist() if not n.endswith("/") and token in n.split("/")[-1]]
    return sorted(matches, key=lambda s: (len(s), s))[0] if matches else None


def read_csv_all(zf: zipfile.ZipFile, member: str) -> Tuple[List[str], List[Dict[str, str]]]:
    raw = binary = text = None
    try:
        raw, binary, text = zip_csv_reader(zf, member)
        reader = csv.DictReader(text)
        headers = reader.fieldnames or []
        rows = list(reader)
        return headers, rows
    finally:
        for obj in (text, binary, raw):
            try:
                if obj is not None:
                    obj.close()
            except Exception:
                pass


def read_feature_long_layer(zf: zipfile.ZipFile, member: str,
                            ma_stats: Dict[str, Dict[str, Any]], sample_rows: List[Dict[str, Any]]) -> int:
    raw = binary = text = None
    rows = 0
    samples_by_label = Counter()
    try:
        raw, binary, text = zip_csv_reader(zf, member)
        reader = csv.DictReader(text)
        fields = reader.fieldnames or []
        required = {"特徵欄位", "原始已存值", "重算值", "差額"}
        if not required.issubset(set(fields)):
            raise RuntimeError(f"v4.66長格式欄位不符：{fields}")
        for row in reader:
            rows += 1
            label = norm_key(row.get("特徵欄位"))
            if label not in ("收盤相對MA20", "收盤相對MA60"):
                continue
            st = ma_stats.setdefault(label, {
                "rows": 0, "old_n": 0, "recalc_n": 0, "delta_n": 0,
                "new_minus_old_match": 0, "old_minus_new_match": 0,
                "arithmetic_compared": 0, "max_abs_residual_new_minus_old": 0.0,
                "max_abs_residual_old_minus_new": 0.0,
                "segment_counts": Counter(), "market_counts": Counter(),
                "old_sum": 0.0, "recalc_sum": 0.0, "delta_sum": 0.0,
                "samples": 0,
            })
            st["rows"] += 1
            old = parse_float(row.get("原始已存值"))
            new = parse_float(row.get("重算值"))
            delta = parse_float(row.get("差額"))
            if old is not None:
                st["old_n"] += 1; st["old_sum"] += old
            if new is not None:
                st["recalc_n"] += 1; st["recalc_sum"] += new
            if delta is not None:
                st["delta_n"] += 1; st["delta_sum"] += delta
            if old is not None and new is not None and delta is not None:
                st["arithmetic_compared"] += 1
                r1 = abs(delta - (new - old))
                r2 = abs(delta - (old - new))
                st["max_abs_residual_new_minus_old"] = max(st["max_abs_residual_new_minus_old"], r1)
                st["max_abs_residual_old_minus_new"] = max(st["max_abs_residual_old_minus_new"], r2)
                if math.isclose(delta, new - old, rel_tol=1e-7, abs_tol=1e-10):
                    st["new_minus_old_match"] += 1
                if math.isclose(delta, old - new, rel_tol=1e-7, abs_tol=1e-10):
                    st["old_minus_new_match"] += 1
            segment = norm_key(row.get("價格研究區段"))
            if segment:
                st["segment_counts"][segment] += 1
            market = norm_key(row.get("市場"))
            if market:
                st["market_counts"][market] += 1
            if samples_by_label[label] < 20:
                keep = ["市場", "證券代號", "日期", "特徵欄位", "原始已存值", "重算值", "差額", "變動類型", "影響補行日", "價格研究區段"]
                sample_rows.append({k: row.get(k, "") for k in keep} | {"來源成員": member})
                samples_by_label[label] += 1
            if rows % 250000 == 0:
                log(f"v4.66長格式進度：{rows:,} rows；MA20={ma_stats.get('收盤相對MA20',{}).get('rows',0):,}；MA60={ma_stats.get('收盤相對MA60',{}).get('rows',0):,}")
        return rows
    finally:
        for obj in (text, binary, raw):
            try:
                if obj is not None:
                    obj.close()
            except Exception:
                pass


def summarize_mean(total: float, n: int) -> Any:
    return total / n if n else ""


def scan_exact_header_files(archive_paths: List[Path], archive_names: Dict[Path, str],
                            header_rows: List[Dict[str, Any]], exact_headers: Dict[str, List[Dict[str, Any]]]) -> None:
    for path in archive_paths:
        if "SSO_v2.50_" not in path.name and "SSO_v2.51_" not in path.name:
            continue
        with zipfile.ZipFile(path, "r") as zf:
            for member in zf.namelist():
                base = member.split("/")[-1]
                if not base.startswith(FEATURE_EXACT_FILE_TOKEN) or not base.lower().endswith(".csv"):
                    continue
                raw = binary = text = None
                try:
                    raw, binary, text = zip_csv_reader(zf, member)
                    reader = csv.DictReader(text)
                    headers = reader.fieldnames or []
                    count = 0
                    value_counts = Counter()
                    for row in reader:
                        count += 1
                        for feature in FEATURES:
                            if parse_float(row.get(feature)) is not None:
                                value_counts[feature] += 1
                    entry = {"archive": path.name, "member": member, "data_rows": count,
                             "column_count": len(headers), "feature_headers_present": sum(1 for x in FEATURES if x in headers),
                             "missing_feature_headers": json.dumps([x for x in FEATURES if x not in headers], ensure_ascii=False),
                             "feature_nonempty_counts": json.dumps(dict(value_counts), ensure_ascii=False)}
                    header_rows.append(entry)
                    for feature in FEATURES:
                        if feature in headers:
                            exact_headers[feature].append({"archive": path.name, "member": member,
                                                           "data_rows": count, "nonempty_values": value_counts[feature]})
                    log(f"24項欄位表核驗：{path.name}/{base}；rows={count:,}；exact_headers={entry['feature_headers_present']}/24")
                finally:
                    for obj in (text, binary, raw):
                        try:
                            if obj is not None:
                                obj.close()
                        except Exception:
                            pass


def table_key(row: Dict[str, str], table_kind: str) -> Tuple[str, ...]:
    h = norm_key(row.get("持有期間H"))
    d = norm_key(row.get("訊號日期"))
    idx = norm_key(row.get("訊號全市場日序"))
    if table_kind == "full_market":
        return (h, d, norm_key(row.get("市場")), norm_key(row.get("股票代號")))
    if table_kind == "oos":
        return (h, d, idx, norm_key(row.get("symbol")))
    return (h, d, norm_key(row.get("市場")), norm_key(row.get("股票代號")))


def scan_candidate_table(zf: zipfile.ZipFile, member: str, kind: str) -> Tuple[List[str], List[Dict[str, str]]]:
    return read_csv_all(zf, member)


def compare_tables(v250: List[Dict[str, str]], v251: List[Dict[str, str]],
                   kind: str, archive_251: str) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    key_field = "來源20根波動因子" if kind != "oos" else "factor"
    a: Dict[Tuple[str, ...], List[Dict[str, str]]] = defaultdict(list)
    b: Dict[Tuple[str, ...], List[Dict[str, str]]] = defaultdict(list)
    for r in v250:
        a[table_key(r, kind)].append(r)
    for r in v251:
        b[table_key(r, kind)].append(r)
    common = set(a) & set(b)
    equal = diff = missing_a = missing_b = dup_a = dup_b = 0
    abs_diffs: List[float] = []
    samples: List[Dict[str, Any]] = []
    for k, values in a.items():
        if len(values) > 1: dup_a += len(values) - 1
    for k, values in b.items():
        if len(values) > 1: dup_b += len(values) - 1
    for k in common:
        ra, rb = a[k][0], b[k][0]
        xa, xb = parse_float(ra.get(key_field)), parse_float(rb.get(key_field))
        if xa is None or xb is None:
            continue
        delta = xb - xa
        abs_diffs.append(abs(delta))
        if math.isclose(xa, xb, rel_tol=1e-7, abs_tol=1e-10):
            equal += 1
        else:
            diff += 1
            if len(samples) < 100:
                samples.append({"table_kind": kind, "v2.51_archive": archive_251,
                                "key": json.dumps(k, ensure_ascii=False), "v2.50_value": xa,
                                "v2.51_value": xb, "v2.51_minus_v2.50": delta})
    missing_a = len(set(a) - set(b))
    missing_b = len(set(b) - set(a))
    summary = {"table_kind": kind, "v2.51_archive": archive_251,
               "v2.50_rows": len(v250), "v2.51_rows": len(v251),
               "v2.50_unique_keys": len(a), "v2.51_unique_keys": len(b),
               "common_keys": len(common), "value_pairs_compared": equal + diff,
               "equal_values": equal, "different_values": diff,
               "v2.50_only_keys": missing_a, "v2.51_only_keys": missing_b,
               "v2.50_duplicate_key_excess": dup_a, "v2.51_duplicate_key_excess": dup_b,
               "mean_abs_value_difference": (sum(abs_diffs) / len(abs_diffs)) if abs_diffs else "",
               "max_abs_value_difference": max(abs_diffs) if abs_diffs else ""}
    return summary, samples


def extract_candidate_tables(archive_paths: List[Path], table_data: Dict[Tuple[str, str], List[Dict[str, str]]],
                             schema_rows: List[Dict[str, Any]]) -> None:
    for path in archive_paths:
        if "SSO_v2.50_" not in path.name and "SSO_v2.51_" not in path.name:
            continue
        label = "v2.50" if "SSO_v2.50_" in path.name else ("v2.51_individual" if "個股獨立評估" in path.name else "v2.51_no_volume")
        with zipfile.ZipFile(path, "r") as zf:
            specs = [
                ("full_market", "最新全市場候選持續評估"),
                ("oos", "歷史樣本外每日10檔候選"),
                ("latest10", "最新10檔獲利可能性排行"),
            ]
            for kind, token in specs:
                member = find_member(zf, token)
                if not member:
                    continue
                headers, rows = scan_candidate_table(zf, member, kind)
                table_data[(label, kind)] = rows
                schema_rows.append({"archive": path.name, "version_label": label, "table_kind": kind,
                                    "member": member, "rows": len(rows), "columns": len(headers),
                                    "header": json.dumps(headers, ensure_ascii=False)})
                log(f"候選資料表讀取：{path.name}/{member.split('/')[-1]}；{len(rows):,} rows；{len(headers)} columns")


def scan_source_code_files(roots: Sequence[Path], code_rows: List[Dict[str, Any]],
                           snippet_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    seen = set()
    scanned = matched = skipped_large = errors = 0
    ext_ok = {".py", ".ipynb", ".md"}
    total_candidates = 0
    paths: List[Path] = []
    for root in roots:
        if not root.exists():
            continue
        for current, dirs, files in os.walk(root):
            dirs[:] = [d for d in dirs if d not in {".Trash-1000", ".ipynb_checkpoints"}]
            for filename in files:
                p = Path(current) / filename
                if p.suffix.lower() in ext_ok:
                    try:
                        resolved = str(p.resolve())
                        if resolved not in seen:
                            seen.add(resolved); paths.append(p)
                    except OSError:
                        pass
    total_candidates = len(paths)
    log(f"原始程式／Notebook候選檔案：{total_candidates:,} 個")
    term_lowers = {t.lower(): t for t in CODE_TERMS}
    for i, path in enumerate(paths, 1):
        try:
            size = path.stat().st_size
            if size > 100 * 1024 * 1024:
                skipped_large += 1
                continue
            raw = path.read_text(encoding="utf-8", errors="replace")
            scanned += 1
            matched_terms = [t for tl, t in term_lowers.items() if tl in raw.lower()]
            if matched_terms:
                matched += 1
                code_rows.append({"path": str(path), "bytes": size,
                                  "modified_time": datetime.fromtimestamp(path.stat().st_mtime, TZ).isoformat(timespec="seconds"),
                                  "matched_terms": json.dumps(matched_terms, ensure_ascii=False),
                                  "term_count": len(matched_terms)})
                lines = raw.splitlines()
                added = 0
                for no, line in enumerate(lines, 1):
                    low = line.lower()
                    hits = [t for tl, t in term_lowers.items() if tl in low]
                    if hits:
                        snippet_rows.append({"path": str(path), "line_no": no,
                                             "matched_terms": json.dumps(hits, ensure_ascii=False),
                                             "excerpt": line.strip()[:700]})
                        added += 1
                        if added >= 60:
                            break
        except Exception as exc:
            errors += 1
            code_rows.append({"path": str(path), "status": f"READ_ERROR:{type(exc).__name__}:{exc}"})
        if i % 250 == 0:
            log(f"原始程式／Notebook搜尋進度：{i:,}/{total_candidates:,}；已讀 {scanned:,}；命中 {matched:,}")
    log(f"原始程式搜尋完成：候選 {total_candidates:,}；已讀 {scanned:,}；命中 {matched:,}；大檔略過 {skipped_large:,}；錯誤 {errors:,}")
    return {"candidate_files": total_candidates, "scanned_files": scanned,
            "matched_files": matched, "skipped_large_files": skipped_large, "read_errors": errors}


def write_csv(path: Path, headers: Sequence[str], rows: Iterable[Dict[str, Any]]) -> int:
    count = 0
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(headers), extrasaction="ignore")
        w.writeheader()
        for row in rows:
            w.writerow(row); count += 1
    return count


def safe_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, default=str)


def find_script_path(out_dir: Path) -> Path:
    """Locate this script; if executed from a notebook cell, restore it from IPython history."""
    candidates = []
    if "__file__" in globals():
        try:
            candidates.append(Path(__file__).resolve())
        except Exception:
            pass
    if sys.argv and sys.argv[0] and sys.argv[0] not in ("-", "<stdin>"):
        try:
            candidates.append(Path(sys.argv[0]).resolve())
        except Exception:
            pass
    candidates.extend([
        Path("/content") / SCRIPT_FILENAME,
        Path.cwd() / SCRIPT_FILENAME,
        out_dir / SCRIPT_FILENAME,
    ])
    seen = set()
    for candidate in candidates:
        if not candidate:
            continue
        try:
            resolved = candidate.resolve()
            if str(resolved) in seen:
                continue
            seen.add(str(resolved))
            if resolved.is_file() and resolved.stat().st_size > 20000:
                source = resolved.read_text(encoding="utf-8", errors="replace")
                if 'VERSION = "v4.82"' in source and "def main()" in source and "def upload_to_github(" in source:
                    return resolved
        except OSError:
            pass

    # Colab 以整段程式碼儲存格執行時，__file__ 不存在；從 Notebook 輸入歷史還原。
    try:
        ip = get_ipython()  # type: ignore[name-defined]
        history = list(getattr(ip.history_manager, "input_hist_raw", []) or [])
        if not history:
            history = list(ip.user_ns.get("In", []) or [])
        for source in reversed(history[-100:]):
            if not isinstance(source, str) or not (20000 < len(source) < 8 * 1024 * 1024):
                continue
            if ('VERSION = "v4.82"' not in source or "def main()" not in source
                    or "def upload_to_github(" not in source):
                continue
            target = out_dir / SCRIPT_FILENAME
            compile(source, str(target), "exec")
            target.write_text(source, encoding="utf-8")
            log(f"已從 Colab Notebook history 還原完整 Python：{target}")
            return target.resolve()
    except Exception as exc:
        log(f"Notebook history 還原未成功：{type(exc).__name__}: {exc}")
    raise FileNotFoundError(f"找不到或無法還原 {SCRIPT_FILENAME}；請將修正版 Python 放在 /content 後執行")


REQUIRED_REPORTS = [
    "01_24項特徵正確證據分級.csv",
    "02_24項精確特徵欄位表頭與空表核驗.csv",
    "03_MA20_MA60重算差異逐列核對摘要.csv",
    "04_MA20_MA60差異層代表資料列.csv",
    "05_v250_v251候選因子同鍵比較.csv",
    "06_v250_v251因子差異代表資料列.csv",
    "07_原始程式與Notebook檔案命中.csv",
    "08_原始程式與Notebook命中程式行.csv",
    "09_候選CSV欄位與列數.csv",
    "10_封存唯讀核驗.csv",
    "v4.82_manifest.json",
    "v4.82_執行摘要.txt",
]


def find_resumable_output_dir() -> Optional[Path]:
    """Find complete v4.82 reports left behind before packaging/upload."""
    folders = sorted(
        (x for x in DRIVE_ROOT.glob(OUTPUT_PREFIX + "*") if x.is_dir()),
        key=lambda x: x.name,
        reverse=True,
    )
    for folder in folders:
        if (folder / "v4.82_GitHub上傳回執.json").exists():
            continue
        if all((folder / name).is_file() for name in REQUIRED_REPORTS):
            return folder
    return None


def finalize_and_upload(out_dir: Path, timestamp: str) -> None:
    """Package existing reports once, validate, then upload to a fresh audit branch."""
    manifest_path = out_dir / "v4.82_manifest.json"
    manifest_out = json.loads(manifest_path.read_text(encoding="utf-8"))
    script_path = out_dir / SCRIPT_FILENAME
    if not script_path.is_file() or script_path.stat().st_size < 20000:
        source_script = find_script_path(out_dir)
        if source_script.resolve() != script_path.resolve():
            shutil.copy2(source_script, script_path)
    log(f"本次 Python 程式：{script_path}")

    recovery_note = {
        "version": "v4.82-packaging-recovery",
        "reused_existing_reports": True,
        "analysis_rerun": False,
        "reports_folder": out_dir.name,
        "recovery_at_taipei": now_taipei().isoformat(timespec="seconds"),
        "reason": "原始分析已完成；前次僅因程式檔路徑未定位而未能封裝及上傳。",
        "source_files_modified": False,
        "github_main_modified": False,
    }
    (out_dir / "v4.82_封裝續跑紀錄.json").write_text(
        json.dumps(recovery_note, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    zip_path = DRIVE_ROOT / f"{OUTPUT_PREFIX}{timestamp}.zip"
    report_files = sorted(x for x in out_dir.iterdir() if x.is_file() and x.name != SCRIPT_FILENAME)
    log(f"開始打包既有 v4.82 報表：{len(report_files)} 個報表／紀錄檔")
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
        for item in report_files:
            zf.write(item, arcname=item.name)
        zf.write(script_path, arcname=SCRIPT_FILENAME)
    with zipfile.ZipFile(zip_path, "r") as zf:
        names = zf.namelist()
        duplicates = [n for n, count in Counter(names).items() if count > 1]
        bad = zf.testzip()
        if bad or duplicates:
            raise RuntimeError(f"ZIP驗證失敗：bad={bad}; duplicates={duplicates}")
    log(f"v4.82 ZIP完成：{zip_path.name}；{zip_path.stat().st_size:,} bytes；CRC PASS；無重複成員")

    upload = upload_to_github(zip_path, script_path, timestamp)
    receipt = {
        "version": "v4.82", "packaging_recovery": True, "timestamp": timestamp,
        "branch": upload["branch"], "branch_url": upload["branch_url"],
        "main_sha": upload["main_sha"], "files": upload["files"],
        "drive_sources_read_only": True, "github_main_modified": False,
        "model_retrained": False, "backtest_run": False,
    }
    (out_dir / "v4.82_GitHub上傳回執.json").write_text(
        json.dumps(receipt, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print("=" * 96)
    print("v4.82封裝續跑完成（沿用既有報表，未重新掃描）")
    print(f"輸出資料夾：{out_dir}")
    print(f"結果ZIP：{zip_path}")
    print(f"GitHub分支：{upload['branch']}")
    print(f"GitHub分支網址：{upload['branch_url']}")
    for item in upload["files"]:
        print(f"GitHub檔案：{item['path']}｜{item['bytes']:,} bytes｜{item['url']}")
    print("確認：沿用既有分析報表；Drive來源唯讀；GitHub main 未修改；未重訓、未回測。")
    print("=" * 96)


def get_token() -> str:
    token = os.environ.get("GITHUB_TOKEN", "").strip()
    if not token:
        try:
            from google.colab import userdata  # type: ignore
            token = (userdata.get("GITHUB_TOKEN") or "").strip()
        except Exception:
            pass
    if not token:
        raise RuntimeError("未取得 GitHub Token；請在 Colab Secrets 設定 GITHUB_TOKEN")
    return token


def github_api(token: str, method: str, path: str, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    url = "https://api.github.com" + path
    body = None if payload is None else json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(url, data=body, method=method, headers={
        "Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28", "User-Agent": "CHLINX-SSO-v482-audit/1.0",
        "Content-Type": "application/json; charset=utf-8"})
    try:
        with urllib.request.urlopen(req, timeout=180) as response:
            data = response.read()
            return json.loads(data.decode("utf-8")) if data else {}
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")[:2000]
        raise RuntimeError(f"GitHub API {method} {path} HTTP {exc.code}: {detail}") from exc


def upload_to_github(zip_path: Path, py_path: Path, timestamp: str) -> Dict[str, Any]:
    token = get_token()
    root = f"/repos/{REPO}"
    main_sha = github_api(token, "GET", root + "/git/ref/heads/main")["object"]["sha"]
    branch_base = f"audit/v4.82-corrected-evidence-{timestamp}"
    branch = branch_base
    for attempt in range(20):
        if attempt:
            branch = f"{branch_base}-retry-{attempt:02d}"
        try:
            github_api(token, "POST", root + "/git/refs", {"ref": f"refs/heads/{branch}", "sha": main_sha})
            break
        except RuntimeError as exc:
            if "HTTP 422" in str(exc) and "already exists" in str(exc).lower():
                continue
            raise
    else:
        raise RuntimeError("無法建立 v4.82 GitHub 獨立分支")
    log(f"GitHub 獨立分支：{branch}；起點 main SHA={main_sha}（不修改 main）")
    folder = f"audit_uploads/v4.82_{timestamp[:8]}"
    files = []
    for index, path in enumerate((zip_path, py_path), 1):
        rel = f"{folder}/{path.name}"
        payload = path.read_bytes()
        api_path = root + "/contents/" + urllib.parse.quote(rel, safe="/")
        result = github_api(token, "PUT", api_path, {
            "message": f"Upload SSO v4.82 evidence audit: {path.name}",
            "content": __import__("base64").b64encode(payload).decode("ascii"), "branch": branch})
        remote = result.get("content") or {}
        if int(remote.get("size") or 0) != len(payload):
            raise RuntimeError(f"GitHub 上傳大小不符：{path.name}")
        url = f"https://github.com/{REPO}/blob/{branch}/{urllib.parse.quote(rel, safe='/')}"
        files.append({"path": rel, "bytes": len(payload), "url": url})
        log(f"GitHub 上傳 {index}/2 完成：{path.name}；{len(payload):,} bytes")
    return {"branch": branch, "main_sha": main_sha,
            "branch_url": f"https://github.com/{REPO}/tree/{branch}", "files": files}


def main() -> None:
    started = now_taipei().isoformat(timespec="seconds")
    mount_drive()

    # 前次若已完成全部報表、僅在找程式檔時失敗，直接續跑封裝與上傳，不重新掃描來源。
    resumable = find_resumable_output_dir()
    if resumable is not None:
        timestamp = resumable.name[len(OUTPUT_PREFIX):]
        log(f"偵測到可續跑的 v4.82 完整報表：{resumable}")
        log("本次只執行程式定位、ZIP封裝及GitHub上傳；略過所有來源重掃。")
        finalize_and_upload(resumable, timestamp)
        return

    v479_folder, v479_manifest = load_v479_manifest()
    timestamp = now_taipei().strftime("%Y%m%d_%H%M%S")
    out_dir = DRIVE_ROOT / f"{OUTPUT_PREFIX}{timestamp}"
    out_dir.mkdir(parents=True, exist_ok=False)
    log(f"v4.82 輸出資料夾：{out_dir}")

    chosen = choose_archives(v479_manifest)
    archive_paths: List[Path] = []
    archive_rows: List[Dict[str, Any]] = []
    archive_rec_by_name: Dict[str, Dict[str, Any]] = {}
    for i, rec in enumerate(chosen, 1):
        path = locate_archive(rec)
        if path is None:
            archive_rows.append({"filename": rec.get("filename"), "status": "NOT_FOUND_OR_SIZE_CHANGED"})
            log(f"封存找不到或大小變動，略過：{rec.get('filename')}")
            continue
        log(f"封存完整性核驗：{i}/{len(chosen)}；{path.name}")
        verify = verify_archive(path, rec)
        archive_rows.append(verify)
        if verify.get("status") == "PASS":
            archive_paths.append(path)
            archive_rec_by_name[path.name] = rec
        else:
            log(f"封存核驗未通過，略過：{path.name}；{verify.get('status')}")

    # 讀取24項精確特徵欄位表；只有零列的欄位只列為 header-only，不算有數值。
    header_rows: List[Dict[str, Any]] = []
    exact_headers: Dict[str, List[Dict[str, Any]]] = {f: [] for f in FEATURES}
    scan_exact_header_files(archive_paths, archive_rec_by_name, header_rows, exact_headers)

    # 重新讀取 MA20／MA60 長格式差異層，進行逐列算術關係檢查。
    ma_stats: Dict[str, Dict[str, Any]] = {}
    ma_samples: List[Dict[str, Any]] = []
    v466_path = next((p for p in archive_paths if "v4_66_" in p.name), None)
    ma_rows_scanned = 0
    if v466_path:
        with zipfile.ZipFile(v466_path, "r") as zf:
            member = find_member(zf, "03_既有樣本特徵重算差異層.csv.gz")
            if member:
                log(f"開始逐列核對 MA20／MA60：{v466_path.name}/{member}")
                ma_rows_scanned = read_feature_long_layer(zf, member, ma_stats, ma_samples)
                log(f"MA20／MA60 長格式掃描完成：{ma_rows_scanned:,} rows")

    # 讀取 v2.50 與兩份 v2.51 的候選明細，以日期／股票鍵比較原始 factor 數值。
    table_data: Dict[Tuple[str, str], List[Dict[str, str]]] = {}
    table_schema_rows: List[Dict[str, Any]] = []
    extract_candidate_tables(archive_paths, table_data, table_schema_rows)
    comparisons: List[Dict[str, Any]] = []
    mismatch_samples: List[Dict[str, Any]] = []
    for kind in ("full_market", "oos", "latest10"):
        v250 = table_data.get(("v2.50", kind))
        if v250 is None:
            continue
        for label in ("v2.51_no_volume", "v2.51_individual"):
            v251 = table_data.get((label, kind))
            if v251 is None:
                continue
            summary, sample = compare_tables(v250, v251, kind, label)
            comparisons.append(summary)
            mismatch_samples.extend(sample)
            log(f"跨版本同鍵比較：{kind}/{label}；共同鍵={summary['common_keys']:,}；相同值={summary['equal_values']:,}；不同值={summary['different_values']:,}")

    # 唯讀搜尋程式與Notebook；保留命中程式行，不修改檔案。
    code_rows: List[Dict[str, Any]] = []
    code_snippets: List[Dict[str, Any]] = []
    code_scan_summary = scan_source_code_files((DRIVE_ROOT, NOTEBOOK_ROOT), code_rows, code_snippets)

    # 重新分級：只要來源表頭命中，但資料列為0或數值全空，必須明確標示無歷史值。
    feature_rows = []
    for feature in FEATURES:
        refs = exact_headers[feature]
        zero_refs = [r for r in refs if int(r.get("data_rows") or 0) == 0]
        valued_refs = [r for r in refs if int(r.get("nonempty_values") or 0) > 0]
        raw_alias = RAW_ALIASES.get(feature, [])
        raw_counts = []
        for label, stats in ma_stats.items():
            if label in raw_alias:
                raw_counts.append({"field": label, "rows": stats["rows"],
                                   "recomputed_n": stats["recalc_n"], "old_n": stats["old_n"]})
        if feature == "source_factor_rank":
            for r in table_schema_rows:
                if r["table_kind"] in ("full_market", "oos", "latest10") and r["rows"] > 0:
                    header = json.loads(r["header"])
                    if "來源20根波動因子" in header or "factor" in header:
                        raw_counts.append({"field": "來源20根波動因子/factor", "rows": r["rows"],
                                           "source": r["member"], "note": "原始欄位，不等於rank"})
        if valued_refs:
            status = "EXACT_FEATURE_VALUES_PRESENT_NEEDS_FORMULA_VERIFICATION"
        elif raw_counts:
            status = "RAW_INPUT_CANDIDATE_VALUES_ONLY_NOT_THE_RANK_OR_SCALED_FEATURE"
        elif zero_refs:
            status = "EXACT_FEATURE_HEADER_ONLY_ZERO_ROWS_NO_HISTORICAL_VALUES"
        elif refs:
            status = "EXACT_FEATURE_HEADER_PRESENT_BUT_NO_NONEMPTY_NUMERIC_VALUES"
        else:
            status = "NO_EXACT_OR_USABLE_RAW_VALUE_EVIDENCE_IN_SELECTED_ARCHIVES"
        feature_rows.append({"feature": feature, "evidence_status_v4_82": status,
                             "exact_header_archive_count": len(refs),
                             "exact_header_zero_row_count": len(zero_refs),
                             "exact_header_with_nonempty_data_count": len(valued_refs),
                             "exact_header_refs": safe_json(refs),
                             "raw_input_candidates_not_equivalent": safe_json(raw_counts),
                             "original_formula_confirmed": False})

    # MA 摘要展開為 CSV；不把差額欄位的方向預設為已知。
    ma_summary_rows = []
    for label, st in sorted(ma_stats.items()):
        ma_summary_rows.append({
            "feature_label": label, "rows": st["rows"], "original_value_n": st["old_n"],
            "recomputed_value_n": st["recalc_n"], "difference_value_n": st["delta_n"],
            "rows_with_all_three_numeric": st["arithmetic_compared"],
            "delta_equals_recomputed_minus_original": st["new_minus_old_match"],
            "delta_equals_original_minus_recomputed": st["old_minus_new_match"],
            "max_abs_residual_recomputed_minus_original": st["max_abs_residual_new_minus_old"],
            "max_abs_residual_original_minus_recomputed": st["max_abs_residual_old_minus_new"],
            "original_value_mean": summarize_mean(st["old_sum"], st["old_n"]),
            "recomputed_value_mean": summarize_mean(st["recalc_sum"], st["recalc_n"]),
            "difference_mean": summarize_mean(st["delta_sum"], st["delta_n"]),
            "price_segment_counts": safe_json(dict(st["segment_counts"])),
            "market_counts": safe_json(dict(st["market_counts"])),
            "interpretation": "MA距離原始值／重算值候選；不等於模型rank特徵，差額方向另由逐列算術結果判讀",
        })

    report_defs = [
        ("01_24項特徵正確證據分級.csv", list(feature_rows[0].keys()), feature_rows),
        ("02_24項精確特徵欄位表頭與空表核驗.csv",
         ["archive", "member", "data_rows", "column_count", "feature_headers_present", "missing_feature_headers", "feature_nonempty_counts"], header_rows),
        ("03_MA20_MA60重算差異逐列核對摘要.csv", list(ma_summary_rows[0].keys()) if ma_summary_rows else
         ["feature_label", "rows", "original_value_n", "recomputed_value_n", "difference_value_n", "interpretation"], ma_summary_rows),
        ("04_MA20_MA60差異層代表資料列.csv",
         ["市場", "證券代號", "日期", "特徵欄位", "原始已存值", "重算值", "差額", "變動類型", "影響補行日", "價格研究區段", "來源成員"], ma_samples),
        ("05_v250_v251候選因子同鍵比較.csv",
         list(comparisons[0].keys()) if comparisons else ["table_kind", "v2.51_archive", "v2.50_rows", "v2.51_rows", "common_keys", "equal_values", "different_values"], comparisons),
        ("06_v250_v251因子差異代表資料列.csv",
         ["table_kind", "v2.51_archive", "key", "v2.50_value", "v2.51_value", "v2.51_minus_v2.50"], mismatch_samples),
        ("07_原始程式與Notebook檔案命中.csv",
         sorted({k for r in code_rows for k in r.keys()} | {"path", "bytes", "modified_time", "matched_terms", "term_count"}), code_rows),
        ("08_原始程式與Notebook命中程式行.csv",
         ["path", "line_no", "matched_terms", "excerpt"], code_snippets),
        ("09_候選CSV欄位與列數.csv",
         ["archive", "version_label", "table_kind", "member", "rows", "columns", "header"], table_schema_rows),
        ("10_封存唯讀核驗.csv",
         sorted({k for r in archive_rows for k in r.keys()} | {"filename", "size_bytes", "expected_size", "sha256", "expected_sha256", "size_match", "sha_match", "crc", "members", "status"}), archive_rows),
    ]
    counts = {}
    for filename, headers, rows in report_defs:
        n = write_csv(out_dir / filename, headers, rows)
        counts[filename] = n
        log(f"報告完成：{filename}；{n:,} rows；{(out_dir / filename).stat().st_size:,} bytes")

    manifest_out = {
        "version": VERSION, "started_at_taipei": started,
        "finished_at_taipei": now_taipei().isoformat(timespec="seconds"),
        "v479_source_folder": v479_folder.name,
        "archives_selected": len(chosen), "archives_verified": sum(1 for r in archive_rows if r.get("status") == "PASS"),
        "v466_long_layer_rows_scanned": ma_rows_scanned,
        "v250_v251_candidate_tables": len(table_schema_rows),
        "v250_v251_key_comparisons": len(comparisons),
        "feature_evidence_status_counts": dict(Counter(r["evidence_status_v4_82"] for r in feature_rows)),
        "source_code_scan": code_scan_summary,
        "original_24_formulas_confirmed": False,
        "drive_sources_read_only": True, "github_main_modified": False,
        "model_retrained": False, "backtest_run": False,
        "important_corrections": [
            "v4.81把零資料列的精確特徵表頭也算成候選紀錄；v4.82改以非空數值判斷。",
            "收盤相對MA20／MA60是原始均線距離候選，不可直接等同dist_ma20_rank／dist_ma60_rank。",
            "來源20根波動因子與factor是原始輸入候選，不可直接等同source_factor_rank。",
            "只有當精確特徵欄本身存在非空值時才列為exact feature values；仍須找出產生公式及排名母體。",
        ],
        "reports": counts,
    }
    (out_dir / "v4.82_manifest.json").write_text(json.dumps(manifest_out, ensure_ascii=False, indent=2), encoding="utf-8")
    with (out_dir / "v4.82_執行摘要.txt").open("w", encoding="utf-8") as f:
        f.write("看股最速適 v4.82｜修正候選證據分級、同鍵跨版本比較與原始程式追查\n")
        f.write("=" * 96 + "\n")
        f.write(f"v4.79來源清冊：{v479_folder.name}\n")
        f.write(f"封存核驗：{manifest_out['archives_verified']}/{len(chosen)}；v4.66長格式掃描：{ma_rows_scanned:,}列。\n")
        f.write(f"24項證據分級：{safe_json(manifest_out['feature_evidence_status_counts'])}\n")
        f.write(f"跨版本因子同鍵比較：{len(comparisons)}組。\n")
        f.write(f"程式／Notebook搜尋：{safe_json(code_scan_summary)}\n\n")
        f.write("關鍵修正：零資料列的欄位表頭不再視為找到數值；原始均線距離及原始波動因子不等於rank或scaled特徵。\n")
        f.write("本版唯讀來源；不修改GitHub main；不重訓、不回測。\n")

    # 正常分析完成後共用同一封裝與上傳流程。
    finalize_and_upload(out_dir, timestamp)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log("使用者中斷；來源資料與 GitHub main 均未修改。")
    except Exception as exc:
        log(f"執行失敗：{type(exc).__name__}: {exc}")
        raise