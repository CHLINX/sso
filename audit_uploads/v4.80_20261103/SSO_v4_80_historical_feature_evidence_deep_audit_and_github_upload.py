#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""看股最速適 v4.80｜24項特徵歷史輸出與公式來源深度追查

本版承接 v4.79，不重掃整個 Drive。以 v4.79 manifest 所列的 Drive 封存 ZIP 為唯一本機封存清單，
再擴充檢查 GitHub 固定 commit 中 v2.45–v2.62 的 17 份公開歷史 ZIP。

工作內容：
1. 核對 v4.79 manifest 與既有 18 份 Drive ZIP 的檔案大小、SHA-256；來源唯讀。
2. 深入掃描封存成員的 Python／Notebook、文字報告、JSON、CSV／CSV.GZ。
3. 不只看24個精確欄名，也盤點相關欄位、實際列數、非空值數與少量樣本，以查找可能的歷史替代欄位。
4. 彙整24項特徵逐項證據狀態，區分程式公式、數值列、文字描述、只有表頭及無證據。
5. 對照 v2.45、v2.48.1、v2.50、v2.51 與 v2.53–v2.54 的版本差異、排名方向診斷。
6. 產生報告 ZIP，並將 ZIP 與本 Python 上傳至 CHLINX/sso 獨立 audit 分支。

安全原則：
- Drive 原始檔與既有 ZIP 一律唯讀；只在新 v4.80 輸出資料夾產生結果。
- 不重訓、不回測、不產生買賣建議；不修改或刪除 GitHub main。
- 公開封存檔固定使用已核對的 Git commit SHA，並逐一檢查 bytes、SHA-256、ZIP CRC。
- 報告只記錄必要摘錄，疑似 Token／密碼內容遮蔽，不輸出完整來源程式。
- 不使用 SystemExit: 0；正常完成後由 main() 返回。

Colab 執行：
!python /content/SSO_v4_80_historical_feature_evidence_deep_audit_and_github_upload.py

GitHub Token：在 Colab Secrets 設定 GITHUB_TOKEN，需具 CHLINX/sso Contents Read and write 權限。
"""
from __future__ import annotations

import base64
import csv
import gzip
import hashlib
import io
import json
import os
import re
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

VERSION = "v4.80"
REPO = "CHLINX/sso"
SCRIPT_FILENAME = "SSO_v4_80_historical_feature_evidence_deep_audit_and_github_upload.py"
OUTPUT_PREFIX = "看股最速適_24項特徵歷史輸出與公式來源深度追查_v4.80_"
V479_PREFIX = "看股最速適_24項原始特徵程式與歷史數值來源追查_v4.79_"
TZ = ZoneInfo("Asia/Taipei")
DRIVE_ROOT = Path("/content/drive/MyDrive/看股最速適")
NOTEBOOK_ROOT = Path("/content/drive/MyDrive/Colab Notebooks")
PUBLIC_CACHE = Path("/content/SSO_v4_80_public_archive_cache")
PUBLIC_COMMIT = "aaabc2433fb5df2bb68af06461178305d6bd30d1"

FEATURES = [
    "ret5_rank", "ret20_rank", "ret60_rank", "dist_ma20_rank", "dist_ma60_rank",
    "range_pos20_rank", "lowvol20_rank", "source_factor_rank",
    "ret5_abs_scaled", "ret20_abs_scaled", "ret60_abs_scaled", "dist_ma20_abs_scaled", "dist_ma60_abs_scaled",
    "market_median_ret5", "market_median_ret20", "market_median_ret60",
    "market_breadth_pos_ret5", "market_breadth_pos_ret20", "market_breadth_pos_ret60",
    "market_breadth_above_ma20", "market_breadth_above_ma60",
    "ret5_rank_x_market_breadth", "ret20_rank_x_market_breadth", "ret60_rank_x_market_breadth",
]
FEATURE_SET = set(FEATURES)
FEATURE_GROUPS = {
    "相對排名特徵（8項）": FEATURES[:8],
    "個股絕對動能／均線縮放（5項）": FEATURES[8:13],
    "全市場報酬中位數（3項）": FEATURES[13:16],
    "市場廣度比例（5項）": FEATURES[16:21],
    "排名×市場廣度互動（3項）": FEATURES[21:24],
}

TEXT_TERMS = [
    *FEATURES, "RANK_FEATURES", "FEATURE_COLUMNS", "feature_columns", "來源20根波動因子",
    "公式", "計算方式", "計算公式", "橫斷面", "訊號日", "市場廣度", "中位數",
    "rolling", "rankdata", "percentile", "quantile", "standardscaler", "zscore",
    "abs_scaled", "ddof", "method=average", "ascending", "descending", "tie", "平手",
    "排名方向", "分位方向", "樣本外", "特徵變更",
]
TEXT_REGEX = re.compile(
    r"公式|計算|rank|percentile|quantile|scaled|breadth|median|rolling|zscore|standard|"
    r"橫斷面|訊號日|市場廣度|中位數|分位|排名方向|平手|RANK_FEATURES|FEATURE_COLUMNS|來源20根波動因子",
    re.I,
)
RELATED_COLUMN_REGEX = re.compile(
    r"ret|return|moment|ma\s*20|ma\s*60|均線|range|區間|vol|波動|factor|因子|rank|排名|scaled|"
    r"breadth|廣度|median|中位|market|市場|feature|特徵|動能|報酬|振幅",
    re.I,
)
SENSITIVE_RE = re.compile(
    r"(?i)(gh[pousr]_[A-Za-z0-9_]{20,}|github_pat_[A-Za-z0-9_]{20,}|"
    r"(token|password|passwd|secret|api[_-]?key)\s*[:=]\s*['\"][^'\"]{6,}['\"]|"
    r"AIza[0-9A-Za-z_\-]{25,})"
)

# 固定於 v4.79 核驗時的公開 repository commit，不追隨 main 浮動。
PUBLIC_ARCHIVES = [
    ("v2.45", "SSO_v2.45_樣本外校準門檻與公司名稱結果.zip", 20731287, "3dc4e9501c9fdf8b22a8f103742cddadb161c449eb369c197d1f116f1de31aaf"),
    ("v2.46", "SSO_v2.46_欄位補充_不重跑模型結果.zip", 21125807, "853cb4a1dad2e53d5fb328bc373607dc7a4ac577d6801404aed601d251a634c1"),
    ("v2.47", "SSO_v2.47_產業及市場別補正_不重跑模型結果.zip", 21322689, "d3364bf2e0ada7e931260196fece30d20f71ff74c1245e790939ee7432e2169e"),
    ("v2.48.1", "SSO_v2.48.1_樣本外校準門檻與公司名稱結果.zip", 21010200, "c45cec0752e5337130a72ca483accd2bd74b62bf76b24e1205e507aa54a58e2e"),
    ("v2.50", "SSO_v2.50_日成交量特徵與500日檢查點結果.zip", 21654521, "d7908f72d102abffdf1cf1433000f164b36ea95dfec25545f33800ed90a87c8b6"),
    ("v2.51_individual", "SSO_v2.51_個股獨立評估_量能消融與配對比較結果.zip", 23606997, "3b78213b0618678bd883b153e1b8d8d72b074daa4d9d3a2ecd225e80d1f8fdb7"),
    ("v2.51_no_volume", "SSO_v2.51_量能消融_停用量能特徵與配對比較結果.zip", 21011289, "e472aa8a1f6a79bd2511e5209a5ab8a3e74e5d52d93f9196bc80617b59373949"),
    ("v2.53", "SSO_v2.53_排序與統計口徑稽核_20261029_033422.zip", 17116, "0cd57dac0074fe2ddfddce79adb49618f0846a107965ba1c175c6b3af686fa1c"),
    ("v2.54", "SSO_v2.54_排名分數與分位方向稽核_20261029_040349.zip", 10953, "7096464abb8998b2cd1c2d495536773bf2b80b46eaf79ea892ac6bb8b499d208"),
    ("v2.55", "SSO_v2.55_逐筆績效與統計口徑稽核結果_20261029_045750.zip", 6392, "d17f10a294d37859c21d1e4a187349792aa0d6e10454920892e863820ab6fa8e"),
    ("v2.56", "SSO_v2.56_實際淨報酬與分層核算結果_20261029_055725.zip", 17713, "cb2dc45c21ac70962023dbad70a8c7c0cf32ab940e989478fb4ec8e45e2c1bb0"),
    ("v2.57", "SSO_v2.57_條件式交易模式回溯核驗結果_20261029_062605.zip", 12721426, "9693fbfc18eb923649fbd4643d5e4da0375a18c69c50f9650cd3bf2bbfee49b1"),
    ("v2.58", "SSO_v2.58_候選排除門檻拆解結果_20261029_075004.zip", 2229698, "3fd718631cb6fb2de312011733eba3c31236ec35bc594393d8834694fd1a3644"),
    ("v2.59", "SSO_v2.59_時間切分穩定性診斷結果_20261029_080204.zip", 225716, "9eb06a7c67601cf8e9942dce18f7b8915ec5d0b2839be7af9b29fd5f7bf487ba"),
    ("v2.60", "SSO_v2.60_訊號日層級穩定性診斷結果_20261029_082147.zip", 589833, "f8b53d0748c223d1b7ebf43e45edc860e4dfba1cf28bdd58e1bee53aff051f9e"),
    ("v2.61", "SSO_v2.61_訊號日極端日穩健性診斷結果_20261029_084332.zip", 29432, "a9f777cc9c862ed92ab66db2f65955477520b817b80990890870251b1cfbc385"),
    ("v2.62", "SSO_v2.62_前瞻影子驗證規格封存結果_20261029_165713.zip", 9039, "40fc95e60f3fb72272094b982a4687c1da860294efb7facc9f548dee1a23b77d"),
]

# 由公開歷史報告已能確認的版本文字敘述；這些描述不等於完整計算公式。
KNOWN_VERSION_EVIDENCE = [
    {"version": "v2.45", "evidence": "報告／設定檔列出8個相對排名特徵；僅能確認欄名與特徵組成，未取得原始計算程式。", "scope": "8個相對排名特徵"},
    {"version": "v2.48.1", "evidence": "報告明載保留原8個相對排名；新增5個個股絕對動能／均線縮放、3個全市場報酬中位數、5個市場廣度比例、3個排名×市場廣度互動項；市場狀態按訊號日橫斷面計算。", "scope": "24項價格與市場特徵"},
    {"version": "v2.50", "evidence": "承接 v2.48.1 特徵，另新增4個日線成交量特徵；當日量相對20日均量、5／20日均量趨勢、相對量能排名、近5日報酬×相對量能確認。", "scope": "24項特徵之外另有4項量能特徵"},
    {"version": "v2.51", "evidence": "停用量能特徵，不將4個成交量特徵加入 RANK_FEATURES；其餘價格／市場狀態特徵、標籤、切分、分類器、校準、成本及回測參數沿用 v2.50。", "scope": "停用4個量能特徵，保留價格／市場特徵"},
    {"version": "v2.53", "evidence": "排序稽核報告明示分位順序仍由標籤文字推定，原始定義未明時應以模型產出程式為準；彙總 CSV 不足以證明排名鍵與逐筆口徑。", "scope": "排名／分位口徑限制"},
    {"version": "v2.54", "evidence": "排名1代表最佳時，排名與校準獲利機率呈高度負相關；預測淨報酬未呈清楚單調方向。支持排名可能依校準機率，但報告明示仍須以原始排名程式確認。", "scope": "輸出排名分數方向線索"},
]


def now_text() -> str:
    return datetime.now(TZ).isoformat(timespec="seconds")


def log(message: str) -> None:
    print(f"[{datetime.now(TZ).strftime('%H:%M:%S')}] {message}", flush=True)


def sha256_file(path: Path, chunk_size: int = 4 *  1024 * 1024) -> str:
    digest = hashlib.sha256()
    total = 0
    with path.open("rb") as f:
        while True:
            block = f.read(chunk_size)
            if not block:
                break
            digest.update(block)
            total += len(block)
            if total and total % (256 * 1024 * 1024) < chunk_size:
                log(f"SHA-256進度：{path.name}；{total:,} bytes")
    return digest.hexdigest()


def mount_drive() -> None:
    if DRIVE_ROOT.exists():
        log(f"Google Drive 已可讀取：{DRIVE_ROOT}")
        return
    try:
        from google.colab import drive  # type: ignore
        drive.mount("/content/drive", force_remount=False)
    except Exception as exc:
        raise RuntimeError(f"無法掛載 Google Drive：{type(exc).__name__}: {exc}")
    if not DRIVE_ROOT.exists():
        raise FileNotFoundError(f"找不到專案資料夾：{DRIVE_ROOT}")


def get_github_token() -> str:
    token = os.environ.get("GITHUB_TOKEN", "").strip()
    if token:
        return token
    try:
        from google.colab import userdata  # type: ignore
        token = (userdata.get("GITHUB_TOKEN") or "").strip()
    except Exception:
        token = ""
    if not token:
        raise RuntimeError("未取得 GitHub Token；請在 Colab Secrets 設定 GITHUB_TOKEN（Contents Read and write）。")
    return token


def find_latest_v479_folder() -> Tuple[Path, Dict[str, Any]]:
    folders = sorted(
        (p for p in DRIVE_ROOT.glob(V479_PREFIX + "*") if p.is_dir() and (p / "v4.79_manifest.json").is_file()),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    for folder in folders:
        try:
            manifest = json.loads((folder / "v4.79_manifest.json").read_text(encoding="utf-8"))
            if manifest.get("version") == "v4.79" and isinstance(manifest.get("archive_summary"), list):
                log(f"採用 v4.79 既有清冊：{folder}")
                return folder, manifest
        except Exception as exc:
            log(f"略過無法讀取的 v4.79 manifest：{folder.name}；{exc}")
    raise FileNotFoundError("找不到有效的 v4.79_manifest.json；請先保留 v4.79 輸出資料夾，不需重新上傳 ZIP。")


def resolve_manifest_archive(record: Dict[str, Any]) -> Optional[Path]:
    filename = str(record.get("filename") or "")
    rel = str(record.get("relative_path") or filename)
    candidates = [DRIVE_ROOT / rel, NOTEBOOK_ROOT / rel, DRIVE_ROOT / filename, NOTEBOOK_ROOT / filename]
    for p in candidates:
        try:
            if p.is_file() and p.stat().st_size == int(record.get("size_bytes") or -1):
                return p
        except OSError:
            pass
    return None


def safe_excerpt(text: str, limit: int = 420) -> str:
    if SENSITIVE_RE.search(text):
        return "[疑似敏感值已遮蔽]"
    return re.sub(r"\s+", " ", text).strip()[:limit]


def decode_bytes(raw: bytes) -> str:
    for enc in ("utf-8-sig", "utf-8", "cp950", "big5"):
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="replace")


def is_missing(value: str) -> bool:
    return str(value).strip() in {"", "NA", "NaN", "nan", "None", "null", "<NA>"}


def open_member_text(zf: zipfile.ZipFile, member: str):
    raw_stream = zf.open(member, "r")
    if member.lower().endswith(".gz"):
        binary_stream = gzip.GzipFile(fileobj=raw_stream, mode="rb")
    else:
        binary_stream = raw_stream
    text_stream = io.TextIOWrapper(binary_stream, encoding="utf-8-sig", errors="replace", newline="")
    return raw_stream, binary_stream, text_stream


def inspect_text_member(zf: zipfile.ZipFile, info: zipfile.ZipInfo, version: str) -> Tuple[List[Dict[str, Any]], List[str]]:
    name = info.filename
    ext = name.lower().rsplit(".", 1)[-1]
    if info.file_size > 20 * 1024 * 1024:
        return [], []
    try:
        raw = zf.read(name)
        if name.lower().endswith(".gz"):
            raw = gzip.decompress(raw)
        text = decode_bytes(raw)
    except Exception as exc:
        return [{"version": version, "member": name, "line_no": "", "evidence_type": "READ_ERROR", "matched_terms": "", "excerpt": safe_excerpt(str(exc))}], []

    # Notebook 需解析 cell，避免只掃描 compact JSON 導致漏掉來源碼。
    code_sources: List[str] = []
    if ext == "ipynb":
        try:
            nb = json.loads(text)
            for cell in nb.get("cells", []):
                source = cell.get("source", "")
                if isinstance(source, list):
                    source = "".join(source)
                if cell.get("cell_type") == "code" and source:
                    code_sources.append(str(source))
        except Exception:
            pass

    out: List[Dict[str, Any]] = []
    scan_text = "\n".join(code_sources) if code_sources else text
    for line_no, line in enumerate(scan_text.splitlines(), 1):
        matches = [term for term in TEXT_TERMS if term.lower() in line.lower()]
        if not matches and not TEXT_REGEX.search(line):
            continue
        out.append({
            "version": version, "member": name, "line_no": line_no,
            "evidence_type": "CODE_LINE" if ext in {"py", "ipynb", "r", "sql", "jl"} else "TEXT_LINE",
            "matched_terms": json.dumps(matches, ensure_ascii=False),
            "excerpt": safe_excerpt(line),
        })
        if len(out) >= 120:
            break
    return out, code_sources


def inspect_csv_member(zf: zipfile.ZipFile, info: zipfile.ZipInfo, version: str) -> Dict[str, Any]:
    name = info.filename
    raw_stream = binary_stream = text_stream = None
    try:
        raw_stream, binary_stream, text_stream = open_member_text(zf, name)
        delimiter = "\t" if ".tsv" in name.lower() else ","
        reader = csv.reader(text_stream, delimiter=delimiter)
        header = next(reader, [])
        if not header:
            return {"version": version, "member": name, "uncompressed_size": info.file_size, "status": "EMPTY_OR_NO_HEADER"}

        exact = [c for c in header if c in FEATURE_SET]
        related = [c for c in header if RELATED_COLUMN_REGEX.search(c)]
        name_related = bool(re.search(r"feature|factor|rank|score|candidate|signal|model|特徵|因子|排名|分數|候選|訊號|樣本|核對|公式|因子|波動", name, re.I))
        scan_rows = bool(exact or related or name_related)
        row_count = 0
        nonempty: Counter = Counter()
        samples: List[Dict[str, str]] = []
        sample_cols = list(dict.fromkeys(exact + related))[:45]
        header_index = {c: i for i, c in enumerate(header)}

        if scan_rows:
            for row in reader:
                row_count += 1
                for col in exact:
                    i = header_index[col]
                    if i < len(row) and not is_missing(row[i]):
                        nonempty[col] += 1
                if len(samples) < 3 and any(i < len(row) and not is_missing(row[i]) for i in [header_index[c] for c in sample_cols]):
                    samples.append({c: (row[header_index[c]][:100] if header_index[c] < len(row) else "") for c in sample_cols})
                if row_count and row_count % 250000 == 0:
                    log(f"CSV深度掃描：{version}/{name}；{row_count:,} rows；{len(exact)} exact features")

        return {
            "version": version,
            "member": name,
            "uncompressed_size": info.file_size,
            "columns": len(header),
            "exact_feature_columns": len(exact),
            "exact_features": json.dumps(exact, ensure_ascii=False),
            "related_columns": json.dumps(related[:100], ensure_ascii=False),
            "rows_counted": row_count if scan_rows else "NOT_SCANNED_UNRELATED_HEADER",
            "nonempty_exact_feature_counts": json.dumps(dict(nonempty), ensure_ascii=False),
            "numeric_value_evidence": bool(row_count and any(nonempty.values())),
            "samples_related_columns": json.dumps(samples, ensure_ascii=False),
            "header": json.dumps(header, ensure_ascii=False),
            "status": "SCANNED" if scan_rows else "HEADER_ONLY",
        }
    except Exception as exc:
        return {"version": version, "member": name, "uncompressed_size": info.file_size, "status": f"ERROR:{type(exc).__name__}:{exc}"}
    finally:
        for obj in (text_stream, binary_stream, raw_stream):
            try:
                if obj is not None:
                    obj.close()
            except Exception:
                pass


def inspect_archive(path: Path, version: str, source_type: str) -> Tuple[Dict[str, Any], List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    digest = sha256_file(path)
    ar = {
        "version": version, "source_type": source_type, "filename": path.name,
        "size_bytes": path.stat().st_size, "sha256": digest, "crc": "NOT_CHECKED",
        "member_count": 0, "source_code_members": [], "model_binary_members": [],
    }
    member_rows: List[Dict[str, Any]] = []
    text_rows: List[Dict[str, Any]] = []
    csv_rows: List[Dict[str, Any]] = []
    with zipfile.ZipFile(path, "r") as zf:
        bad = zf.testzip()
        ar["crc"] = "PASS" if bad is None else f"FAIL:{bad}"
        if bad is not None:
            raise RuntimeError(f"ZIP CRC 失敗：{path.name}；{bad}")
        infos = [x for x in zf.infolist() if not x.is_dir()]
        ar["member_count"] = len(infos)
        uncompressed = sum(x.file_size for x in infos)
        log(f"深度檢查 {version}：{len(infos)} members；uncompressed={uncompressed:,} bytes")
        for idx, info in enumerate(infos, 1):
            name = info.filename
            low = name.lower()
            ext = low.rsplit(".", 1)[-1] if "." in low else ""
            mr: Dict[str, Any] = {
                "version": version, "member": name, "size_bytes": info.file_size,
                "extension": "." + ext if ext else "", "member_type": "OTHER",
            }
            if low.endswith((".py", ".ipynb", ".r", ".jl", ".sql")):
                mr["member_type"] = "SOURCE_CODE"
                ar["source_code_members"].append(name)
                matches, code_sources = inspect_text_member(zf, info, version)
                text_rows.extend(matches)
                mr["text_hit_lines"] = len(matches)
                mr["exact_feature_names_found"] = json.dumps(sorted({f for r in matches for f in FEATURES if f in r.get("excerpt", "")}), ensure_ascii=False)
            elif low.endswith((".pkl", ".pickle", ".joblib", ".cbm", ".pt", ".pth", ".onnx", ".h5", ".keras", ".safetensors")):
                mr["member_type"] = "MODEL_BINARY"
                ar["model_binary_members"].append(name)
            elif low.endswith((".csv", ".csv.gz", ".tsv", ".tsv.gz")):
                mr["member_type"] = "CSV"
                cr = inspect_csv_member(zf, info, version)
                csv_rows.append(cr)
                mr["csv_status"] = cr.get("status", "")
                mr["exact_feature_columns"] = cr.get("exact_feature_columns", 0)
                mr["rows_counted"] = cr.get("rows_counted", "")
                mr["numeric_value_evidence"] = cr.get("numeric_value_evidence", False)
            elif low.endswith((".txt", ".md", ".json", ".yaml", ".yml", ".ini", ".cfg", ".py", ".ipynb", ".r", ".sql")):
                mr["member_type"] = "TEXT_OR_CONFIG"
                matches, code_sources = inspect_text_member(zf, info, version)
                text_rows.extend(matches)
                mr["text_hit_lines"] = len(matches)
                mr["exact_feature_names_found"] = json.dumps(sorted({f for r in matches for f in FEATURES if f in r.get("excerpt", "")}), ensure_ascii=False)
            member_rows.append(mr)
            if idx % 20 == 0 or idx == len(infos):
                log(f"封存成員進度：{version} {idx:,}/{len(infos):,}")
    return ar, member_rows, text_rows, csv_rows


def download_public_archive(meta: Tuple[str, str, int, str]) -> Tuple[Path, Dict[str, Any]]:
    version, filename, expected_size, expected_sha = meta
    PUBLIC_CACHE.mkdir(parents=True, exist_ok=True)
    target = PUBLIC_CACHE / filename
    if target.is_file() and target.stat().st_size == expected_size:
        got = sha256_file(target)
        if got == expected_sha:
            return target, {"version": version, "filename": filename, "size_bytes": expected_size, "sha256": got, "status": "CACHE_SHA256_PASS"}
        log(f"公開 ZIP 快取 SHA 不符，僅刪除本次 v4.80 暫存檔：{filename}")
        target.unlink(missing_ok=True)
    url = f"https://raw.githubusercontent.com/{REPO}/{PUBLIC_COMMIT}/" + urllib.parse.quote(filename, safe="")
    req = urllib.request.Request(url, headers={"User-Agent": "CHLINX-SSO-v480-historical-evidence/1.0"})
    tmp = target.with_suffix(target.suffix + ".part")
    h = hashlib.sha256()
    total = 0
    started = time.time()
    log(f"下載公開歷史 ZIP {version}：預期 {expected_size:,} bytes")
    with urllib.request.urlopen(req, timeout=300) as response, tmp.open("wb") as f:
        while True:
            block = response.read(4 * 1024 * 1024)
            if not block:
                break
            f.write(block)
            h.update(block)
            total += len(block)
            elapsed = max(time.time() - started, 0.001)
            log(f"{version} 下載：{total:,}/{expected_size:,} bytes；{total/elapsed:,.0f} bytes/s")
    got_sha = h.hexdigest()
    if total != expected_size or got_sha != expected_sha:
        tmp.unlink(missing_ok=True)
        raise RuntimeError(f"{version} ZIP 大小或 SHA-256 不符：bytes={total}/{expected_size} SHA={got_sha}")
    tmp.replace(target)
    return target, {"version": version, "filename": filename, "size_bytes": total, "sha256": got_sha, "status": "DOWNLOAD_SHA256_PASS"}


def save_csv(path: Path, headers: Sequence[str], rows: Iterable[Dict[str, Any]]) -> int:
    count = 0
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(headers), extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
            count += 1
    return count


def make_feature_status(text_rows: List[Dict[str, Any]], csv_rows: List[Dict[str, Any]], member_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    status_rows = []
    for feature in FEATURES:
        text_refs = []
        code_refs = []
        value_refs = []
        header_only_refs = []
        for row in text_rows:
            if feature in str(row.get("excerpt", "")):
                ref = f"{row.get('version')}::{row.get('member')}#{row.get('line_no')}"
                text_refs.append(ref)
                if row.get("evidence_type") == "CODE_LINE":
                    code_refs.append(ref)
        for row in csv_rows:
            try:
                fields = json.loads(row.get("exact_features", "[]"))
            except Exception:
                fields = []
            if feature not in fields:
                continue
            n = row.get("rows_counted")
            nonempty = {}
            try:
                nonempty = json.loads(row.get("nonempty_exact_feature_counts", "{}"))
            except Exception:
                pass
            ref = f"{row.get('version')}::{row.get('member')}"
            if isinstance(n, int) and n > 0 and nonempty.get(feature, 0) > 0:
                value_refs.append(ref)
            elif n == 0:
                header_only_refs.append(ref)
        if code_refs:
            status = "找到程式碼線索；仍需人工確認完整計算函式"
        elif value_refs:
            status = "找到含實際值的歷史資料列；可進入逐筆公式核對"
        elif text_refs:
            status = "只找到名稱／文字描述，尚非精確公式"
        elif header_only_refs:
            status = "只找到表頭，沒有實際數值列"
        else:
            status = "本次掃描範圍沒有證據"
        status_rows.append({
            "feature": feature,
            "feature_group": next((g for g, fs in FEATURE_GROUPS.items() if feature in fs), ""),
            "evidence_status": status,
            "code_line_refs": json.dumps(code_refs[:30], ensure_ascii=False),
            "text_refs": json.dumps(text_refs[:60], ensure_ascii=False),
            "numeric_value_refs": json.dumps(value_refs[:30], ensure_ascii=False),
            "header_only_refs": json.dumps(header_only_refs[:30], ensure_ascii=False),
            "original_formula_confirmed": False,
        })
    return status_rows


def github_request(token: str, method: str, api_path: str, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    url = "https://api.github.com" + api_path
    body = None if payload is None else json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(url, data=body, method=method, headers={
        "Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28", "User-Agent": "CHLINX-SSO-v480-evidence-audit/1.0",
        "Content-Type": "application/json; charset=utf-8",
    })
    try:
        with urllib.request.urlopen(req, timeout=180) as response:
            raw = response.read()
            return json.loads(raw.decode("utf-8")) if raw else {}
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")[:2000]
        raise RuntimeError(f"GitHub API {method} {api_path} 失敗：HTTP {exc.code} {detail}") from exc


def upload_outputs_to_github(zip_path: Path, script_path: Path, timestamp: str) -> Dict[str, Any]:
    token = get_github_token()
    repo_path = f"/repos/{REPO}"
    main_ref = github_request(token, "GET", repo_path + "/git/ref/heads/main")
    main_sha = main_ref["object"]["sha"]
    base_branch = f"audit/v4.80-historical-evidence-{timestamp}"
    branch = base_branch
    for attempt in range(20):
        if attempt:
            branch = f"{base_branch}-retry-{attempt:02d}"
        try:
            github_request(token, "POST", repo_path + "/git/refs", {"ref": f"refs/heads/{branch}", "sha": main_sha})
            break
        except RuntimeError as exc:
            msg = str(exc)
            if "HTTP 422" in msg and ("Reference already exists" in msg or "already exists" in msg):
                log(f"GitHub 分支已存在，改用續傳分支：{branch}")
                continue
            raise
    else:
        raise RuntimeError(f"無法建立 v4.80 獨立分支：{base_branch}")
    log(f"GitHub 獨立分支已建立：{branch}；起點 main SHA={main_sha}（main 不修改）")

    uploaded = []
    remote_folder = f"audit_uploads/v4.80_{timestamp[:8]}"
    for i, file_path in enumerate((zip_path, script_path), 1):
        repo_file = f"{remote_folder}/{file_path.name}"
        api_path = repo_path + "/contents/" + urllib.parse.quote(repo_file, safe="/")
        data = file_path.read_bytes()
        log(f"GitHub 上傳 {i}/2：{file_path.name}；{len(data):,} bytes")
        result = github_request(token, "PUT", api_path, {
            "message": f"Upload SSO v4.80 historical evidence audit: {file_path.name}",
            "content": base64.b64encode(data).decode("ascii"),
            "branch": branch,
        })
        c = result.get("content") or {}
        remote_size = int(c.get("size") or 0)
        if remote_size != len(data):
            raise RuntimeError(f"GitHub 上傳大小不符：{file_path.name} local={len(data)} remote={remote_size}")
        uploaded.append({"local_file": file_path.name, "repo_path": repo_file, "size_bytes": len(data), "sha": c.get("sha", ""), "url": c.get("html_url", "")})
    return {"branch": branch, "branch_url": f"https://github.com/{REPO}/tree/{urllib.parse.quote(branch, safe='/')}", "main_sha": main_sha, "files": uploaded}


def resolve_script_path(out_dir: Path) -> Path:
    candidates = []
    file_value = globals().get("__file__")
    if isinstance(file_value, str) and file_value not in {"", "<stdin>", "<string>"}:
        candidates.append(Path(file_value))
    env = os.environ.get("SSO_V480_SCRIPT_PATH", "").strip()
    if env:
        candidates.append(Path(env).expanduser())
    if sys.argv and sys.argv[0] and sys.argv[0] not in {"-c", "-"}:
        candidates.append(Path(sys.argv[0]).expanduser())
    candidates.extend([Path("/content") / SCRIPT_FILENAME, Path.cwd() / SCRIPT_FILENAME, DRIVE_ROOT / SCRIPT_FILENAME])
    for p in candidates:
        try:
            if p.is_file() and p.stat().st_size > 15000:
                t = p.read_text(encoding="utf-8-sig", errors="replace")
                if 'VERSION = "v4.80"' in t and "def upload_outputs_to_github(" in t:
                    return p.resolve()
        except OSError:
            pass
    try:
        ip = get_ipython()  # type: ignore[name-defined]
        hist = list(getattr(ip.history_manager, "input_hist_raw", []) or [])
        if not hist:
            hist = list(ip.user_ns.get("In", []) or [])
        for source in reversed(hist[-50:]):
            if isinstance(source, str) and 15000 < len(source) < 5 * 1024 * 1024 and 'VERSION = "v4.80"' in source and "def main()" in source:
                recovered = out_dir / SCRIPT_FILENAME
                recovered.write_text(source, encoding="utf-8")
                log(f"已從 Colab Notebook history 還原 v4.80 Python：{recovered}")
                return recovered
    except Exception:
        pass
    raise RuntimeError(f"無法定位 {SCRIPT_FILENAME}；請將下載的 .py 放在 /content 後執行。")


def create_reports(out_dir: Path, archive_records: List[Dict[str, Any]], member_rows: List[Dict[str, Any]], text_rows: List[Dict[str, Any]], csv_rows: List[Dict[str, Any]], public_records: List[Dict[str, Any]], v479_folder: Path, v479_manifest: Dict[str, Any], started: str) -> Dict[str, Any]:
    feature_status = make_feature_status(text_rows, csv_rows, member_rows)
    reports = []
    reports.append(("01_24項特徵逐項證據狀態.csv", ["feature", "feature_group", "evidence_status", "code_line_refs", "text_refs", "numeric_value_refs", "header_only_refs", "original_formula_confirmed"], feature_status))
    reports.append(("02_Drive歷史封存逐檔核驗.csv", ["version", "source_type", "filename", "size_bytes", "sha256", "crc", "member_count", "source_code_members", "model_binary_members", "status"], [dict(r, source_code_members=json.dumps(r.get("source_code_members", []), ensure_ascii=False), model_binary_members=json.dumps(r.get("model_binary_members", []), ensure_ascii=False), status=r.get("status", "OK")) for r in archive_records]))
    reports.append(("03_封存成員深度盤點.csv", ["version", "member", "size_bytes", "extension", "member_type", "text_hit_lines", "exact_feature_names_found", "csv_status", "exact_feature_columns", "rows_counted", "numeric_value_evidence"], member_rows))
    reports.append(("04_CSV欄位列數與數值樣本.csv", ["version", "member", "uncompressed_size", "columns", "exact_feature_columns", "exact_features", "related_columns", "rows_counted", "nonempty_exact_feature_counts", "numeric_value_evidence", "samples_related_columns", "header", "status"], csv_rows))
    reports.append(("05_歷史文字與程式公式線索.csv", ["version", "member", "line_no", "evidence_type", "matched_terms", "excerpt"], text_rows))
    reports.append(("06_GitHub公開封存核驗.csv", ["version", "filename", "size_bytes", "sha256", "status"], public_records))
    reports.append(("07_已知版本敘述與判讀限制.csv", ["version", "scope", "evidence", "formula_status"], [dict(r, formula_status="有版本文字證據；不代表精確公式已確認") for r in KNOWN_VERSION_EVIDENCE]))
    for fname, headers, rows in reports:
        n = save_csv(out_dir / fname, headers, rows)
        log(f"報告完成：{fname}；{n:,} rows；{(out_dir/fname).stat().st_size:,} bytes")

    status_counts = Counter(r["evidence_status"] for r in feature_status)
    exact_value_features = [r["feature"] for r in feature_status if r["numeric_value_refs"] != "[]"]
    code_features = [r["feature"] for r in feature_status if r["code_line_refs"] != "[]"]
    source_code_members = sum(len(r.get("source_code_members", [])) for r in archive_records)
    model_members = sum(len(r.get("model_binary_members", [])) for r in archive_records)
    csv_with_exact_feature = [r for r in csv_rows if int(r.get("exact_feature_columns") or 0) > 0]
    csv_with_actual_feature_values = [r for r in csv_with_exact_feature if r.get("numeric_value_evidence")]

    manifest = {
        "version": VERSION,
        "started_at_taipei": started,
        "finished_at_taipei": now_text(),
        "v479_source_folder": v479_folder.name,
        "v479_drive_files_scanned_reused": v479_manifest.get("drive_files_scanned"),
        "drive_full_inventory_repeated": False,
        "drive_archives_from_v479_manifest": len([r for r in archive_records if r.get("source_type") == "Drive" and r.get("status") == "OK"]),
        "public_archives_expected": len(PUBLIC_ARCHIVES),
        "public_archives_verified": len([r for r in public_records if "PASS" in r.get("status", "")]),
        "archive_members_scanned": len(member_rows),
        "source_code_members_found": source_code_members,
        "model_binary_members_found": model_members,
        "csv_members_examined": len(csv_rows),
        "csv_members_with_exact_feature_columns": len(csv_with_exact_feature),
        "csv_members_with_actual_exact_feature_values": len(csv_with_actual_feature_values),
        "feature_evidence_status_counts": dict(status_counts),
        "features_with_actual_numeric_values": exact_value_features,
        "features_with_code_lines": code_features,
        "all_24_original_formulas_confirmed": False,
        "model_retrained": False,
        "backtest_run": False,
        "drive_sources_read_only": True,
        "github_main_modified": False,
        "public_archive_commit": PUBLIC_COMMIT,
        "public_archives": public_records,
        "limitations": [
            "報告或設定檔列出特徵名稱與組成，不代表已取得逐項精確公式。",
            "只有實際資料列與特徵欄位非空值，才視為歷史數值證據；只有表頭不算。",
            "相關欄位名稱只能列為線索，不會自動視為24項原始特徵的同義欄位。",
            "如沒有原始公式程式碼，排名母體、平手規則、縮放方式、廣度分母、互動項定義仍不得宣稱已重建。",
        ],
    }
    (out_dir / "v4.80_manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    summary_path = out_dir / "v4.80_執行摘要.txt"
    with summary_path.open("w", encoding="utf-8") as f:
        f.write("看股最速適 v4.80｜24項特徵歷史輸出與公式來源深度追查\n")
        f.write("=" * 92 + "\n")
        f.write(f"開始時間：{started}\n完成時間：{manifest['finished_at_taipei']}\n")
        f.write(f"v4.79 清冊：{v479_folder.name}\n")
        f.write("本版不重掃整個 Drive；只讀取 v4.79 manifest 所列的原有 Drive ZIP，另核驗17份固定 commit 的 GitHub 歷史 ZIP。\n\n")
        f.write("一、執行統計\n")
        f.write(f"- Drive 歷史 ZIP 核驗：{manifest['drive_archives_from_v479_manifest']} 份。\n")
        f.write(f"- GitHub 公開歷史 ZIP：{manifest['public_archives_verified']}/{len(PUBLIC_ARCHIVES)} 份 SHA-256 通過。\n")
        f.write(f"- 封存成員：{len(member_rows):,} 個；CSV：{len(csv_rows):,} 個；文字／程式線索：{len(text_rows):,} 筆。\n")
        f.write(f"- 原始程式成員：{source_code_members}；模型二進位成員：{model_members}。\n")
        f.write(f"- 含精確24項欄位的 CSV：{len(csv_with_exact_feature)} 個；有實際非空特徵值：{len(csv_with_actual_feature_values)} 個。\n\n")
        f.write("二、24項特徵證據分級\n")
        for label, count in status_counts.items():
            f.write(f"- {label}：{count} 項。\n")
        if exact_value_features:
            f.write("- 找到實際特徵值的欄位：" + "、".join(exact_value_features) + "。\n")
        else:
            f.write("- 尚未找到任何一項24特徵欄位具備實際非空歷史值的資料列。\n")
        f.write("\n三、已確認的版本文字證據（不是完整公式）\n")
        for r in KNOWN_VERSION_EVIDENCE:
            f.write(f"- {r['version']}｜{r['scope']}：{r['evidence']}\n")
        f.write("\n四、目前可以與不可以下的結論\n")
        f.write("- 可以確認 v2.45 先使用8個相對排名特徵；v2.48.1 擴充至24個價格／市場狀態特徵。\n")
        f.write("- 可以確認市場狀態特徵按訊號日橫斷面計算；但各欄位排名母體、精確分母與 tie 規則未被原始程式證實。\n")
        f.write("- v2.50 另增4個量能特徵；v2.51 將4個量能特徵排除，價格／市場特徵沿用。\n")
        f.write("- v2.53 提醒分位方向不可只憑標籤文字；v2.54 的摘要顯示排名與校準獲利機率的關係比預測淨報酬更一致，但仍不是原始排名程式證明。\n")
        f.write("- 不能把欄名或說明文字當成精確公式；不可據此重訓或回測。\n")
        f.write("\n五、安全與限制\n")
        f.write("- Drive 原始封存 ZIP 唯讀；不重訓、不回測、不刪除或修改來源檔。\n")
        f.write("- GitHub 使用獨立 audit 分支；不修改 main。\n")
        f.write("- 本版產出的是來源證據盤點，不宣稱已成功重建24項原始公式。\n")
    return manifest


def write_upload_receipt(out_dir: Path, timestamp: str, upload: Dict[str, Any]) -> Path:
    payload = {
        "version": VERSION, "timestamp": timestamp, "branch": upload["branch"],
        "branch_url": upload["branch_url"], "main_sha_at_branch_creation": upload["main_sha"],
        "files": upload["files"], "main_modified": False,
    }
    p = out_dir / "v4.80_GitHub上傳回執.json"
    p.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return p


def main() -> None:
    started = now_text()
    timestamp = datetime.now(TZ).strftime("%Y%m%d_%H%M%S")
    print("=" * 96)
    print("看股最速適 v4.80｜24項特徵歷史輸出與公式來源深度追查")
    print("沿用 v4.79 清冊；Drive 來源唯讀；不重訓、不回測、不改 GitHub main")
    print("=" * 96)
    mount_drive()
    v479_folder, v479_manifest = find_latest_v479_folder()
    out_dir = DRIVE_ROOT / f"{OUTPUT_PREFIX}{timestamp}"
    out_dir.mkdir(parents=True, exist_ok=False)
    log(f"v4.80 輸出資料夾：{out_dir}")

    archive_records: List[Dict[str, Any]] = []
    member_rows: List[Dict[str, Any]] = []
    text_rows: List[Dict[str, Any]] = []
    csv_rows: List[Dict[str, Any]] = []
    seen_sha = set()

    drive_archive_records = [r for r in v479_manifest.get("archive_summary", []) if not r.get("source")]
    log(f"依 v4.79 manifest 取得 Drive ZIP 清單：{len(drive_archive_records)} 份；不重掃 Drive 全部檔案")
    for i, rec in enumerate(drive_archive_records, 1):
        path = resolve_manifest_archive(rec)
        if path is None:
            archive_records.append({"version": rec.get("version", ""), "source_type": "Drive", "filename": rec.get("filename", ""), "status": "NOT_FOUND_OR_SIZE_CHANGED"})
            log(f"找不到清冊所列原始 ZIP，略過：{rec.get('filename')}")
            continue
        expected_sha = str(rec.get("sha256") or "")
        actual_sha = sha256_file(path)
        if expected_sha and actual_sha != expected_sha:
            archive_records.append({"version": rec.get("version", path.name), "source_type": "Drive", "filename": path.name, "size_bytes": path.stat().st_size, "sha256": actual_sha, "status": "SHA256_CHANGED_SKIP"})
            log(f"來源 SHA 與 v4.79 清冊不同，為避免讀取變動來源而略過：{path.name}")
            continue
        if actual_sha in seen_sha:
            log(f"相同 SHA 封存 ZIP 已檢查，略過重複項目：{path.name}")
            continue
        seen_sha.add(actual_sha)
        try:
            ar, mr, tr, cr = inspect_archive(path, str(rec.get("version") or path.name), "Drive")
            ar["status"] = "OK"
            ar["expected_sha256_v479"] = expected_sha
            archive_records.append(ar)
            member_rows.extend(mr)
            text_rows.extend(tr)
            csv_rows.extend(cr)
        except Exception as exc:
            archive_records.append({"version": rec.get("version", path.name), "source_type": "Drive", "filename": path.name, "status": f"ERROR:{type(exc).__name__}:{exc}"})
            log(f"Drive ZIP 檢查失敗，繼續其他來源：{path.name}；{exc}")
        log(f"Drive ZIP 深度追查進度：{i}/{len(drive_archive_records)}")

    public_records: List[Dict[str, Any]] = []
    for i, meta in enumerate(PUBLIC_ARCHIVES, 1):
        version, filename, expected_size, expected_sha = meta
        try:
            path, dl_rec = download_public_archive(meta)
            if sha256_file(path) != expected_sha:
                raise RuntimeError("下載後 SHA-256 二次核驗失敗")
            ar, mr, tr, cr = inspect_archive(path, version + "_GitHub", "GitHub public")
            ar.update({"expected_size": expected_size, "expected_sha256": expected_sha, "status": "PASS"})
            public_records.append({**dl_rec, "crc": ar["crc"], "member_count": ar["member_count"], "status": "PASS"})
            archive_records.append(ar)
            member_rows.extend(mr)
            text_rows.extend(tr)
            csv_rows.extend(cr)
        except Exception as exc:
            public_records.append({"version": version, "filename": filename, "size_bytes": expected_size, "sha256": expected_sha, "status": f"ERROR:{type(exc).__name__}:{exc}"})
            log(f"公開 ZIP 檢查失敗，繼續其他來源：{version}；{exc}")
        log(f"GitHub 公開 ZIP 追查進度：{i}/{len(PUBLIC_ARCHIVES)}")

    log(f"全部來源檢查完成：archives={len(archive_records)}；members={len(member_rows)}；CSV={len(csv_rows)}；text hits={len(text_rows)}")
    manifest = create_reports(out_dir, archive_records, member_rows, text_rows, csv_rows, public_records, v479_folder, v479_manifest, started)

    script_path = resolve_script_path(out_dir)
    log(f"本次 Python 程式：{script_path}")
    zip_path = DRIVE_ROOT / f"{OUTPUT_PREFIX}{timestamp}.zip"
    report_files = sorted(p for p in out_dir.iterdir() if p.is_file() and p.name != "v4.80_GitHub上傳回執.json")
    log(f"開始建立報告 ZIP：{len(report_files)} 個檔案")
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
        for p in report_files:
            zf.write(p, arcname=p.name)
    with zipfile.ZipFile(zip_path, "r") as zf:
        bad = zf.testzip()
        if bad is not None:
            raise RuntimeError(f"v4.80 報告 ZIP CRC 失敗：{bad}")
    log(f"v4.80 報告 ZIP 完成：{zip_path.name}；{zip_path.stat().st_size:,} bytes；CRC PASS")

    upload = upload_outputs_to_github(zip_path, script_path, timestamp)
    receipt = write_upload_receipt(out_dir, timestamp, upload)
    log(f"GitHub 上傳回執：{receipt}")
    print("\n" + "=" * 96)
    print("v4.80 完成")
    print(f"輸出資料夾：{out_dir}")
    print(f"結果 ZIP：{zip_path}")
    print(f"GitHub 分支：{upload['branch']}")
    print(f"GitHub 分支網址：{upload['branch_url']}")
    for item in upload["files"]:
        print(f"GitHub 檔案：{item['repo_path']}｜{item['size_bytes']:,} bytes｜{item['url']}")
    print(f"證據狀態：{json.dumps(manifest['feature_evidence_status_counts'], ensure_ascii=False)}")
    print("確認：Drive 來源唯讀；GitHub main 未修改；未重訓、未回測。")
    print("=" * 96)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log("使用者中斷；Drive 來源與 GitHub main 均未修改。")
    except Exception as exc:
        log(f"執行失敗：{type(exc).__name__}: {exc}")
        raise