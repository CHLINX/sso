#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""看股最速適 v4.79｜24項原始特徵程式與歷史數值來源追查

目的：
1. 唯讀搜尋「看股最速適」Drive 專案與 Colab Notebooks 中的 Python／Notebook 原始程式。
2. 直接下載並核驗 v2.45、v2.48.1、v2.50、v2.51 停用量能四份公開封存 ZIP。
3. 檢查 ZIP 成員是否包含原始程式、模型二進位檔、具有24項特徵欄位且實際有資料列的 CSV。
4. 區分「只有特徵欄名」、「有特徵數值」及「真正公式程式碼」，不把空表當成公式證據。
5. 產生來源程式線索、封存成員、特徵資料列及 SHA-256 核驗報告。
6. 一次執行完成報告 ZIP、v4.79 Python 與 GitHub 獨立分支上傳。

安全原則：
- Drive 原始資料、原始程式及既有 ZIP 一律唯讀；僅在新 v4.79 輸出資料夾建立報告。
- 不重訓、不回測、不修改／刪除來源資料、不修改 GitHub main。
- 上傳至 CHLINX/sso 的獨立 audit 分支；Repository 為公開狀態，上傳檔案公開可讀。
- 不輸出來源程式全文、不輸出疑似 Token／密碼內容；只記錄相對路徑、行號、命中關鍵字與摘要。
- 不使用 SystemExit: 0；正常完成後由 main() 返回。

Colab 執行：
!python /content/SSO_v4_79_original_feature_source_trace_and_github_upload.py

GitHub Token：建議在 Colab Secrets 設定 GITHUB_TOKEN，需有 CHLINX/sso Contents Read and write 權限。
"""
from __future__ import annotations

import base64
import csv
import gzip
import hashlib
import io
import json
import math
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

VERSION = "v4.79"
REPO = "CHLINX/sso"
SCRIPT_FILENAME = "SSO_v4_79_original_feature_source_trace_and_github_upload.py"
OUTPUT_PREFIX = "看股最速適_24項原始特徵程式與歷史數值來源追查_v4.79_"
TZ = ZoneInfo("Asia/Taipei")
DRIVE_ROOT = Path("/content/drive/MyDrive/看股最速適")
NOTEBOOK_ROOT = Path("/content/drive/MyDrive/Colab Notebooks")

FEATURES = [
    "ret5_rank", "ret20_rank", "ret60_rank", "dist_ma20_rank", "dist_ma60_rank",
    "range_pos20_rank", "lowvol20_rank", "source_factor_rank",
    "ret5_abs_scaled", "ret20_abs_scaled", "ret60_abs_scaled", "dist_ma20_abs_scaled", "dist_ma60_abs_scaled",
    "market_median_ret5", "market_median_ret20", "market_median_ret60",
    "market_breadth_pos_ret5", "market_breadth_pos_ret20", "market_breadth_pos_ret60",
    "market_breadth_above_ma20", "market_breadth_above_ma60",
    "ret5_rank_x_market_breadth", "ret20_rank_x_market_breadth", "ret60_rank_x_market_breadth",
]
FEATURE_TOKENS = set(FEATURES)
SOURCE_TERMS = [
    "ret5_rank", "ret20_rank", "ret60_rank", "dist_ma20_rank", "dist_ma60_rank",
    "range_pos20_rank", "lowvol20_rank", "source_factor_rank", "abs_scaled",
    "market_breadth_pos_ret", "market_breadth_above_ma", "rank_x_market_breadth",
    "RANK_FEATURES", "FEATURE_COLUMNS", "feature_columns", "percentile", "rankdata",
    "method=average", "method='average'", 'method="average"', "ddof=1", "ddof=0",
]
SENSITIVE_RE = re.compile(
    r"(?i)(gh[pousr]_[A-Za-z0-9_]{20,}|github_pat_[A-Za-z0-9_]{20,}|"
    r"(token|password|passwd|secret|api[_-]?key)\s*[:=]\s*['\"][^'\"]{6,}['\"]|"
    r"AIza[0-9A-Za-z_\-]{25,})"
)

# SHA-256／檔案大小取自已核對的 v4.77 封存清冊；本程式仍會重新驗證。
PUBLIC_ARCHIVES = [
    {
        "version": "v2.45",
        "filename": "SSO_v2.45_樣本外校準門檻與公司名稱結果.zip",
        "size": 20731287,
        "sha256": "3dc4e9501c9fdf8b22a8f103742cddadb161c449eb369c197d1f116f1de31aaf",
    },
    {
        "version": "v2.48.1",
        "filename": "SSO_v2.48.1_樣本外校準門檻與公司名稱結果.zip",
        "size": 21010200,
        "sha256": "c45cec0752e5337130a72ca483accd2bd74b62bf76b24e1205e507aa54a58e2e",
    },
    {
        "version": "v2.50",
        "filename": "SSO_v2.50_日成交量特徵與500日檢查點結果.zip",
        "size": 21654521,
        "sha256": "d7908f72d102abffdf1cf143300f164b36ea95dfec25545f33800ed90a87c8b6",
    },
    {
        "version": "v2.51_no_volume",
        "filename": "SSO_v2.51_量能消融_停用量能特徵與配對比較結果.zip",
        "size": 21011289,
        "sha256": "e472aa8a1f6a79bd2511e5209a5ab8a3e74e5d52d93f9196bc80617b59373949",
    },
]


def now_text() -> str:
    return datetime.now(TZ).isoformat(timespec="seconds")


def log(message: str) -> None:
    print(f"[{datetime.now(TZ).strftime('%H:%M:%S')}] {message}", flush=True)


def sha256_file(path: Path, chunk_size: int = 4 * 1024 * 1024) -> str:
    h = hashlib.sha256()
    total = 0
    with path.open("rb") as f:
        while True:
            block = f.read(chunk_size)
            if not block:
                break
            h.update(block)
            total += len(block)
            if total and total % (128 * 1024 * 1024) < chunk_size:
                log(f"SHA256讀取：{path.name}；已讀 {total:,} bytes")
    return h.hexdigest()


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


def walk_scoped_files(roots: Sequence[Path]) -> Tuple[List[Path], int, int]:
    files: List[Path] = []
    total_bytes = 0
    seen = set()
    walked = 0
    for base in roots:
        if not base.exists():
            log(f"略過不存在的搜尋位置：{base}")
            continue
        for root, dirs, names in os.walk(base):
            dirs[:] = [d for d in dirs if d not in {".git", "__pycache__", ".ipynb_checkpoints", "node_modules"}]
            for name in names:
                path = Path(root) / name
                try:
                    rp = str(path.resolve())
                    if rp in seen:
                        continue
                    seen.add(rp)
                    size = path.stat().st_size
                except OSError:
                    continue
                files.append(path)
                total_bytes += size
                walked += 1
                if walked % 200 == 0:
                    log(f"Drive檔案盤點：已掃描 {walked:,} 個檔案；累計大小 {total_bytes:,} bytes；位置 {path.parent}")
    log(f"Drive檔案盤點完成：{len(files):,} 個檔案；總大小 {total_bytes:,} bytes")
    return files, len(files), total_bytes


def relative_path(path: Path, roots: Sequence[Path]) -> str:
    for root in roots:
        try:
            return str(path.relative_to(root)).replace("\\", "/")
        except Exception:
            pass
    return path.name


def safe_excerpt(line: str, limit: int = 260) -> str:
    if SENSITIVE_RE.search(line):
        return "[疑似敏感值已遮蔽]"
    cleaned = re.sub(r"\s+", " ", line).strip()
    return cleaned[:limit]


def scan_source_file(path: Path, roots: Sequence[Path]) -> Dict[str, Any]:
    terms = set(SOURCE_TERMS)
    hit_counts: Counter = Counter()
    hit_lines: List[Dict[str, Any]] = []
    line_count = 0
    try:
        with path.open("r", encoding="utf-8-sig", errors="replace") as f:
            for line_count, line in enumerate(f, 1):
                low = line.lower()
                matched = [term for term in terms if term.lower() in low]
                if not matched:
                    continue
                for term in matched:
                    hit_counts[term] += 1
                if len(hit_lines) < 80:
                    hit_lines.append({
                        "line": line_count,
                        "terms": sorted(matched),
                        "excerpt": safe_excerpt(line),
                    })
                if line_count % 100000 == 0:
                    log(f"原始程式掃描：{path.name}；已讀 {line_count:,} 行；命中 {sum(hit_counts.values()):,} 次")
    except Exception as exc:
        return {
            "relative_path": relative_path(path, roots), "size_bytes": path.stat().st_size,
            "status": f"READ_ERROR:{type(exc).__name__}:{exc}", "line_count": line_count,
            "hit_counts": {}, "hit_lines": [],
        }
    return {
        "relative_path": relative_path(path, roots),
        "size_bytes": path.stat().st_size,
        "sha256": sha256_file(path),
        "status": "SCANNED",
        "line_count": line_count,
        "hit_counts": dict(hit_counts),
        "hit_lines": hit_lines,
        "source_candidate": sum(hit_counts.values()) >= 3 or len(hit_counts) >= 2,
    }


def read_csv_member_header(zf: zipfile.ZipFile, member: str) -> Tuple[List[str], Any]:
    raw_stream = zf.open(member, "r")
    if member.lower().endswith(".gz"):
        binary_stream = gzip.GzipFile(fileobj=raw_stream, mode="rb")
    else:
        binary_stream = raw_stream
    text_stream = io.TextIOWrapper(binary_stream, encoding="utf-8-sig", errors="replace", newline="")
    reader = csv.reader(text_stream)
    header = next(reader, [])
    return header, (reader, text_stream, raw_stream)


def inspect_feature_csv(zf: zipfile.ZipFile, member: str, expected_names: set, archive_version: str) -> Dict[str, Any]:
    header, streams = read_csv_member_header(zf, member)
    reader, text_stream, raw_stream = streams
    hits = [col for col in header if col in expected_names]
    code_columns = [col for col in header if re.search(r"feature|特徵|factor|因子|rank|scaled|breadth", col, re.I)]
    should_count = len(hits) >= 4 or (len(hits) >= 1 and any(k in member for k in ("特徵", "候選", "模型", "feature", "factor")))
    row_count = 0
    nonempty: Counter = Counter()
    sample_rows: List[Dict[str, str]] = []
    if should_count:
        index = {name: i for i, name in enumerate(header)}
        for row in reader:
            row_count += 1
            for col in hits:
                i = index.get(col, -1)
                if i >= 0 and i < len(row) and row[i].strip() not in {"", "NA", "NaN", "nan", "None", "null"}:
                    nonempty[col] += 1
            if len(sample_rows) < 2 and any((row[index[c]].strip() if index.get(c, -1) >= 0 and index[c] < len(row) else "") for c in hits):
                sample_rows.append({
                    col: (row[index[col]][:120] if index[col] < len(row) else "")
                    for col in hits[:24]
                })
            if row_count % 250000 == 0:
                log(f"ZIP CSV掃描：{archive_version}/{member}；已讀 {row_count:,} 列；特徵欄 {len(hits)} 項")
    try:
        text_stream.close()
    except Exception:
        try:
            raw_stream.close()
        except Exception:
            pass
    return {
        "member": member,
        "uncompressed_size": zf.getinfo(member).file_size,
        "columns": len(header),
        "feature_columns": hits,
        "feature_column_count": len(hits),
        "related_header_columns": code_columns[:80],
        "rows_counted": row_count if should_count else None,
        "nonempty_value_counts": dict(nonempty),
        "sample_rows": sample_rows,
        "data_evidence": bool(row_count and any(nonempty.values())),
        "count_skipped": not should_count,
    }


def inspect_zip(path: Path, version: str, roots: Sequence[Path]) -> Tuple[Dict[str, Any], List[Dict[str, Any]], List[Dict[str, Any]]]:
    archive_record: Dict[str, Any] = {
        "version": version,
        "relative_path": relative_path(path, roots) if path.exists() else path.name,
        "filename": path.name,
        "size_bytes": path.stat().st_size,
        "sha256": sha256_file(path),
        "crc": "NOT_CHECKED",
        "member_count": 0,
        "python_or_notebook_members": [],
        "model_binary_members": [],
    }
    member_rows: List[Dict[str, Any]] = []
    data_rows: List[Dict[str, Any]] = []
    with zipfile.ZipFile(path, "r") as zf:
        bad = zf.testzip()
        archive_record["crc"] = "PASS" if bad is None else f"FAIL:{bad}"
        if bad is not None:
            raise RuntimeError(f"ZIP CRC 失敗：{path.name}：{bad}")
        infos = [x for x in zf.infolist() if not x.is_dir()]
        archive_record["member_count"] = len(infos)
        log(f"檢查 ZIP：{version}；成員 {len(infos)} 個；未解壓大小 {sum(x.file_size for x in infos):,} bytes")
        for idx, info in enumerate(infos, 1):
            name = info.filename
            low = name.lower()
            rec: Dict[str, Any] = {
                "version": version, "member": name, "size_bytes": info.file_size,
                "extension": Path(name).suffix.lower(), "feature_name_hits": [],
                "source_code_member": False, "model_binary_member": False,
            }
            if low.endswith((".py", ".ipynb", ".r", ".jl", ".sql")):
                archive_record["python_or_notebook_members"].append(name)
                rec["source_code_member"] = True
                try:
                    raw = zf.read(name)
                    text = raw.decode("utf-8-sig", errors="replace")
                    hits = [t for t in SOURCE_TERMS if t.lower() in text.lower()]
                    rec["feature_name_hits"] = hits
                    rec["code_keyword_counts"] = {t: text.lower().count(t.lower()) for t in hits}
                except Exception as exc:
                    rec["read_error"] = f"{type(exc).__name__}:{exc}"
            elif low.endswith((".pkl", ".pickle", ".joblib", ".cbm", ".pt", ".pth", ".onnx", ".h5", ".keras", ".safetensors")):
                archive_record["model_binary_members"].append(name)
                rec["model_binary_member"] = True
            elif low.endswith((".csv", ".csv.gz", ".tsv", ".tsv.gz")):
                try:
                    result = inspect_feature_csv(zf, name, FEATURE_TOKENS, version)
                    rec.update(result)
                    if result["feature_column_count"] or result["related_header_columns"]:
                        data_rows.append({"version": version, **result})
                except Exception as exc:
                    rec["read_error"] = f"{type(exc).__name__}:{exc}"
            elif low.endswith((".json", ".txt", ".md", ".yaml", ".yml", ".ini", ".cfg")) and info.file_size <= 5 * 1024 * 1024:
                try:
                    raw = zf.read(name)
                    if low.endswith(".gz"):
                        raw = gzip.decompress(raw)
                    text = raw.decode("utf-8-sig", errors="replace")
                    rec["feature_name_hits"] = [t for t in SOURCE_TERMS if t.lower() in text.lower()]
                    rec["formula_keyword_hits"] = [t for t in ("公式", "計算方式", "rank(", "percentile", "zscore", "standardscaler", "ddof", "rolling(") if t.lower() in text.lower()]
                except Exception as exc:
                    rec["read_error"] = f"{type(exc).__name__}:{exc}"
            member_rows.append(rec)
            if idx % 10 == 0 or idx == len(infos):
                log(f"ZIP成員進度：{version} {idx:,}/{len(infos):,}；目前 {name}")
    return archive_record, member_rows, data_rows


def download_public_archive(meta: Dict[str, Any], cache: Path) -> Path:
    cache.mkdir(parents=True, exist_ok=True)
    target = cache / meta["filename"]
    if target.exists() and target.stat().st_size == meta["size"]:
        digest = sha256_file(target)
        if digest == meta["sha256"]:
            log(f"沿用已核驗的暫存 ZIP：{target.name}")
            return target
        target.unlink()
    url = "https://raw.githubusercontent.com/CHLINX/sso/main/" + urllib.parse.quote(meta["filename"], safe="")
    log(f"從公開 GitHub 下載 {meta['version']}：{meta['filename']}；預期 {meta['size']:,} bytes")
    request = urllib.request.Request(url, headers={"User-Agent": "CHLINX-SSO-v479-source-trace/1.0"})
    digest = hashlib.sha256()
    total = 0
    started = time.time()
    tmp = target.with_suffix(target.suffix + ".part")
    with urllib.request.urlopen(request, timeout=240) as response, tmp.open("wb") as f:
        while True:
            block = response.read(4 * 1024 * 1024)
            if not block:
                break
            f.write(block)
            digest.update(block)
            total += len(block)
            elapsed = max(0.001, time.time() - started)
            log(f"下載進度 {meta['version']}：{total:,}/{meta['size']:,} bytes；{total/elapsed:,.0f} bytes/s")
    got_sha = digest.hexdigest()
    if total != meta["size"] or got_sha != meta["sha256"]:
        tmp.unlink(missing_ok=True)
        raise RuntimeError(f"{meta['version']} ZIP 驗證失敗：bytes={total} sha256={got_sha}")
    tmp.replace(target)
    log(f"下載與 SHA-256 驗證完成：{meta['version']}；{total:,} bytes")
    return target


def save_csv(path: Path, headers: Sequence[str], rows: Iterable[Dict[str, Any]]) -> int:
    n = 0
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(headers), extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
            n += 1
    return n


def github_request(token: str, method: str, api_path: str, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    url = "https://api.github.com" + api_path
    body = None if payload is None else json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        url, data=body, method=method,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "CHLINX-SSO-v479-source-trace/1.0",
            "Content-Type": "application/json; charset=utf-8",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as response:
            raw = response.read()
            return json.loads(raw.decode("utf-8")) if raw else {}
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")[:2000]
        raise RuntimeError(f"GitHub API {method} {api_path} 失敗：HTTP {exc.code} {detail}") from exc


def upload_outputs_to_github(out_dir: Path, zip_path: Path, script_path: Path, timestamp: str) -> Dict[str, Any]:
    token = get_github_token()
    repo_path = "/repos/CHLINX/sso"
    main_ref = github_request(token, "GET", repo_path + "/git/ref/heads/main")
    main_sha = main_ref["object"]["sha"]
    base_branch = f"audit/v4.79-source-trace-{timestamp}"
    branch = base_branch
    # 若前次上傳曾建立分支但中途失敗，使用新的 retry 分支，不覆寫既有分支，也不碰 main。
    for attempt in range(0, 20):
        branch = base_branch if attempt == 0 else f"{base_branch}-retry-{attempt:02d}"
        try:
            github_request(token, "POST", repo_path + "/git/refs", {"ref": f"refs/heads/{branch}", "sha": main_sha})
            break
        except RuntimeError as exc:
            message = str(exc)
            if "HTTP 422" in message and ("Reference already exists" in message or "already exists" in message):
                log(f"GitHub 分支已存在，改用續傳分支：{branch}")
                continue
            raise
    else:
        raise RuntimeError(f"無法建立唯一的 GitHub 上傳分支：{base_branch}")
    log(f"已建立 GitHub 獨立分支：{branch}；起點 main SHA={main_sha}（未修改 main）")

    files = [zip_path, script_path]
    uploaded = []
    for i, file_path in enumerate(files, 1):
        repo_file = f"audit_uploads/v4.79_{timestamp[:8]}/{file_path.name}"
        api_path = repo_path + "/contents/" + urllib.parse.quote(repo_file, safe="/")
        data = file_path.read_bytes()
        encoded = base64.b64encode(data).decode("ascii")
        log(f"GitHub 上傳 {i}/{len(files)}：{file_path.name}；{len(data):,} bytes")
        result = github_request(token, "PUT", api_path, {
            "message": f"Upload SSO v4.79 source trace: {file_path.name}",
            "content": encoded,
            "branch": branch,
        })
        content = result.get("content") or {}
        remote_size = int(content.get("size") or 0)
        if remote_size != len(data):
            raise RuntimeError(f"GitHub 上傳大小不符：{file_path.name} local={len(data)} remote={remote_size}")
        uploaded.append({
            "local_file": file_path.name,
            "repo_path": repo_file,
            "size_bytes": len(data),
            "sha": content.get("sha", ""),
            "html_url": content.get("html_url", ""),
            "raw_url": f"https://raw.githubusercontent.com/CHLINX/sso/{urllib.parse.quote(branch, safe='/')}/{urllib.parse.quote(repo_file, safe='/')}",
        })
    branch_url = f"https://github.com/CHLINX/sso/tree/{urllib.parse.quote(branch, safe='/')}"
    log(f"GitHub 上傳完成：{branch_url}")
    for item in uploaded:
        log(f"GitHub 檔案：{item['repo_path']}；{item['size_bytes']:,} bytes；{item['html_url']}")
    return {"branch": branch, "branch_url": branch_url, "main_sha": main_sha, "files": uploaded}


def _looks_like_this_script(path: Path) -> bool:
    """確認候選檔案確實是本版 v4.79 程式，避免誤上傳 kernel.json 等檔案。"""
    try:
        if not path.is_file() or path.stat().st_size < 12000 or path.stat().st_size > 5 * 1024 * 1024:
            return False
        text = path.read_text(encoding="utf-8-sig", errors="replace")
        return (
            'VERSION = "v4.79"' in text
            and "def upload_outputs_to_github(" in text
            and "24項原始特徵程式與歷史數值來源追查" in text
        )
    except OSError:
        return False


def _notebook_history_sources(ipython_obj: Any) -> List[str]:
    """取得最近的 IPython 輸入歷史；只回傳含完整 v4.79 主程式結構的程式碼。"""
    candidates: List[str] = []
    try:
        history = list(getattr(ipython_obj.history_manager, "input_hist_raw", []) or [])
    except Exception:
        history = []
    if not history:
        try:
            history = list(ipython_obj.user_ns.get("In", []) or [])
        except Exception:
            history = []
    for source in reversed(history[-40:]):
        if not isinstance(source, str) or len(source) < 12000 or len(source) > 5 * 1024 * 1024:
            continue
        if (
            'VERSION = "v4.79"' in source
            and "def upload_outputs_to_github(" in source
            and "def main()" in source
            and "24項原始特徵程式與歷史數值來源追查" in source
        ):
            candidates.append(source)
    return candidates


def resolve_script_path(out_dir: Path, scanned_files: Sequence[Path]) -> Path:
    """同時支援 .py、Colab 儲存格直接執行及由 Notebook 歷史還原程式。"""
    candidates: List[Path] = []

    # 1. 正常 .py 執行時，__file__ 是最可靠來源。
    file_value = globals().get("__file__")
    if isinstance(file_value, str) and file_value not in {"", "<stdin>", "<string>"}:
        candidates.append(Path(file_value))

    # 2. 使用者可明確指定路徑；再檢查常見 Colab 暫存位置。
    env_path = os.environ.get("SSO_V479_SCRIPT_PATH", "").strip()
    if env_path:
        candidates.append(Path(env_path).expanduser())
    if sys.argv and sys.argv[0] and sys.argv[0] not in {"-c", "-"}:
        candidates.append(Path(sys.argv[0]).expanduser())
    candidates.extend([
        Path("/content") / SCRIPT_FILENAME,
        Path.cwd() / SCRIPT_FILENAME,
        DRIVE_ROOT / SCRIPT_FILENAME,
    ])

    # 3. 若程式本身已存在 Drive 掃描清單，直接從已盤點項目尋找，不重掃 Drive。
    candidates.extend(p for p in scanned_files if p.name == SCRIPT_FILENAME)

    seen = set()
    for candidate in candidates:
        try:
            resolved = candidate.resolve()
        except OSError:
            continue
        if str(resolved) in seen:
            continue
        seen.add(str(resolved))
        if _looks_like_this_script(resolved):
            log(f"已定位 v4.79 Python 程式：{resolved}")
            return resolved

    # 4. Colab 直接執行整個程式碼儲存格時沒有 __file__；由 IPython 輸入歷史還原完整程式。
    try:
        ipython_obj = get_ipython()  # type: ignore[name-defined]
    except Exception:
        ipython_obj = None
    if ipython_obj is not None:
        history_sources = _notebook_history_sources(ipython_obj)
        if history_sources:
            recovered = out_dir / SCRIPT_FILENAME
            recovered.write_text(history_sources[0], encoding="utf-8")
            if _looks_like_this_script(recovered):
                log(f"Colab 儲存格沒有 __file__；已從 Notebook 輸入歷史還原完整 Python：{recovered}")
                return recovered

    raise RuntimeError(
        "無法定位可上傳的 v4.79 Python 原始檔。若以 Colab 儲存格執行，請確認本次完整程式碼仍在 Notebook 輸入歷史；"
        "也可將程式存成 " + SCRIPT_FILENAME + " 後執行，或設定環境變數 SSO_V479_SCRIPT_PATH。"
    )


def find_resumable_run() -> Optional[Tuple[Path, Path, str]]:
    """尋找上次已完成報告與 ZIP、但在 GitHub 上傳前中斷的 v4.79 工作。"""
    required_reports = [
        "01_Drive原始程式與Notebook掃描.csv",
        "02_ZIP封存成員與程式模型清單.csv",
        "03_含24項特徵欄位的CSV列數與數值證據.csv",
        "04_GitHub封存ZIP核驗.csv",
        "v4.79_manifest.json",
        "v4.79_執行摘要.txt",
    ]
    try:
        folders = sorted(
            (p for p in DRIVE_ROOT.glob(OUTPUT_PREFIX + "*") if p.is_dir()),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
    except OSError:
        return None

    for folder in folders:
        timestamp = folder.name[len(OUTPUT_PREFIX):]
        if not re.fullmatch(r"\d{8}_\d{6}", timestamp):
            continue
        receipt = folder / "v4.79_GitHub上傳回執.json"
        if receipt.exists():
            continue
        if not all((folder / name).is_file() for name in required_reports):
            continue
        zip_path = DRIVE_ROOT / f"{OUTPUT_PREFIX}{timestamp}.zip"
        if not zip_path.is_file():
            continue
        try:
            manifest = json.loads((folder / "v4.79_manifest.json").read_text(encoding="utf-8"))
            if manifest.get("version") != VERSION:
                continue
            with zipfile.ZipFile(zip_path, "r") as zf:
                if zf.testzip() is not None:
                    continue
                names = set(zf.namelist())
                if not set(required_reports).issubset(names):
                    continue
        except Exception:
            continue
        return folder, zip_path, timestamp
    return None


def write_upload_receipt(out_dir: Path, timestamp: str, upload: Dict[str, Any]) -> Path:
    receipt = {
        "version": VERSION,
        "timestamp": timestamp,
        "branch": upload["branch"],
        "branch_url": upload["branch_url"],
        "main_sha_at_branch_creation": upload["main_sha"],
        "files": upload["files"],
        "main_modified": False,
    }
    receipt_path = out_dir / "v4.79_GitHub上傳回執.json"
    receipt_path.write_text(json.dumps(receipt, ensure_ascii=False, indent=2), encoding="utf-8")
    return receipt_path


def print_completion(out_dir: Path, zip_path: Path, upload: Dict[str, Any], resumed: bool = False) -> None:
    print("\n" + "=" * 96)
    print("v4.79 完成" + ("（續傳既有報告，未重做掃描）" if resumed else ""))
    print(f"輸出資料夾：{out_dir}")
    print(f"結果 ZIP：{zip_path}")
    print(f"GitHub 分支：{upload['branch']}")
    print(f"GitHub 分支網址：{upload['branch_url']}")
    for item in upload["files"]:
        print(f"GitHub 檔案：{item['repo_path']}｜{item['size_bytes']:,} bytes｜{item['html_url']}")
    print("確認：Drive 來源唯讀；GitHub main 未修改；未重訓、未回測。")
    print("=" * 96)


def main() -> None:
    started = now_text()
    timestamp = datetime.now(TZ).strftime("%Y%m%d_%H%M%S")
    print("=" * 96)
    print("看股最速適 v4.79｜24項原始特徵程式與歷史數值來源追查")
    print("來源唯讀、不重訓、不回測、不改 GitHub main；一次執行整合 ZIP 與 Python 上傳")
    print("=" * 96)

    mount_drive()

    # 前次若已完成全量掃描、報告及 ZIP，只在最後上傳步驟中斷，直接續傳避免重做。
    resumable = find_resumable_run()
    if resumable is not None:
        out_dir, zip_path, timestamp = resumable
        log(f"找到上次未完成上傳的完整 v4.79 報告：{out_dir}")
        log("既有報告與 ZIP 已通過完整性檢查；直接續傳，不重新掃描 Drive 或公開封存 ZIP。")
        script_path = resolve_script_path(out_dir, [])
        upload = upload_outputs_to_github(out_dir, zip_path, script_path, timestamp)
        receipt_path = write_upload_receipt(out_dir, timestamp, upload)
        log(f"GitHub 上傳回執已另存：{receipt_path}")
        print_completion(out_dir, zip_path, upload, resumed=True)
        return

    roots = [DRIVE_ROOT]
    if NOTEBOOK_ROOT.exists():
        roots.append(NOTEBOOK_ROOT)
    files, file_count, total_bytes = walk_scoped_files(roots)

    out_dir = DRIVE_ROOT / f"看股最速適_24項原始特徵程式與歷史數值來源追查_v4.79_{timestamp}"
    out_dir.mkdir(parents=True, exist_ok=False)
    cache = Path("/content/SSO_v4_79_public_archive_cache")
    cache.mkdir(parents=True, exist_ok=True)
    log(f"輸出資料夾：{out_dir}")

    source_paths = [p for p in files if p.suffix.lower() in {".py", ".ipynb", ".r", ".jl", ".sql"}]
    source_records = []
    log(f"開始掃描原始程式／Notebook：{len(source_paths):,} 個檔案")
    for i, path in enumerate(source_paths, 1):
        # 排除 v4.x 資料核驗／重建工具本身，避免將稽核程式誤認為 v2.x 模型原始程式。
        if re.search(r"v4[_\.]?\d|source_trace|lineage|reconstruction", path.name, re.I):
            continue
        try:
            if path.stat().st_size > 150 * 1024 * 1024:
                log(f"略過超過150MB的程式／Notebook：{path.name}；{path.stat().st_size:,} bytes")
                continue
        except OSError:
            continue
        source_records.append(scan_source_file(path, roots))
        if i % 25 == 0 or i == len(source_paths):
            log(f"原始程式掃描進度：{i:,}/{len(source_paths):,}；已完成 {len(source_records):,} 個")

    # 只掃描名稱與 SSO 特徵重建有關的 ZIP，排除龐大的官方行情備份包。
    zip_paths = []
    zip_name_re = re.compile(r"(v2[._-]?(45|46|47|48|50|51)|v4[._-]?(66|68|69|70|77|78)|特徵|公式|因子|候選|樣本外|模型)", re.I)
    for path in files:
        if path.suffix.lower() != ".zip":
            continue
        if any(k in path.name for k in ("官方行情備份", "500交易日備份", "星期六補行官方行情")):
            continue
        if zip_name_re.search(path.name):
            # v4.78 是本次重新計算的候選值，不是歷史原始特徵輸出，不能列為原始數值證據。
            if re.search(r"v4[._-]?78|v4[._-]?79", path.name, re.I):
                continue
            zip_paths.append(path)
    log(f"Drive 相關 ZIP 候選：{len(zip_paths):,} 個；不掃描官方行情備份 ZIP")

    archive_records = []
    member_records = []
    feature_data_records = []
    seen_sha = set()
    for i, path in enumerate(zip_paths, 1):
        try:
            if path.stat().st_size > 800 * 1024 * 1024:
                log(f"略過超過800MB的 ZIP：{path.name}；{path.stat().st_size:,} bytes")
                continue
            sha = sha256_file(path)
            if sha in seen_sha:
                log(f"略過重複 ZIP（SHA-256 相同）：{path.name}")
                continue
            seen_sha.add(sha)
            version = path.name[:80]
            ar, mr, dr = inspect_zip(path, version, roots)
            archive_records.append(ar)
            member_records.extend(mr)
            feature_data_records.extend(dr)
        except Exception as exc:
            archive_records.append({
                "version": path.name, "relative_path": relative_path(path, roots),
                "size_bytes": path.stat().st_size, "status": f"ERROR:{type(exc).__name__}:{exc}",
            })
            log(f"ZIP 檢查錯誤（繼續其他檔案）：{path.name}；{type(exc).__name__}: {exc}")
        if i % 10 == 0 or i == len(zip_paths):
            log(f"Drive ZIP 掃描進度：{i:,}/{len(zip_paths):,}；已完成 {len(archive_records):,} 個")

    # 另外從 GitHub 公開封存版本下載四個已核對 ZIP，避免 Drive 未保存這些檔案而漏查。
    public_records = []
    for i, meta in enumerate(PUBLIC_ARCHIVES, 1):
        try:
            path = download_public_archive(meta, cache)
            ar, mr, dr = inspect_zip(path, meta["version"] + "_GitHub", [])
            ar["source"] = "GitHub main public archive"
            ar["expected_size"] = meta["size"]
            ar["expected_sha256"] = meta["sha256"]
            public_records.append(ar)
            member_records.extend(mr)
            feature_data_records.extend(dr)
        except Exception as exc:
            public_records.append({"version": meta["version"], "filename": meta["filename"], "status": f"ERROR:{type(exc).__name__}:{exc}"})
            log(f"公開 ZIP 追查失敗（繼續）：{meta['version']}；{type(exc).__name__}: {exc}")
        log(f"公開封存版本追查進度：{i}/{len(PUBLIC_ARCHIVES)}")

    # Evidence counts: source code, feature-bearing tables, and tables with actual values.
    source_candidates = [r for r in source_records if r.get("source_candidate")]
    archive_code = [r for r in member_records if r.get("source_code_member")]
    archive_models = [r for r in member_records if r.get("model_binary_member")]
    feature_members = [r for r in feature_data_records if r.get("feature_column_count", 0) > 0]
    data_members = [r for r in feature_members if r.get("data_evidence")]
    empty_feature_members = [r for r in feature_members if r.get("rows_counted") == 0]

    source_csv = out_dir / "01_Drive原始程式與Notebook掃描.csv"
    member_csv = out_dir / "02_ZIP封存成員與程式模型清單.csv"
    feature_csv = out_dir / "03_含24項特徵欄位的CSV列數與數值證據.csv"
    public_csv = out_dir / "04_GitHub封存ZIP核驗.csv"
    save_csv(source_csv,
             ["relative_path", "size_bytes", "sha256", "status", "line_count", "source_candidate", "hit_counts", "hit_lines"],
             ({"relative_path": r.get("relative_path"), "size_bytes": r.get("size_bytes"), "sha256": r.get("sha256"),
               "status": r.get("status"), "line_count": r.get("line_count"), "source_candidate": r.get("source_candidate"),
               "hit_counts": json.dumps(r.get("hit_counts", {}), ensure_ascii=False),
               "hit_lines": json.dumps(r.get("hit_lines", []), ensure_ascii=False)} for r in source_records))
    save_csv(member_csv,
             ["version", "member", "size_bytes", "extension", "source_code_member", "model_binary_member", "feature_name_hits", "formula_keyword_hits", "read_error"],
             ({"version": r.get("version"), "member": r.get("member"), "size_bytes": r.get("size_bytes"),
               "extension": r.get("extension"), "source_code_member": r.get("source_code_member"),
               "model_binary_member": r.get("model_binary_member"),
               "feature_name_hits": json.dumps(r.get("feature_name_hits", []), ensure_ascii=False),
               "formula_keyword_hits": json.dumps(r.get("formula_keyword_hits", []), ensure_ascii=False),
               "read_error": r.get("read_error", "")} for r in member_records))
    save_csv(feature_csv,
             ["version", "member", "uncompressed_size", "columns", "feature_column_count", "feature_columns", "rows_counted", "nonempty_value_counts", "data_evidence", "count_skipped", "sample_rows"],
             ({"version": r.get("version"), "member": r.get("member"), "uncompressed_size": r.get("uncompressed_size"),
               "columns": r.get("columns"), "feature_column_count": r.get("feature_column_count"),
               "feature_columns": json.dumps(r.get("feature_columns", []), ensure_ascii=False),
               "rows_counted": r.get("rows_counted"),
               "nonempty_value_counts": json.dumps(r.get("nonempty_value_counts", {}), ensure_ascii=False),
               "data_evidence": r.get("data_evidence"), "count_skipped": r.get("count_skipped"),
               "sample_rows": json.dumps(r.get("sample_rows", []), ensure_ascii=False)} for r in feature_data_records))
    save_csv(public_csv,
             ["version", "filename", "size_bytes", "sha256", "expected_size", "expected_sha256", "crc", "member_count", "python_or_notebook_members", "model_binary_members", "status"],
             ({"version": r.get("version"), "filename": r.get("filename"), "size_bytes": r.get("size_bytes"),
               "sha256": r.get("sha256"), "expected_size": r.get("expected_size"), "expected_sha256": r.get("expected_sha256"),
               "crc": r.get("crc"), "member_count": r.get("member_count"),
               "python_or_notebook_members": json.dumps(r.get("python_or_notebook_members", []), ensure_ascii=False),
               "model_binary_members": json.dumps(r.get("model_binary_members", []), ensure_ascii=False),
               "status": r.get("status", "OK")} for r in public_records))

    summary = {
        "version": VERSION,
        "started_at_taipei": started,
        "finished_at_taipei": now_text(),
        "drive_project_root": "看股最速適/ (relative path only)",
        "colab_notebook_root_scanned": NOTEBOOK_ROOT.exists(),
        "drive_files_scanned": file_count,
        "drive_total_file_bytes": total_bytes,
        "source_files_scanned": len(source_records),
        "source_files_with_formula_keyword_hits": len(source_candidates),
        "source_code_members_in_scanned_drive_zips": len(archive_code),
        "model_binary_members_in_scanned_drive_zips": len(archive_models),
        "feature_bearing_csv_members": len(feature_members),
        "feature_bearing_csv_members_with_data_rows": len(data_members),
        "feature_bearing_csv_members_header_only": len(empty_feature_members),
        "feature_names": FEATURES,
        "drive_zip_archives_scanned": len(archive_records),
        "github_archives_scanned": len(public_records),
        "source_code_candidates": [{"path": x.get("relative_path"), "hit_counts": x.get("hit_counts"), "line_count": x.get("line_count")} for x in source_candidates],
        "data_evidence_members": [{"version": x.get("version"), "member": x.get("member"), "rows": x.get("rows_counted"), "feature_columns": x.get("feature_columns")} for x in data_members],
        "empty_feature_members": [{"version": x.get("version"), "member": x.get("member"), "feature_columns": x.get("feature_columns")} for x in empty_feature_members],
        "archive_summary": archive_records + public_records,
        "source_read_only": True,
        "model_retrained": False,
        "backtest_run": False,
        "github_main_modified": False,
        "interpretation": (
            "只有找到原始計算程式或含實際24項特徵數值的歷史資料列，才可進入逐項公式比對；"
            "只有欄名、空表或報告文字均不視為公式驗證證據。"
        ),
    }
    manifest_path = out_dir / "v4.79_manifest.json"
    manifest_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    report_path = out_dir / "v4.79_執行摘要.txt"
    with report_path.open("w", encoding="utf-8") as f:
        f.write("看股最速適 v4.79｜24項原始特徵程式與歷史數值來源追查\n")
        f.write("=" * 88 + "\n")
        f.write(f"開始時間：{started}\n完成時間：{summary['finished_at_taipei']}\n")
        f.write("來源範圍：Drive 看股最速適專案及 Colab Notebooks；GitHub 公開 v2.45／v2.48.1／v2.50／v2.51 停用量能封存 ZIP。\n")
        f.write("\n一、掃描統計\n")
        f.write(f"- Drive 檔案：{file_count:,} 個；盤點大小 {total_bytes:,} bytes。\n")
        f.write(f"- Python／Notebook／R／SQL 程式：{len(source_records):,} 個；含特徵公式關鍵字線索 {len(source_candidates):,} 個。\n")
        f.write(f"- Drive 相關 ZIP：{len(archive_records):,} 個；GitHub 公開封存 ZIP：{len(public_records):,} 個。\n")
        f.write(f"- ZIP 內程式成員：{len(archive_code):,} 個；模型二進位成員：{len(archive_models):,} 個。\n")
        f.write(f"- 含24項特徵欄名的 CSV：{len(feature_members):,} 個；有實際資料列：{len(data_members):,} 個；只有表頭：{len(empty_feature_members):,} 個。\n")
        f.write("\n二、找到的原始程式線索\n")
        if source_candidates:
            for x in source_candidates:
                f.write(f"- {x.get('relative_path')}；行數 {x.get('line_count')}；命中 {json.dumps(x.get('hit_counts', {}), ensure_ascii=False)}\n")
        else:
            f.write("- 未在指定 Drive 專案與 Colab Notebooks 範圍找到足以辨識原始特徵計算程式的關鍵字組合。\n")
        f.write("\n三、含特徵欄位的封存 CSV\n")
        if feature_members:
            for x in feature_members:
                f.write(f"- {x.get('version')} / {x.get('member')}：特徵欄 {x.get('feature_column_count')} 項；資料列 {x.get('rows_counted')}；有數值證據={x.get('data_evidence')}。\n")
        else:
            f.write("- 未發現含原24項特徵欄名的 CSV。\n")
        f.write("\n四、判讀限制\n")
        f.write("- 只有特徵欄名但資料列為0，不構成原始特徵數值證據。\n")
        f.write("- 報告文字提到公式名稱，不等於已取得精確計算公式。\n")
        f.write("- 若未找到原始 Python／Notebook 或帶有實際24項特徵值的歷史資料，便不能精確還原原公式；應保留候選公式狀態。\n")
        f.write("\n五、資料安全\n")
        f.write("- 所有 Drive 來源唯讀；只在新 v4.79 輸出資料夾建立報告。\n")
        f.write("- 不重訓、不回測、不修改 GitHub main。\n")

    log("報告已建立：")
    for p in (source_csv, member_csv, feature_csv, public_csv, manifest_path, report_path):
        log(f"  {p.name}｜{p.stat().st_size:,} bytes")

    zip_path = DRIVE_ROOT / f"看股最速適_24項原始特徵程式與歷史數值來源追查_v4.79_{timestamp}.zip"
    log("開始建立 v4.79 結果 ZIP……")
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
        for p in (source_csv, member_csv, feature_csv, public_csv, manifest_path, report_path):
            zf.write(p, arcname=p.name)
    log(f"v4.79 ZIP 建立完成：{zip_path.name}｜{zip_path.stat().st_size:,} bytes")

    script_path = resolve_script_path(out_dir, files)
    upload = upload_outputs_to_github(out_dir, zip_path, script_path, timestamp)
    receipt_path = write_upload_receipt(out_dir, timestamp, upload)
    log(f"GitHub 上傳回執已另存：{receipt_path}")
    print_completion(out_dir, zip_path, upload, resumed=False)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log("使用者中斷；來源資料與 GitHub main 均未修改。")
    except Exception as exc:
        log(f"執行失敗：{type(exc).__name__}: {exc}")
        raise