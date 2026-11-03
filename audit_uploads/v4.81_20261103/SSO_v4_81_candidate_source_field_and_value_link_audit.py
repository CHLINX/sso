#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""看股最速適 v4.81｜24項特徵候選原始欄位與歷史值關聯分析

承接 v4.80 的實際發現，只讀取少數高價值封存，不重掃整個 Drive 或34份封存。
重點：
1. 深入分析 v4.66「既有樣本特徵重算差異層」長格式資料（約114萬列）。
2. 讀取 v4.66 新增星期六樣本、v4.68.3 patch、v4.69、v4.70.1 既有特徵核驗資料。
3. 讀取 v2.50／v2.51 的歷史十檔、全市場候選與24特徵表頭，追查 factor／來源20根波動因子。
4. 對24項特徵建立候選來源欄位對照；只標記候選，不將相似欄位冒充原始公式或原始特徵值。
5. 重新核驗 v2.50 GitHub 公開 ZIP 的 SHA，單獨記錄預期值與實際值，不因單一異常中止其他工作。
6. 產生報告、ZIP，整合上傳 GitHub 獨立分支；不修改 main。

來源唯讀；不重訓、不回測、不產生買賣建議。

Colab：
!python /content/SSO_v4_81_candidate_source_field_and_value_link_audit.py

GitHub Token：Colab Secrets 設定 GITHUB_TOKEN，需 CHLINX/sso Contents Read and write。
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

VERSION = "v4.81"
REPO = "CHLINX/sso"
TZ = ZoneInfo("Asia/Taipei")
DRIVE_ROOT = Path("/content/drive/MyDrive/看股最速適")
NOTEBOOK_ROOT = Path("/content/drive/MyDrive/Colab Notebooks")
SCRIPT_FILENAME = "SSO_v4_81_candidate_source_field_and_value_link_audit.py"
OUTPUT_PREFIX = "看股最速適_24項特徵候選原始欄位與歷史值關聯分析_v4.81_"
V479_PREFIX = "看股最速適_24項原始特徵程式與歷史數值來源追查_v4.79_"
V480_PREFIX = "看股最速適_24項特徵歷史輸出與公式來源深度追查_v4.80_"
PUBLIC_COMMIT = "aaabc2433fb5df2bb68af06461178305d6bd30d1"

FEATURES: Dict[str, List[str]] = {
    "ret5_rank": ["ret5_rank", "ret5", "return5", "return_5", "5日報酬", "近5日報酬"],
    "ret20_rank": ["ret20_rank", "ret20", "return20", "return_20", "20日報酬", "近20日報酬"],
    "ret60_rank": ["ret60_rank", "ret60", "return60", "return_60", "60日報酬", "近60日報酬"],
    "dist_ma20_rank": ["dist_ma20_rank", "dist_ma20", "close_vs_ma20", "收盤相對MA20", "收盤距離MA20", "距離MA20"],
    "dist_ma60_rank": ["dist_ma60_rank", "dist_ma60", "close_vs_ma60", "收盤相對MA60", "收盤距離MA60", "距離MA60"],
    "range_pos20_rank": ["range_pos20_rank", "range_pos20", "20日區間位置", "20根區間位置", "區間位置20"],
    "lowvol20_rank": ["lowvol20_rank", "lowvol20", "20根有效K棒報酬波動率", "20根波動率", "近20根波動率"],
    "source_factor_rank": ["source_factor_rank", "來源20根波動因子", "source_factor", "factor"],
    "ret5_abs_scaled": ["ret5_abs_scaled", "ret5_scaled", "5日報酬縮放", "近5日動能縮放"],
    "ret20_abs_scaled": ["ret20_abs_scaled", "ret20_scaled", "20日報酬縮放", "近20日動能縮放"],
    "ret60_abs_scaled": ["ret60_abs_scaled", "ret60_scaled", "60日報酬縮放", "近60日動能縮放"],
    "dist_ma20_abs_scaled": ["dist_ma20_abs_scaled", "dist_ma20_scaled", "收盤相對MA20縮放"],
    "dist_ma60_abs_scaled": ["dist_ma60_abs_scaled", "dist_ma60_scaled", "收盤相對MA60縮放"],
    "market_median_ret5": ["market_median_ret5", "全市場5日報酬中位數", "市場5日報酬中位數"],
    "market_median_ret20": ["market_median_ret20", "全市場20日報酬中位數", "市場20日報酬中位數"],
    "market_median_ret60": ["market_median_ret60", "全市場60日報酬中位數", "市場60日報酬中位數"],
    "market_breadth_pos_ret5": ["market_breadth_pos_ret5", "全市場5日正報酬比例", "市場5日正報酬比例"],
    "market_breadth_pos_ret20": ["market_breadth_pos_ret20", "全市場20日正報酬比例", "市場20日正報酬比例"],
    "market_breadth_pos_ret60": ["market_breadth_pos_ret60", "全市場60日正報酬比例", "市場60日正報酬比例"],
    "market_breadth_above_ma20": ["market_breadth_above_ma20", "全市場站上MA20比例", "市場站上MA20比例"],
    "market_breadth_above_ma60": ["market_breadth_above_ma60", "全市場站上MA60比例", "市場站上MA60比例"],
    "ret5_rank_x_market_breadth": ["ret5_rank_x_market_breadth", "5日排名市場廣度互動"],
    "ret20_rank_x_market_breadth": ["ret20_rank_x_market_breadth", "20日排名市場廣度互動"],
    "ret60_rank_x_market_breadth": ["ret60_rank_x_market_breadth", "60日排名市場廣度互動"],
}

# 只選 v4.80 已證實具較高價值的封存，不重掃其他來源。
ARCHIVE_KEYWORDS = (
    "v4_66_", "v4683", "v4684", "v4_69_", "v4.70.1", "SSO_v2.50_", "SSO_v2.51_"
)
TARGET_MEMBER_KEYWORDS = (
    "既有樣本特徵重算差異層", "新增星期六樣本影子OHLC", "五項特徵patch值",
    "特徵公式狀態重新解析", "整體診斷決策", "兩項未驗證特徵公式候選比較",
    "兩項未驗證特徵公式最終判定", "模型特徵範圍與覆蓋率",
    "最新全市場候選持續評估", "歷史樣本外每日10檔候選", "最新10檔獲利可能性排行",
    "最新達標獲利候選最多10檔", "最新達標獲利候選"
)


def now_taipei() -> datetime:
    return datetime.now(TZ)


def log(message: str) -> None:
    print(f"[{now_taipei().strftime('%H:%M:%S')}] {message}", flush=True)


def sha256_file(path: Path, chunk_size: int = 4 *  1024 * 1024) -> str:
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
    if DRIVE_ROOT.exists():
        log(f"Google Drive 已可讀取：{DRIVE_ROOT}")
    else:
        from google.colab import drive  # type: ignore
        drive.mount("/content/drive", force_remount=False)
    if not DRIVE_ROOT.exists():
        raise FileNotFoundError(f"找不到專案資料夾：{DRIVE_ROOT}")


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


def find_script_path(out_dir: Path) -> Path:
    candidates = [Path(__file__).resolve() if "__file__" in globals() else None,
                  Path("/content") / SCRIPT_FILENAME, Path.cwd() / SCRIPT_FILENAME,
                  DRIVE_ROOT / SCRIPT_FILENAME]
    for p in candidates:
        if p and p.is_file() and p.stat().st_size > 15000:
            return p.resolve()
    # Colab cell 執行時從輸入歷史還原，避免上傳錯誤的 kernel json。
    try:
        ip = get_ipython()  # type: ignore[name-defined]
        hist = list(getattr(ip.history_manager, "input_hist_raw", []) or [])
        if not hist:
            hist = list(ip.user_ns.get("In", []) or [])
        for src in reversed(hist[-60:]):
            if isinstance(src, str) and 15000 < len(src) < 4 * 1024 * 1024 and 'VERSION = "v4.81"' in src and "def main()" in src:
                p = out_dir / SCRIPT_FILENAME
                p.write_text(src, encoding="utf-8")
                log(f"已從 Colab Notebook history 還原完整 Python：{p}")
                return p
    except Exception as exc:
        log(f"Notebook history 還原未成功：{type(exc).__name__}: {exc}")
    raise RuntimeError(f"無法定位 {SCRIPT_FILENAME}；請將下載的 Python 放在 /content 後執行")


def locate_archive(rec: Dict[str, Any]) -> Optional[Path]:
    filename = str(rec.get("filename") or "")
    rel = str(rec.get("relative_path") or filename)
    expected_size = int(rec.get("size_bytes") or -1)
    for p in (DRIVE_ROOT / rel, NOTEBOOK_ROOT / rel, DRIVE_ROOT / filename, NOTEBOOK_ROOT / filename):
        try:
            if p.is_file() and p.stat().st_size == expected_size:
                return p
        except OSError:
            pass
    return None


def norm(s: str) -> str:
    return re.sub(r"[^a-z0-9\u4e00-\u9fff]+", "", str(s).lower())


def candidate_features(label: str) -> List[str]:
    n = norm(label)
    exact = []
    for feature, aliases in FEATURES.items():
        if any(n == norm(alias) for alias in aliases if norm(alias)):
            exact.append(feature)
    if exact:
        return exact
    # 只對較長且具辨識性的別名做包含比對，避免 ret5_rank_x... 同時誤配 ret5_rank。
    found = []
    for feature, aliases in FEATURES.items():
        for alias in aliases:
            a = norm(alias)
            if len(a) >= 10 and a in n:
                found.append(feature)
                break
    return found


def semantic_family(label: str) -> str:
    n = norm(label)
    if "ma" in n or "均線" in n:
        return "MA_DISTANCE_OR_TREND"
    if "波動" in n or "vol" in n or "振幅" in n:
        return "VOLATILITY"
    if "報酬" in n or "return" in n or "ret" in n or "動能" in n:
        return "RETURN_OR_MOMENTUM"
    if "區間" in n or "range" in n or "最高" in n or "最低" in n:
        return "RANGE_POSITION"
    if "廣度" in n or "breadth" in n or "中位數" in n or "median" in n:
        return "MARKET_STATE"
    return "OTHER"


class NumericStats:
    def __init__(self) -> None:
        self.n = 0
        self.total = 0.0
        self.total_sq = 0.0
        self.minimum = math.inf
        self.maximum = -math.inf
        self.samples: List[str] = []

    def add(self, raw: str) -> None:
        try:
            x = float(str(raw).strip())
            if not math.isfinite(x):
                return
        except Exception:
            return
        self.n += 1
        self.total += x
        self.total_sq += x * x
        self.minimum = min(self.minimum, x)
        self.maximum = max(self.maximum, x)
        if len(self.samples) < 4:
            self.samples.append(str(raw)[:80])

    def export(self) -> Dict[str, Any]:
        if not self.n:
            return {"n": 0, "min": "", "max": "", "mean": "", "std": "", "samples": "[]"}
        mean = self.total / self.n
        var = max(0.0, self.total_sq / self.n - mean * mean)
        return {"n": self.n, "min": self.minimum, "max": self.maximum, "mean": mean,
                "std": math.sqrt(var), "samples": json.dumps(self.samples, ensure_ascii=False)}


def open_csv_text(zf: zipfile.ZipFile, member: str):
    raw = zf.open(member, "r")
    if member.lower().endswith(".gz"):
        binary = gzip.GzipFile(fileobj=raw, mode="rb")
    else:
        binary = raw
    text = io.TextIOWrapper(binary, encoding="utf-8-sig", errors="replace", newline="")
    return raw, binary, text


def select_member(name: str) -> bool:
    return name.lower().endswith((".csv", ".csv.gz", ".tsv", ".tsv.gz")) and any(k.lower() in name.lower() for k in TARGET_MEMBER_KEYWORDS)


def scan_csv_member(zf: zipfile.ZipFile, info: zipfile.ZipInfo, version: str,
                    out_sample_rows: List[Dict[str, Any]], feature_value_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    raw = binary = text = None
    try:
        raw, binary, text = open_csv_text(zf, info.filename)
        delimiter = "\t" if ".tsv" in info.filename.lower() else ","
        reader = csv.DictReader(text, delimiter=delimiter)
        header = reader.fieldnames or []
        if not header:
            return {"version": version, "member": info.filename, "rows": 0, "columns": 0, "header": "[]", "status": "NO_HEADER"}
        alias_cols: Dict[str, List[str]] = {feature: [] for feature in FEATURES}
        for col in header:
            for f in candidate_features(col):
                alias_cols[f].append(col)
        # 特徵長格式通常以「特徵欄位」存放特徵名稱，其他數值欄位存值／差異。
        label_col = next((c for c in header if norm(c) in {"特徵欄位", "featurename", "feature", "featurekey", "特徵名稱"}), "")
        identity_cols = [c for c in header if re.search(r"日期|symbol|股票代號|證券代號|市場|segment|stock|code", c, re.I)]
        numeric_cols = [c for c in header if c not in identity_cols and c != label_col]
        stats = {c: NumericStats() for c in header if c in set(sum(alias_cols.values(), []))}
        grouped: Dict[Tuple[str, str], NumericStats] = {}
        grouped_counts: Counter = Counter()
        rows = 0
        examples = 0
        for row in reader:
            rows += 1
            if label_col:
                label = str(row.get(label_col, "")).strip()
                if label:
                    features = candidate_features(label) or ["UNMAPPED_FEATURE_LABEL"]
                    for f in features:
                        grouped_counts[(f, label)] += 1
                        for col in numeric_cols:
                            # 所有長格式標籤都留下分布；未映射者不會被當成24項特徵。
                            key = (f, label + "||" + col)
                            if key not in grouped:
                                grouped[key] = NumericStats()
                            grouped[key].add(str(row.get(col, "")))
            for col, st in stats.items():
                st.add(str(row.get(col, "")))
            if examples < 8:
                interesting = {k: str(v)[:160] for k, v in row.items() if k in identity_cols or k == label_col or k in stats}
                if interesting:
                    out_sample_rows.append({"version": version, "member": info.filename,
                                            "sample_no": examples + 1,
                                            "sample_json": json.dumps(interesting, ensure_ascii=False)})
                    examples += 1
            if rows and rows % 250000 == 0:
                elapsed = max(time.monotonic() - scan_csv_member.started, 0.001)
                log(f"目標 CSV 進度：{version}/{info.filename}；{rows:,} rows；{rows/elapsed:,.0f} rows/s")
        scan_csv_member.started = time.monotonic()

        for feature in FEATURES:
            for col in alias_cols[feature]:
                feature_value_rows.append({"version": version, "member": info.filename, "feature": feature,
                    "candidate_source_column": col, "evidence_level": "欄名候選；尚未證明同義",
                    **stats[col].export()})
        for (feature, key), st in grouped.items():
            label, col = key.split("||", 1)
            feature_value_rows.append({"version": version, "member": info.filename, "feature": feature,
                "candidate_source_column": label, "semantic_family": semantic_family(label), "value_column": col,
                "evidence_level": ("長格式標籤與候選特徵有名稱關聯；仍需核對" if feature != "UNMAPPED_FEATURE_LABEL" else "長格式特徵標籤；未能對應24項，保留原名供追查"),
                **st.export()})
        status = "SCANNED" if rows >= 0 else "ERROR"
        return {"version": version, "member": info.filename, "compressed_member_bytes": info.file_size,
                "rows": rows, "columns": len(header), "header": json.dumps(header, ensure_ascii=False),
                "label_column": label_col, "identity_columns": json.dumps(identity_cols, ensure_ascii=False),
                "candidate_columns": json.dumps(alias_cols, ensure_ascii=False),
                "long_format_feature_groups": len(grouped_counts),
                "long_format_feature_labels": json.dumps([{"feature": f, "label": label, "rows": n}
                    for (f, label), n in grouped_counts.items()], ensure_ascii=False), "status": status}
    except Exception as exc:
        return {"version": version, "member": info.filename, "rows": 0, "columns": 0,
                "header": "[]", "status": f"ERROR:{type(exc).__name__}:{exc}"}
    finally:
        for obj in (text, binary, raw):
            try:
                if obj is not None:
                    obj.close()
            except Exception:
                pass

scan_csv_member.started = time.monotonic()


def read_small_text(zf: zipfile.ZipFile, info: zipfile.ZipInfo, version: str,
                    text_rows: List[Dict[str, Any]]) -> None:
    if info.file_size > 15 * 1024 * 1024:
        return
    try:
        raw = zf.read(info.filename)
        if info.filename.lower().endswith(".gz"):
            raw = gzip.decompress(raw)
        try:
            content = raw.decode("utf-8-sig")
        except UnicodeDecodeError:
            content = raw.decode("cp950", errors="replace")
        terms = re.compile(r"公式|計算|RANK_FEATURES|feature|factor|rank|波動|均線|MA20|MA60|補行|patch|差異|median|breadth|縮放|分位|排名", re.I)
        for no, line in enumerate(content.splitlines(), 1):
            if terms.search(line):
                text_rows.append({"version": version, "member": info.filename, "line_no": no,
                                  "excerpt": re.sub(r"\s+", " ", line).strip()[:500]})
    except Exception as exc:
        text_rows.append({"version": version, "member": info.filename, "line_no": "", "excerpt": f"READ_ERROR:{type(exc).__name__}:{exc}"})


def verify_and_scan_archive(path: Path, rec: Dict[str, Any],
                            schema_rows: List[Dict[str, Any]], samples: List[Dict[str, Any]],
                            values: List[Dict[str, Any]], text_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    size = path.stat().st_size
    got_sha = sha256_file(path)
    expected_sha = str(rec.get("sha256") or "")
    expected_size = int(rec.get("size_bytes") or -1)
    result = {"filename": path.name, "size_bytes": size, "expected_size": expected_size,
              "sha256": got_sha, "expected_sha256": expected_sha,
              "size_match": size == expected_size, "sha_match": not expected_sha or got_sha == expected_sha,
              "crc": "NOT_CHECKED", "members": 0, "selected_members": 0, "source": "Drive"}
    if size != expected_size or (expected_sha and got_sha != expected_sha):
        result["status"] = "SOURCE_CHANGED_SKIP"
        log(f"來源與 v4.79 清冊不符，唯讀略過：{path.name}")
        return result
    with zipfile.ZipFile(path, "r") as zf:
        bad = zf.testzip()
        result["crc"] = "PASS" if bad is None else f"FAIL:{bad}"
        if bad:
            result["status"] = "CRC_FAIL"
            return result
        infos = [x for x in zf.infolist() if not x.is_dir()]
        result["members"] = len(infos)
        selected = [x for x in infos if select_member(x.filename)]
        result["selected_members"] = len(selected)
        log(f"目標封存核驗：{path.name}；{len(infos)} members；命中 {len(selected)} 個指定 CSV")
        for idx, info in enumerate(selected, 1):
            log(f"指定成員分析：{path.name} {idx}/{len(selected)}；{info.filename}；{info.file_size:,} bytes")
            sr = scan_csv_member(zf, info, path.name, samples, values)
            schema_rows.append(sr)
            if info.file_size <= 15 * 1024 * 1024 and not info.filename.lower().endswith((".csv", ".csv.gz", ".tsv", ".tsv.gz")):
                read_small_text(zf, info, path.name, text_rows)
        # 另掃描小型文字／JSON規格，擷取公式與特徵變更線索。
        for info in infos:
            low = info.filename.lower()
            if low.endswith((".txt", ".md", ".json", ".py", ".ipynb")) and info not in selected:
                read_small_text(zf, info, path.name, text_rows)
    result["status"] = "PASS"
    return result


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
        "X-GitHub-Api-Version": "2022-11-28", "User-Agent": "CHLINX-SSO-v481-evidence-audit/1.0",
        "Content-Type": "application/json; charset=utf-8"})
    try:
        with urllib.request.urlopen(req, timeout=180) as r:
            data = r.read()
            return json.loads(data.decode("utf-8")) if data else {}
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")[:2000]
        raise RuntimeError(f"GitHub API {method} {path} HTTP {exc.code}: {detail}") from exc


def upload_to_github(zip_path: Path, py_path: Path, timestamp: str) -> Dict[str, Any]:
    token = get_token()
    root = f"/repos/{REPO}"
    main_sha = github_api(token, "GET", root + "/git/ref/heads/main")["object"]["sha"]
    branch_base = f"audit/v4.81-candidate-source-values-{timestamp}"
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
        raise RuntimeError("無法建立獨立 GitHub 分支")
    log(f"GitHub 獨立分支：{branch}；起點 main SHA={main_sha}（不修改 main）")
    folder = f"audit_uploads/v4.81_{timestamp[:8]}"
    records = []
    for i, p in enumerate((zip_path, py_path), 1):
        rel = f"{folder}/{p.name}"
        data = p.read_bytes()
        api_path = root + "/contents/" + urllib.parse.quote(rel, safe="/")
        result = github_api(token, "PUT", api_path, {
            "message": f"Upload SSO v4.81 candidate source audit: {p.name}",
            "content": base64.b64encode(data).decode("ascii"), "branch": branch})
        remote = result.get("content") or {}
        if int(remote.get("size") or 0) != len(data):
            raise RuntimeError(f"GitHub 上傳大小不符：{p.name}")
        url = f"https://github.com/{REPO}/blob/{branch}/{urllib.parse.quote(rel, safe='/')}"
        records.append({"path": rel, "bytes": len(data), "url": url})
        log(f"GitHub 上傳 {i}/2 完成：{p.name}；{len(data):,} bytes")
    return {"branch": branch, "main_sha": main_sha, "branch_url": f"https://github.com/{REPO}/tree/{branch}", "files": records}


def save_csv(path: Path, headers: Sequence[str], rows: Iterable[Dict[str, Any]]) -> int:
    count = 0
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(headers), extrasaction="ignore")
        w.writeheader()
        for row in rows:
            w.writerow(row)
            count += 1
    return count


def choose_archives(manifest: Dict[str, Any]) -> List[Dict[str, Any]]:
    records = manifest["archive_summary"]
    chosen = []
    seen = set()
    for rec in records:
        name = str(rec.get("filename") or rec.get("relative_path") or "")
        if any(k.lower() in name.lower() for k in ARCHIVE_KEYWORDS):
            key = str(rec.get("sha256") or name)
            if key not in seen:
                chosen.append(rec)
                seen.add(key)
    # 按關鍵字分類確認是否有找到主要封存。
    found = " | ".join(str(r.get("filename", "")) for r in chosen)
    log(f"v4.81 指定封存命中 {len(chosen)} 份：{found}")
    if not any("v4_66_" in str(r.get("filename", "")) for r in chosen):
        raise FileNotFoundError("v4.79 清冊未找到 v4.66 特徵重算差異層封存，停止以避免漏掉核心來源")
    return chosen


def write_reports(out_dir: Path, archive_rows: List[Dict[str, Any]], schema_rows: List[Dict[str, Any]],
                  sample_rows: List[Dict[str, Any]], value_rows: List[Dict[str, Any]],
                  text_rows: List[Dict[str, Any]], v479_folder: Path, selected_count: int,
                  started: str) -> Dict[str, Any]:
    # 24項欄位對照報告：所有匹配都明確標記為候選，不代表同義或原始公式。
    mapping = []
    for feature, aliases in FEATURES.items():
        hits = []
        for r in value_rows:
            if r.get("feature") == feature:
                hits.append(r)
        if hits:
            status = "找到候選欄位／數值；尚未證明與原始特徵同義"
        else:
            status = "本輪指定來源未找到可用候選數值"
        mapping.append({"feature": feature, "candidate_aliases": json.dumps(aliases, ensure_ascii=False),
                        "candidate_evidence_status": status,
                        "candidate_records": len(hits),
                        "source_members": json.dumps(sorted({f"{x.get('version')}::{x.get('member')}" for x in hits}), ensure_ascii=False),
                        "original_formula_confirmed": False})

    reports = [
        ("01_24項特徵候選來源欄位對照.csv", list(mapping[0].keys()), mapping),
        ("02_指定封存CSV完整欄位與列數.csv", ["version", "member", "compressed_member_bytes", "rows", "columns", "header", "label_column", "identity_columns", "candidate_columns", "long_format_feature_groups", "long_format_feature_labels", "status"], schema_rows),
        ("03_候選來源欄位數值分布.csv", sorted({k for r in value_rows for k in r.keys()}), value_rows),
        ("04_歷史資料樣本列.csv", ["version", "member", "sample_no", "sample_json"], sample_rows),
        ("05_公式與版本文字線索.csv", ["version", "member", "line_no", "excerpt"], text_rows),
        ("06_封存ZIP唯讀核驗.csv", ["filename", "size_bytes", "expected_size", "sha256", "expected_sha256", "size_match", "sha_match", "crc", "members", "selected_members", "source", "status"], archive_rows),
    ]
    for filename, headers, rows in reports:
        n = save_csv(out_dir / filename, headers, rows)
        log(f"報告完成：{filename}；{n:,} rows；{(out_dir/filename).stat().st_size:,} bytes")

    group_counts = Counter(r.get("candidate_evidence_status", "") for r in mapping)
    manifest = {
        "version": VERSION, "started_at_taipei": started, "finished_at_taipei": now_taipei().isoformat(timespec="seconds"),
        "v479_source_folder": v479_folder.name, "archives_selected_from_v479": selected_count,
        "archives_verified": sum(1 for r in archive_rows if r.get("status") == "PASS"),
        "target_csv_members": len(schema_rows), "target_csv_rows_scanned": sum(int(r.get("rows") or 0) for r in schema_rows),
        "long_format_feature_value_rows": len([r for r in value_rows if "value_column" in r]),
        "candidate_value_records": len(value_rows), "feature_mapping_status_counts": dict(group_counts),
        "original_24_formulas_confirmed": False, "model_retrained": False, "backtest_run": False,
        "drive_sources_read_only": True, "github_main_modified": False,
        "limitations": [
            "欄名相似、別名、來源因子欄位均只列為候選證據，不代表原始24項特徵的確切公式。",
            "factor／來源20根波動因子可能是來源因子原始值，不能直接等同 source_factor_rank。",
            "收盤相對MA20／MA60 等欄位不能在未核對計算母體、價格調整口徑及缺值規則前視為原始 dist_ma 特徵。",
            "市場中位數、廣度及交互項須取得完整訊號日橫斷面母體與原始排名規則才能核實。",
        ],
    }
    (out_dir / "v4.81_manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    with (out_dir / "v4.81_執行摘要.txt").open("w", encoding="utf-8") as f:
        f.write("看股最速適 v4.81｜24項特徵候選原始欄位與歷史值關聯分析\n")
        f.write("=" * 90 + "\n")
        f.write(f"來源清冊：{v479_folder.name}\n")
        f.write(f"指定封存：{selected_count} 份；通過完整性核驗：{manifest['archives_verified']} 份。\n")
        f.write(f"目標CSV：{len(schema_rows)} 個；實際掃描列數：{manifest['target_csv_rows_scanned']:,}。\n")
        f.write(f"候選欄位／數值紀錄：{len(value_rows):,}；候選欄位狀態：{json.dumps(dict(group_counts), ensure_ascii=False)}。\n\n")
        f.write("重要判讀：factor／來源20根波動因子是來源因子候選值，不等於 source_factor_rank；收盤相對MA20/MA60等欄位亦僅為價格特徵候選。\n")
        f.write("本版不確認任何原始24項公式，不重訓、不回測、不修改 GitHub main。\n")
    return manifest


def main() -> None:
    started = now_taipei().isoformat(timespec="seconds")
    mount_drive()
    v479_folder, manifest = load_v479_manifest()
    timestamp = now_taipei().strftime("%Y%m%d_%H%M%S")
    out_dir = DRIVE_ROOT / f"{OUTPUT_PREFIX}{timestamp}"
    out_dir.mkdir(parents=True, exist_ok=False)
    log(f"v4.81 輸出資料夾：{out_dir}")

    selected = choose_archives(manifest)
    archive_rows: List[Dict[str, Any]] = []
    schema_rows: List[Dict[str, Any]] = []
    sample_rows: List[Dict[str, Any]] = []
    value_rows: List[Dict[str, Any]] = []
    text_rows: List[Dict[str, Any]] = []
    for i, rec in enumerate(selected, 1):
        path = locate_archive(rec)
        if path is None:
            archive_rows.append({"filename": rec.get("filename"), "status": "NOT_FOUND_OR_SIZE_CHANGED", "source": "Drive"})
            log(f"指定封存找不到或大小變動，略過：{rec.get('filename')}")
            continue
        log(f"v4.81 指定封存進度：{i}/{len(selected)}")
        row = verify_and_scan_archive(path, rec, schema_rows, sample_rows, value_rows, text_rows)
        archive_rows.append(row)
    manifest_out = write_reports(out_dir, archive_rows, schema_rows, sample_rows, value_rows,
                                 text_rows, v479_folder, len(selected), started)

    script_path = find_script_path(out_dir)
    log(f"本次 Python 程式：{script_path}")
    zip_path = DRIVE_ROOT / f"{OUTPUT_PREFIX}{timestamp}.zip"
    report_files = sorted(p for p in out_dir.iterdir() if p.is_file())
    log(f"開始打包 v4.81：{len(report_files)} 個報告檔案")
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
        for p in report_files:
            zf.write(p, arcname=p.name)
        zf.write(script_path, arcname=SCRIPT_FILENAME)
    with zipfile.ZipFile(zip_path, "r") as zf:
        bad = zf.testzip()
        if bad:
            raise RuntimeError(f"v4.81 ZIP CRC 失敗：{bad}")
    log(f"v4.81 ZIP 完成：{zip_path.name}；{zip_path.stat().st_size:,} bytes；CRC PASS")

    upload = upload_to_github(zip_path, script_path, timestamp)
    receipt = {"version": VERSION, "timestamp": timestamp, "branch": upload["branch"],
               "branch_url": upload["branch_url"], "main_sha": upload["main_sha"], "files": upload["files"],
               "source_read_only": True, "github_main_modified": False,
               "model_retrained": False, "backtest_run": False}
    (out_dir / "v4.81_GitHub上傳回執.json").write_text(json.dumps(receipt, ensure_ascii=False, indent=2), encoding="utf-8")
    log(f"GitHub 上傳回執已保存：{out_dir / 'v4.81_GitHub上傳回執.json'}")

    print("=" * 96)
    print("v4.81 完成")
    print(f"輸出資料夾：{out_dir}")
    print(f"結果 ZIP：{zip_path}")
    print(f"GitHub 分支：{upload['branch']}")
    print(f"GitHub 分支網址：{upload['branch_url']}")
    for item in upload["files"]:
        print(f"GitHub 檔案：{item['path']}｜{item['bytes']:,} bytes｜{item['url']}")
    print(f"候選來源狀態：{json.dumps(manifest_out['feature_mapping_status_counts'], ensure_ascii=False)}")
    print("確認：Drive 來源唯讀；GitHub main 未修改；未重訓、未回測。")
    print("=" * 96)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log("使用者中斷；來源資料與 GitHub main 均未修改。")
    except Exception as exc:
        log(f"執行失敗：{type(exc).__name__}: {exc}")
        raise