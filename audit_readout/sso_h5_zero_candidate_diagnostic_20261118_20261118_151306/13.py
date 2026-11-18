# -*- coding: utf-8 -*-
"""SSO H5 zero-candidate diagnostic + GitHub REST audit uploader.

Run in the same Google Colab runtime that produced the 2026-11-18 v2.73 run.
The script does NOT rerun the model and does NOT modify Drive source files or GitHub main.
It collects the exact v2.73 report and v2.50 model log, produces a traceable
keyword/metric index, creates a ZIP, and creates Git blobs/tree/commit/ref through the GitHub REST API, based on the pinned main SHA.
A previous completed ZIP from a failed push is reused instead of rebuilding it.
"""
from __future__ import annotations

import argparse
import base64
import csv
import datetime as dt
import hashlib
import json
import os
import re
import shutil
import sys
import zipfile
from pathlib import Path
from typing import Any
import urllib.error
import urllib.parse
import urllib.request

REPO = "CHLINX/sso"
OWNER = "CHLINX"
REPO_NAME = "sso"
PROTECTED_MAIN_SHA = "aaabc2433fb5df2bb68af06461178305d6bd30d1"
SIGNAL_DATE = "2026-11-18"
RUN_ID = "20261118_193832_200a91"
REPORT_BASENAME = "SSO_v2.73_歷史重跑報告_20261118_214050_f393dc.json"
MODEL_LOG_BASENAME = "08_SSO_v2.50_前瞻刷新模型.log"
H5_CSV_BASENAME = "SSO_v2.66_H5_2026-11-18_v2.50_達標候選最多10檔.csv"
MAX_SINGLE_FILE_BYTES = 90 * 1024 * 1024
MAX_MATCH_LINES = 12000
GITHUB_API = "https://api.github.com"

# The patterns below are diagnostic filters only. Their counts are line hits,
# not counts of securities, unless the source line itself explicitly states one.
KEYWORD_GROUPS: dict[str, list[str]] = {
    "universe_and_scope": [r"股票池", r"證券代號", r"universe", r"eligible universe", r"市場別"],
    "feature_and_data_readiness": [r"feature[_\s-]?ready", r"特徵", r"因子", r"缺值", r"OHLC", r"segment", r"warm.?up", r"窗口不足"],
    "training_and_sample_size": [r"train[_\s-]?sample", r"訓練樣本", r"樣本數", r"訓練", r"refit", r"walk.?forward"],
    "predicted_net_return": [r"預測淨報酬", r"net[_\s-]?return", r"expected return", r"報酬門檻", r"淨報酬"],
    "profit_probability": [r"獲利機率", r"估計淨獲利機率", r"win[_\s-]?prob", r"probability", r"勝率"],
    "calibration": [r"校準", r"calibrat", r"校準方向", r"校準樣本", r"calibration"],
    "historical_probability_bin": [r"歷史同機率", r"同機率區間", r"歷史區間", r"bin[_\s-]?pass", r"歷史同區間"],
    "candidate_gate_and_rejection": [r"候選", r"qualified", r"reject", r"排除", r"淘汰", r"不通過", r"未達標", r"門檻", r"gate"],
    "volume_and_liquidity": [r"成交量", r"量能", r"volume", r"liquidity", r"零成交"],
    "model_completion_and_outputs": [r"完成", r"PASS", r"輸出", r"snapshot", r"H5", r"signal_date", r"訊號日"],
}


def log(msg: str) -> None:
    print(f"[{dt.datetime.now().astimezone().strftime('%H:%M:%S')}] {msg}", flush=True)


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(8 * 1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def get_token() -> str:
    token = os.environ.get("GITHUB_SSO_TOKEN", "").strip()
    if not token:
        try:
            from google.colab import userdata  # type: ignore
            token = str(userdata.get("GITHUB_SSO_TOKEN") or "").strip()
        except Exception:
            token = ""
    if not token:
        raise RuntimeError("找不到 Colab Secret：GITHUB_SSO_TOKEN。未嘗試其他 token 名稱。")
    return token


def candidate_roots() -> list[Path]:
    roots = [
        Path("/content") / f"SSO_v2.73_十年研究鏈重跑_{RUN_ID}",
        Path("/content") / f"SSO_v2.73_報告_{RUN_ID}",
        Path("/content/drive/MyDrive/看股最速適/推薦結果/非前瞻參考/SSO_main_v5_15_20261118_192959_1730c0"),
        Path("/content/drive/MyDrive/看股最速適/推薦結果/非前瞻參考"),
        Path("/content"),
    ]
    out: list[Path] = []
    seen: set[str] = set()
    for p in roots:
        key = str(p)
        if key not in seen and p.exists():
            out.append(p)
            seen.add(key)
    return out


def find_exact_file(basename: str, roots: list[Path], preferred: list[Path] | None = None) -> Path:
    preferred = preferred or []
    for p in preferred:
        if p.is_file():
            return p
    hits: list[Path] = []
    visited: set[str] = set()
    for root in roots:
        if root.is_file() and root.name == basename:
            hits.append(root)
            continue
        if not root.is_dir():
            continue
        try:
            for p in root.rglob(basename):
                if p.is_file():
                    s = str(p)
                    if s not in visited:
                        hits.append(p)
                        visited.add(s)
        except (PermissionError, OSError):
            continue
    if not hits:
        raise FileNotFoundError(
            f"找不到必要檔案：{basename}。請確認仍在產生 v2.73 結果的同一個 Colab Runtime；\n"
            f"預期模型日誌：/content/SSO_v2.73_十年研究鏈重跑_{RUN_ID}/logs/{MODEL_LOG_BASENAME}\n"
            f"預期重跑報告：/content/SSO_v2.73_報告_{RUN_ID}/{REPORT_BASENAME}"
        )
    # Prefer the file inside the exact v2.73 run id; otherwise newest mtime.
    run_hits = [p for p in hits if RUN_ID in str(p)]
    pool = run_hits or hits
    pool.sort(key=lambda p: (p.stat().st_mtime_ns, len(str(p))), reverse=True)
    if len(pool) > 1:
        log(f"[SOURCE LOCATOR] {basename}: 找到 {len(pool)} 份；採用最新且符合 run id 的 {pool[0]}")
    return pool[0]


def load_json(path: Path) -> Any:
    raw = path.read_text(encoding="utf-8-sig")
    return json.loads(raw)


def walk_json_metrics(obj: Any, prefix: str = "$") -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    interesting = re.compile(r"count|rows|candidate|signal|feature|train|sample|threshold|gate|reject|exclude|eligible|qualified|date|status|horizon|prob|return", re.I)
    def walk(x: Any, p: str, depth: int) -> None:
        if depth > 12:
            return
        if isinstance(x, dict):
            for k, v in x.items():
                q = f"{p}.{k}"
                if interesting.search(str(k)) and (v is None or isinstance(v, (str, int, float, bool))):
                    rows.append({"json_path": q, "value": v})
                if isinstance(v, (dict, list)):
                    walk(v, q, depth + 1)
        elif isinstance(x, list):
            for i, v in enumerate(x[:5000]):
                if isinstance(v, (dict, list)):
                    walk(v, f"{p}[{i}]", depth + 1)
    walk(obj, prefix, 0)
    return rows


def analyze_log(path: Path, out_dir: Path) -> dict[str, Any]:
    text_data = path.read_text(encoding="utf-8-sig", errors="replace")
    lines = text_data.splitlines()
    keyword_rows: list[dict[str, Any]] = []
    keyword_counts: dict[str, int] = {}
    compiled = {k: [re.compile(p, re.I) for p in pats] for k, pats in KEYWORD_GROUPS.items()}
    for group, pats in compiled.items():
        keyword_counts[group] = sum(1 for line in lines if any(p.search(line) for p in pats))
    for i, line in enumerate(lines, 1):
        matched = [g for g, pats in compiled.items() if any(p.search(line) for p in pats)]
        if matched and len(keyword_rows) < MAX_MATCH_LINES:
            keyword_rows.append({"line_number": i, "groups": ";".join(matched), "line": line[:3000]})
    extracted = {
        "signal_date_mentions": sum(line.count(SIGNAL_DATE) for line in lines),
        "candidate_zero_mentions": sum(1 for line in lines if re.search(r"(?:0\s*/\s*10|qualified\s*[=:]\s*0|候選.{0,12}0\s*檔|0\s*檔達標)", line, re.I)),
        "line_hits_by_diagnostic_group": keyword_counts,
        "matched_lines_written": len(keyword_rows),
        "matched_lines_limit": MAX_MATCH_LINES,
        "note": "各分類數值是日誌命中行數，不代表個股數；個股淘汰數只採用原始日誌明確記載的數字。",
    }
    hit_csv = out_dir / "v2.50模型日誌關鍵行.csv"
    with hit_csv.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["line_number", "groups", "line"])
        w.writeheader()
        w.writerows(keyword_rows)
    tail_path = out_dir / "v2.50模型日誌末段.txt"
    tail_path.write_text("\n".join(lines[-500:]) + "\n", encoding="utf-8-sig")
    return {
        "line_count": len(lines),
        "character_count": len(text_data),
        "keyword_analysis": extracted,
        "artifacts": {"keyword_lines_csv": hit_csv.name, "tail_500_lines": tail_path.name},
    }


def build_diagnostic(report_path: Path, model_log_path: Path, h5_csv_path: Path | None,
                     out_dir: Path) -> dict[str, Any]:
    report = load_json(report_path)
    log_info = analyze_log(model_log_path, out_dir)
    report_metrics = walk_json_metrics(report)
    metrics_path = out_dir / "v2.73報告重要指標索引.csv"
    with metrics_path.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["json_path", "value"])
        w.writeheader()
        w.writerows(report_metrics)

    h5_info: dict[str, Any] | None = None
    if h5_csv_path and h5_csv_path.is_file():
        with h5_csv_path.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.reader(f)
            header = next(reader, [])
            rows = sum(1 for _ in reader)
        h5_info = {"path": str(h5_csv_path), "header_columns": len(header), "candidate_rows": rows,
                   "sha256": sha256_file(h5_csv_path)}

    diag = {
        "diagnostic_version": "SSO_H5_ZERO_CANDIDATE_DIAGNOSTIC_v1",
        "signal_date": SIGNAL_DATE,
        "source_run_id": RUN_ID,
        "interpretation_scope": "診斷資料匯整；不重跑模型、不調整 v2.50、不產生新的股票推薦。",
        "source_files": {
            "v2_73_report": {"path": str(report_path), "bytes": report_path.stat().st_size,
                             "sha256": sha256_file(report_path), "top_level_keys": list(report.keys()) if isinstance(report, dict) else []},
            "v2_50_model_log": {"path": str(model_log_path), "bytes": model_log_path.stat().st_size,
                                "sha256": sha256_file(model_log_path)},
            "h5_candidate_csv": h5_info,
        },
        "report_metrics_index": {"rows": len(report_metrics), "file": metrics_path.name},
        "model_log_analysis": log_info,
        "explicit_zero_candidate_facts_from_previous_run": {
            "horizon": 5,
            "qualified_candidates": 0,
            "max_candidates": 10,
            "padded_to_ten": False,
            "model_rerun_during_recovery": False,
            "note": "這些數字來自已封存的 H5 摘要／執行日誌；不是本腳本重新計算的模型結果。",
        },
        "limitations": [
            "日誌關鍵字命中行數不能直接當作個股淘汰數。",
            "若 v2.50 原始日誌未列逐層個股計數，本診斷只會標示未提供，不會推算或補造。",
            "本腳本只讀取報告與日誌，不會修改 Drive 原始行情或前瞻帳本。",
        ],
    }
    diag_path = out_dir / "H5零達標原因診斷.json"
    diag_path.write_text(json.dumps(diag, ensure_ascii=False, indent=2) + "\n", encoding="utf-8-sig")

    txt = [
        "SSO｜2026-11-18 H5 零達標原因診斷",
        "=" * 72,
        f"來源 run：{RUN_ID}",
        f"訊號日：{SIGNAL_DATE}",
        "結果：H5 達標 0/10；未補足、未放寬門檻；此次只匯整既有日誌，不重跑模型。",
        "",
        "模型日誌分類命中行數（注意：是日誌行數，不是個股數）：",
    ]
    for k, v in log_info["keyword_analysis"]["line_hits_by_diagnostic_group"].items():
        txt.append(f"- {k}: {v}")
    txt += [
        "",
        "逐層個股淘汰數：請以 v2.50 原始日誌中明確列出的計數為準；本腳本不將關鍵字命中行數轉換為個股數。",
        f"重要報告指標索引：{metrics_path.name}（{len(report_metrics)} 項）",
        f"日誌關鍵行：{log_info['artifacts']['keyword_lines_csv']}",
        f"日誌末段：{log_info['artifacts']['tail_500_lines']}",
        "",
        "原始來源 SHA256 已記錄於 H5零達標原因診斷.json。",
    ]
    (out_dir / "H5零達標原因診斷摘要.txt").write_text("\n".join(txt) + "\n", encoding="utf-8-sig")
    return diag


def github_api(token: str, method: str, path: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    """Call GitHub REST API without invoking git or requiring terminal credentials."""
    url = GITHUB_API + path
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "SSO-H5-Audit-Uploader/2",
    }
    data = None
    if payload is not None:
        headers["Content-Type"] = "application/json; charset=utf-8"
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers=headers, method=method.upper())
    try:
        with urllib.request.urlopen(req, timeout=90) as response:
            raw = response.read()
            if not raw:
                return {}
            return json.loads(raw.decode("utf-8"))
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        if exc.code == 401:
            hint = "GitHub 拒絕 GITHUB_SSO_TOKEN（401）；請確認 Colab Secret 的 token 有效且未過期。"
        elif exc.code == 403:
            hint = "GitHub 回傳 403；token 可能缺少 CHLINX/sso Contents 權限（需可寫入 Git database/ref）。"
        elif exc.code == 404:
            hint = "GitHub 回傳 404；請確認 CHLINX/sso、main 分支或指定 ref 存在且 token 可讀取。"
        elif exc.code == 422:
            hint = "GitHub 回傳 422；可能是 audit 分支已存在或 Git tree/ref 內容不合法。"
        else:
            hint = f"GitHub API HTTP {exc.code}。"
        # Never print request headers or the token; only return the response body with token redaction.
        body = body.replace(token, "[REDACTED]")
        raise RuntimeError(f"{hint}\nAPI={method.upper()} {path}\n回應={body[:2500]}") from None
    except urllib.error.URLError as exc:
        raise RuntimeError(f"無法連線 GitHub API：{exc.reason}") from None
    except json.JSONDecodeError:
        raise RuntimeError(f"GitHub API 回應不是有效 JSON：{method.upper()} {path}") from None


def verify_main_sha(token: str) -> str:
    ref = github_api(token, "GET", f"/repos/{REPO}/git/ref/heads/main")
    sha = str(ref.get("object", {}).get("sha", ""))
    if sha != PROTECTED_MAIN_SHA:
        raise RuntimeError(
            f"GitHub main SHA 與受保護基準不符，拒絕上傳：remote={sha or 'MISSING'}；expected={PROTECTED_MAIN_SHA}"
        )
    return sha


def publish_audit_bundle(token: str, out_dir: Path, stamp: str) -> dict[str, Any]:
    """Create a one-commit audit branch via GitHub Git Database REST endpoints."""
    repo_meta = github_api(token, "GET", f"/repos/{REPO}")
    login = str(repo_meta.get("owner", {}).get("login", "unknown"))
    log(f"[GITHUB AUTH PASS] repository={repo_meta.get('full_name', REPO)}；owner={login}")

    main_before = verify_main_sha(token)
    main_commit = github_api(token, "GET", f"/repos/{REPO}/git/commits/{main_before}")
    base_tree = str(main_commit.get("tree", {}).get("sha", ""))
    if not base_tree:
        raise RuntimeError("無法讀取受保護 main commit 的 tree SHA；停止上傳。")

    folder = f"sso_h5_zero_candidate_diagnostic_{SIGNAL_DATE.replace('-', '')}_{stamp}"
    branch = f"audit/sso-h5-zero-candidate-diagnostic-{SIGNAL_DATE.replace('-', '')}_{stamp}"
    local_files = sorted(p for p in out_dir.rglob("*") if p.is_file() and p.name != "GitHub_Audit_Upload_Result.json")
    if not local_files:
        raise RuntimeError("診斷封存資料夾沒有可上傳檔案。")
    oversized = [(p, p.stat().st_size) for p in local_files if p.stat().st_size > MAX_SINGLE_FILE_BYTES]
    if oversized:
        detail = "; ".join(f"{p.name}={n:,}" for p, n in oversized)
        raise RuntimeError("有檔案超過 90 MiB，為避免 GitHub 拒收而停止上傳：" + detail)

    tree_entries: list[dict[str, str]] = []
    uploaded_files: list[dict[str, Any]] = []
    for local_path in local_files:
        rel = local_path.relative_to(out_dir).as_posix()
        if rel.startswith("../") or rel.startswith("/"):
            raise RuntimeError(f"不合法的封存相對路徑：{rel}")
        raw = local_path.read_bytes()
        blob_payload = {
            "content": base64_encode(raw),
            "encoding": "base64",
        }
        blob = github_api(token, "POST", f"/repos/{REPO}/git/blobs", blob_payload)
        blob_sha = str(blob.get("sha", ""))
        if not blob_sha:
            raise RuntimeError(f"GitHub 未回傳 blob SHA：{rel}")
        remote_path = f"audit_readout/{folder}/{rel}"
        tree_entries.append({"path": remote_path, "mode": "100644", "type": "blob", "sha": blob_sha})
        uploaded_files.append({
            "path": remote_path,
            "bytes": len(raw),
            "sha256": hashlib.sha256(raw).hexdigest(),
            "git_blob_sha": blob_sha,
        })

    tree = github_api(token, "POST", f"/repos/{REPO}/git/trees", {
        "base_tree": base_tree,
        "tree": tree_entries,
    })
    tree_sha = str(tree.get("sha", ""))
    if not tree_sha:
        raise RuntimeError("GitHub 未回傳 audit tree SHA。")

    commit_msg = f"SSO H5 zero-candidate diagnostic {SIGNAL_DATE} {stamp}"
    commit = github_api(token, "POST", f"/repos/{REPO}/git/commits", {
        "message": commit_msg,
        "tree": tree_sha,
        "parents": [PROTECTED_MAIN_SHA],
    })
    commit_sha = str(commit.get("sha", ""))
    if not commit_sha:
        raise RuntimeError("GitHub 未回傳 audit commit SHA。")

    # Recheck main immediately before creating the audit ref. The new commit's
    # sole parent is the pinned main SHA; this script never PATCHes main.
    verify_main_sha(token)
    github_api(token, "POST", f"/repos/{REPO}/git/refs", {
        "ref": f"refs/heads/{branch}",
        "sha": commit_sha,
    })

    encoded_ref = urllib.parse.quote(f"heads/{branch}", safe="")
    remote_branch_ref = github_api(token, "GET", f"/repos/{REPO}/git/ref/{encoded_ref}")
    remote_branch_sha = str(remote_branch_ref.get("object", {}).get("sha", ""))
    main_after = verify_main_sha(token)
    if remote_branch_sha != commit_sha:
        raise RuntimeError(f"Audit branch readback SHA 不符：remote={remote_branch_sha}；expected={commit_sha}")
    if main_after != PROTECTED_MAIN_SHA:
        raise RuntimeError(f"main SHA 改變，需立即人工核對：{main_after}")

    url = f"https://github.com/{REPO}/tree/{branch}/audit_readout/{folder}"
    zip_files = [p for p in local_files if p.suffix.lower() == ".zip"]
    zip_info = None
    if zip_files:
        z = zip_files[0]
        zip_info = {"path": str(z), "bytes": z.stat().st_size, "sha256": sha256_file(z)}
    return {
        "status": "PASS",
        "repository": REPO,
        "authenticated_login": login,
        "branch": branch,
        "commit_sha": commit_sha,
        "main_before": main_before,
        "main_after": main_after,
        "main_unchanged": main_after == PROTECTED_MAIN_SHA,
        "zip": zip_info,
        "uploaded_files": uploaded_files,
        "url": url,
        "finished_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
    }


def base64_encode(data: bytes) -> str:
    return base64.b64encode(data).decode("ascii")


def find_reusable_bundle(work_root: Path) -> tuple[Path, Path] | None:
    """Reuse a ZIP already completed before a previous GitHub push failure."""
    if not work_root.exists():
        return None
    candidates = [p for p in work_root.glob("SSO_H5_Zero_Candidate_Diagnostic_*") if p.is_dir()]
    candidates.sort(key=lambda p: p.stat().st_mtime_ns, reverse=True)
    for folder in candidates:
        report = folder / REPORT_BASENAME
        model_log = folder / MODEL_LOG_BASENAME
        diag = folder / "H5零達標原因診斷.json"
        zips = sorted(folder.glob("SSO_H5零達標診斷封存_*.zip"), key=lambda p: p.stat().st_mtime_ns, reverse=True)
        if not (report.is_file() and model_log.is_file() and diag.is_file() and zips):
            continue
        try:
            with zipfile.ZipFile(zips[0], "r") as zf:
                if zf.testzip() is not None or not zf.namelist():
                    continue
        except (OSError, zipfile.BadZipFile):
            continue
        return folder, zips[0]
    return None


def create_zip(source_files: list[Path], out_dir: Path, run_stamp: str) -> Path:
    zip_path = out_dir / f"SSO_H5零達標診斷封存_{run_stamp}.zip"
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
        for p in sorted(out_dir.rglob("*")):
            if p.is_file() and p != zip_path:
                zf.write(p, arcname=p.relative_to(out_dir).as_posix())
    with zipfile.ZipFile(zip_path, "r") as zf:
        bad = zf.testzip()
        if bad:
            raise RuntimeError(f"ZIP CRC 失敗：{bad}")
        names = zf.namelist()
        if not names:
            raise RuntimeError("ZIP 封存沒有任何檔案。")
    return zip_path


def main() -> None:
    ap = argparse.ArgumentParser(description="Collect and upload SSO 2026-11-18 H5 zero-candidate diagnostics.")
    ap.add_argument("--signal-date", default=SIGNAL_DATE)
    ap.add_argument("--run-id", default=RUN_ID)
    ap.add_argument("--no-github", action="store_true", help="只建立／保留本機診斷 ZIP，不上傳 GitHub。")
    args = ap.parse_args()
    if args.signal_date != SIGNAL_DATE or args.run_id != RUN_ID:
        raise RuntimeError("此版本已固定對應 2026-11-18 / run 20261118_193832_200a91，拒絕誤抓其他執行資料。")

    started = dt.datetime.now(dt.timezone.utc)
    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    log("SSO｜H5 0 檔診斷資料 GitHub REST audit 上傳 v2")
    log("不重跑模型；優先重用前次已完成的 ZIP；Drive 來源唯讀；GitHub main 僅核對，不修改。")

    work_root = Path("/content") if Path("/content").is_dir() else Path.cwd()
    reusable = find_reusable_bundle(work_root)
    if reusable:
        out_dir, zip_path = reusable
        log(f"[REUSE PASS] 偵測到前次推送失敗前已完成的診斷封存：{out_dir}")
        log(f"[ZIP REUSE] {zip_path}；sha256={sha256_file(zip_path)}；CRC/readback=PASS")
    else:
        roots = candidate_roots()
        report = find_exact_file(REPORT_BASENAME, roots)
        model_log = find_exact_file(MODEL_LOG_BASENAME, roots,
                                    [Path("/content") / f"SSO_v2.73_十年研究鏈重跑_{RUN_ID}" / "logs" / MODEL_LOG_BASENAME])
        h5_candidates = [
            Path("/content/drive/MyDrive/看股最速適/推薦結果/非前瞻參考/SSO_main_v5_15_20261118_192959_1730c0") / H5_CSV_BASENAME,
            Path("/content/SSO_main_v5_15_20261118_192959_1730c0") / "H5_10檔達標候選" / "SSO_main_v5.15_H5_2026-11-18_達標候選最多10檔.csv",
        ]
        h5_csv = next((p for p in h5_candidates if p.is_file()), None)
        for p in (report, model_log):
            if p.stat().st_size > MAX_SINGLE_FILE_BYTES:
                raise RuntimeError(f"來源檔超過 GitHub 單檔安全上限 90 MiB：{p}（{p.stat().st_size:,} bytes）")
        log(f"[SOURCE] v2.73 report={report} ({report.stat().st_size:,} bytes)")
        log(f"[SOURCE] v2.50 log={model_log} ({model_log.stat().st_size:,} bytes)")
        log(f"[SOURCE] H5 CSV={'FOUND ' + str(h5_csv) if h5_csv else 'not found locally; previous GitHub snapshot remains the reference'}")

        out_dir = work_root / f"SSO_H5_Zero_Candidate_Diagnostic_{SIGNAL_DATE.replace('-', '')}_{stamp}"
        out_dir.mkdir(parents=True, exist_ok=False)
        shutil.copy2(report, out_dir / REPORT_BASENAME)
        shutil.copy2(model_log, out_dir / MODEL_LOG_BASENAME)
        if h5_csv:
            shutil.copy2(h5_csv, out_dir / H5_CSV_BASENAME)
        else:
            (out_dir / "H5_CSV_SOURCE_NOTE.txt").write_text(
                "本 Runtime 未找到 H5 CSV 原檔；既有 GitHub audit bundle 已保存該訊號日的空候選 CSV（僅表頭）。\n",
                encoding="utf-8-sig")

        diag = build_diagnostic(report, model_log, h5_csv, out_dir)
        source_manifest = []
        for p in sorted(out_dir.iterdir()):
            if p.is_file():
                source_manifest.append({"file": p.name, "bytes": p.stat().st_size, "sha256": sha256_file(p)})
        (out_dir / "artifact_sha256_manifest.json").write_text(
            json.dumps({"signal_date": SIGNAL_DATE, "run_id": RUN_ID, "files": source_manifest}, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8-sig")
        zip_path = create_zip([], out_dir, stamp)
        log(f"[DIAGNOSTIC PASS] log_lines={diag['model_log_analysis']['line_count']}；report_metrics={diag['report_metrics_index']['rows']}")
        log(f"[ZIP PASS] {zip_path}；sha256={sha256_file(zip_path)}；CRC/readback=PASS")

    # Preserve this revised uploader alongside the diagnostic evidence on GitHub.
    try:
        self_path = Path(__file__).resolve()
        if self_path.is_file() and self_path.suffix.lower() == ".py":
            target = out_dir / "13.py"
            if self_path != target:
                shutil.copy2(self_path, target)
    except Exception as exc:
        log(f"[WARN] 無法將 v2 程式副本放入 audit 封存：{type(exc).__name__}: {exc}")

    if args.no_github:
        log(f"[LOCAL ONLY] {out_dir}")
        return

    token = get_token()
    result = publish_audit_bundle(token, out_dir, stamp)
    result["started_at_utc"] = started.isoformat()
    result["output_dir"] = str(out_dir)
    result_path = out_dir / "GitHub_Audit_Upload_Result.json"
    result_path.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8-sig")
    log("[GITHUB AUDIT PASS]")
    log(f"[BRANCH] {result['branch']}")
    log(f"[COMMIT] {result['commit_sha']}")
    log(f"[MAIN UNCHANGED] {result['main_after']}")
    log(f"[URL] {result['url']}")
    log(f"[OUTPUT] {out_dir}")
    log("[COMPLETE] 原始報告、模型日誌、診斷索引與 ZIP 已上傳；未重跑模型、未修改 Drive 來源、未修改 GitHub main。")

if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"[STOP] {type(exc).__name__}: {exc}", file=sys.stderr, flush=True)
        raise
