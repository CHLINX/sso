#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""看股最速適 v4.78｜原十檔24項特徵候選公式重建與來源資料核對

本版目的：
1. 以 v2.31 原始研究資料（含 OHLC）作為候選公式計算主表。
2. 在每檔股票、每個價格研究區段內重建報酬、均線距離、區間位置及20根波動候選輸入。
3. 以自然鍵對齊 v4.70.1，僅核對該版已驗證的5項特徵；v4.70.1 獨有且無 OHLC 的補行資料不納入價格衍生計算。
4. 對 24 項特徵逐項提出可檢查的候選公式與尚未公開的參數，不擅自認定哪一式是原始公式。
5. 對最近5個市場日期輸出跨市場／分市場、正向／反向排名與 abs_scaled 等候選值樣本。
6. 一次執行完成報告 ZIP，並將 v4.77 ZIP、v4.78 ZIP、v4.78 Python 程式上傳至 CHLINX/sso 的同一個獨立分支。

安全原則：來源唯讀；不重訓、不回測、不修改或刪除來源、不修改 GitHub main。
GitHub Repository 為公開狀態；上傳檔案會公開可讀。本版依 UPLOADS 清單直接上傳，不要求輸入 YES。

Colab 執行：!python /content/SSO_v4_78_candidate_feature_reconstruction_and_github_upload.py
GitHub Token：建議放在 Colab Secrets，名稱 GITHUB_TOKEN；需對 CHLINX/sso 有 Contents Read and write 權限。
"""
from __future__ import annotations

import base64
import csv
import getpass
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
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable
from zoneinfo import ZoneInfo

VERSION = "v4.78"
REPO = "CHLINX/sso"
BASE_BRANCH = "main"
API = "https://api.github.com"
API_VERSION = "2022-11-28"
USER_AGENT = "SSO-v4.78-Candidate-Feature-Reconstruction/1.2"
DRIVE_ROOT = Path("/content/drive/MyDrive/看股最速適")
TZ = ZoneInfo("Asia/Taipei")
MAX_UPLOAD_MB = 80
SAMPLE_MARKET_DATES = 5
CHUNK_ROWS = 400_000

# 原十檔 24 項模型輸入；名稱及分組由 v2.48.1/v2.51 封存設定確認。
FEATURES = [
    ("個股相對排名", "ret5_rank", "5根有效K棒個股報酬的橫斷面排名", "ret5；日期；市場；價格研究區段", "報酬端點、排名方向、排名母體、百分位／名次及平手規則"),
    ("個股相對排名", "ret20_rank", "20根有效K棒個股報酬的橫斷面排名", "ret20；日期；市場；價格研究區段", "報酬端點、排名方向、排名母體、百分位／名次及平手規則"),
    ("個股相對排名", "ret60_rank", "60根有效K棒個股報酬的橫斷面排名", "ret60；日期；市場；價格研究區段", "報酬端點、排名方向、排名母體、百分位／名次及平手規則"),
    ("個股相對排名", "dist_ma20_rank", "個股相對MA20距離的橫斷面排名", "close、MA20；日期；市場；價格研究區段", "均線距離分子／分母、排名方向、排名母體及平手規則"),
    ("個股相對排名", "dist_ma60_rank", "個股相對MA60距離的橫斷面排名", "close、MA60；日期；市場；價格研究區段", "均線距離分子／分母、排名方向、排名母體及平手規則"),
    ("個股相對排名", "range_pos20_rank", "20根有效K棒區間位置的橫斷面排名", "high、low、close；日期；市場；價格研究區段", "視窗是否含當根、區間高低點、零振幅及排名規則"),
    ("個股相對排名", "lowvol20_rank", "20根有效K棒低波動特徵的橫斷面排名", "close報酬序列；日期；市場；價格研究區段", "簡單／對數報酬、ddof、視窗、年化、方向與排名母體"),
    ("個股相對排名", "source_factor_rank", "原始20根有效K棒報酬波動率的橫斷面排名", "v2.31來源因子；日期；市場橫斷面", "來源因子排名方向、排名母體、同日有效母體與補行日因子來源"),
    ("個股絕對動能／均線縮放", "ret5_abs_scaled", "5根有效K棒個股報酬縮放特徵", "ret5；日期；價格研究區段", "abs_scaled縮放基準、尺度、截尾與估計母體"),
    ("個股絕對動能／均線縮放", "ret20_abs_scaled", "20根有效K棒個股報酬縮放特徵", "ret20；日期；價格研究區段", "abs_scaled縮放基準、尺度、截尾與估計母體"),
    ("個股絕對動能／均線縮放", "ret60_abs_scaled", "60根有效K棒個股報酬縮放特徵", "ret60；日期；價格研究區段", "abs_scaled縮放基準、尺度、截尾與估計母體"),
    ("個股絕對動能／均線縮放", "dist_ma20_abs_scaled", "個股相對MA20距離縮放特徵", "close、MA20；日期；價格研究區段", "均線距離定義、abs_scaled縮放基準與截尾"),
    ("個股絕對動能／均線縮放", "dist_ma60_abs_scaled", "個股相對MA60距離縮放特徵", "close、MA60；日期；價格研究區段", "均線距離定義、abs_scaled縮放基準與截尾"),
    ("全市場報酬中位數", "market_median_ret5", "訊號日全市場5根有效K棒報酬中位數", "ret5；日期；市場；全市場股票母體", "全市場是TWSE/TPEX合併或分組、停牌／缺行情排除及分母"),
    ("全市場報酬中位數", "market_median_ret20", "訊號日全市場20根有效K棒報酬中位數", "ret20；日期；市場；全市場股票母體", "全市場是TWSE/TPEX合併或分組、停牌／缺行情排除及分母"),
    ("全市場報酬中位數", "market_median_ret60", "訊號日全市場60根有效K棒報酬中位數", "ret60；日期；市場；全市場股票母體", "全市場是TWSE/TPEX合併或分組、停牌／缺行情排除及分母"),
    ("全市場報酬廣度", "market_breadth_pos_ret5", "訊號日5根有效K棒正報酬股票比例", "ret5；日期；市場；全市場股票母體", "正報酬是否嚴格大於0、分母及缺行情排除規則"),
    ("全市場報酬廣度", "market_breadth_pos_ret20", "訊號日20根有效K棒正報酬股票比例", "ret20；日期；市場；全市場股票母體", "正報酬是否嚴格大於0、分母及缺行情排除規則"),
    ("全市場報酬廣度", "market_breadth_pos_ret60", "訊號日60根有效K棒正報酬股票比例", "ret60；日期；市場；全市場股票母體", "正報酬是否嚴格大於0、分母及缺行情排除規則"),
    ("全市場均線廣度", "market_breadth_above_ma20", "訊號日收盤價高於MA20股票比例", "close、MA20；日期；市場；全市場股票母體", "是否大於或大於等於、分母及缺行情排除規則"),
    ("全市場均線廣度", "market_breadth_above_ma60", "訊號日收盤價高於MA60股票比例", "close、MA60；日期；市場；全市場股票母體", "是否大於或大於等於、分母及缺行情排除規則"),
    ("排名×市場廣度交互作用", "ret5_rank_x_market_breadth", "5日動能排名與市場廣度交互作用", "ret5_rank；市場廣度候選", "精確廣度配對、排名尺度、乘法順序與缺值處理"),
    ("排名×市場廣度交互作用", "ret20_rank_x_market_breadth", "20日動能排名與市場廣度交互作用", "ret20_rank；市場廣度候選", "正報酬廣度或MA20廣度、排名尺度及缺值處理"),
    ("排名×市場廣度交互作用", "ret60_rank_x_market_breadth", "60日動能排名與市場廣度交互作用", "ret60_rank；市場廣度候選", "正報酬廣度或MA60廣度、排名尺度及缺值處理"),
]

ALIASES = {
    "date": ["日期", "交易日期", "訊號日期", "date", "Date"],
    "market": ["市場", "市場別", "交易市場", "market", "Market"],
    "symbol": ["股票代號", "證券代號", "股票代碼", "證券代碼", "symbol", "ticker", "stock_id"],
    "segment": ["價格研究區段", "價格區段", "研究區段", "segment", "price_segment"],
    "open": ["影子開盤價", "影子開盤", "開盤價", "開盤", "open", "Open"],
    "high": ["影子最高價", "影子最高", "最高價", "最高", "high", "High"],
    "low": ["影子最低價", "影子最低", "最低價", "最低", "low", "Low"],
    "close": ["影子收盤價", "影子收盤", "收盤價", "收盤", "close", "Close"],
    "factor": ["20根有效K棒報酬波動率", "20根有效K棒報酬波動率因子", "來源20根波動因子", "source_factor", "factor"],
}
VERIFIED_FEATURES = [
    "收盤相對MA5", "收盤相對MA20", "收盤相對MA60", "收盤相對MA120", "相對前20有效K棒最高價"
]

class SSOError(RuntimeError):
    pass


def log(msg: str) -> None:
    print(f"[{datetime.now(TZ).strftime('%H:%M:%S')}] {msg}", flush=True)


def mount_drive() -> None:
    if DRIVE_ROOT.exists():
        return
    try:
        from google.colab import drive  # type: ignore
        log("Google Drive 尚未掛載，嘗試掛載……")
        drive.mount("/content/drive", force_remount=False)
    except Exception as exc:
        raise SSOError(f"無法掛載 Google Drive：{exc}") from exc
    if not DRIVE_ROOT.exists():
        raise FileNotFoundError(f"找不到專案根目錄：{DRIVE_ROOT}")


def walk_find(root: Path, predicate) -> list[Path]:
    found: list[Path] = []
    for current, dirs, files in os.walk(root):
        dirs[:] = [d for d in dirs if d not in {"__pycache__", ".ipynb_checkpoints", ".Trash", "緊急備份"}]
        for name in files:
            p = Path(current) / name
            if predicate(name, p):
                found.append(p)
    return found


def latest_or_best(paths: list[Path], preferred: str = "") -> Path | None:
    if not paths:
        return None
    def score(p: Path):
        s = str(p)
        pref = 0 if preferred and preferred in s else 1
        return (pref, -p.stat().st_mtime, len(s))
    return sorted(paths, key=score)[0]


def _zip_manifest_version(path: Path) -> str | None:
    """只讀取 ZIP 中小型 manifest／摘要，判斷版本；不解壓整包。"""
    try:
        with zipfile.ZipFile(path, "r") as zf:
            names = zf.namelist()
            # 明確manifest優先，避免v4.78 ZIP因引用v4.77檔名而被誤認。
            for name in names:
                low = name.lower()
                if not (low.endswith(".json") and ("manifest" in low or "版本" in low)):
                    continue
                info = zf.getinfo(name)
                if info.file_size > 2 * 1024 * 1024:
                    continue
                try:
                    obj = json.loads(zf.read(name).decode("utf-8-sig", "replace"))
                    version = str(obj.get("version", "")) if isinstance(obj, dict) else ""
                    if version:
                        return version
                except Exception:
                    continue
            # 無可讀manifest時，才依ZIP內成員名稱及小型摘要辨識。
            for name in names:
                if re.search(r"v4[._-]?77", name, re.I):
                    return "v4.77"
            for name in names:
                low = name.lower()
                if not (low.endswith((".txt", ".md")) and ("摘要" in name or "readme" in low)):
                    continue
                info = zf.getinfo(name)
                if info.file_size > 512 * 1024:
                    continue
                try:
                    content = zf.read(name).decode("utf-8-sig", "replace")
                    if re.search(r"(?i)v4[._-]?77", content):
                        return "v4.77"
                except Exception:
                    continue
    except (OSError, zipfile.BadZipFile, RuntimeError):
        return None
    return None


def _find_v477_zip() -> Path | None:
    """跨專案資料夾及 MyDrive 搜尋 v4.77 ZIP，接受不同檔名但驗證內容。"""
    exact = DRIVE_ROOT / "看股最速適_原十檔24特徵公式追溯與重建可行性核驗_v4.77_20261102_145729.zip"
    if exact.is_file():
        return exact

    roots = [DRIVE_ROOT, Path("/content/drive/MyDrive")]
    roots = [r for i, r in enumerate(roots) if r.exists() and r not in roots[:i]]
    candidates: list[Path] = []
    seen: set[str] = set()
    for root in roots:
        try:
            matches = walk_find(
                root,
                lambda n, p: p.suffix.lower() == ".zip"
                and (re.search(r"v4[._-]?77", n, re.I)
                     or re.search(r"v4[._-]?77", str(p), re.I)
                     or ("24" in n and ("特徵" in n or "公式" in n or "追溯" in n))),
            )
        except Exception:
            matches = []
        for item in matches:
            key = str(item.resolve())
            if key not in seen:
                seen.add(key)
                candidates.append(item)
    for item in Path("/content").glob("*.zip"):
        key = str(item.resolve())
        if key not in seen and re.search(r"v4[._-]?77", item.name, re.I):
            seen.add(key)
            candidates.append(item)

    # 對檔名未包含版本號的候選 ZIP，檢查內部 manifest／摘要，避免誤選其他版本。
    verified: list[Path] = []
    for item in candidates:
        version = _zip_manifest_version(item)
        filename_has_v477 = bool(re.search(r"v4[._-]?77", item.name, re.I))
        if version is not None:
            is_v477 = bool(re.fullmatch(r"v4[._-]?77", version, re.I))
        else:
            is_v477 = filename_has_v477
        if is_v477:
            verified.append(item)

    if verified:
        def score(item: Path):
            name = item.name.lower()
            preferred = 0 if "原十檔" in name and ("24" in name or "特徵" in name) else 1
            exact_version = 0 if "v4.77" in name or "v4_77" in name else 1
            try:
                mtime = item.stat().st_mtime
            except OSError:
                mtime = 0
            return (preferred, exact_version, -mtime, len(str(item)))
        chosen = sorted(verified, key=score)[0]
        log(f"找到v4.77 ZIP：{chosen}｜{chosen.stat().st_size:,} bytes")
        if len(verified) > 1:
            log(f"另有 {len(verified)-1} 個符合版本條件的ZIP候選；依檔名、版本及修改時間選取。")
        return chosen

    # 若原 ZIP 遺失但 v4.77 報告資料夾仍在，僅在 manifest 明確標示 v4.77 時重建新 ZIP。
    folder_candidates: list[Path] = []
    for root in roots[:2]:
        try:
            for current, dirs, files in os.walk(root):
                dirs[:] = [d for d in dirs if d not in {"__pycache__", ".ipynb_checkpoints", ".Trash", "緊急備份"}]
                folder = Path(current)
                if re.search(r"v4[._-]?77", str(folder), re.I):
                    if any(n.lower().endswith("manifest.json") for n in files):
                        folder_candidates.append(folder)
        except Exception:
            continue
    for folder in folder_candidates:
        for manifest_name in [n for n in os.listdir(folder) if n.lower().endswith("manifest.json")]:
            try:
                manifest = json.loads((folder / manifest_name).read_text(encoding="utf-8-sig"))
                if str(manifest.get("version", "")) != "v4.77":
                    continue
                target = folder.parent / f"{folder.name}.zip"
                if target.exists():
                    target = folder.parent / f"{folder.name}_repacked.zip"
                with zipfile.ZipFile(target, "x", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
                    for file in sorted(folder.rglob("*")):
                        if file.is_file():
                            zf.write(file, arcname=str(file.relative_to(folder)))
                log(f"原v4.77 ZIP未找到，但找到版本明確的報告資料夾；已另建未覆寫ZIP：{target}")
                return target
            except FileExistsError:
                continue
            except Exception as exc:
                log(f"無法從v4.77報告資料夾重建ZIP：{folder}｜{exc}")

    # 留下可診斷的清單，讓下一次錯誤不再只有「找不到檔案」。
    nearby: list[str] = []
    for root in roots[:2]:
        try:
            zips = walk_find(root, lambda n, p: p.suffix.lower() == ".zip")
            nearby.extend(str(x) for x in zips if ("v4.7" in x.name.lower() or "24" in x.name or "特徵" in x.name))
        except Exception:
            pass
    for item in nearby[:20]:
        log(f"ZIP候選但未能確認v4.77：{item}")
    return None


def locate_sources() -> dict[str, Path | None]:
    """先用已核實固定路徑；固定路徑失效才遞迴搜尋Drive。"""
    known = {
        "v4701": DRIVE_ROOT / "看股最速適_資料品質修復特徵公式辨識與PurgedWalkForward驗證_v4.70.1_20261102_104620" / "01_v231補行日模型研究資料表.csv.gz",
        "v231": DRIVE_ROOT / "分析重跑_v1.2_20261026_155023" / "SSO_研究指標獨立驗證與前瞻分析_v2.31_20261027_200759" / "相對價格因子與前瞻報酬研究資料_v2.31.csv.gz",
        "v464_zip": DRIVE_ROOT / "看股最速適_星期六補行官方行情獨立增補包_v4_64_20261101_151715.zip",
    }
    found: dict[str, Path | None] = {key: path if path.is_file() else None for key, path in known.items()}
    if found["v4701"] is None:
        candidates = walk_find(DRIVE_ROOT, lambda n, p: n == "01_v231補行日模型研究資料表.csv.gz")
        found["v4701"] = latest_or_best(candidates, "v4.70.1")
    if found["v231"] is None:
        candidates = walk_find(DRIVE_ROOT, lambda n, p: n == "相對價格因子與前瞻報酬研究資料_v2.31.csv.gz")
        found["v231"] = latest_or_best(candidates, "v2.31_20261027")
    found["v477_zip"] = _find_v477_zip()
    if found["v464_zip"] is None:
        candidates = walk_find(DRIVE_ROOT, lambda n, p: n.endswith(".zip") and "星期六補行官方行情獨立增補包_v4_64" in n)
        found["v464_zip"] = latest_or_best(candidates, "v4_64_20261101")
    return found


def read_header(path: Path) -> list[str]:
    opener = gzip.open if path.name.lower().endswith(".gz") else open
    with opener(path, "rt", encoding="utf-8-sig", errors="replace", newline="") as f:
        return next(csv.reader(f), [])


def resolve_cols(header: list[str], required: Iterable[str]) -> dict[str, str]:
    result = {}
    for role in required:
        hit = next((name for name in ALIASES[role] if name in header), None)
        if hit:
            result[role] = hit
    return result


def sha256_file(path: Path, chunk: int = 8 * 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            b = f.read(chunk)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def _normalize_keys(df):
    import pandas as pd
    df["日期"] = pd.to_datetime(df["日期"], errors="coerce")
    if df["日期"].isna().any():
        raise SSOError(f"資料日期有 {int(df['日期'].isna().sum())} 筆無法解析，停止避免錯序。")
    for c in ["市場", "股票代號", "價格研究區段"]:
        df[c] = df[c].astype("string").fillna("UNKNOWN")
    return df


def read_main_dataset(path: Path):
    """以 v2.31 原始資料作主表；載入日期／識別鍵／OHLC，不從 v4.70.1 補造價格。"""
    import pandas as pd
    header = read_header(path)
    roles = ["date", "market", "symbol", "segment", "open", "high", "low", "close"]
    cols = resolve_cols(header, roles)
    missing = [x for x in roles if x not in cols]
    if missing:
        raise SSOError(f"v2.31原始價格研究資料缺少必要來源欄位：{missing}；實際表頭：{header}")
    usecols = list(dict.fromkeys(cols.values()))
    dtype = {cols[k]: "string" for k in ["market", "symbol", "segment"]}
    for k in ["open", "high", "low", "close"]:
        dtype[cols[k]] = "float32"
    log(f"讀取 v2.31 原始價格研究資料（候選公式計算主表）：{path}")
    log(f"僅載入必要欄位 {len(usecols)} 欄；不讀取／修改原始檔案。")
    df = pd.read_csv(path, compression="gzip" if path.name.lower().endswith(".gz") else None,
                     usecols=usecols, dtype=dtype, low_memory=False, encoding="utf-8-sig")
    canonical = {"date": "日期", "market": "市場", "symbol": "股票代號", "segment": "價格研究區段",
                 "open": "開盤", "high": "最高", "low": "最低", "close": "收盤"}
    df = df.rename(columns={cols[role]: canonical[role] for role in roles})
    df = _normalize_keys(df)
    for c in ["開盤", "最高", "最低", "收盤"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").astype("float32")
    keys = ["市場", "股票代號", "價格研究區段", "日期"]
    df = df.sort_values(["市場", "股票代號", "價格研究區段", "日期"], kind="mergesort", ignore_index=True)
    dup = int(df.duplicated(keys).sum())
    if dup:
        raise SSOError(f"v2.31主資料自然鍵（市場、股票代號、區段、日期）重複 {dup:,} 筆，停止。")
    return df, cols


def read_verified_dataset(path: Path):
    """只讀取 v4.70.1 的五項已驗證特徵與自然鍵，絕不把它當作 OHLC 主表。"""
    import pandas as pd
    header = read_header(path)
    key_roles = ["date", "market", "symbol", "segment"]
    cols = resolve_cols(header, key_roles)
    missing = [x for x in key_roles if x not in cols]
    if missing:
        raise SSOError(f"v4.70.1核對資料缺少自然鍵欄位：{missing}；實際表頭：{header}")
    feature_cols = [x for x in VERIFIED_FEATURES if x in header]
    usecols = list(dict.fromkeys([cols[k] for k in key_roles] + feature_cols))
    dtype = {cols[k]: "string" for k in ["market", "symbol", "segment"]}
    for c in feature_cols:
        dtype[c] = "float32"
    log(f"讀取 v4.70.1 五項已驗證特徵作交叉核對：{path}")
    log(f"僅載入自然鍵及 {len(feature_cols)} 項既有特徵；不載入不存在的 OHLC 欄位。")
    ver = pd.read_csv(path, compression="gzip" if path.name.lower().endswith(".gz") else None,
                      usecols=usecols, dtype=dtype, low_memory=False, encoding="utf-8-sig")
    rename = {cols["date"]: "日期", cols["market"]: "市場", cols["symbol"]: "股票代號", cols["segment"]: "價格研究區段"}
    ver = ver.rename(columns=rename)
    ver = _normalize_keys(ver)
    for c in feature_cols:
        ver[c] = pd.to_numeric(ver[c], errors="coerce").astype("float32")
    keys = ["日期", "市場", "股票代號", "價格研究區段"]
    dup = int(ver.duplicated(keys).sum())
    if dup:
        raise SSOError(f"v4.70.1核對資料自然鍵重複 {dup:,} 筆，停止一對一比對。")
    return ver, feature_cols, cols


def merge_verified_features(main_df, verified_df, feature_cols):
    """依自然鍵左連接；v4.70.1獨有列不反向加入沒有OHLC的計算主表。"""
    keys = ["日期", "市場", "股票代號", "價格研究區段"]
    before = len(main_df)
    merged = main_df.merge(verified_df[keys + feature_cols], on=keys, how="left",
                           validate="one_to_one", indicator="_v4701_match")
    overlap = int((merged["_v4701_match"] == "both").sum())
    main_only = int((merged["_v4701_match"] == "left_only").sum())
    verified_only = int(len(verified_df) - overlap)
    merged = merged.drop(columns=["_v4701_match"])
    if len(merged) != before:
        raise SSOError("核對資料合併後主表列數發生變化，停止。")
    stats = {"v231_main_rows": before, "v4701_rows": int(len(verified_df)),
             "natural_key_overlap_rows": overlap, "v231_only_rows": main_only,
             "v4701_unique_rows_without_main_ohlc": verified_only,
             "verified_features_found": feature_cols}
    log(f"v4.70.1核對完成：自然鍵重疊 {overlap:,}；v4.70.1獨有 {verified_only:,}（無OHLC，不納入價格衍生計算）")
    return merged, stats


def compute_candidate_base(df):
    import numpy as np
    log("在每檔股票／價格研究區段內計算候選輸入；區段切換時重新起算。")
    keys = ["市場", "股票代號", "價格研究區段"]
    gclose = df.groupby(keys, sort=False, observed=True, dropna=False)["收盤"]

    def valid_return(series, n):
        valid = series.dropna()
        return (valid.div(valid.shift(n)).sub(1.0)).reindex(series.index)

    def valid_mean(series, n):
        valid = series.dropna()
        return valid.rolling(n, min_periods=n).mean().reindex(series.index)

    def valid_vol(series, n):
        valid = series.dropna()
        return valid.pct_change().rolling(n, min_periods=n).std(ddof=1).reindex(series.index)

    for h in (5, 20, 60):
        log(f"計算 {h} 根有效收盤觀測報酬候選……")
        df[f"ret{h}"] = gclose.transform(lambda s, n=h: valid_return(s, n)).astype("float32")
    for h in (20, 60):
        log(f"計算 MA{h} 相對距離候選……")
        ma = gclose.transform(lambda s, n=h: valid_mean(s, n))
        df[f"ma{h}"] = ma.astype("float32")
        df[f"dist_ma{h}"] = (df["收盤"] / df[f"ma{h}"] - 1.0).astype("float32")
    log("計算20根區間位置候選（最高、最低、收盤均有效；含當根／排除當根兩種端點）……")
    df["ohlc_valid_candidate"] = (
        np.isfinite(df["最高"]) & np.isfinite(df["最低"]) & np.isfinite(df["收盤"])
        & (df["最高"] > 0) & (df["最低"] > 0) & (df["收盤"] > 0)
    )
    df["_valid_high"] = df["最高"].where(df["ohlc_valid_candidate"])
    df["_valid_low"] = df["最低"].where(df["ohlc_valid_candidate"])
    ghigh = df.groupby(keys, sort=False, observed=True, dropna=False)["_valid_high"]
    glow = df.groupby(keys, sort=False, observed=True, dropna=False)["_valid_low"]

    def rolling_valid(series, window, op, exclude_current=False):
        valid = series.dropna()
        if exclude_current:
            valid = valid.shift(1)
        if op == "max":
            result = valid.rolling(window, min_periods=window).max()
        else:
            result = valid.rolling(window, min_periods=window).min()
        return result.reindex(series.index)

    high_now = ghigh.transform(lambda s: rolling_valid(s, 20, "max", False))
    low_now = glow.transform(lambda s: rolling_valid(s, 20, "min", False))
    high_prev = ghigh.transform(lambda s: rolling_valid(s, 20, "max", True))
    low_prev = glow.transform(lambda s: rolling_valid(s, 20, "min", True))
    denom_now = high_now - low_now
    denom_prev = high_prev - low_prev
    df["range_pos20_include_current"] = ((df["收盤"] - low_now) / denom_now.where(denom_now != 0)).astype("float32")
    df["range_pos20_exclude_current"] = ((df["收盤"] - low_prev) / denom_prev.where(denom_prev != 0)).astype("float32")
    df = df.drop(columns=["_valid_high", "_valid_low"])
    log("計算20根有效收盤觀測報酬波動率候選（簡單報酬、樣本標準差；不是原公式確認）……")
    df["lowvol20_candidate"] = gclose.transform(lambda s: valid_vol(s, 20)).astype("float32")
    return df


def compare_verified_features(df, feature_cols: list[str]):
    import numpy as np
    rows = []
    keymap = {
        "收盤相對MA20": "dist_ma20",
        "收盤相對MA60": "dist_ma60",
    }
    # MA5/MA120 are independently constructed only for the comparison table.
    gclose = df.groupby(["市場", "股票代號", "價格研究區段"], sort=False, observed=True, dropna=False)["收盤"]
    if "收盤相對MA5" in feature_cols:
        ma5 = gclose.transform(lambda s: s.dropna().rolling(5, min_periods=5).mean().reindex(s.index))
        df["dist_ma5_candidate"] = (df["收盤"] / ma5 - 1.0).astype("float32")
        keymap["收盤相對MA5"] = "dist_ma5_candidate"
    if "收盤相對MA120" in feature_cols:
        ma120 = gclose.transform(lambda s: s.dropna().rolling(120, min_periods=120).mean().reindex(s.index))
        df["dist_ma120_candidate"] = (df["收盤"] / ma120 - 1.0).astype("float32")
        keymap["收盤相對MA120"] = "dist_ma120_candidate"
    # 前20根有效K棒最高價排除當根；此為候選式，需與已驗證欄位核對。
    ghigh = df.groupby(["市場", "股票代號", "價格研究區段"], sort=False, observed=True, dropna=False)["最高"]
    prev20_high = ghigh.transform(lambda s: s.dropna().shift(1).rolling(20, min_periods=20).max().reindex(s.index))
    df["relative_prev20_high_candidate"] = (df["收盤"] / prev20_high - 1.0).astype("float32")
    keymap["相對前20有效K棒最高價"] = "relative_prev20_high_candidate"
    for target in VERIFIED_FEATURES:
        if target not in feature_cols:
            rows.append({"既有驗證特徵": target, "候選公式": "", "比較列數": 0, "MAE": "", "P95絕對誤差": "", "誤差<=0.001比例": "", "判定": "SOURCE_COLUMN_NOT_FOUND"})
            continue
        cand = keymap.get(target)
        if not cand:
            continue
        a = pd_to_numpy(df[target])
        b = pd_to_numpy(df[cand])
        mask = np.isfinite(a) & np.isfinite(b)
        if mask.sum() == 0:
            rows.append({"既有驗證特徵": target, "候選公式": cand, "比較列數": 0, "MAE": "", "P95絕對誤差": "", "誤差<=0.001比例": "", "判定": "NO_OVERLAP"})
            continue
        base = np.abs(a[mask] - b[mask])
        variants = [("候選原值", 1.0), ("候選×100", 100.0), ("候選×0.01", 0.01)]
        for label, scale in variants:
            err = np.abs(b[mask] * scale - a[mask])
            mae = float(np.mean(err)); p95 = float(np.quantile(err, .95)); rate = float(np.mean(err <= .001))
            rows.append({"既有驗證特徵": target, "候選公式": f"{cand} × {scale:g}", "比較列數": int(mask.sum()),
                         "MAE": mae, "P95絕對誤差": p95, "誤差<=0.001比例": rate,
                         "判定": "MATCH_CANDIDATE_ONLY" if rate >= .995 and mae <= .001 else "NOT_MATCHED"})
    return rows


def pd_to_numpy(series):
    import numpy as np
    return series.to_numpy(dtype=np.float64, na_value=np.nan)


def read_factor_for_dates(path: Path | None, dates: set[str]):
    import pandas as pd
    if path is None or not dates:
        return None, {"status": "V231_FACTOR_SOURCE_NOT_FOUND_OR_NO_DATES", "matched_rows": 0}
    header = read_header(path)
    cols = resolve_cols(header, ["date", "market", "symbol", "segment", "factor"])
    needed = ["date", "market", "symbol", "factor"]
    missing = [x for x in needed if x not in cols]
    if missing:
        return None, {"status": "V231_FACTOR_COLUMNS_NOT_FOUND:" + ";".join(missing), "matched_rows": 0}
    usecols = list(dict.fromkeys(cols.values()))
    dtype = {cols[k]: "string" for k in ("market", "symbol", "segment") if k in cols}
    dtype[cols["factor"]] = "float32"
    chunks = []
    log(f"從 v2.31 原始表分批讀取最近 {len(dates)} 日的來源波動因子；不載入其餘欄位。")
    for chunk in pd.read_csv(path, compression="gzip", usecols=usecols, dtype=dtype, chunksize=CHUNK_ROWS,
                             low_memory=False, encoding="utf-8-sig"):
        chunk[cols["date"]] = pd.to_datetime(chunk[cols["date"]], errors="coerce")
        mask = chunk[cols["date"]].dt.strftime("%Y-%m-%d").isin(dates)
        if mask.any():
            take = chunk.loc[mask].copy()
            take = take.rename(columns={cols["date"]: "日期", cols["market"]: "市場", cols["symbol"]: "股票代號", cols["factor"]: "source_factor"})
            if "segment" in cols:
                take = take.rename(columns={cols["segment"]: "價格研究區段"})
                merge_keys = ["日期", "市場", "股票代號", "價格研究區段"]
            else:
                merge_keys = ["日期", "市場", "股票代號"]
            keep = merge_keys + ["source_factor"]
            chunks.append(take[keep])
    if not chunks:
        return None, {"status": "NO_FACTOR_ROWS_FOR_SAMPLE_DATES", "matched_rows": 0}
    out = pd.concat(chunks, ignore_index=True).drop_duplicates()
    return out, {"status": "FOUND", "matched_rows": int(len(out)), "merge_keys": [c for c in out.columns if c != "source_factor"]}


def rank_pct(frame, value_col: str, group_cols: list[str], ascending: bool):
    return frame.groupby(group_cols, sort=False, observed=True, dropna=False)[value_col].rank(method="average", pct=True, ascending=ascending).astype("float32")


def add_sample_hypotheses(df, factor_path: Path | None):
    import numpy as np
    import pandas as pd
    latest_date_values = sorted(df["日期"].drop_duplicates().tolist())[-SAMPLE_MARKET_DATES:]
    dates = [x.strftime("%Y-%m-%d") for x in latest_date_values]
    date_set = set(dates)
    sample = df.loc[df["日期"].isin(latest_date_values)].copy()
    sample["日期文字"] = sample["日期"].dt.strftime("%Y-%m-%d")
    factor_df, factor_status = read_factor_for_dates(factor_path, date_set)
    if factor_df is not None:
        keys = factor_status.get("merge_keys", [])
        sample = sample.merge(factor_df, on=keys, how="left", validate="one_to_one")
    else:
        sample["source_factor"] = np.nan
    log(f"候選公式樣本：最近 {len(dates)} 個市場日、{len(sample):,} 列。")
    by_date = ["日期文字"]
    by_market = ["日期文字", "市場"]
    raw_map = {
        "ret5": "ret5", "ret20": "ret20", "ret60": "ret60",
        "dist_ma20": "dist_ma20", "dist_ma60": "dist_ma60",
        "range_pos20_include_current": "range_pos20_include_current",
        "range_pos20_exclude_current": "range_pos20_exclude_current",
        "lowvol20_candidate": "lowvol20_candidate", "source_factor": "source_factor",
    }
    rank_cols = {}
    for feature, value_col in raw_map.items():
        if value_col not in sample.columns:
            continue
        for pop, groups in (("all_market", by_date), ("by_market", by_market)):
            for direction, asc in (("ascending", True), ("descending", False)):
                col = f"{feature}_rank__{pop}__{direction}"
                sample[col] = rank_pct(sample, value_col, groups, asc)
                rank_cols[(feature, pop, direction)] = col
    # abs_scaled 候選：原始比例、百分比尺度、同日Z分數（全市場／分市場）。
    abs_base = {"ret5_abs_scaled": "ret5", "ret20_abs_scaled": "ret20", "ret60_abs_scaled": "ret60",
                "dist_ma20_abs_scaled": "dist_ma20", "dist_ma60_abs_scaled": "dist_ma60"}
    for feature, raw in abs_base.items():
        if raw not in sample:
            continue
        sample[f"{feature}__raw"] = sample[raw].astype("float32")
        sample[f"{feature}__percent_x100"] = (sample[raw] * 100.0).astype("float32")
        for pop, groups in (("all_market", by_date), ("by_market", by_market)):
            gb = sample.groupby(groups, sort=False, observed=True, dropna=False)[raw]
            mu = gb.transform("mean")
            sd = gb.transform("std").replace(0, np.nan)
            sample[f"{feature}__zscore_{pop}"] = ((sample[raw] - mu) / sd).astype("float32")
    # 市場中位數與廣度：保留「全市場合併」及「依市場分開」兩種母體。
    horizons = (5, 20, 60)
    breadth_cols = {}
    for pop, groups in (("all_market", by_date), ("by_market", by_market)):
        for h in horizons:
            ret = f"ret{h}"
            med = f"market_median_ret{h}__{pop}"
            pos = f"market_breadth_pos_ret{h}__{pop}"
            sample[med] = sample.groupby(groups, sort=False, observed=True, dropna=False)[ret].transform("median").astype("float32")
            sample[pos] = sample.groupby(groups, sort=False, observed=True, dropna=False)[ret].transform(lambda s: (s > 0).sum() / s.notna().sum() if s.notna().sum() else np.nan).astype("float32")
            breadth_cols[(f"pos_ret{h}", pop)] = pos
        for h in (20, 60):
            raw = f"dist_ma{h}"
            col = f"market_breadth_above_ma{h}__{pop}"
            sample[col] = sample.groupby(groups, sort=False, observed=True, dropna=False)[raw].transform(lambda s: (s > 0).sum() / s.notna().sum() if s.notna().sum() else np.nan).astype("float32")
            breadth_cols[(f"above_ma{h}", pop)] = col
    # 交互作用有明確的同期間正報酬廣度配對候選；H20/H60另列對應均線廣度替代案。
    for pop in ("all_market", "by_market"):
        for h in (5, 20, 60):
            feature = f"ret{h}"
            rank_col = rank_cols.get((feature, pop, "ascending"))
            if not rank_col:
                continue
            bcol = breadth_cols.get((f"pos_ret{h}", pop))
            if bcol:
                sample[f"ret{h}_rank_x_pos_breadth__{pop}"] = (sample[rank_col] * sample[bcol]).astype("float32")
            if h in (20, 60):
                bcol = breadth_cols.get((f"above_ma{h}", pop))
                if bcol:
                    sample[f"ret{h}_rank_x_above_ma_breadth__{pop}"] = (sample[rank_col] * sample[bcol]).astype("float32")
    # 只輸出必要識別欄位、基礎候選輸入與計算方案欄位。
    id_cols = ["日期文字", "市場", "股票代號", "價格研究區段", "收盤"]
    raw_cols = [c for c in ["ret5", "ret20", "ret60", "dist_ma20", "dist_ma60", "range_pos20_include_current", "range_pos20_exclude_current", "lowvol20_candidate", "source_factor"] if c in sample]
    calc_cols = [c for c in sample.columns if c not in set(df.columns) and c not in id_cols and c not in raw_cols]
    keep = list(dict.fromkeys(id_cols + raw_cols + calc_cols))
    return sample[keep], dates, factor_status


def formula_catalog_rows() -> list[dict[str, str]]:
    rows = []
    for group, feature, definition, inputs, gaps in FEATURES:
        if feature in {"ret5_rank", "ret20_rank", "ret60_rank", "dist_ma20_rank", "dist_ma60_rank"}:
            candidates = "候選A：日期×全市場 percentile rank；候選B：日期×市場 percentile rank；各保留 ascending／descending 兩方向；method=average 僅為稽核計算設定"
        elif feature == "range_pos20_rank":
            candidates = "候選A：20根高低點視窗含當根；候選B：使用前20根高低點排除當根；兩候選再分別計算全市場／分市場及正向／反向percentile rank"
        elif feature == "lowvol20_rank":
            candidates = "候選A：20根簡單報酬樣本標準差（ddof=1）；保留全市場／分市場及正向／反向rank；公式僅為候選"
        elif feature == "source_factor_rank":
            candidates = "v2.31來源20根有效K棒報酬波動率；保留全市場／分市場及正向／反向rank；補行日來源因子尚未確認"
        elif feature.endswith("_abs_scaled"):
            raw = feature.replace("_abs_scaled", "")
            candidates = f"候選A：{raw}原始比例；候選B：{raw}×100；候選C：同日全市場Z分數；候選D：同日市場內Z分數"
        elif feature.startswith("market_median_"):
            candidates = "候選A：同日TWSE+TPEX合併中位數；候選B：同日依TWSE/TPEX分組中位數"
        elif feature.startswith("market_breadth_pos_"):
            candidates = "候選A：同日TWSE+TPEX合併的有效正報酬比例；候選B：同日依TWSE/TPEX分組的有效正報酬比例"
        elif feature.startswith("market_breadth_above_"):
            candidates = "候選A：同日TWSE+TPEX合併的有效站上均線比例；候選B：同日依TWSE/TPEX分組的有效站上均線比例"
        elif "ret5_" in feature:
            candidates = "候選A：ret5_rank×market_breadth_pos_ret5；全市場／分市場母體分別計算"
        elif "ret20_" in feature:
            candidates = "候選A：ret20_rank×market_breadth_pos_ret20；候選B：ret20_rank×market_breadth_above_ma20；全市場／分市場母體分別計算"
        else:
            candidates = "候選A：ret60_rank×market_breadth_pos_ret60；候選B：ret60_rank×market_breadth_above_ma60；全市場／分市場母體分別計算"
        rows.append({"特徵群": group, "特徵": feature, "封存資料可支持的概念": definition,
                     "必要輸入": inputs, "本版候選計算方案": candidates,
                     "仍未確認事項": gaps, "官方公式狀態": "NOT_PUBLISHED_OR_NOT_VERIFIABLE",
                     "本版採用狀態": "僅輸出候選值，不選定原始公式"})
    return rows


def data_inventory_rows(sources: dict[str, Path | None], df, cols: dict[str, str], verified_cols: list[str], factor_status=None, verified_stats=None):
    rows = []
    for key, label in [("v231", "v2.31原始OHLC價格研究資料（候選公式主表）"),
                       ("v4701", "v4.70.1五項已驗證特徵核對資料"),
                       ("v477_zip", "v4.77追溯報告ZIP"), ("v464_zip", "v4.64星期六補行情增補ZIP")]:
        p = sources.get(key)
        role = ("v2.31 OHLC計算主表" if key == "v231" else
                "僅核對五項特徵，不作OHLC計算" if key == "v4701" else "補充來源／待核對")
        rows.append({"來源": label, "找到": "是" if p else "否", "路徑": str(p) if p else "",
                     "大小_bytes": p.stat().st_size if p and p.exists() else "",
                     "SHA256": sha256_file(p) if p and p.is_file() else "", "欄位／內容說明": role})
    rows.append({"來源": "v2.31主表載入結果", "找到": "是", "路徑": str(sources["v231"]),
                 "大小_bytes": sources["v231"].stat().st_size, "SHA256": "",
                 "欄位／內容說明": f"rows={len(df):,}; dates={df['日期'].nunique():,}; securities={df['股票代號'].nunique():,}; source_column_map={json.dumps(cols, ensure_ascii=False)}"})
    if verified_stats:
        rows.append({"來源": "v4.70.1自然鍵核對結果", "找到": "是", "路徑": str(sources["v4701"]),
                     "大小_bytes": sources["v4701"].stat().st_size, "SHA256": "",
                     "欄位／內容說明": json.dumps(verified_stats, ensure_ascii=False)})
    if factor_status:
        rows.append({"來源": "v2.31原始來源因子抽樣", "找到": factor_status.get("status"), "路徑": str(sources.get("v231") or ""),
                     "大小_bytes": factor_status.get("matched_rows", 0), "SHA256": "", "欄位／內容說明": json.dumps(factor_status, ensure_ascii=False)})
    return rows


def zip_member_audit(path: Path | None):
    rows = []
    if path is None:
        return [{"來源ZIP": "v4.64補行日增補包", "成員": "", "大小_bytes": "", "欄數": "", "表頭": "SOURCE_NOT_FOUND"}]
    try:
        with zipfile.ZipFile(path) as zf:
            bad = zf.testzip()
            for info in zf.infolist():
                if info.is_dir():
                    continue
                row = {"來源ZIP": path.name, "成員": info.filename, "大小_bytes": info.file_size, "欄數": "", "表頭": ""}
                if info.filename.lower().endswith((".csv", ".csv.gz")):
                    with zf.open(info, "r") as member:
                        if info.filename.lower().endswith(".gz"):
                            with gzip.GzipFile(fileobj=member, mode="rb") as gz:
                                with io.TextIOWrapper(gz, encoding="utf-8-sig", errors="replace", newline="") as text_stream:
                                    header = next(csv.reader(text_stream), [])
                        else:
                            with io.TextIOWrapper(member, encoding="utf-8-sig", errors="replace", newline="") as text_stream:
                                header = next(csv.reader(text_stream), [])
                    row["欄數"] = len(header)
                    row["表頭"] = "|".join(header)
                rows.append(row)
            rows.append({"來源ZIP": path.name, "成員": "__ZIP_INTEGRITY__", "大小_bytes": path.stat().st_size, "欄數": "", "表頭": "PASS" if bad is None else f"FAIL:{bad}"})
    except Exception as exc:
        rows.append({"來源ZIP": path.name, "成員": "__READ_ERROR__", "大小_bytes": path.stat().st_size, "欄數": "", "表頭": str(exc)})
    return rows


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8-sig")
        return
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()), extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def build_report(out: Path, sources, df, cols, verified_cols, compare_rows, sample_df, sample_dates, factor_status, verified_stats):
    import pandas as pd
    cat_rows = formula_catalog_rows()
    write_csv(out / "01_24項特徵候選公式與未確認參數.csv", cat_rows)
    write_csv(out / "02_v4.70.1五項已驗證特徵候選公式一致性.csv", compare_rows)
    write_csv(out / "03_來源資料盤點與欄位映射.csv", data_inventory_rows(sources, df, cols, verified_cols, factor_status, verified_stats))
    write_csv(out / "04_v4.64星期六補行情ZIP成員盤點.csv", zip_member_audit(sources.get("v464_zip")))
    sample_path = out / "05_最近五個市場日24特徵候選計算樣本.csv.gz"
    sample_df.to_csv(sample_path, index=False, compression="gzip", encoding="utf-8-sig")
    # 日別市場母體列數，保留可核對的市場分組差異。
    latest_dates = pd.to_datetime(sample_dates)
    latest = df[df["日期"].isin(latest_dates)]
    market_rows = []
    for date, d in latest.groupby("日期", sort=True):
        market_rows.append({"日期": date.strftime("%Y-%m-%d"), "母體": "TWSE+TPEX合併", "市場": "ALL",
                            "有效列數": int(d[["收盤"]].notna().all(axis=1).sum()), "證券數": int(d["股票代號"].nunique())})
        for market, g in d.groupby("市場", dropna=False):
            market_rows.append({"日期": date.strftime("%Y-%m-%d"), "母體": "依市場分組", "市場": str(market),
                                "有效列數": int(g["收盤"].notna().sum()), "證券數": int(g["股票代號"].nunique())})
    write_csv(out / "06_最近五日市場橫斷面母體列數.csv", market_rows)

    full_rows = len(df)
    dates_n = int(df["日期"].nunique())
    securities_n = int(df["股票代號"].nunique())
    segments_n = int(df[["市場", "股票代號", "價格研究區段"]].drop_duplicates().shape[0])
    saturday_rows = int((df["日期"].dt.weekday == 5).sum())
    dup = int(df.duplicated(["市場", "股票代號", "價格研究區段", "日期"]).sum())
    valid_close_rows = int(df["收盤"].notna().sum())
    valid_ohlc_rows = int(df.get("ohlc_valid_candidate", pd.Series(False, index=df.index)).fillna(False).sum())
    text = [
        "看股最速適 v4.78｜原十檔24項特徵候選公式重建與來源資料核對",
        "=" * 92,
        f"產生時間（Asia/Taipei）：{datetime.now(TZ).isoformat(timespec='seconds')}",
        f"價格計算主來源（v2.31）：{sources['v231']}",
        f"五項既有特徵核對來源（v4.70.1）：{sources['v4701']}",
        "來源唯讀；本版不重訓、不回測、不修改GitHub main。",
        "",
        "一、主資料品質與覆蓋",
        f"- 列數：{full_rows:,}；有效收盤列數：{valid_close_rows:,}；最高、最低、收盤均有效列數：{valid_ohlc_rows:,}；市場日期數：{dates_n:,}；證券數：{securities_n:,}；股票／市場／價格區段組合數：{segments_n:,}。",
        f"- 日期範圍：{df['日期'].min().date()} 至 {df['日期'].max().date()}；星期六資料列數：{saturday_rows:,}。",
        f"- 自然鍵重複數（市場、股票代號、價格研究區段、日期）：{dup:,}。",
        f"- 最近五個市場日期：{', '.join(sample_dates)}；候選計算樣本列數：{len(sample_df):,}。",
        "- 報酬／均線／波動候選以各區段內非缺值收盤觀測計算有效根數；區間位置以最高、最低、收盤均有效K棒計算。此有效K棒定義仍是本版候選假設，未證明等同原策略。",
        "- 所有個股時間序列運算均以市場、股票代號、價格研究區段分組；不跨價格研究區段計算移動視窗。",
        "",
        "二、24項候選公式處理方式",
        "- 本版計算 ret5/ret20/ret60、MA20/MA60距離、20根區間位置（含當根與排除當根兩版本）、20根簡單報酬樣本標準差。",
        "- 橫斷面排名同時保留全市場合併／依TWSE-TPEX分組，以及ascending／descending方向；percentile rank(method=average)只作候選，不視為原始定義。",
        "- abs_scaled同時輸出原始比例、×100及同日Z分數（全市場／分市場）候選值；不把其中任何一種指定為原始公式。",
        "- 市場中位數、正報酬廣度及站上均線比例，同時輸出TWSE+TPEX合併與分市場計算方案。比例的分母僅含該特徵當日非缺值觀測；這是稽核候選定義，不是原策略已確認規格。",
        "- 互動項輸出動能排名×同期間正報酬廣度；H20/H60另輸出排名×對應MA20/MA60廣度候選。精確配對仍未確認。",
        "- source_factor_rank由v2.31原始來源因子抽取最近五日樣本；v4.70.1獨有補行資料沒有OHLC，不納入價格衍生計算，也不自行補造來源因子。",
        "",
        "三、與v4.70.1五項已驗證研究特徵的公式一致性",
        "- v4.70.1五項已驗證特徵先按日期、市場、股票代號、價格研究區段與v2.31主表一對一對齊，再對收盤相對MA5/20/60/120及相對前20有效K棒最高價進行候選值比較。",
        "- `MATCH_CANDIDATE_ONLY`僅表示該候選數值在此資料上達到誤差門檻；不能自動推論為v2.45/v2.51原24項特徵的公式。",
        "- 未比對成功或缺少既有欄位時保留未確認，不會填補或改寫既有資料。",
        "",
        "四、仍需原程式／原始輸出才能確認的關鍵事項",
        "- 相對排名的實際母體（上市上櫃合併或分開）、排名方向、rank方法、平手規則與百分位尺度。",
        "- abs_scaled的真正縮放公式、估計母體、截尾及極值處理。",
        "- 市場廣度的有效母體、缺行情／停牌／新上市／下市股處理及分母。",
        "- 三個互動項的精確市場廣度配對與縮放順序。",
        "- source_factor_rank在補行日如何取得原始因子。",
        "- 原始分類器程式、全部超參數及訓練實作仍未找到；即使特徵公式補齊，也不能宣稱精確重現原模型。",
        "",
        "五、結論",
        "本版完成24項特徵的候選公式計算與資料欄位核對，但不對未公開參數作推定。候選結果供下一輪與原始輸出值核對，不能當成正式原策略特徵或投資訊號。",
        "",
        "六、資料安全與輸出",
        "本版只讀取v2.31 OHLC主表、v4.70.1五項特徵核對資料、v2.31來源因子樣本及v4.64 ZIP目錄；只在新的v4.78資料夾產生報告，不修改來源資料。",
        "輸出：01_24項特徵候選公式與未確認參數.csv；02_v4.70.1五項已驗證特徵候選公式一致性.csv；03_來源資料盤點與欄位映射.csv；04_v4.64星期六補行情ZIP成員盤點.csv；05_最近五個市場日24特徵候選計算樣本.csv.gz；06_最近五日市場橫斷面母體列數.csv；v4.78_manifest.json；v4.78_執行摘要.txt。",
    ]
    (out / "v4.78_執行摘要.txt").write_text("\n".join(text), encoding="utf-8-sig")
    manifest = {
        "version": VERSION,
        "generated_at_taipei": datetime.now(TZ).isoformat(timespec="seconds"),
        "read_only_sources": True,
        "model_retrained": False,
        "backtest_run": False,
        "github_main_modified": False,
        "main_source": "v2.31_with_OHLC",
        "verification_source": "v4.70.1_five_features_only",
        "verification_stats": verified_stats,
        "main_rows": full_rows,
        "market_dates": dates_n,
        "securities": securities_n,
        "valid_close_rows": valid_close_rows,
        "valid_ohlc_candidate_rows": valid_ohlc_rows,
        "saturday_rows": saturday_rows,
        "natural_key_duplicates": dup,
        "sample_market_dates": sample_dates,
        "sample_rows": len(sample_df),
        "feature_count": len(FEATURES),
        "source_paths": {k: str(v) if v else None for k, v in sources.items()},
        "source_columns": cols,
        "verified_feature_columns_found": verified_cols,
        "formula_status": "CANDIDATE_FORMULAS_COMPUTED_NOT_CONFIRMED_AS_ORIGINAL_STRATEGY",
    }
    (out / "v4.78_manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8-sig")
    return manifest


def http_json(method: str, url: str, token: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    headers = {"Accept": "application/vnd.github+json", "Authorization": "Bearer " + token,
               "X-GitHub-Api-Version": API_VERSION, "User-Agent": USER_AGENT}
    body = None
    if payload is not None:
        headers["Content-Type"] = "application/json"
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(url, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=180) as resp:
            raw = resp.read()
        return json.loads(raw.decode("utf-8")) if raw else {}
    except urllib.error.HTTPError as exc:
        try:
            msg = json.loads(exc.read().decode("utf-8", "replace")).get("message", "")
        except Exception:
            msg = ""
        raise SSOError(f"GitHub API HTTP {exc.code}：{msg or url}") from None
    except urllib.error.URLError as exc:
        raise SSOError(f"GitHub連線失敗：{exc.reason}") from None


def get_github_token() -> str:
    token = os.environ.get("GITHUB_TOKEN", "").strip()
    if token:
        return token
    try:
        from google.colab import userdata  # type: ignore
        token = (userdata.get("GITHUB_TOKEN") or "").strip()
        if token:
            log("已從 Colab Secrets 取得 GITHUB_TOKEN。")
            return token
    except Exception:
        pass
    token = getpass.getpass("請輸入 GitHub Token（輸入內容不會顯示）：").strip()
    if not token:
        raise SSOError("未取得 GitHub Token；分析結果已留在 Drive，但 GitHub 上傳未執行。")
    return token


def upload_batch(files: list[tuple[Path, str]]) -> dict[str, Any]:
    if not files:
        raise SSOError("GitHub上傳清單為空。")
    limit = MAX_UPLOAD_MB * 1024 * 1024
    for p, _ in files:
        if not p.is_file():
            raise FileNotFoundError(f"待上傳檔案不存在：{p}")
        if p.stat().st_size <= 0 or p.stat().st_size > limit:
            raise SSOError(f"檔案大小不符或超過{MAX_UPLOAD_MB}MB限制：{p} ({p.stat().st_size:,} bytes)")
    token = get_github_token()
    repo = http_json("GET", f"{API}/repos/{REPO}", token)
    if repo.get("full_name", "").lower() != REPO.lower():
        raise SSOError("GitHub Repository 驗證失敗。")
    main_ref = http_json("GET", f"{API}/repos/{REPO}/git/ref/heads/{BASE_BRANCH}", token)
    base_sha = main_ref.get("object", {}).get("sha")
    if not base_sha:
        raise SSOError("無法取得GitHub main HEAD。")
    stamp = datetime.now(TZ).strftime("%Y%m%d-%H%M%S")
    branch = f"uploads/v4.78-batch-{stamp}"
    http_json("POST", f"{API}/repos/{REPO}/git/refs", token, {"ref": f"refs/heads/{branch}", "sha": base_sha})
    log(f"已建立GitHub獨立分支：{branch}（以main為唯讀起點）")
    uploaded = []
    for idx, (local, repo_path) in enumerate(files, start=1):
        escaped = urllib.parse.quote(repo_path, safe="/")
        # 僅新增檔案；若路徑已存在立即停止，絕不覆寫。
        try:
            http_json("GET", f"{API}/repos/{REPO}/contents/{escaped}?ref={urllib.parse.quote(branch, safe='')}", token)
        except SSOError as exc:
            if "HTTP 404" not in str(exc):
                raise
        else:
            raise SSOError(f"GitHub目標路徑已存在，為避免覆寫而停止：{repo_path}")
        data = local.read_bytes()
        encoded = base64.b64encode(data).decode("ascii")
        log(f"GitHub上傳 {idx}/{len(files)}：{local.name}｜{len(data):,} bytes")
        result = http_json("PUT", f"{API}/repos/{REPO}/contents/{escaped}", token, {
            "message": f"Upload SSO v4.78 audit artifact: {local.name}", "content": encoded, "branch": branch
        })
        content = result.get("content") or {}
        commit = result.get("commit") or {}
        if not content.get("sha") or not commit.get("sha"):
            raise SSOError(f"GitHub未回傳完整上傳結果：{repo_path}")
        uploaded.append({"local_path": str(local), "repo_path": repo_path, "size": len(data),
                         "blob_sha": content["sha"], "commit_sha": commit["sha"],
                         "url": f"https://github.com/{REPO}/blob/{branch}/{escaped}",
                         "raw_url": f"https://raw.githubusercontent.com/{REPO}/{branch}/{escaped}"})
    return {"repository": REPO, "branch": branch, "branch_url": f"https://github.com/{REPO}/tree/{branch}", "files": uploaded}


def create_zip(out: Path) -> Path:
    zpath = out.parent / f"{out.name}.zip"
    with zipfile.ZipFile(zpath, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
        for p in sorted(out.iterdir()):
            if p.is_file():
                zf.write(p, arcname=p.name)
    log(f"已建立v4.78結果ZIP：{zpath}｜{zpath.stat().st_size:,} bytes")
    return zpath


SCRIPT_FILENAME = "SSO_v4_78_candidate_feature_reconstruction_and_github_upload.py"
RECOVERY_FOLDER_NAME = "看股最速適_原十檔24特徵候選公式重建與來源資料核對_v4.78_20261102_232952"


def _looks_like_v478_source(source: str) -> bool:
    return (len(source) >= 25000 and "看股最速適 v4.78" in source and
            "def compute_candidate_base" in source and "def upload_batch" in source and
            "v2.31 原始價格研究資料" in source)


def resolve_script_path(out: Path) -> Path:
    """支援一般 .py 執行及 Colab 儲存格執行；不再直接假設 __file__ 存在。"""
    try:
        current_file = Path(__file__).resolve()
        if current_file.is_file():
            return current_file
    except NameError:
        pass
    except Exception:
        pass

    # 儲存格執行時，優先從目前 Notebook 的原始輸入歷史還原整份程式。
    histories = []
    try:
        ip = get_ipython()  # noqa: F821 - provided by IPython/Colab
        raw = getattr(getattr(ip, "history_manager", None), "input_hist_raw", [])
        histories.extend(reversed(list(raw)))
    except Exception:
        pass
    # linecache可保留目前正在執行的 <ipython-input-...> 儲存格全文。
    try:
        import linecache
        for name, entry in list(linecache.cache.items()):
            if str(name).startswith("<ipython-input-"):
                lines = entry[2] if len(entry) > 2 else []
                if lines:
                    histories.append("".join(lines))
    except Exception:
        pass
    for source in histories:
        if isinstance(source, str) and _looks_like_v478_source(source):
            target = out / SCRIPT_FILENAME
            target.write_text(source, encoding="utf-8")
            log(f"Colab儲存格執行：已從Notebook輸入歷史還原本次v4.78完整程式：{target}")
            return target

    # 最後才尋找明確檔名的既有程式，並驗證內容，避免誤上傳其他版本。
    candidates = [Path.cwd() / SCRIPT_FILENAME, Path("/content") / SCRIPT_FILENAME,
                  Path("/mnt/data") / SCRIPT_FILENAME]
    for candidate in candidates:
        try:
            if candidate.is_file() and _looks_like_v478_source(candidate.read_text(encoding="utf-8")):
                return candidate.resolve()
        except Exception:
            continue
    raise SSOError("無法定位本次執行的v4.78完整Python原始碼。請以 !python /content/SSO_v4_78_candidate_feature_reconstruction_and_github_upload.py 執行，或確認Colab儲存格保留完整程式碼。")


def upload_completed_run(out: Path, zip_path: Path, v477_zip: Path, timestamp: str) -> dict[str, Any]:
    script_path = resolve_script_path(out)
    batch = [
        (v477_zip, f"audit_uploads/v4.77_20261102/{v477_zip.name}"),
        (zip_path, f"audit_uploads/v4.78_{timestamp[:8]}/{zip_path.name}"),
        (script_path, f"audit_uploads/v4.78_{timestamp[:8]}/{SCRIPT_FILENAME}"),
    ]
    result = upload_batch(batch)
    receipt = out / "v4.78_GitHub上傳回執.json"
    receipt.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8-sig")
    log("GitHub上傳回執已另存於輸出資料夾；本地ZIP與GitHub上傳ZIP保持完全相同。")
    return result


def try_resume_previous_failed_run(v477_zip: Path) -> bool:
    """前次分析與ZIP已完成但上傳失敗時，僅續傳；不要求重新讀取原始資料。"""
    out = DRIVE_ROOT / RECOVERY_FOLDER_NAME
    zip_path = DRIVE_ROOT / f"{RECOVERY_FOLDER_NAME}.zip"
    receipt = out / "v4.78_GitHub上傳回執.json"
    if not (out.is_dir() and zip_path.is_file() and not receipt.exists()):
        return False
    # 讀取ZIP摘要確認這是已有完整報告的失敗批次，不會誤用空資料夾。
    try:
        with zipfile.ZipFile(zip_path) as zf:
            names = set(zf.namelist())
            required = {"01_24項特徵候選公式與未確認參數.csv", "02_v4.70.1五項已驗證特徵候選公式一致性.csv",
                        "05_最近五個市場日24特徵候選計算樣本.csv.gz", "v4.78_manifest.json", "v4.78_執行摘要.txt"}
            if not required.issubset(names) or zf.testzip() is not None:
                return False
            manifest = json.loads(zf.read("v4.78_manifest.json").decode("utf-8-sig"))
            if manifest.get("version") != VERSION or manifest.get("main_rows") != 4186723:
                return False
    except Exception:
        return False
    log("偵測到上一輪分析及ZIP均已完成、僅上傳前發生__file__錯誤；直接續傳，不重新計算4.18M列。")
    timestamp = "20261102_232952"
    result = upload_completed_run(out, zip_path, v477_zip, timestamp)
    print("\n" + "=" * 92)
    print("v4.78既有成果續傳完成（沒有重跑分析）")
    print(f"結果ZIP：{zip_path}")
    print(f"GitHub分支：{result['branch']}")
    print(f"分支網址：{result['branch_url']}")
    for item in result["files"]:
        print(f"\nGitHub檔案：{item['repo_path']}\n大小：{item['size']:,} bytes\n網址：{item['url']}")
    print("確認：GitHub main及Drive來源資料均未修改。")
    print("=" * 92)
    return True


def main() -> int:
    print("=" * 92)
    print("看股最速適 v4.78｜原十檔24項特徵候選公式重建與來源資料核對")
    print("v2.31含OHLC作計算主表；v4.70.1僅核對5項特徵；來源唯讀、不重訓、不回測、不改main")
    print("GitHub上傳整合於本次執行：v4.77 ZIP + v4.78 ZIP + v4.78 Python；不要求輸入YES")
    print("CHLINX/sso為公開Repository，上傳檔案會公開可讀。")
    print("=" * 92)
    mount_drive()
    sources = locate_sources()

    # 優先檢查是否可續傳上一輪已完成的分析成果。續傳只需 v4.77 ZIP、
    # 已存在的 v4.78 報告及本程式，不應再依賴 v2.31／v4.70.1 原始來源可讀性。
    if not sources["v477_zip"]:
        raise FileNotFoundError("找不到可確認為v4.77的結果ZIP。已搜尋專案資料夾、MyDrive及/content，並檢查ZIP檔名、內部manifest與摘要；請查看前述ZIP候選清單。程式未略過v4.77，也未以其他版本冒充。")
    if try_resume_previous_failed_run(sources["v477_zip"]):
        return 0

    # 只有沒有可續傳的既有成果、確實需要重新分析時，才檢查來源資料。
    if not sources["v231"]:
        raise FileNotFoundError("找不到v2.31原始OHLC研究資料：相對價格因子與前瞻報酬研究資料_v2.31.csv.gz")
    if not sources["v4701"]:
        raise FileNotFoundError("找不到v4.70.1五項已驗證特徵核對資料：01_v231補行日模型研究資料表.csv.gz；已檢查固定路徑及Drive遞迴搜尋。")

    timestamp = datetime.now(TZ).strftime("%Y%m%d_%H%M%S")
    out = DRIVE_ROOT / f"看股最速適_原十檔24特徵候選公式重建與來源資料核對_v4.78_{timestamp}"
    out.mkdir(parents=True, exist_ok=False)
    log(f"輸出資料夾：{out}")

    df, cols = read_main_dataset(sources["v231"])
    log(f"v2.31價格主表載入完成：{len(df):,} rows；{df['日期'].nunique():,} market dates；{df['股票代號'].nunique():,} securities")
    verified_df, verified_cols, verified_key_cols = read_verified_dataset(sources["v4701"])
    df, verified_stats = merge_verified_features(df, verified_df, verified_cols)
    del verified_df

    df = compute_candidate_base(df)
    compare_rows = compare_verified_features(df, verified_cols)
    sample_df, sample_dates, factor_status = add_sample_hypotheses(df, sources["v231"])
    manifest = build_report(out, sources, df, cols, verified_cols, compare_rows, sample_df,
                            sample_dates, factor_status, verified_stats)
    del sample_df
    del df

    zip_path = create_zip(out)
    upload_result = upload_completed_run(out, zip_path, sources["v477_zip"], timestamp)
    print("\n" + "=" * 92)
    print("v4.78完成")
    print(f"輸出資料夾：{out}")
    print(f"結果ZIP：{zip_path}")
    print(f"v2.31主資料列數：{manifest['main_rows']:,}；最近樣本日期：{', '.join(manifest['sample_market_dates'])}")
    print(f"v4.70.1自然鍵重疊：{verified_stats['natural_key_overlap_rows']:,}；v4.70.1獨有：{verified_stats['v4701_unique_rows_without_main_ohlc']:,}")
    print(f"特徵候選：{manifest['feature_count']} 項；重訓：否；回測：否")
    print(f"GitHub分支：{upload_result['branch']}")
    print(f"分支網址：{upload_result['branch_url']}")
    for item in upload_result["files"]:
        print(f"\nGitHub檔案：{item['repo_path']}\n大小：{item['size']:,} bytes\n網址：{item['url']}\nRaw：{item['raw_url']}\nBlob SHA：{item['blob_sha']}")
    print("\n確認：GitHub main及Drive來源資料均未修改。")
    print("=" * 92)
    return 0


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log("使用者中斷；原始資料與GitHub main均未修改。")
    except Exception as exc:
        log(f"執行失敗：{type(exc).__name__}: {exc}")
        log("已產生的v4.78分析報告會留在Drive；請保留完整錯誤訊息。")
        raise