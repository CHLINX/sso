# -*- coding: utf-8 -*-
"""
SSO v1.2.3｜HAC 重算與 238 檔 Holm 多重檢定稽核

用途：
1. 唯讀讀取最新 v1.2.2 結果資料夾。
2. 從 05_Q5 個股逐筆明細，重新計算所有 238 檔訓練候選的測試期 HAC 標準誤、95% 下界。
3. 與 v1.2.2 的 03_訓練候選時間外驗證.csv 逐檔核對訊號數、平均淨報酬及 HAC 下界。
4. 以兩種成本皆須為正報酬的 intersection-union test（p_joint=max(p_base,p_stress））形成每檔 p 值。
5. 對完整 238 檔候選家族計算 Holm FWER 與 Benjamini-Hochberg FDR 校正。
6. 補上九檔候選的現行公司名稱／交易市場對照；清楚標示這不等於歷史逐日證券分類認證。
7. 產生報告、CSV、JSON、ZIP，並在同一次執行中推送至 CHLINX/sso 的獨立 audit branch。

安全：
- Google Drive 來源唯讀，不修改、不刪除、不移動、不覆寫任何既有來源或結果。
- 不呼叫官方 API；股票名稱對照是本次研究使用的固定來源紀錄，來源 URL 一併輸出。
- 不重新計算因子、不重訓、不重跑 v1.2.2 主研究。
- 不更新 main；只建立新 audit branch。
- 使用既有 GITHUB_TOKEN（環境變數或 Colab Secret），不輸出 Token。

解讀限制：
- p 值以 HAC Newey-West 標準誤的漸近常態近似計算。
- family 定義為訓練期預先篩選的 238 檔候選；兩種成本採 intersection-union p=max(p_base,p_stress)。
- Holm / BH 是對 v1.2.2 既定測試期的事後多重比較校正，不是新的獨立樣本外驗證。
- 新的獨立確認仍需 2026-10-23 之後的新增行情，且訊號需完成 H=10 出場。
"""

from __future__ import annotations

import os
import re
import sys
import json
import math
import time
import shutil
import hashlib
import zipfile
import tempfile
import subprocess
import uuid
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo
from urllib.parse import quote

import numpy as np
import pandas as pd

# =============================================================================
# 固定設定
# =============================================================================

REPO_OWNER = "CHLINX"
REPO_NAME = "sso"
REPO_URL = f"https://github.com/{REPO_OWNER}/{REPO_NAME}.git"
DEFAULT_BRANCH = "main"
DRIVE_ROOT = Path("/content/drive/MyDrive/看股最速適")
RESULT_PREFIX = "看股最速適_全市場個股實際獲利驗證_v1_2_2_"
EXPECTED_SOURCE_SHA256 = "7debd5cde9d47a42091f7e8a0dec3b5c50808e28c6f1f6c59a55ddaaeb182dd8"
EXPECTED_CANDIDATES = 238
EXPECTED_PRELIMINARY_PASSES = 9
EXPECTED_GLOBAL_DATES = 2447
TEST_START = pd.Timestamp("2023-10-13")
HAC_Z = 1.96
HAC_LAGS = 9
MAX_GITHUB_FILE_BYTES = 100 * 1024 * 1024

REQUIRED_SOURCE_FILES = [
    "01_訓練期個股候選排名.csv",
    "02_時間外測試期全體個股結果.csv",
    "03_訓練候選時間外驗證.csv",
    "04_個股年度淨報酬.csv",
    "05_Q5個股逐筆訊號與淨報酬.csv.gz",
    "06_證券範圍排除清單.csv",
    "07_時間外驗證通過候選前十檔.csv",
    "08_來源檢核與研究限制.json",
    "09_執行摘要.txt",
]

# 現行公司名稱／市場參考來源。名稱及現行市場可供候選辨識；不宣稱這是逐日歷史證券類別資料。
IDENTITY = {
    ("TWSE", "8046"): ("南電", "TWSE公司資訊", "https://www.twse.com.tw/pdf/ch/8046_ch.pdf"),
    ("TPEx", "6640"): ("均華", "發行公司官網：明載上櫃及股票代號6640", "https://www.gmmcorp.com.tw/aboutus/關於均華"),
    ("TPEx", "3624"): ("光頡", "發行公司官網：股價資訊明載TPEx及股票代碼3624", "https://www.viking.com.tw/zh-TW/investors/Investors-T0303.html"),
    ("TWSE", "2368"): ("金像電", "TWSE上市公司交易資料", "https://www.twse.com.tw/exchangeReport/BWIBBU_d?response=html"),
    ("TWSE", "2383"): ("台光電", "TWSE上市公司交易資料", "https://www.twse.com.tw/exchangeReport/BWIBBU_d?response=html"),
    ("TWSE", "3017"): ("奇鋐", "TWSE公司資訊", "https://www.twse.com.tw/pdf/ch/3017_ch.pdf"),
    ("TPEx", "6223"): ("旺矽", "現行市場及公司基本資料", "https://www.entrust.com.tw/entrust/agent/comIn.do?cpcmpno=84&type=1"),
    ("TWSE", "2455"): ("全新", "TWSE上市公司資訊揭露", "https://www.twse.com.tw/IIH2/zh/company/information.html?code=2455"),
    ("TPEx", "8358"): ("金居", "發行公司官網：明載上櫃代碼8358", "https://www.co-tech.com/tw"),
}

# =============================================================================
# 基本工具
# =============================================================================

def log(message: str) -> None:
    print(message, flush=True)


def taipei_stamp() -> str:
    return datetime.now(ZoneInfo("Asia/Taipei")).strftime("%Y%m%d_%H%M%S")


def fmt_bytes(n: int) -> str:
    n = int(n or 0)
    if n >= 1024 ** 3:
        return f"{n / 1024**3:,.3f} GB"
    return f"{n / 1024**2:,.2f} MB"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def normal_sf(z: float) -> float:
    """標準常態右尾機率，使用標準函式庫避免額外依賴。"""
    if not math.isfinite(z):
        return 1.0
    return min(1.0, max(0.0, 0.5 * math.erfc(float(z) / math.sqrt(2.0))))


def find_token() -> str:
    token = os.environ.get("GITHUB_TOKEN", "").strip()
    if not token:
        try:
            from google.colab import userdata  # type: ignore
            token = (userdata.get("GITHUB_TOKEN") or "").strip()
        except Exception:
            token = ""
    if not token:
        raise RuntimeError(
            "找不到 GITHUB_TOKEN。請確認 Colab 環境變數或 Colab Secret 已設定；不要把 Token 貼在程式碼或對話中。"
        )
    return token


def run_git(args: list[str], *, cwd: Path | None, env: dict[str, str], capture: bool = True) -> str:
    result = subprocess.run(
        ["git", *args], cwd=str(cwd) if cwd else None, env=env,
        text=True, encoding="utf-8", errors="replace",
        stdout=subprocess.PIPE if capture else None,
        stderr=subprocess.PIPE if capture else None,
        check=False,
    )
    if result.returncode != 0:
        stdout = (result.stdout or "")[-3000:]
        stderr = (result.stderr or "")[-5000:]
        raise RuntimeError(
            f"Git 命令失敗（退出代碼 {result.returncode}）：git {' '.join(args[:5])}\n"
            f"STDOUT:\n{stdout}\nSTDERR:\n{stderr}"
        )
    return (result.stdout or "").strip()


def make_askpass(directory: Path) -> Path:
    askpass = directory / "git-askpass.sh"
    askpass.write_text(
        "#!/bin/sh\ncase \"$1\" in\n"
        "  *Username*|*username*) printf '%s\\n' 'x-access-token' ;;\n"
        "  *Password*|*password*) printf '%s\\n' \"$GITHUB_TOKEN\" ;;\n"
        "  *) printf '\\n' ;;\nesac\n",
        encoding="utf-8",
    )
    askpass.chmod(0o700)
    return askpass


def find_latest_v122_folder() -> Path:
    if not DRIVE_ROOT.is_dir():
        raise RuntimeError(f"找不到 Google Drive 路徑：{DRIVE_ROOT}。請先掛載 Google Drive。")
    folders = [p for p in DRIVE_ROOT.glob(RESULT_PREFIX + "*") if p.is_dir()]
    valid = []
    for folder in folders:
        if all((folder / name).is_file() for name in REQUIRED_SOURCE_FILES):
            try:
                meta = json.loads((folder / "08_來源檢核與研究限制.json").read_text(encoding="utf-8-sig"))
                if str(meta.get("來源SHA256", "")).lower() == EXPECTED_SOURCE_SHA256:
                    valid.append(folder)
            except Exception:
                continue
    if not valid:
        raise RuntimeError("找不到完整且來源 SHA256 相符的 v1.2.2 結果資料夾。")
    return sorted(valid, key=lambda p: p.name)[-1]


def one_sided_p_from_hac(mean_value: float, standard_error: float, n: int) -> float:
    if n < 2 or not math.isfinite(mean_value) or not math.isfinite(standard_error):
        return 1.0
    if mean_value <= 0 or standard_error <= 0:
        return 1.0
    return normal_sf(mean_value / standard_error)


def hac_stats(values: np.ndarray, locations: np.ndarray, total_dates: int, max_lag: int = HAC_LAGS) -> dict:
    """與 v1.2.2 相同的全市場交易日時間軸 Newey-West HAC 算式，額外回傳 SE。"""
    if total_dates <= 1 or len(values) < 2:
        return {"n": int(len(values)), "mean": np.nan, "se": np.nan, "lower95": np.nan}
    values = np.asarray(values, dtype=np.float64)
    locations = np.asarray(locations, dtype=np.int64)
    valid = (locations >= 0) & (locations < total_dates) & np.isfinite(values)
    values = values[valid]
    locations = locations[valid]
    n = len(values)
    if n < 2:
        return {"n": int(n), "mean": np.nan, "se": np.nan, "lower95": np.nan}
    mean_value = float(np.mean(values))
    score = np.zeros(total_dates, dtype=np.float64)
    np.add.at(score, locations, values - mean_value)
    gamma0 = float(np.dot(score, score) / total_dates)
    lrv = gamma0
    lag_limit = min(int(max_lag), total_dates - 1)
    for lag in range(1, lag_limit + 1):
        gamma = float(np.dot(score[lag:], score[:-lag]) / total_dates)
        weight = 1.0 - lag / (lag_limit + 1.0)
        lrv += 2.0 * weight * gamma
    lrv = max(0.0, lrv)
    se = math.sqrt(total_dates * lrv) / n
    return {"n": int(n), "mean": mean_value, "se": float(se), "lower95": float(mean_value - HAC_Z * se)}


def holm_adjust(p_values: np.ndarray) -> np.ndarray:
    p = np.asarray(p_values, dtype=np.float64)
    m = len(p)
    order = np.argsort(p, kind="mergesort")
    adjusted_sorted = np.empty(m, dtype=np.float64)
    running = 0.0
    for rank, idx in enumerate(order):
        raw = min(1.0, (m - rank) * float(p[idx]))
        running = max(running, raw)
        adjusted_sorted[rank] = running
    out = np.empty(m, dtype=np.float64)
    out[order] = adjusted_sorted
    return out


def bh_adjust(p_values: np.ndarray) -> np.ndarray:
    p = np.asarray(p_values, dtype=np.float64)
    m = len(p)
    order = np.argsort(p, kind="mergesort")
    sorted_p = p[order]
    q_sorted = np.empty(m, dtype=np.float64)
    running = 1.0
    for i in range(m - 1, -1, -1):
        rank = i + 1
        running = min(running, float(sorted_p[i]) * m / rank)
        q_sorted[i] = min(1.0, running)
    out = np.empty(m, dtype=np.float64)
    out[order] = q_sorted
    return out


def write_csv(df: pd.DataFrame, path: Path) -> None:
    df.to_csv(path, index=False, encoding="utf-8-sig")
    log(f"[SAVE] {path.name} | {len(df):,} rows | {fmt_bytes(path.stat().st_size)}")


# =============================================================================
# 主分析
# =============================================================================

def analyze(folder: Path, out_dir: Path) -> dict:
    log(f"[SOURCE] v1.2.2 結果資料夾：{folder}")
    meta = json.loads((folder / "08_來源檢核與研究限制.json").read_text(encoding="utf-8-sig"))
    if str(meta.get("來源SHA256", "")).lower() != EXPECTED_SOURCE_SHA256:
        raise RuntimeError("v1.2.2 JSON 的原始來源 SHA256 不符。")

    candidate_path = folder / "03_訓練候選時間外驗證.csv"
    detail_path = folder / "05_Q5個股逐筆訊號與淨報酬.csv.gz"

    log("[01/08] 讀取訓練候選時間外驗證總表……")
    candidates = pd.read_csv(candidate_path, encoding="utf-8-sig", dtype={"市場": "string", "股票代號": "string"})
    required_cols = [
        "市場", "股票代號", "訓練期_訓練期排名", "測試期_訊號數",
        "測試期_基準成本平均淨報酬", "測試期_壓力成本平均淨報酬",
        "測試期_基準成本_HAC95下界", "測試期_壓力成本_HAC95下界",
        "時間外驗證通過",
    ]
    missing = [c for c in required_cols if c not in candidates.columns]
    if missing:
        raise RuntimeError("候選表缺少欄位：" + repr(missing))
    if len(candidates) != EXPECTED_CANDIDATES:
        raise RuntimeError(f"候選數不是預期的 {EXPECTED_CANDIDATES}，實際 {len(candidates)}。")
    if candidates.duplicated(["市場", "股票代號"]).any():
        raise RuntimeError("候選表市場＋代號自然鍵重複。")

    candidates["市場"] = candidates["市場"].astype(str).str.strip()
    candidates["股票代號"] = candidates["股票代號"].astype(str).str.strip().str.replace(r"\.0$", "", regex=True)
    candidates["時間外驗證通過"] = candidates["時間外驗證通過"].astype(str).str.strip().str.lower().isin(["true", "1", "yes"])

    log("[02/08] 讀取 05 逐筆明細（只讀必要欄位）……")
    usecols = [
        "期間", "市場", "股票代號", "訊號日期", "訊號全市場交易日序號",
        "基準成本淨報酬", "壓力成本淨報酬",
    ]
    detail = pd.read_csv(
        detail_path,
        compression="gzip",
        encoding="utf-8-sig",
        usecols=usecols,
        dtype={"期間": "string", "市場": "string", "股票代號": "string"},
        low_memory=False,
    )
    detail["訊號日期"] = pd.to_datetime(detail["訊號日期"], errors="coerce").dt.normalize()
    detail["訊號全市場交易日序號"] = pd.to_numeric(detail["訊號全市場交易日序號"], errors="coerce")
    detail["基準成本淨報酬"] = pd.to_numeric(detail["基準成本淨報酬"], errors="coerce")
    detail["壓力成本淨報酬"] = pd.to_numeric(detail["壓力成本淨報酬"], errors="coerce")
    if detail["訊號日期"].isna().any() or detail["訊號全市場交易日序號"].isna().any():
        raise RuntimeError("05 逐筆明細含無法解析的訊號日期或全市場交易日序號。")
    detail["市場"] = detail["市場"].astype(str).str.strip()
    detail["股票代號"] = detail["股票代號"].astype(str).str.strip().str.replace(r"\.0$", "", regex=True)

    test = detail.loc[detail["期間"].astype(str).str.strip().eq("時間外測試期")].copy()
    if test.empty:
        raise RuntimeError("05 逐筆明細找不到『時間外測試期』資料。")
    start_rows = test.loc[test["訊號日期"].eq(TEST_START), "訊號全市場交易日序號"].dropna().unique()
    if len(start_rows) != 1:
        raise RuntimeError(
            "無法由逐筆明細唯一定位 2023-10-13 的全市場交易日序號；"
            f"找到 {len(start_rows)} 個。為避免錯算 HAC，停止。"
        )
    test_start_idx = int(start_rows[0])
    total_dates = EXPECTED_GLOBAL_DATES - test_start_idx
    if total_dates <= HAC_LAGS:
        raise RuntimeError(f"測試期全市場交易日數異常：{total_dates}")
    test["date_offset"] = test["訊號全市場交易日序號"].astype(np.int64) - test_start_idx
    if test["date_offset"].min() < 0 or test["date_offset"].max() >= total_dates:
        raise RuntimeError("測試期逐筆序號超出預期全市場日期軸。")
    duplicate_detail = int(test.duplicated(["市場", "股票代號", "訊號全市場交易日序號"]).sum())
    if duplicate_detail:
        raise RuntimeError(f"測試期同一股票同一交易日有重複訊號：{duplicate_detail} 筆。")

    log(f"[PASS] 測試訊號列數：{len(test):,}；測試期全市場日期數：{total_dates:,}；起始 date_idx：{test_start_idx}")
    log("[03/08] 對 238 檔候選逐檔重算 HAC……")

    candidate_keys = set(zip(candidates["市場"], candidates["股票代號"]))
    test_candidates = test.loc[
        [(m, c) in candidate_keys for m, c in zip(test["市場"], test["股票代號"])]
    ].copy()

    detail_rows = []
    groups = test_candidates.groupby(["市場", "股票代號"], sort=False, observed=True)
    for (market, code), group in groups:
        loc = group["date_offset"].to_numpy(dtype=np.int64)
        base = hac_stats(group["基準成本淨報酬"].to_numpy(dtype=np.float64), loc, total_dates)
        stress = hac_stats(group["壓力成本淨報酬"].to_numpy(dtype=np.float64), loc, total_dates)
        detail_rows.append({
            "市場": market,
            "股票代號": code,
            "逐筆重算訊號數": base["n"],
            "重算基準成本平均淨報酬": base["mean"],
            "重算基準成本HAC標準誤": base["se"],
            "重算基準成本HAC95下界": base["lower95"],
            "重算壓力成本平均淨報酬": stress["mean"],
            "重算壓力成本HAC標準誤": stress["se"],
            "重算壓力成本HAC95下界": stress["lower95"],
        })
    recalculated = pd.DataFrame(detail_rows)
    if recalculated.empty:
        raise RuntimeError("候選股票沒有對應的逐筆測試交易資料。")

    log("[04/08] 與 v1.2.2 原始總表逐檔核對……")
    merged = candidates.merge(recalculated, on=["市場", "股票代號"], how="left")
    numeric_pairs = [
        ("測試期_基準成本平均淨報酬", "重算基準成本平均淨報酬", "基準平均差"),
        ("測試期_壓力成本平均淨報酬", "重算壓力成本平均淨報酬", "壓力平均差"),
        ("測試期_基準成本_HAC95下界", "重算基準成本HAC95下界", "基準HAC下界差"),
        ("測試期_壓力成本_HAC95下界", "重算壓力成本HAC95下界", "壓力HAC下界差"),
    ]
    for source_col, calc_col, diff_col in numeric_pairs:
        merged[diff_col] = pd.to_numeric(merged[source_col], errors="coerce") - pd.to_numeric(merged[calc_col], errors="coerce")
    merged["訊號數差"] = pd.to_numeric(merged["測試期_訊號數"], errors="coerce").fillna(0) - pd.to_numeric(merged["逐筆重算訊號數"], errors="coerce").fillna(0)
    diff_cols = [x[2] for x in numeric_pairs]
    merged["HAC重算核對通過"] = (
        merged["訊號數差"].eq(0)
        & merged[diff_cols].abs().max(axis=1, skipna=True).fillna(0).le(1e-8)
    )
    # 原本測試期無交易者，兩邊均無數值，可視為樣本數核對通過，但不會產生顯著 p 值。
    no_test = pd.to_numeric(merged["測試期_訊號數"], errors="coerce").fillna(0).eq(0)
    merged.loc[no_test, "HAC重算核對通過"] = merged.loc[no_test, "逐筆重算訊號數"].isna()

    log("[05/08] 建立兩種成本聯合 p 值，執行 Holm / BH 校正……")
    p_base_list = []
    p_stress_list = []
    p_joint_list = []
    for _, row in merged.iterrows():
        n = int(row["逐筆重算訊號數"]) if pd.notna(row["逐筆重算訊號數"]) else 0
        matched = bool(row["HAC重算核對通過"])
        pb = one_sided_p_from_hac(
            float(row["重算基準成本平均淨報酬"]),
            float(row["重算基準成本HAC標準誤"]), n
        ) if matched and pd.notna(row["重算基準成本平均淨報酬"]) else 1.0
        ps = one_sided_p_from_hac(
            float(row["重算壓力成本平均淨報酬"]),
            float(row["重算壓力成本HAC標準誤"]), n
        ) if matched and pd.notna(row["重算壓力成本平均淨報酬"]) else 1.0
        p_base_list.append(pb)
        p_stress_list.append(ps)
        p_joint_list.append(max(pb, ps))

    merged["基準成本單尾常態近似p"] = p_base_list
    merged["壓力成本單尾常態近似p"] = p_stress_list
    merged["雙成本聯合p_IUT"] = p_joint_list
    merged["Holm_FWER校正p"] = holm_adjust(np.asarray(p_joint_list, dtype=float))
    merged["BH_FDR校正q"] = bh_adjust(np.asarray(p_joint_list, dtype=float))
    merged["Holm_5pct通過"] = merged["Holm_FWER校正p"].le(0.05)
    merged["BH_5pct通過"] = merged["BH_FDR校正q"].le(0.05)
    merged["未校正雙成本p小於0.05"] = merged["雙成本聯合p_IUT"].lt(0.05)
    merged.sort_values(["Holm_FWER校正p", "訓練期_訓練期排名"], kind="mergesort", inplace=True)
    merged.reset_index(drop=True, inplace=True)
    merged.insert(0, "多重檢定順位", np.arange(1, len(merged) + 1))

    log("[06/08] 建立九檔現行名稱／市場對照……")
    pass_rows = merged.loc[merged["時間外驗證通過"]].copy()
    identity_rows = []
    for _, row in pass_rows.iterrows():
        key = (str(row["市場"]), str(row["股票代號"]))
        name, source_type, source_url = IDENTITY.get(key, ("", "未找到名稱來源", ""))
        identity_rows.append({
            "市場": key[0],
            "股票代號": key[1],
            "現行公司簡稱": name,
            "v1.2.2測試期訊號數": int(row["測試期_訊號數"]),
            "測試期壓力成本平均淨報酬": row["測試期_壓力成本平均淨報酬"],
            "雙成本聯合p_IUT": row["雙成本聯合p_IUT"],
            "Holm_FWER校正p": row["Holm_FWER校正p"],
            "BH_FDR校正q": row["BH_FDR校正q"],
            "現行名稱市場來源類型": source_type,
            "來源URL": source_url,
            "現行名稱市場核對日期": "2026-11-10",
            "歷史逐日官方證券類別核驗": "未完成；不得據此宣稱全歷史普通股類型已核實",
        })
    identities = pd.DataFrame(identity_rows)

    # 嚴格核對 v1.2.2 原來的初步通過數及九檔代號集合。
    preliminary_passes = int(merged["時間外驗證通過"].sum())
    if preliminary_passes != EXPECTED_PRELIMINARY_PASSES:
        raise RuntimeError(
            f"原始初步通過數與預期不符：預期 {EXPECTED_PRELIMINARY_PASSES}，實際 {preliminary_passes}。"
        )
    if len(identities) != EXPECTED_PRELIMINARY_PASSES:
        raise RuntimeError("九檔現行名稱對照數量不符，請人工核對。")

    merged.to_csv(out_dir / "01_238檔HAC重算與多重比較校正.csv", index=False, encoding="utf-8-sig")
    log(f"[SAVE] 01_238檔HAC重算與多重比較校正.csv | {len(merged):,} rows")
    pass_report = merged.loc[merged["時間外驗證通過"]].copy()
    pass_report.sort_values("Holm_FWER校正p", inplace=True)
    pass_report.to_csv(out_dir / "02_原九檔初步通過候選校正結果.csv", index=False, encoding="utf-8-sig")
    log(f"[SAVE] 02_原九檔初步通過候選校正結果.csv | {len(pass_report):,} rows")
    identities.to_csv(out_dir / "03_九檔現行名稱市場對照.csv", index=False, encoding="utf-8-sig")
    log(f"[SAVE] 03_九檔現行名稱市場對照.csv | {len(identities):,} rows")

    # 摘要數值
    best = merged.iloc[0]
    holm_count = int(merged["Holm_5pct通過"].sum())
    bh_count = int(merged["BH_5pct通過"].sum())
    mismatch_count = int((~merged["HAC重算核對通過"]).sum())
    best_prelim = pass_report.iloc[0] if not pass_report.empty else None

    report = {
        "版本": "SSO_v1.2.3_HAC重算與Holm多重檢定稽核",
        "執行時間台北": datetime.now(ZoneInfo("Asia/Taipei")).isoformat(),
        "來源結果資料夾": str(folder),
        "原始來源SHA256": EXPECTED_SOURCE_SHA256,
        "候選家族數": int(len(merged)),
        "v1.2.2原始初步通過數": preliminary_passes,
        "逐筆測試訊號數": int(len(test)),
        "測試期全市場交易日數": int(total_dates),
        "測試期起始全市場date_idx": int(test_start_idx),
        "HAC逐檔重算核對不通過數": mismatch_count,
        "HAC核對容許誤差": 1e-8,
        "HAC方法": "與v1.2.2一致的全市場交易日序列Newey-West；lag=9；lower95=mean-1.96*SE",
        "p值方法": "由逐筆資料重算HAC SE，使用單尾漸近常態近似；p_joint=max(p_base,p_stress)，要求兩種成本皆正報酬的intersection-union test",
        "多重比較家族": "訓練期先選出的全部238檔候選，不因測試期結果縮小家族",
        "Holm_FWER_5pct通過數": holm_count,
        "BH_FDR_5pct通過數": bh_count,
        "原始初步九檔校正後狀態": "全部仍保留作描述性候選；Holm校正未達0.05者不得宣稱統計確認",
        "Holm最佳候選": {
            "市場": str(best["市場"]),
            "股票代號": str(best["股票代號"]),
            "現行名稱": IDENTITY.get((str(best["市場"]), str(best["股票代號"])), ("", "", ""))[0],
            "雙成本聯合p_IUT": float(best["雙成本聯合p_IUT"]),
            "Holm_FWER校正p": float(best["Holm_FWER校正p"]),
            "BH_FDR校正q": float(best["BH_FDR校正q"]),
        },
        "原九檔中Holm最佳": None if best_prelim is None else {
            "市場": str(best_prelim["市場"]),
            "股票代號": str(best_prelim["股票代號"]),
            "現行名稱": IDENTITY.get((str(best_prelim["市場"]), str(best_prelim["股票代號"])), ("", "", ""))[0],
            "雙成本聯合p_IUT": float(best_prelim["雙成本聯合p_IUT"]),
            "Holm_FWER校正p": float(best_prelim["Holm_FWER校正p"]),
        },
        "現行公司名稱市場對照數": int(len(identities)),
        "歷史逐日官方證券分類": "未完成；現行名稱／市場資料不能取代歷史逐日證券主檔",
        "獨立確認資料缺口": "v1.2.2價格資料止於2026-10-22；需要2026-10-23後的官方逐日行情，且H=10訊號完成出場後才能納入獨立確認。",
        "安全限制": [
            "Google Drive來源唯讀",
            "不呼叫官方API",
            "不修改、不刪除、不移動、不覆寫v1.2.2既有結果",
            "不重訓、不重建因子、不改變v1.2.2原始候選門檻",
            "GitHub只建立獨立audit branch，不更新main",
        ],
    }
    with (out_dir / "04_HAC重算與多重檢定稽核.json").open("w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    summary_lines = [
        "=" * 96,
        "SSO v1.2.3｜HAC重算與238檔Holm多重檢定稽核",
        "=" * 96,
        f"執行時間：{report['執行時間台北']}",
        f"來源 v1.2.2：{folder.name}",
        f"來源 SHA256：{EXPECTED_SOURCE_SHA256}",
        "",
        f"訓練期候選家族：{len(merged):,} 檔",
        f"v1.2.2原始初步通過：{preliminary_passes} 檔",
        f"逐筆測試訊號：{len(test):,} 筆",
        f"測試期全市場交易日：{total_dates:,} 日",
        f"HAC重算核對不通過：{mismatch_count} 檔",
        "",
        f"Holm FWER 5%通過：{holm_count} 檔",
        f"BH FDR 5%通過：{bh_count} 檔",
        "",
        "Holm校正最佳候選：",
        f"  {best['市場']} {best['股票代號']} {IDENTITY.get((str(best['市場']), str(best['股票代號'])), ('', '', ''))[0]}",
        f"  雙成本聯合 p：{best['雙成本聯合p_IUT']:.8g}",
        f"  Holm校正 p：{best['Holm_FWER校正p']:.8g}",
        f"  BH校正 q：{best['BH_FDR校正q']:.8g}",
        "",
    ]
    if best_prelim is not None:
        summary_lines += [
            "原九檔初步通過候選中，Holm校正證據最佳：",
            f"  {best_prelim['市場']} {best_prelim['股票代號']} {IDENTITY.get((str(best_prelim['市場']), str(best_prelim['股票代號'])), ('', '', ''))[0]}",
            f"  雙成本聯合 p：{best_prelim['雙成本聯合p_IUT']:.8g}",
            f"  Holm校正 p：{best_prelim['Holm_FWER校正p']:.8g}",
            "",
        ]
    summary_lines += [
        "統計解讀：p值由逐筆資料重算HAC標準誤後，以單尾漸近常態近似；兩種成本使用intersection-union p=max，再對全部238檔候選作Holm及BH校正。",
        "若Holm校正未通過0.05，原v1.2.2的單檔HAC通過只能視為描述性候選，不可宣稱家族錯誤率控制下的統計確認。",
        "現行名稱／市場對照只補足候選辨識；未取得逐日官方歷史證券主檔，因此歷史逐日普通股分類仍未完成。",
        "獨立確認仍需2026-10-23以後的新行情及完整H=10出場觀察；本報告不重複使用新資料或反向調整選股條件。",
        "Drive原始來源及v1.2.2結果全程唯讀；只新增本次v1.2.3稽核資料夾。",
        "=" * 96,
    ]
    summary_text = "\n".join(summary_lines) + "\n"
    (out_dir / "05_執行摘要.txt").write_text(summary_text, encoding="utf-8")

    # 附上本次實際執行腳本，確保 GitHub 留存研究程式與輸出對應。
    try:
        current_script = Path(__file__).resolve()
        if current_script.is_file():
            shutil.copy2(current_script, out_dir / current_script.name)
    except Exception as exc:
        log(f"[WARN] 無法將執行程式複製到結果資料夾：{exc}")

    return report


# =============================================================================
# ZIP 與 GitHub audit branch
# =============================================================================

def make_zip(out_dir: Path) -> Path:
    zip_path = out_dir.with_suffix(".zip")
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as archive:
        for path in sorted(out_dir.iterdir()):
            if path.is_file():
                archive.write(path, arcname=path.name)
    with zipfile.ZipFile(zip_path, "r") as archive:
        bad = archive.testzip()
        if bad:
            raise RuntimeError(f"v1.2.3 ZIP CRC 失敗：{bad}")
    return zip_path


def upload_github(out_dir: Path, zip_path: Path, source_folder: Path, report: dict) -> dict:
    if shutil.which("git") is None:
        raise RuntimeError("Colab 環境找不到 git。")
    token = find_token()
    upload_files = [p for p in sorted(out_dir.iterdir()) if p.is_file()] + [zip_path]
    too_large = [(p.name, p.stat().st_size) for p in upload_files if p.stat().st_size >= MAX_GITHUB_FILE_BYTES]
    if too_large:
        raise RuntimeError("有單檔達到 GitHub 100 MB 限制：" + repr(too_large))

    result_stamp = re.search(r"(\d{8}_\d{6})$", source_folder.name)
    source_stamp = result_stamp.group(1) if result_stamp else taipei_stamp()
    run_stamp = taipei_stamp()
    branch = f"audit/sso-v1.2.3-multipletest-{source_stamp}-gitpush-{run_stamp[-6:]}-{uuid.uuid4().hex[:4]}"
    artifact_dir = f"audit/sso-v1.2.3-candidate-multipletest/{source_stamp}/{run_stamp}"

    readme_lines = [
        "# SSO v1.2.3｜HAC重算與238檔多重檢定稽核",
        "",
        f"- v1.2.2來源結果資料夾：`{source_folder.name}`",
        f"- 原始行情來源 SHA256：`{EXPECTED_SOURCE_SHA256}`",
        "- 分析：由逐筆資料重算HAC SE及95%下界；兩種成本採IUT p=max；對238檔候選進行Holm FWER與BH FDR校正。",
        "- 限制：p值採單尾漸近常態近似；現行名稱／市場對照不等於歷史逐日證券分類認證。",
        "- Google Drive來源唯讀；不修改既有v1.2.2結果；只推送獨立audit branch，不更新main。",
        "",
        "## 本次檔案",
        "",
    ]
    for p in upload_files:
        readme_lines.append(f"- `{p.name}` — {p.stat().st_size:,} bytes — SHA256 `{sha256_file(p)}`")
    readme_lines += ["", "## 執行摘要", "", "```text", (out_dir / "05_執行摘要.txt").read_text(encoding="utf-8"), "```", ""]
    readme_bytes = "\n".join(readme_lines).encode("utf-8")

    with tempfile.TemporaryDirectory(prefix="sso_v123_gitpush_") as temp_name:
        temp = Path(temp_name)
        git_dir = temp / "repo.git"
        askpass = make_askpass(temp)
        env = os.environ.copy()
        env.update({
            "GITHUB_TOKEN": token,
            "GIT_ASKPASS": str(askpass),
            "GIT_TERMINAL_PROMPT": "0",
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_AUTHOR_NAME": "SSO Audit Uploader",
            "GIT_AUTHOR_EMAIL": "sso-audit-uploader@users.noreply.github.com",
            "GIT_COMMITTER_NAME": "SSO Audit Uploader",
            "GIT_COMMITTER_EMAIL": "sso-audit-uploader@users.noreply.github.com",
        })
        log("[GITHUB 01/06] 初始化暫存 Git bare repository……")
        run_git(["init", "--bare", str(git_dir)], cwd=None, env=env)
        run_git(["remote", "add", "origin", REPO_URL], cwd=git_dir, env=env)
        log("[GITHUB 02/06] 從 main 取得 shallow partial history……")
        run_git(["-c", "protocol.version=2", "fetch", "--depth=1", "--filter=blob:none", "origin", f"refs/heads/{DEFAULT_BRANCH}"], cwd=git_dir, env=env)
        main_sha = run_git(["rev-parse", "FETCH_HEAD^{commit}"], cwd=git_dir, env=env)
        log(f"[PASS] main 基準 SHA：{main_sha}")
        run_git(["read-tree", main_sha], cwd=git_dir, env=env)

        log("[GITHUB 03/06] 將稽核輸出加入 Git objects……")
        upload_items: list[tuple[Path | None, str, bytes | None]] = [
            (p, f"{artifact_dir}/{p.name}", None) for p in upload_files
        ]
        upload_items.append((None, f"{artifact_dir}/README_GitHub上傳索引.md", readme_bytes))
        for i, (local, remote_path, inline) in enumerate(upload_items, 1):
            if local is not None:
                blob = run_git(["hash-object", "-w", str(local)], cwd=git_dir, env=env)
                label, size = local.name, local.stat().st_size
            else:
                proc = subprocess.run(
                    ["git", "hash-object", "-w", "--stdin"], cwd=str(git_dir), env=env,
                    input=inline, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False,
                )
                if proc.returncode != 0:
                    raise RuntimeError("Git hash-object 寫入 README 失敗：" + proc.stderr.decode("utf-8", "replace")[-2000:])
                blob = proc.stdout.decode("ascii").strip()
                label, size = "README_GitHub上傳索引.md", len(inline or b"")
            run_git(["update-index", "--add", "--cacheinfo", "100644", blob, remote_path], cwd=git_dir, env=env)
            log(f"[ADD {i}/{len(upload_items)}] {label} | {fmt_bytes(size)}")

        log("[GITHUB 04/06] 建立 audit commit……")
        tree_sha = run_git(["write-tree"], cwd=git_dir, env=env)
        commit_sha = run_git([
            "-c", "user.name=SSO Audit Uploader", "-c", "user.email=sso-audit-uploader@users.noreply.github.com",
            "commit-tree", tree_sha, "-p", main_sha, "-m",
            f"Add SSO v1.2.3 HAC and multiple-testing audit ({source_stamp})",
        ], cwd=git_dir, env=env)
        log("[GITHUB 05/06] 推送獨立 audit branch……")
        run_git(["push", "origin", f"{commit_sha}:refs/heads/{branch}"], cwd=git_dir, env=env)
        log("[GITHUB 06/06] 驗證遠端 branch 與 main……")
        remote = run_git(["ls-remote", "origin", f"refs/heads/{branch}"], cwd=git_dir, env=env).split()
        if not remote or remote[0] != commit_sha:
            raise RuntimeError("遠端 audit branch SHA 驗證失敗。")
        main_after = run_git(["ls-remote", "origin", f"refs/heads/{DEFAULT_BRANCH}"], cwd=git_dir, env=env).split()
        main_after_sha = main_after[0] if main_after else ""
        if main_after_sha != main_sha:
            log(f"[WARN] 執行期間 main SHA 有變動；本程式未對 main 執行 push。before={main_sha}; after={main_after_sha}")
        else:
            log(f"[PASS] main 維持原 SHA：{main_after_sha}")
        env["GITHUB_TOKEN"] = ""

    branch_url = f"https://github.com/{REPO_OWNER}/{REPO_NAME}/tree/{branch}/{artifact_dir}"
    zip_url = f"https://github.com/{REPO_OWNER}/{REPO_NAME}/blob/{branch}/{artifact_dir}/{quote(zip_path.name)}"
    commit_url = f"https://github.com/{REPO_OWNER}/{REPO_NAME}/commit/{commit_sha}"
    return {
        "branch": branch,
        "commit": commit_sha,
        "main_sha_before": main_sha,
        "main_sha_after": main_after_sha,
        "folder_url": branch_url,
        "zip_url": zip_url,
        "commit_url": commit_url,
        "uploaded_files": len(upload_items),
    }


def main() -> int:
    log("=" * 96)
    log("SSO v1.2.3｜HAC重算與238檔Holm多重檢定稽核")
    log("=" * 96)
    log("[安全] Drive唯讀；不呼叫官方API；不修改、不刪除、不移動、不覆寫既有檔案。")
    log("[安全] GitHub只建立獨立audit branch；不修改main。")

    # 若尚未掛載，安全地掛載 Drive。
    if not DRIVE_ROOT.is_dir():
        try:
            from google.colab import drive  # type: ignore
            drive.mount("/content/drive")
        except Exception as exc:
            raise RuntimeError("無法掛載 Google Drive：" + str(exc))
    if not DRIVE_ROOT.is_dir():
        raise RuntimeError(f"Drive 路徑不存在：{DRIVE_ROOT}")

    source_folder = find_latest_v122_folder()
    stamp = taipei_stamp()
    out_dir = DRIVE_ROOT / f"看股最速適_v1_2_3_HAC重算與多重檢定稽核_{stamp}"
    out_dir.mkdir(parents=False, exist_ok=False)

    report = analyze(source_folder, out_dir)
    zip_path = make_zip(out_dir)
    log(f"[PASS] ZIP CRC：{zip_path.name} | {fmt_bytes(zip_path.stat().st_size)}")
    log(f"[RESULT] Holm FWER 5%通過：{report['Holm_FWER_5pct通過數']} / {report['候選家族數']}")
    log(f"[RESULT] BH FDR 5%通過：{report['BH_FDR_5pct通過數']} / {report['候選家族數']}")
    log(f"[OUTPUT] {out_dir}")

    upload = upload_github(out_dir, zip_path, source_folder, report)
    log("\n" + "=" * 96)
    log("[PASS] GitHub 上傳完成")
    log(f"Repository：{REPO_OWNER}/{REPO_NAME}")
    log(f"Audit branch：{upload['branch']}")
    log(f"Commit：{upload['commit']}")
    log(f"上傳檔案數：{upload['uploaded_files']}（稽核輸出 + ZIP + README）")
    log(f"Drive 結果資料夾：{out_dir}")
    log(f"GitHub 資料夾：{upload['folder_url']}")
    log(f"GitHub ZIP：{upload['zip_url']}")
    log(f"Commit：{upload['commit_url']}")
    log(f"main 基準 SHA：{upload['main_sha_before']}")
    log(f"main 執行後 SHA：{upload['main_sha_after']}")
    log("[PASS] Drive來源唯讀；main未被本程式更新。")
    log("=" * 96)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SystemExit:
        raise
    except Exception as exc:
        log("\n[FAIL] v1.2.3 稽核未完成：" + str(exc))
        raise
