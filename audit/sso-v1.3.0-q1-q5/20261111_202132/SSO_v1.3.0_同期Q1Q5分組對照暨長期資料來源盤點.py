
# =============================================================================
# SSO v1.3.0｜同期 Q1-Q5 分組對照暨長期資料來源盤點
#
# 研究目的：
# 1. 沿用 v1.2.6 / v1.2.7 / v1.2.8 / v1.2.9 固定程式。
# 2. 官方普通股每日 range_pos20 百分位切成 Q1-Q5。
# 3. H=1/3/5/10 使用同一市場交易日曆、同一價格配對函式、同一成本公式。
# 4. 對照 Q1 與 Q5，確認 Q5 是否真的優於低分位組。
# 5. 凍結 v1.2.9 找出的 12 檔候選，不依本次結果更換名單。
# 6. 盤點更早期 OHLC 主檔與歷史 ISIN/CFI 分類資料。
#
# 本版不會：
# - 修改 Q5 原定義
# - 重訓模型
# - 呼叫 TWSE / TPEx API
# - 修改或覆寫 Drive 原始行情
# - 把現在的 ISIN 普通股分類回套成十年前的官方分類
# - 把回溯檢查稱為樣本外績效
# - 修改 GitHub main
#
# 輸出：
# - Q1-Q5 四週期績效摘要
# - 每日 Q1-Q5 績效
# - Q5-Q1 差異
# - 完整逐筆排程
# - 凍結 12 檔候選
# - 歷史 OHLC 與歷史證券分類來源盤點
# - 結論 JSON、Python、README、ZIP
# - GitHub 獨立 audit branch + Git blob read-back
# =============================================================================

from __future__ import annotations

import os
import re
import io
import sys
import json
import csv
import uuid
import base64
import hashlib
import zipfile
import tempfile
import urllib.request
import unicodedata
from pathlib import Path
from datetime import datetime
from urllib.parse import quote
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import requests


# =============================================================================
# 01. 固定版本與路徑
# =============================================================================

VERSION = "SSO_v1.3.0_同期Q1Q5分組對照暨長期資料來源盤點"

ROOT = Path("/content/drive/MyDrive/看股最速適")

V129_BRANCH = (
    "audit/sso-v1.2.9-multi-horizon-"
    "20261111_162816-3dfc"
)

V129_SCRIPT_PATH = (
    "audit/sso-v1.2.9-multi-horizon/"
    "20261111_162816/"
    "SSO_v1.2.9_官方普通股Q5多週期成熟淨報酬驗證.py"
)

V129_SCRIPT_GIT_BLOB_SHA1 = (
    "bab6929d4eb6c070b341ba15c596a428831fea81"
)

V129_OUTPUT_DIR = (
    ROOT /
    "看股最速適_v1_2_9_多週期成熟淨報酬驗證_20261111_162816"
)

V129_ZIP = (
    ROOT /
    "看股最速適_v1_2_9_多週期成熟淨報酬驗證_20261111_162816.zip"
)

V129_ZIP_SHA256_EXPECTED = (
    "26db7fe64580d10c72581951f794b8bb2f71e179"
    "dde798c66a3ec1682605394c"
)

V129_CROSS_HORIZON_FILE = (
    "16_個股跨週期表現總表.csv"
)

HORIZONS = (1, 3, 5, 10)

TAIPEI = ZoneInfo("Asia/Taipei")

REPO_OWNER = "CHLINX"
REPO_NAME = "sso"

EXPECTED_CANDIDATE_CODES = {
    "1416", "2033", "2465", "2468",
    "2923", "4526", "4741", "5013",
    "6233", "6708", "6870", "8099",
}

V129_SUMMARY_LABELS = {
    "v1.2.7原凍結Q5_官方普通股過濾",
    "v1.2.8官方普通股重新排名Q5",
}


# =============================================================================
# 02. 基礎工具
# =============================================================================

def log(message):
    now = datetime.now(TAIPEI).strftime("%H:%M:%S")
    print(f"[{now}] {message}", flush=True)


def run_stamp():
    return datetime.now(TAIPEI).strftime("%Y%m%d_%H%M%S")


def sha256_file(path):
    h = hashlib.sha256()

    with open(path, "rb") as f:
        for chunk in iter(
            lambda: f.read(8 * 1024 * 1024),
            b"",
        ):
            h.update(chunk)

    return h.hexdigest()


def git_blob_sha1(data):
    header = f"blob {len(data)}\0".encode("utf-8")
    return hashlib.sha1(header + data).hexdigest()


def json_safe(value):
    if value is None or value is pd.NA:
        return None

    if isinstance(value, dict):
        return {
            str(k): json_safe(v)
            for k, v in value.items()
        }

    if isinstance(value, (list, tuple, set)):
        return [json_safe(x) for x in value]

    if isinstance(value, Path):
        return str(value)

    if isinstance(value, (pd.Timestamp, datetime)):
        return value.isoformat()

    if isinstance(value, np.generic):
        return json_safe(value.item())

    if isinstance(value, float) and not np.isfinite(value):
        return None

    return value


def csv_write(frame, path):
    frame.to_csv(
        path,
        index=False,
        encoding="utf-8-sig",
        na_rep="",
    )


def json_write(data, path):
    Path(path).write_text(
        json.dumps(
            json_safe(data),
            ensure_ascii=False,
            indent=2,
            allow_nan=False,
        ),
        encoding="utf-8-sig",
    )


def require_columns(frame, columns, label):
    missing = [c for c in columns if c not in frame.columns]

    if missing:
        raise RuntimeError(
            f"{label} 缺少必要欄位：{missing}"
        )


# =============================================================================
# 03. Drive 與固定程式載入
# =============================================================================

def ensure_drive():
    if ROOT.is_dir():
        return

    log("[DRIVE] 掛載 Google Drive。")

    try:
        from google.colab import drive
        drive.mount(
            "/content/drive",
            force_remount=False,
        )
    except Exception as exc:
        raise RuntimeError(
            f"Google Drive 掛載失敗：{exc}"
        )

    if not ROOT.is_dir():
        raise FileNotFoundError(
            f"找不到 SSO 根目錄：{ROOT}"
        )


def load_pinned_v129():
    url = (
        "https://raw.githubusercontent.com/"
        f"{REPO_OWNER}/{REPO_NAME}/"
        f"{V129_BRANCH}/"
        f"{quote(V129_SCRIPT_PATH, safe='/')}"
    )

    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": "SSO-v1.3.0-audit"
        },
    )

    with urllib.request.urlopen(
        request,
        timeout=120,
    ) as response:
        source_bytes = response.read()

    actual = git_blob_sha1(source_bytes)

    if actual != V129_SCRIPT_GIT_BLOB_SHA1:
        raise RuntimeError(
            "v1.2.9 固定程式 Git blob SHA1 不符。"
            f"\n預期：{V129_SCRIPT_GIT_BLOB_SHA1}"
            f"\n實際：{actual}"
        )

    temp_script = (
        Path(tempfile.gettempdir())
        / f"SSO_v129_pinned_{uuid.uuid4().hex[:8]}.py"
    )

    temp_script.write_bytes(source_bytes)

    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "sso_v129_pinned_dependency_v130",
        str(temp_script),
    )

    if spec is None or spec.loader is None:
        raise RuntimeError("無法載入 v1.2.9 固定程式。")

    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)

    required = [
        "load_v128_module",
        "build_multi_horizon_returns",
        "summarize_by_horizon",
        "summarize_by_signal_date",
    ]

    missing = [
        name for name in required
        if not hasattr(module, name)
    ]

    if missing:
        raise RuntimeError(
            f"v1.2.9 固定程式缺少函式：{missing}"
        )

    log("[PASS] v1.2.9 固定程式 SHA1 核驗完成。")

    return module


# =============================================================================
# 04. v1.2.9 封存 ZIP 與候選名單核驗
# =============================================================================

def verify_v129_archive():
    if not V129_OUTPUT_DIR.is_dir():
        raise FileNotFoundError(
            f"找不到 v1.2.9 輸出資料夾：{V129_OUTPUT_DIR}"
        )

    if not V129_ZIP.is_file():
        raise FileNotFoundError(
            f"找不到 v1.2.9 ZIP：{V129_ZIP}"
        )

    actual_sha = sha256_file(V129_ZIP)

    if actual_sha != V129_ZIP_SHA256_EXPECTED:
        raise RuntimeError(
            "v1.2.9 ZIP SHA256 與已核對版本不符。"
            f"\n預期：{V129_ZIP_SHA256_EXPECTED}"
            f"\n實際：{actual_sha}"
        )

    archive_records = []

    with zipfile.ZipFile(V129_ZIP, "r") as zf:

        bad = zf.testzip()

        if bad:
            raise RuntimeError(
                f"v1.2.9 ZIP CRC 失敗：{bad}"
            )

        names = [
            x for x in zf.namelist()
            if not x.endswith("/")
        ]

        for member in names:
            basename = Path(member).name
            local = V129_OUTPUT_DIR / basename

            if not local.is_file():
                raise FileNotFoundError(
                    f"ZIP 成員在 Drive 資料夾不存在：{basename}"
                )

            archive_bytes = zf.read(member)
            local_bytes = local.read_bytes()

            archive_sha = hashlib.sha256(
                archive_bytes
            ).hexdigest()

            local_sha = hashlib.sha256(
                local_bytes
            ).hexdigest()

            if archive_sha != local_sha:
                raise RuntimeError(
                    f"v1.2.9 ZIP 與 Drive 檔案不一致：{basename}"
                )

            archive_records.append({
                "檔案": basename,
                "大小bytes": len(local_bytes),
                "ZIP與Drive逐檔一致": True,
                "SHA256": local_sha,
            })

    log(
        f"[PASS] v1.2.9 ZIP CRC 及 "
        f"{len(archive_records)} 個成員逐檔核對通過。"
    )

    return {
        "zip_sha256": actual_sha,
        "archive_members": len(archive_records),
        "records": archive_records,
    }


def derive_frozen_candidates():
    path = V129_OUTPUT_DIR / V129_CROSS_HORIZON_FILE

    if not path.is_file():
        raise FileNotFoundError(
            f"找不到 v1.2.9 跨週期個股表：{path}"
        )

    frame = pd.read_csv(
        path,
        dtype={"股票代號": "string"},
        encoding="utf-8-sig",
        low_memory=False,
    )

    require_columns(
        frame,
        [
            "訊號口徑", "股票代號", "股票名稱",
        ],
        "v1.2.9 跨週期個股表",
    )

    modes = set(frame["訊號口徑"].dropna().astype(str))

    if not V129_SUMMARY_LABELS.issubset(modes):
        raise RuntimeError(
            "v1.2.9 跨週期表缺少兩種固定 Q5 口徑。"
            f"\n實際：{sorted(modes)}"
        )

    selected = []

    for code, code_rows in frame.groupby(
        "股票代號",
        sort=True,
        dropna=True,
    ):
        mode_pass = {}

        for mode in V129_SUMMARY_LABELS:
            sub = code_rows.loc[
                code_rows["訊號口徑"].eq(mode)
            ]

            if len(sub) != 1:
                mode_pass[mode] = False
                continue

            row = sub.iloc[0]
            all_horizons_pass = True

            for h in (1, 3, 5):
                fields = [
                    f"H{h}_獨立訊號日期數",
                    f"H{h}_基準成本平均淨報酬",
                    f"H{h}_基準成本中位數淨報酬",
                    f"H{h}_壓力成本平均淨報酬",
                    f"H{h}_壓力成本中位數淨報酬",
                ]

                values = {
                    key: pd.to_numeric(
                        pd.Series([row.get(key)]),
                        errors="coerce",
                    ).iloc[0]
                    for key in fields
                }

                passed = (
                    pd.notna(values[fields[0]])
                    and values[fields[0]] >= 3
                    and all(
                        pd.notna(values[k]) and values[k] > 0
                        for k in fields[1:]
                    )
                )

                if not passed:
                    all_horizons_pass = False
                    break

            mode_pass[mode] = all_horizons_pass

        if all(mode_pass.values()):
            selected.append(str(code))

    selected = sorted(set(selected))

    if set(selected) != EXPECTED_CANDIDATE_CODES:
        raise RuntimeError(
            "凍結候選名單與 v1.2.9 已核對結果不一致。"
            f"\n預期：{sorted(EXPECTED_CANDIDATE_CODES)}"
            f"\n實際：{selected}"
        )

    candidate_rows = []

    for code in selected:
        code_rows = frame.loc[
            frame["股票代號"].astype(str).eq(code)
        ]

        name = str(code_rows["股票名稱"].iloc[0])

        row = {
            "股票代號": code,
            "股票名稱": name,
            "候選來源": "v1.2.9 兩種 Q5 口徑共同通過 H1/H3/H5",
            "名單狀態": "FROZEN，不依 v1.3.0 結果調整",
        }

        for mode in sorted(V129_SUMMARY_LABELS):
            sub = code_rows.loc[
                code_rows["訊號口徑"].eq(mode)
            ]

            if len(sub) != 1:
                raise RuntimeError(
                    f"{code} 的固定口徑資料不唯一。"
                )

            source = sub.iloc[0]

            for h in (1, 3, 5):
                for field in [
                    "獨立訊號日期數",
                    "基準成本平均淨報酬",
                    "基準成本中位數淨報酬",
                    "壓力成本平均淨報酬",
                    "壓力成本中位數淨報酬",
                ]:
                    key = f"H{h}_{field}"
                    row[f"{mode}_H{h}_{field}"] = source.get(key)

        candidate_rows.append(row)

    candidates = pd.DataFrame(candidate_rows)

    if len(candidates) != 12:
        raise RuntimeError(
            f"凍結候選應為 12 檔，實際 {len(candidates)} 檔。"
        )

    log("[PASS] v1.2.9 兩種口徑共同通過的 12 檔候選已凍結。")

    return candidates, frame


# =============================================================================
# 05. 歷史行情與歷史證券分類來源盤點
# =============================================================================

def normalize_col(value):
    value = unicodedata.normalize(
        "NFKC",
        str(value),
    ).lower()

    return re.sub(
        r"[^0-9a-z\u4e00-\u9fff]",
        "",
        value,
    )


def detect_column(columns, aliases):
    normalized = {
        normalize_col(c): c
        for c in columns
    }

    for alias in aliases:
        key = normalize_col(alias)

        if key in normalized:
            return normalized[key]

    for col in columns:
        nk = normalize_col(col)

        for alias in aliases:
            na = normalize_col(alias)

            if na and na in nk:
                return col

    return None


def history_candidate_score(path):
    name = path.name.lower()

    score = 0

    if "v2.31" in name:
        score += 100

    if "v231" in name:
        score += 90

    if "含ohlc" in name:
        score += 70

    if "計算主表" in name:
        score += 60

    if "ohlc" in name:
        score += 40

    if path.suffix.lower() in {".csv", ".gz"}:
        score += 10

    return score


def scan_history_files(root):
    """
    只盤點檔名疑似歷史 OHLC 主檔。
    不修改、不搬動、不解壓覆寫來源檔。
    """

    log("[HISTORY 1/3] 掃描疑似歷史 OHLC 檔案名稱。")

    candidates = []
    classification_candidates = []

    skip_dir_names = {
        "__pycache__",
        ".ipynb_checkpoints",
    }

    for current, dirs, files in os.walk(root):

        dirs[:] = [
            d for d in dirs
            if d not in skip_dir_names
        ]

        current_path = Path(current)

        # 不把本次及 v1.2.9 新輸出誤認成歷史來源。
        normalized_path = str(current_path).replace("\\", "/")

        if (
            "看股最速適_v1_2_9_" in normalized_path
            or "看股最速適_v1_3_0_" in normalized_path
        ):
            continue

        for filename in files:
            path = current_path / filename
            lower = filename.lower()

            if any(x in lower for x in [
                "isin", "cfi", "商品種類",
                "證券分類", "普通股清單",
                "security_type",
            ]):
                classification_candidates.append(path)

            suffixes = "".join(path.suffixes).lower()

            supported = (
                suffixes.endswith(".csv")
                or suffixes.endswith(".csv.gz")
                or suffixes.endswith(".parquet")
                or suffixes.endswith(".zip")
            )

            if not supported:
                continue

            likely = any(x in lower for x in [
                "v2.31", "v231",
                "含ohlc", "ohlc",
                "計算主表",
            ])

            if not likely:
                continue

            candidates.append(path)

    candidates = sorted(
        candidates,
        key=lambda p: (
            -history_candidate_score(p),
            -p.stat().st_size,
            str(p),
        ),
    )

    classification_candidates = sorted(
        set(classification_candidates),
        key=lambda p: str(p),
    )

    log(
        f"[INFO] 疑似歷史 OHLC 檔案：{len(candidates)}；"
        f"疑似證券分類檔案：{len(classification_candidates)}。"
    )

    inventory_rows = []

    # 只分析前 10 個最高分來源的欄位結構。
    # 真正的逐列日期掃描只對第一個可用 CSV 做一次。
    scanned_full_date_range = False

    for path in candidates[:10]:

        row = {
            "檔案": path.name,
            "路徑": str(path),
            "大小bytes": path.stat().st_size,
            "SHA256": "",
            "欄位辨識狀態": "",
            "日期欄": "",
            "代號欄": "",
            "開盤欄": "",
            "最高欄": "",
            "最低欄": "",
            "收盤欄": "",
            "市場欄": "",
            "名稱欄": "",
            "CFI或ISIN欄": "",
            "讀取列數": None,
            "起日": "",
            "迄日": "",
            "官方歷史分類欄位存在": False,
            "備註": "",
        }

        suffixes = "".join(path.suffixes).lower()

        if suffixes.endswith(".zip"):
            row["欄位辨識狀態"] = "ZIP容器，未自動解壓或讀取"
            row["備註"] = "保留原始 ZIP；需另核對內部成員及來源中繼資料。"
            inventory_rows.append(row)
            continue

        if suffixes.endswith(".parquet"):
            try:
                import pyarrow.parquet as pq

                pf = pq.ParquetFile(path)
                columns = list(pf.schema.names)

                row["讀取列數"] = int(pf.metadata.num_rows)
                row["欄位辨識狀態"] = "PARQUET_SCHEMA"

            except Exception as exc:
                row["欄位辨識狀態"] = "PARQUET讀取失敗"
                row["備註"] = str(exc)[:300]
                inventory_rows.append(row)
                continue

        else:
            try:
                header = pd.read_csv(
                    path,
                    nrows=0,
                    encoding="utf-8-sig",
                    encoding_errors="replace",
                    compression="infer",
                )

                columns = list(header.columns)
                row["欄位辨識狀態"] = "CSV_HEADER_OK"

            except Exception as exc:
                row["欄位辨識狀態"] = "CSV_HEADER_FAILED"
                row["備註"] = str(exc)[:300]
                inventory_rows.append(row)
                continue

        date_col = detect_column(
            columns,
            ["trade_date", "date", "日期", "交易日期", "交易日", "資料日期"],
        )

        code_col = detect_column(
            columns,
            ["instrument_code", "code", "symbol", "股票代號", "證券代號", "股票代碼"],
        )

        open_col = detect_column(
            columns,
            ["open", "開盤價", "開盤"],
        )

        high_col = detect_column(
            columns,
            ["high", "最高價", "最高"],
        )

        low_col = detect_column(
            columns,
            ["low", "最低價", "最低"],
        )

        close_col = detect_column(
            columns,
            ["close", "收盤價", "收盤"],
        )

        market_col = detect_column(
            columns,
            ["market", "市場", "市場別", "交易市場"],
        )

        name_col = detect_column(
            columns,
            ["instrument_name", "name", "股票名稱", "證券名稱", "名稱"],
        )

        cfi_col = detect_column(
            columns,
            ["CFI", "ISIN CFI", "CFI碼", "商品種類代碼", "金融工具分類"],
        )

        isin_col = detect_column(
            columns,
            ["ISIN", "國際證券識別碼"],
        )

        row.update({
            "日期欄": date_col or "",
            "代號欄": code_col or "",
            "開盤欄": open_col or "",
            "最高欄": high_col or "",
            "最低欄": low_col or "",
            "收盤欄": close_col or "",
            "市場欄": market_col or "",
            "名稱欄": name_col or "",
            "CFI或ISIN欄": cfi_col or isin_col or "",
            "官方歷史分類欄位存在": bool(cfi_col or isin_col),
        })

        has_ohlc_schema = all([
            date_col,
            code_col,
            open_col,
            high_col,
            low_col,
            close_col,
        ])

        if not has_ohlc_schema:
            row["欄位辨識狀態"] = "OHLC_SCHEMA_INCOMPLETE"
            inventory_rows.append(row)
            continue

        if (
            suffixes.endswith(".csv")
            or suffixes.endswith(".csv.gz")
        ) and not scanned_full_date_range:

            log(
                f"[HISTORY] 掃描第一個可辨識 OHLC CSV 日期範圍："
                f"{path.name}"
            )

            try:
                total_rows = 0
                min_date = None
                max_date = None

                for chunk in pd.read_csv(
                    path,
                    usecols=[date_col],
                    dtype="string",
                    encoding="utf-8-sig",
                    encoding_errors="replace",
                    compression="infer",
                    chunksize=400_000,
                    low_memory=False,
                ):
                    parsed = pd.to_datetime(
                        chunk[date_col],
                        errors="coerce",
                    )

                    # 日期若含民國年格式，改用 v1.2.6 解析器。
                    if parsed.notna().sum() == 0:
                        parsed = v126.parse_date_series(
                            chunk[date_col]
                        )

                    parsed = pd.Series(parsed).dropna()

                    total_rows += len(chunk)

                    if len(parsed):
                        chunk_min = pd.Timestamp(parsed.min()).normalize()
                        chunk_max = pd.Timestamp(parsed.max()).normalize()

                        min_date = (
                            chunk_min
                            if min_date is None
                            else min(min_date, chunk_min)
                        )

                        max_date = (
                            chunk_max
                            if max_date is None
                            else max(max_date, chunk_max)
                        )

                row["讀取列數"] = int(total_rows)

                row["起日"] = (
                    str(min_date.date())
                    if min_date is not None else ""
                )

                row["迄日"] = (
                    str(max_date.date())
                    if max_date is not None else ""
                )

                row["SHA256"] = sha256_file(path)
                row["備註"] = (
                    "已逐列掃描日期欄；尚未證明為完整官方普通股歷史資料。"
                )

                scanned_full_date_range = True

            except Exception as exc:
                row["備註"] = (
                    "日期欄逐列掃描失敗：" + str(exc)[:300]
                )

        elif row["欄位辨識狀態"] == "PARQUET_SCHEMA":
            row["備註"] = (
                "已核對欄位及列數；未逐列讀取價格，尚未認證歷史分類。"
            )

        else:
            row["備註"] = (
                "欄位可辨識；日期範圍未重複掃描。"
            )

        inventory_rows.append(row)

    classification_rows = []

    for path in classification_candidates[:30]:
        classification_rows.append({
            "檔案": path.name,
            "路徑": str(path),
            "大小bytes": path.stat().st_size,
            "SHA256": (
                sha256_file(path)
                if path.is_file()
                and path.stat().st_size <= 2 * 1024**3
                else ""
            ),
            "檔名辨識": "ISIN/CFI/證券分類相關",
            "是否可直接代表逐日歷史分類": False,
            "備註": (
                "僅列為待核對來源；必須確認日期欄、有效期間、"
                "CFI分類內容及來源完整性後才能用於歷史普通股宇宙。"
            ),
        })

    return (
        pd.DataFrame(inventory_rows),
        pd.DataFrame(classification_rows),
    )


# =============================================================================
# 06. Q1-Q5 同期分組
# =============================================================================

def build_quintile_signal_sets(detail):
    required = [
        "訊號日期",
        "股票代號",
        "股票名稱",
        "訊號當日市場",
        "range_pos20",
        "橫斷面百分位排名",
    ]

    require_columns(
        detail,
        required,
        "官方普通股特徵明細",
    )

    work = detail.copy()

    work["股票代號"] = work["股票代號"].astype(str)

    pct = pd.to_numeric(
        work["橫斷面百分位排名"],
        errors="coerce",
    )

    if pct.isna().any():
        raise RuntimeError(
            "官方普通股特徵排名存在空值，不能進行五分位分組。"
        )

    if ((pct < 0) | (pct > 1)).any():
        raise RuntimeError(
            "橫斷面百分位排名超出 [0,1] 範圍。"
        )

    work["橫斷面百分位排名"] = pct

    masks = {
        "Q1_最低五分位": pct.le(0.20),
        "Q2_第二五分位": pct.gt(0.20) & pct.le(0.40),
        "Q3_中間五分位": pct.gt(0.40) & pct.le(0.60),
        "Q4_第四五分位": pct.gt(0.60) & pct.lt(0.80),
        "Q5_最高五分位": pct.ge(0.80),
    }

    membership = pd.concat(
        [x.astype(int).rename(k) for k, x in masks.items()],
        axis=1,
    )

    if not membership.sum(axis=1).eq(1).all():
        raise RuntimeError(
            "Q1-Q5 分組沒有完整且互斥地涵蓋全部特徵列。"
        )

    # Q5 定義必須與 v1.2.8 完全相同。
    q5_old = work["橫斷面百分位排名"].ge(0.80)

    if "是否Q5訊號" in work.columns:
        original_q5 = work["是否Q5訊號"].fillna(False).astype(bool)

        if not original_q5.equals(q5_old):
            raise RuntimeError(
                "重建 Q5 與 v1.2.8 原始 Q5 判斷不一致。"
            )

    signal_sets = []
    daily_rows = []

    for label, mask in masks.items():
        group_detail = work.copy()

        group_detail["是否Q5訊號"] = mask.to_numpy(dtype=bool)

        selected = group_detail.loc[mask].copy()

        if selected.duplicated(
            ["訊號日期", "股票代號"]
        ).any():
            raise RuntimeError(
                f"{label} 日期－股票代號重複。"
            )

        signal_sets.append(
            (group_detail, label)
        )

        for date, count in selected.groupby(
            "訊號日期",
            sort=True,
        ).size().items():
            daily_rows.append({
                "分位組": label,
                "訊號日期": str(pd.Timestamp(date).date()),
                "訊號筆數": int(count),
            })

    daily_counts = pd.DataFrame(daily_rows)

    total_selected = sum(
        int(x[0]["是否Q5訊號"].sum())
        for x in signal_sets
    )

    if total_selected != len(work):
        raise RuntimeError(
            "Q1-Q5 分組訊號總數與原始特徵明細筆數不一致。"
        )

    # Q5 與原始 v1.2.8 名單的股票鍵核對。
    q5_detail = next(
        x[0] for x in signal_sets
        if x[1] == "Q5_最高五分位"
    )

    q5_keys = set(
        zip(
            q5_detail.loc[q5_detail["是否Q5訊號"], "訊號日期"].astype(str),
            q5_detail.loc[q5_detail["是否Q5訊號"], "股票代號"].astype(str),
        )
    )

    original_keys = set(
        zip(
            work.loc[q5_old, "訊號日期"].astype(str),
            work.loc[q5_old, "股票代號"].astype(str),
        )
    )

    if q5_keys != original_keys:
        raise RuntimeError(
            "Q5 分組股票鍵與原始 Q5 股票鍵不一致。"
        )

    log(
        f"[PASS] Q1-Q5 分組完成："
        f"{len(work):,} 筆特徵；"
        f"{len(signal_sets)} 個互斥分組。"
    )

    return signal_sets, daily_counts, masks


# =============================================================================
# 07. Q1 與 Q5 差異
# =============================================================================

def build_q5_minus_q1(horizon_summary):
    required = [
        "訊號口徑",
        "H",
        "訊號總筆數",
        "已成熟且完成報酬筆數",
        "基準成本平均淨報酬",
        "基準成本中位數淨報酬",
        "基準成本勝率",
        "壓力成本平均淨報酬",
        "壓力成本中位數淨報酬",
        "壓力成本勝率",
    ]

    require_columns(
        horizon_summary,
        required,
        "Q1-Q5 整體績效摘要",
    )

    rows = []

    for h in HORIZONS:
        q1 = horizon_summary.loc[
            horizon_summary["訊號口徑"].eq("Q1_最低五分位")
            & horizon_summary["H"].eq(h)
        ]

        q5 = horizon_summary.loc[
            horizon_summary["訊號口徑"].eq("Q5_最高五分位")
            & horizon_summary["H"].eq(h)
        ]

        if len(q1) != 1 or len(q5) != 1:
            raise RuntimeError(
                f"H={h} 的 Q1/Q5 摘要列不唯一。"
            )

        a = q1.iloc[0]
        b = q5.iloc[0]

        row = {
            "H": h,
            "Q1訊號筆數": int(a["訊號總筆數"]),
            "Q5訊號筆數": int(b["訊號總筆數"]),
            "Q1成熟筆數": int(a["已成熟且完成報酬筆數"]),
            "Q5成熟筆數": int(b["已成熟且完成報酬筆數"]),
        }

        compare_fields = [
            "基準成本平均淨報酬",
            "基準成本中位數淨報酬",
            "基準成本勝率",
            "壓力成本平均淨報酬",
            "壓力成本中位數淨報酬",
            "壓力成本勝率",
        ]

        for field in compare_fields:
            av = pd.to_numeric(
                pd.Series([a[field]]),
                errors="coerce",
            ).iloc[0]

            bv = pd.to_numeric(
                pd.Series([b[field]]),
                errors="coerce",
            ).iloc[0]

            row[f"Q1_{field}"] = av
            row[f"Q5_{field}"] = bv
            row[f"Q5減Q1_{field}"] = (
                bv - av
                if pd.notna(av) and pd.notna(bv)
                else np.nan
            )

        rows.append(row)

    return pd.DataFrame(rows)


# =============================================================================
# 08. 歷史行情資料來源盤點
# =============================================================================

def history_scope_conclusion(
    history_inventory,
    classification_inventory,
):
    valid_ohlc = pd.DataFrame()

    if not history_inventory.empty:
        valid_ohlc = history_inventory.loc[
            history_inventory["欄位辨識狀態"].eq("CSV_HEADER_OK")
            & history_inventory["日期欄"].astype(str).ne("")
            & history_inventory["代號欄"].astype(str).ne("")
            & history_inventory["開盤欄"].astype(str).ne("")
            & history_inventory["最高欄"].astype(str).ne("")
            & history_inventory["最低欄"].astype(str).ne("")
            & history_inventory["收盤欄"].astype(str).ne("")
        ]

    historical_cfi = pd.DataFrame()

    if not classification_inventory.empty:
        historical_cfi = classification_inventory.loc[
            classification_inventory["檔名辨識"].notna()
        ]

    return {
        "疑似歷史OHLC檔案數": int(len(history_inventory)),
        "具備日期代號OHLC欄位的候選檔案數": int(len(valid_ohlc)),
        "疑似歷史證券分類檔案數": int(len(classification_inventory)),
        "歷史逐日官方分類是否已證明": False,
        "長期回測是否執行": False,
        "限制": (
            "即使找到多年 OHLC 主檔，仍須核對該主檔是否為完整官方行情，"
            "以及歷史每日 ISIN/CFI 普通股分類是否完整。"
            "不得拿 2026-11-10 的普通股清單回套歷史年度。"
        ),
    }


# =============================================================================
# 09. Python 程式封存
# =============================================================================

def archive_current_program(out_dir):
    target = (
        out_dir /
        "SSO_v1.3.0_同期Q1Q5分組對照暨長期資料來源盤點.py"
    )

    try:
        shell = get_ipython()
        history = shell.history_manager.input_hist_raw

        for source in reversed(history):

            if not isinstance(source, str):
                continue

            if (
                "SSO v1.3.0｜同期 Q1-Q5 分組對照暨長期資料來源盤點"
                in source
                and "def run_main_v130():" in source
                and "upload_github_v130" in source
                and len(source) > 20000
            ):
                target.write_text(
                    source,
                    encoding="utf-8",
                )

                log("[PASS] 已封存 v1.3.0 完整程式。")
                return target

    except Exception as exc:
        log(f"[WARN] Colab 程式歷史讀取失敗：{exc}")

    raise RuntimeError(
        "無法封存目前完整程式，停止正式輸出。"
    )


# =============================================================================
# 10. GitHub API 上傳
# =============================================================================

def get_github_token():
    token = (
        os.environ.get("GITHUB_TOKEN", "").strip()
        or os.environ.get("GH_TOKEN", "").strip()
    )

    if not token:
        try:
            from google.colab import userdata
            token = (
                userdata.get("GITHUB_TOKEN") or ""
            ).strip()
        except Exception:
            token = ""

    if not token:
        raise RuntimeError(
            "找不到 GITHUB_TOKEN。成果尚未上傳 GitHub。"
        )

    return token


def upload_github_v130(out_dir, zip_path, run_id):
    token = get_github_token()

    api_base = (
        f"https://api.github.com/repos/"
        f"{REPO_OWNER}/{REPO_NAME}"
    )

    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }

    def api(method, endpoint, payload=None):
        response = requests.request(
            method,
            api_base + "/" + endpoint.lstrip("/"),
            headers=headers,
            json=payload,
            timeout=180,
        )

        if not response.ok:
            raise RuntimeError(
                f"GitHub API {method} {endpoint} 失敗："
                f"{response.status_code}\n"
                f"{response.text[:2000]}"
            )

        if response.content:
            return response.json()

        return {}

    log("[GITHUB 1/7] 取得 main SHA。")

    main_before_obj = api(
        "GET",
        "git/ref/heads/main",
    )

    main_before = main_before_obj["object"]["sha"]

    main_commit_obj = api(
        "GET",
        f"git/commits/{main_before}",
    )

    base_tree_sha = main_commit_obj["tree"]["sha"]

    log(f"[PASS] main before：{main_before}")

    branch = (
        "audit/sso-v1.3.0-q1-q5-"
        f"{run_id}-{uuid.uuid4().hex[:6]}"
    )

    artifact_dir = (
        "audit/sso-v1.3.0-q1-q5/"
        f"{run_id}"
    )

    files = [
        p for p in sorted(out_dir.iterdir())
        if p.is_file()
    ] + [zip_path]

    oversized = [
        (p.name, p.stat().st_size)
        for p in files
        if p.stat().st_size >= 100 * 1024 * 1024
    ]

    if oversized:
        raise RuntimeError(
            "存在超過 GitHub blob 100MB 上限的檔案："
            + repr(oversized)
        )

    log(
        f"[GITHUB 2/7] 準備上傳 {len(files)} 個檔案。"
    )

    tree_elements = []
    expected_blobs = {}

    for i, path in enumerate(files, start=1):

        data = path.read_bytes()

        blob = api(
            "POST",
            "git/blobs",
            {
                "content": base64.b64encode(data).decode("ascii"),
                "encoding": "base64",
            },
        )

        blob_sha = blob["sha"]

        remote_path = (
            f"{artifact_dir}/{path.name}"
        )

        tree_elements.append({
            "path": remote_path,
            "mode": "100644",
            "type": "blob",
            "sha": blob_sha,
        })

        expected_blobs[remote_path] = {
            "sha": blob_sha,
            "bytes": len(data),
            "local_sha256": hashlib.sha256(data).hexdigest(),
        }

        log(
            f"[GITHUB ADD {i}/{len(files)}] "
            f"{path.name} | {len(data):,} bytes"
        )

    log("[GITHUB 3/7] 建立獨立 Git tree。")

    tree = api(
        "POST",
        "git/trees",
        {
            "base_tree": base_tree_sha,
            "tree": tree_elements,
        },
    )

    log("[GITHUB 4/7] 建立以 main 為 parent 的 audit commit。")

    commit = api(
        "POST",
        "git/commits",
        {
            "message": (
                "Add SSO v1.3.0 Q1-Q5 cohort comparison "
                f"and historical source inventory ({run_id})"
            ),
            "tree": tree["sha"],
            "parents": [main_before],
        },
    )

    commit_sha = commit["sha"]

    log("[GITHUB 5/7] 建立獨立 audit branch。")

    api(
        "POST",
        "git/refs",
        {
            "ref": f"refs/heads/{branch}",
            "sha": commit_sha,
        },
    )

    branch_after = api(
        "GET",
        f"git/ref/heads/{branch}",
    )["object"]["sha"]

    if branch_after != commit_sha:
        raise RuntimeError(
            "GitHub audit branch SHA read-back 不一致。"
        )

    log("[GITHUB 6/7] 逐檔 Git blob read-back。")

    for remote_path, expected in expected_blobs.items():
        blob = api(
            "GET",
            f"git/blobs/{expected['sha']}",
        )

        remote_bytes = base64.b64decode(
            blob["content"]
        )

        actual_blob_sha = git_blob_sha1(remote_bytes)

        if actual_blob_sha != expected["sha"]:
            raise RuntimeError(
                f"GitHub blob SHA 不符：{remote_path}"
            )

        if len(remote_bytes) != expected["bytes"]:
            raise RuntimeError(
                f"GitHub blob 大小不符：{remote_path}"
            )

    log(
        f"[PASS] 已逐檔核對 {len(expected_blobs)} 個 Git blob。"
    )

    log("[GITHUB 7/7] 確認 main SHA 前後一致。")

    main_after = api(
        "GET",
        "git/ref/heads/main",
    )["object"]["sha"]

    if main_after != main_before:
        raise RuntimeError(
            "GitHub main SHA 前後不同："
            f"{main_before} -> {main_after}"
        )

    return {
        "branch": branch,
        "commit": commit_sha,
        "main_before": main_before,
        "main_after": main_after,
        "uploaded_files": len(files),
        "folder_url": (
            f"https://github.com/{REPO_OWNER}/{REPO_NAME}"
            f"/tree/{branch}/{artifact_dir}"
        ),
        "commit_url": (
            f"https://github.com/{REPO_OWNER}/{REPO_NAME}"
            f"/commit/{commit_sha}"
        ),
    }


# =============================================================================
# 11. 主流程
# =============================================================================

def run_main_v130():

    print("=" * 100)
    log(VERSION)
    log("同期比較：官方普通股每日 Q1/Q2/Q3/Q4/Q5。")
    log("週期：H=1、3、5、10。")
    log("Q5 定義沿用 v1.2.8：rank(method='average', pct=True) >= 0.80。")
    log("成本公式沿用 v1.2.2，不重新訓練。")
    log("歷史 12 檔候選名單凍結，不依本版結果調整。")
    print("=" * 100)

    ensure_drive()

    run_id = run_stamp()

    # ---------------------------------------------------------
    # A. 核對 v1.2.9 封存成果
    # ---------------------------------------------------------

    log("[01] 核對 v1.2.9 ZIP 與 12 檔候選來源。")

    archive_check = verify_v129_archive()

    candidates, original_cross = derive_frozen_candidates()

    # ---------------------------------------------------------
    # B. 載入固定程式模組
    # ---------------------------------------------------------

    log("[02] 載入 v1.2.9、v1.2.8、v1.2.7、v1.2.6。")

    v129 = load_pinned_v129()

    v128 = v129.load_v128_module()

    v127 = v128.load_v127_module()

    # v1.2.8 函式引用全域 v127。
    v128.v127 = v127

    v126, dependency_path = v127.load_v126_module()

    v126.self_test()

    original_summary = (
        v127.load_v126_reference_summary()
    )

    log("[PASS] 固定計算模組載入完成。")

    # ---------------------------------------------------------
    # C. 讀取既有官方行情及增量
    # ---------------------------------------------------------

    log("[03] 讀取 v1.2.7 固定行情來源。")

    sources = v127.load_and_validate_sources(v126)

    log("[04] 讀取 v1.5.1 官方行情增量。")

    new_rows, overlay_info, overlay_report = (
        v128.load_overlay(v126)
    )

    sources = v128.append_new_overlay(
        sources,
        new_rows,
    )

    log("[PASS] 官方行情接續完成。")

    # ---------------------------------------------------------
    # D. 官方普通股清單
    # ---------------------------------------------------------

    log("[05] 讀取 2026-11-10 官方 ISIN 普通股分類。")

    (
        ordinary_frame,
        ordinary_codes,
        ordinary_name_map,
        ordinary_market_map,
        ordinary_info,
    ) = v128.load_official_ordinary()

    if len(ordinary_frame) != 1951:
        raise RuntimeError(
            "官方普通股分類數量不符。"
        )

    # ---------------------------------------------------------
    # E. 重建 v1.2.6 原始特徵
    # ---------------------------------------------------------

    log("[06] 沿用原始 v1.2.6 函式重建特徵。")

    rebuilt = v127.rebuild_v126_feature_rows(
        v126,
        sources,
    )

    legacy_detail, legacy_daily_summary = (
        v127.build_q5_detail(
            rebuilt["extra"]["feature_rows"],
            rebuilt["universe"],
        )
    )

    qa = v127.verify_against_v126(
        original_summary,
        rebuilt["daily_q5"],
        legacy_detail,
        legacy_daily_summary,
    )

    qa_failures = int(
        qa["核對結果"].ne("PASS").sum()
    )

    if len(qa) != 126 or qa_failures != 0:
        raise RuntimeError(
            "v1.2.6 原始 126 項核對未完整通過。"
        )

    legacy_q5 = legacy_detail.loc[
        legacy_detail["是否Q5訊號"]
    ]

    legacy_first_nine = legacy_q5.loc[
        legacy_q5["訊號日期"].le("2026-11-06")
    ]

    if len(legacy_first_nine) != 3446:
        raise RuntimeError(
            "v1.2.7 原九日 Q5 訊號數不符。"
        )

    log("[PASS] v1.2.6 126 項核對全部通過。")
    log("[PASS] v1.2.7 原九日 Q5 = 3,446。")

    # ---------------------------------------------------------
    # F. 官方普通股每日全橫斷面特徵
    # ---------------------------------------------------------

    log("[07] 重建官方普通股每日全體特徵。")

    official_q5_result = (
        v128.build_official_ordinary_q5(
            rebuilt,
            ordinary_codes,
            ordinary_name_map,
            ordinary_market_map,
            v127,
        )
    )

    official_detail = official_q5_result["detail"]

    official_q5 = official_q5_result["q5"]

    if len(official_q5) != 4148:
        raise RuntimeError(
            "v1.2.8 官方普通股 Q5 總筆數不符。"
            f"實際：{len(official_q5):,}"
        )

    if official_q5_result["latest_date"] != "2026-11-10":
        raise RuntimeError(
            "官方普通股最新 Q5 日期不是 2026-11-10。"
        )

    # ---------------------------------------------------------
    # G. Q1-Q5 分組
    # ---------------------------------------------------------

    log("[08] 建立每日 Q1-Q5 五分位訊號。")

    signal_sets, daily_group_counts, masks = (
        build_quintile_signal_sets(
            official_detail
        )
    )

    # ---------------------------------------------------------
    # H. 共用價格索引及多週期配對
    # ---------------------------------------------------------

    log("[09] Q1-Q5 共用價格索引與 H1/H3/H5/H10 配對。")

    schedule = v129.build_multi_horizon_returns(
        signal_sets=signal_sets,
        official=sources["official"],
        events=rebuilt["extra"]["events"],
        v126=v126,
        v127=v127,
        horizons=HORIZONS,
    )

    matured = schedule.loc[
        schedule["觀察狀態"].eq("MATURED_RETURN_COMPUTED")
    ].copy()

    if matured.empty:
        raise RuntimeError(
            "Q1-Q5 多週期沒有任何成熟淨報酬。"
        )

    # ---------------------------------------------------------
    # I. 整體及每日績效摘要
    # ---------------------------------------------------------

    log("[10] 計算 Q1-Q5 整體及每日績效。")

    horizon_summary = (
        v129.summarize_by_horizon(schedule)
    )

    daily_horizon_summary = (
        v129.summarize_by_signal_date(schedule)
    )

    q5_minus_q1 = build_q5_minus_q1(
        horizon_summary
    )

    # 五分位每日訊號量
    daily_group_counts = daily_group_counts.sort_values(
        ["訊號日期", "分位組"],
        kind="mergesort",
    ).reset_index(drop=True)

    # ---------------------------------------------------------
    # J. 歷史資料源盤點
    # ---------------------------------------------------------

    log("[11] 盤點更早歷史 OHLC 與逐日證券分類來源。")

    history_inventory, classification_inventory = (
        scan_history_files(
            ROOT
        )
    )

    history_conclusion = history_scope_conclusion(
        history_inventory,
        classification_inventory,
    )

    # ---------------------------------------------------------
    # K. 來源核驗及研究結論
    # ---------------------------------------------------------

    source_latest = pd.Timestamp(
        sources["official"]["date"].max()
    ).normalize()

    feature_dates = sorted(
        pd.to_datetime(
            official_detail["訊號日期"],
            errors="coerce",
        ).dropna().dt.strftime("%Y-%m-%d").unique().tolist()
    )

    report = {
        "版本": VERSION,
        "執行時間台北": datetime.now(TAIPEI).isoformat(),
        "v1.2.9固定程式GitBlobSHA1": V129_SCRIPT_GIT_BLOB_SHA1,
        "v1.2.9原始ZIP實際SHA256": archive_check["zip_sha256"],
        "v1.2.9原始ZIP成員數": archive_check["archive_members"],
        "v1.2.9候選名單數": len(candidates),
        "v1.2.9凍結候選代號": candidates["股票代號"].astype(str).tolist(),
        "官方普通股市場代號鍵數": int(len(ordinary_frame)),
        "v1.2.6原始核對項目": int(len(qa)),
        "v1.2.6原始核對失敗": qa_failures,
        "v1.2.7原九日Q5訊號數": 3446,
        "v1.2.8官方普通股Q5訊號數": 4148,
        "官方行情最新日期": str(source_latest.date()),
        "同期特徵交易日": feature_dates,
        "同期分位組": ["Q1", "Q2", "Q3", "Q4", "Q5"],
        "研究週期": list(HORIZONS),
        "Q1Q5完整排程筆數": int(len(schedule)),
        "Q1Q5成熟報酬筆數": int(len(matured)),
        "各週期績效摘要": horizon_summary.to_dict("records"),
        "Q5減Q1差異": q5_minus_q1.to_dict("records"),
        "長期來源盤點": history_conclusion,
        "長期候選回溯是否執行": False,
        "長期驗證限制": (
            "目前僅確認 2026-11-10 的官方普通股 ISIN/CFI 清單。"
            "不得將此單日分類回套至多年歷史。"
            "必須另確認歷史每日官方普通股分類與歷史 OHLC 的"
            "來源、完整性及公司行動處理，才可進行長期 Q5 回溯。"
        ),
        "明確未執行": [
            "未修改 Q5 門檻",
            "未重新訓練模型",
            "未修改 v1.2.7 凍結 Q5",
            "未以 2026 普通股名單回套過去年度",
            "未將回溯結果稱為樣本外績效",
            "未呼叫 TWSE 或 TPEx API",
            "未修改 Drive 原始資料",
            "未修改 GitHub main",
        ],
    }

    # ---------------------------------------------------------
    # L. 建立新的唯一輸出目錄
    # ---------------------------------------------------------

    out_dir = (
        ROOT /
        f"看股最速適_v1_3_0_Q1Q5同期對照與歷史來源盤點_{run_id}"
    )

    out_dir.mkdir(
        parents=True,
        exist_ok=False,
    )

    log(f"[OUTPUT] {out_dir}")

    csv_write(
        horizon_summary,
        out_dir / "01_Q1至Q5四週期整體績效摘要.csv",
    )

    csv_write(
        daily_horizon_summary,
        out_dir / "02_Q1至Q5逐訊號日四週期績效.csv",
    )

    csv_write(
        q5_minus_q1,
        out_dir / "03_Q5減Q1同期績效差異.csv",
    )

    csv_write(
        daily_group_counts,
        out_dir / "04_Q1至Q5每日訊號數.csv",
    )

    # 完整逐筆排程壓縮保存，減少單檔體積。
    schedule.to_csv(
        out_dir / "05_Q1至Q5四週期完整逐筆排程.csv.gz",
        index=False,
        encoding="utf-8-sig",
        compression="gzip",
        na_rep="",
    )

    csv_write(
        candidates,
        out_dir / "06_v129凍結12檔候選.csv",
    )

    csv_write(
        history_inventory,
        out_dir / "07_歷史OHLC來源盤點.csv",
    )

    csv_write(
        classification_inventory,
        out_dir / "08_歷史證券分類來源盤點.csv",
    )

    csv_write(
        pd.DataFrame(archive_check["records"]),
        out_dir / "09_v129來源ZIP逐檔核驗.csv",
    )

    csv_write(
        qa,
        out_dir / "10_v126原始126項相容性核對.csv",
    )

    json_write(
        report,
        out_dir / "11_v130同期Q1Q5及歷史來源盤點結論.json",
    )

    # ---------------------------------------------------------
    # M. 執行摘要
    # ---------------------------------------------------------

    summary_lines = [
        "=" * 100,
        "SSO v1.3.0｜同期 Q1-Q5 分組對照暨長期資料來源盤點",
        "=" * 100,
        f"執行時間：{report['執行時間台北']}",
        f"官方普通股：{len(ordinary_frame):,} 組市場－代號鍵",
        f"官方行情最新日期：{source_latest.date()}",
        f"同期交易日數：{len(feature_dates):,}",
        f"每日全體特徵：{len(official_detail):,} 筆",
        f"Q1-Q5 完整排程：{len(schedule):,} 筆",
        f"已成熟且完成淨報酬：{len(matured):,} 筆",
        "",
        "一、Q1-Q5 四週期整體績效",
    ]

    for row in horizon_summary.to_dict("records"):
        summary_lines.append(
            f"{row['訊號口徑']} | H={row['H']} | "
            f"訊號={row['訊號總筆數']:,} | "
            f"成熟={row['已成熟且完成報酬筆數']:,} | "
            f"基準平均={row['基準成本平均淨報酬']:.4%} | "
            f"基準勝率={row['基準成本勝率']:.2%} | "
            f"壓力平均={row['壓力成本平均淨報酬']:.4%} | "
            f"壓力勝率={row['壓力成本勝率']:.2%}"
        )

    summary_lines.extend([
        "",
        "二、Q5 與 Q1 同期差異",
    ])

    for row in q5_minus_q1.to_dict("records"):
        summary_lines.append(
            f"H={row['H']} | "
            f"Q5-Q1 基準平均差={row['Q5減Q1_基準成本平均淨報酬']:.4%} | "
            f"Q5-Q1 壓力平均差={row['Q5減Q1_壓力成本平均淨報酬']:.4%} | "
            f"Q5-Q1 基準勝率差={row['Q5減Q1_基準成本勝率']:.2%}"
        )

    summary_lines.extend([
        "",
        "三、凍結候選名單",
        f"v1.2.9 兩種 Q5 口徑共同通過 H1/H3/H5 條件：{len(candidates)} 檔",
        "候選名單不依本版 Q1-Q5 對照結果調整。",
        "",
        "四、長期資料來源盤點",
        f"疑似歷史 OHLC 檔案：{history_conclusion['疑似歷史OHLC檔案數']}",
        f"具日期/代號/OHLC 欄位候選：{history_conclusion['具備日期代號OHLC欄位的候選檔案數']}",
        f"疑似歷史證券分類檔案：{history_conclusion['疑似歷史證券分類檔案數']}",
        "本版沒有將單日官方 ISIN/CFI 清單回套歷史年度。",
        "歷史候選回溯尚未執行，避免產生錯誤的官方普通股樣本。",
        "",
        "五、研究限制",
        "五分位分組以本次可核驗的官方普通股名單及同期行情為基礎。",
        "不同 H 的持有期報酬可能重疊，不代表獨立觀測。",
        "同一訊號日的股票受到共同市場環境影響。",
        "v1.2.9 凍結的 12 檔候選來自近期資料，回頭檢查歷史不能稱為真正樣本外。",
        "歷史長期研究需核對逐日官方證券分類、OHLC來源及價格區段。",
        "",
        "六、安全",
        "Drive 原始資料唯讀。",
        "不呼叫官方 API。",
        "不修改 v1.2.9 舊成果。",
        "GitHub 僅新增獨立 audit branch。",
        "GitHub main SHA 前後必須一致。",
        "=" * 100,
    ])

    (out_dir / "12_執行摘要.txt").write_text(
        "\n".join(summary_lines) + "\n",
        encoding="utf-8-sig",
    )

    # ---------------------------------------------------------
    # N. 封存完整 Python 程式
    # ---------------------------------------------------------

    script_path = archive_current_program(out_dir)

    # ---------------------------------------------------------
    # O. README
    # ---------------------------------------------------------

    readme_lines = [
        "# SSO v1.3.0｜同期 Q1-Q5 分組對照暨長期資料來源盤點",
        "",
        f"- 執行時間：`{report['執行時間台北']}`",
        f"- 官方普通股市場－代號鍵：`{len(ordinary_frame):,}`",
        f"- 同期特徵交易日：`{len(feature_dates):,}`",
        f"- Q1-Q5 完整配對：`{len(schedule):,}`",
        f"- 成熟淨報酬：`{len(matured):,}`",
        f"- 凍結候選：`{len(candidates)}`",
        f"- 歷史 OHLC 候選：`{history_conclusion['疑似歷史OHLC檔案數']}`",
        f"- 歷史證券分類候選：`{history_conclusion['疑似歷史證券分類檔案數']}`",
        "",
        "## 研究內容",
        "",
        "Q1-Q5 使用每日官方普通股 range_pos20 百分位分組；",
        "沿用 v1.2.2 成本公式及 v1.2.9 的 H1/H3/H5/H10 配對函式。",
        "",
        "## 凍結候選",
        "",
        "12 檔候選名單承接 v1.2.9，不依本版結果更改。",
        "本版歷史來源盤點不等於已完成長期回測。",
        "",
        "## 重要限制",
        "",
        "不可將 2026-11-10 的 ISIN/CFI 名單回套歷史年度。",
        "不可將以近期資料選出的候選回頭驗證稱為真正樣本外。",
        "",
        "## 安全",
        "",
        "- Google Drive 原始來源唯讀。",
        "- 不呼叫 TWSE / TPEx API。",
        "- GitHub 僅新增獨立 audit branch。",
        "- 不修改 GitHub main。",
    ]

    (out_dir / "README_稽核說明.md").write_text(
        "\n".join(readme_lines) + "\n",
        encoding="utf-8",
    )

    # ---------------------------------------------------------
    # P. ZIP
    # ---------------------------------------------------------

    log("[ZIP] 建立 v1.3.0 ZIP。")

    zip_path = out_dir.with_suffix(".zip")

    with zipfile.ZipFile(
        zip_path,
        "w",
        compression=zipfile.ZIP_DEFLATED,
        compresslevel=6,
    ) as zf:
        for path in sorted(out_dir.iterdir()):
            if path.is_file():
                zf.write(
                    path,
                    arcname=path.name,
                )

    with zipfile.ZipFile(zip_path, "r") as zf:
        bad = zf.testzip()

        if bad:
            raise RuntimeError(
                f"v1.3.0 ZIP CRC 失敗：{bad}"
            )

        zip_members = len([
            x for x in zf.namelist()
            if not x.endswith("/")
        ])

    zip_sha = sha256_file(zip_path)

    log(
        f"[PASS] ZIP CRC 通過；"
        f"成員={zip_members}；"
        f"SHA256={zip_sha}"
    )

    # ---------------------------------------------------------
    # Q. GitHub 獨立 audit branch
    # ---------------------------------------------------------

    log("[GITHUB] 上傳 v1.3.0。")

    github_result = upload_github_v130(
        out_dir=out_dir,
        zip_path=zip_path,
        run_id=run_id,
    )

    # ---------------------------------------------------------
    # R. 最終結果
    # ---------------------------------------------------------

    result = {
        "output_dir": str(out_dir),
        "zip_path": str(zip_path),
        "zip_sha256": zip_sha,
        "zip_members": zip_members,
        "report": report,
        "github": github_result,
        "horizon_summary": horizon_summary,
        "q5_minus_q1": q5_minus_q1,
        "candidates": candidates,
        "history_inventory": history_inventory,
        "classification_inventory": classification_inventory,
    }

    print("=" * 100)
    print("[PASS] SSO v1.3.0 完成。")
    print("=" * 100)

    print(f"輸出資料夾：{out_dir}")
    print(f"ZIP：{zip_path}")
    print(f"ZIP SHA256：{zip_sha}")
    print(f"GitHub branch：{github_result['branch']}")
    print(f"Commit：{github_result['commit']}")
    print(f"GitHub audit folder：{github_result['folder_url']}")
    print(f"GitHub commit：{github_result['commit_url']}")
    print(f"main before：{github_result['main_before']}")
    print(f"main after：{github_result['main_after']}")

    print("\nQ1-Q5 四週期績效摘要：")
    print(
        horizon_summary.to_string(
            index=False
        )
    )

    print("\nQ5-Q1 差異：")
    print(
        q5_minus_q1.to_string(
            index=False
        )
    )

    return result


# =============================================================================
# 12. 執行
# =============================================================================

RESULT = run_main_v130()