
# -*- coding: utf-8 -*-
# =============================================================================
# SSO v1.3.0｜H10 正確口徑修復及中斷成果接續
#
# v1.2.8 H10：
#   原凍結 Q5、官方普通股過濾：375 筆
#   官方普通股重新排名 Q5：372 筆
#   兩者合計：747 筆
#
# v1.3.0 Q1-Q5：
#   Q5 是官方普通股重新排名 Q5
#   因此只應與 372 筆比較，不應與 747 筆比較。
#
# 本程式：
# 1. 從最近一次 traceback 取回 run_v130() 的區域變數。
# 2. 不重讀行情、不重建特徵、不重做 Q1-Q5。
# 3. 由 v1.2.8 原 ZIP 核實 375 + 372 = 747。
# 4. 逐筆核對官方 Q5 H10 的 372 筆。
# 5. 接續歷史來源盤點、報表、ZIP、GitHub audit branch。
# 6. 封存修正後完整 v1.3.0 程式。
#
# Drive 原始來源唯讀；GitHub main 不修改。
# =============================================================================

import sys
import re
import os
import json
import zipfile
import hashlib
import traceback
from pathlib import Path
from datetime import datetime

import numpy as np
import pandas as pd

print("=" * 100)
print("SSO v1.3.0｜H10 正確口徑修復及中斷成果接續")
print("=" * 100)


# =============================================================================
# 01. 從最近一次錯誤 traceback 恢復 run_v130() 狀態
# =============================================================================

def _sso130_collect_exception_frames():

    found = []

    def add_exception(exc):
        if not isinstance(exc, BaseException):
            return

        tb = exc.__traceback__

        while tb is not None:
            found.append(tb.tb_frame)
            tb = tb.tb_next

    def add_traceback(tb):
        while tb is not None:
            found.append(tb.tb_frame)
            tb = tb.tb_next

    # Python 互動環境保留的最後錯誤。
    for name in (
        "last_value",
        "last_exc",
    ):
        try:
            add_exception(getattr(sys, name, None))
        except Exception:
            pass

    try:
        add_traceback(
            getattr(sys, "last_traceback", None)
        )
    except Exception:
        pass

    # IPython / Google Colab 執行紀錄。
    try:
        shell = get_ipython()

        result = getattr(
            shell,
            "last_execution_result",
            None,
        )

        if result is not None:
            add_exception(
                getattr(result, "error_in_exec", None)
            )
            add_exception(
                getattr(result, "error_before_exec", None)
            )

        add_exception(
            getattr(shell, "_last_value", None)
        )

    except Exception:
        pass

    # 保留 run_v130() 的 traceback frame。
    unique = {}

    for frame in found:
        if frame.f_code.co_name == "run_v130":
            unique[id(frame)] = frame

    if not unique:
        raise RuntimeError(
            "找不到最近一次 run_v130() 的 traceback frame。"
            "\n請確認仍在原本發生 H10 錯誤的 Colab runtime。"
            "\n不要重新執行原始 v1.3.0 主程式。"
        )

    return list(unique.values())[-1]


_failed_frame = _sso130_collect_exception_frames()

L = _failed_frame.f_locals.copy()
G = _failed_frame.f_globals

_REQUIRED = [
    "run_id",
    "out_dir",
    "zip_path",
    "v129",
    "v128",
    "v127",
    "v126",
    "sources",
    "rebuilt",
    "ordinary_frame",
    "ordinary_codes",
    "schedule",
    "q5_compat_schedule",
    "matured",
    "quintile_detail",
    "group_counts",
    "daily_group_counts",
    "frozen_candidates",
    "frozen_codes",
    "qa",
    "legacy_first_nine_n",
    "official_detail",
    "official_q5",
    "official_latest_date",
    "overlay_info",
    "new_rows",
    "ordinary_info",
]

missing = [
    name for name in _REQUIRED
    if name not in L
]

if missing:
    raise RuntimeError(
        "traceback 中缺少以下已完成分析變數："
        + repr(missing)
        + "\n停止執行，避免從不完整狀態產生成果。"
    )

# 恢復主要變數。
for name in _REQUIRED:
    globals()[name] = L[name]

run_id_original = str(L["run_id"])

out_dir_original = Path(L["out_dir"])
zip_path_original = Path(L["zip_path"])

v129 = L["v129"]
v128 = L["v128"]
v127 = L["v127"]
v126 = L["v126"]

sources = L["sources"]
rebuilt = L["rebuilt"]

ordinary_frame = L["ordinary_frame"]
ordinary_codes = L["ordinary_codes"]

schedule = L["schedule"]
matured = L["matured"]

quintile_detail = L["quintile_detail"]
group_counts = L["group_counts"]
daily_group_counts = L["daily_group_counts"]

frozen_candidates = L["frozen_candidates"]
frozen_codes = L["frozen_codes"]

qa = L["qa"]
legacy_first_nine_n = L["legacy_first_nine_n"]

official_detail = L["official_detail"]
official_q5 = L["official_q5"]
official_latest_date = L["official_latest_date"]

overlay_info = L["overlay_info"]
new_rows = L["new_rows"]
ordinary_info = L["ordinary_info"]

ROOT = G["ROOT"]

print("[PASS] 已從 traceback 恢復原 run_v130()。")
print("[PASS] 前面的行情、特徵及 Q1-Q5 計算均保留。")
print("[INFO] 原執行識別碼：", run_id_original)
print("[INFO] Q1-Q5 明細：", f"{len(quintile_detail):,}")
print("[INFO] 多週期排程：", f"{len(schedule):,}")
print("[INFO] 已成熟淨報酬：", f"{len(matured):,}")


# =============================================================================
# 02. 選擇不覆寫既有資料的輸出目錄
# =============================================================================

out_dir = out_dir_original

if not out_dir.exists():
    raise FileNotFoundError(
        "原執行輸出資料夾不存在：" + str(out_dir)
    )

program_filename = (
    "SSO_v1.3.0_同期Q1-Q5分組對照暨長期資料來源盤點.py"
)

existing = [
    p.name
    for p in out_dir.iterdir()
    if p.is_file()
]

# 如果先前修復儲存格已留下程式原文，可保留該檔案並校正；
# 如果有其他成果，另建唯一資料夾，不覆寫。
unexpected = [
    name for name in existing
    if name != program_filename
]

if unexpected:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    out_dir = (
        ROOT
        / (
            out_dir_original.name
            + "_H10口徑修正接續_"
            + stamp
        )
    )

    out_dir.mkdir(
        parents=True,
        exist_ok=False,
    )

    print(
        "[INFO] 原資料夾已有其他成果，"
        "改用新的唯一接續資料夾：",
        out_dir,
    )
else:
    print("[PASS] 可在原 v1.3.0 輸出目錄接續。")

zip_path = ROOT / f"{out_dir.name}.zip"

if zip_path.exists():
    zip_path = ROOT / (
        f"{out_dir.name}_H10口徑修正版.zip"
    )

if zip_path.exists():
    raise FileExistsError(
        "修正版 ZIP 仍已存在，停止避免覆寫："
        + str(zip_path)
    )


# =============================================================================
# 03. 讀取 v1.2.8 原始 H10 成熟樣本，拆分 747 筆
# =============================================================================

def verify_q5_h10_official_subset(
    v129_module,
    new_schedule,
):

    LEGACY_LABEL = (
        "v1.2.7原凍結Q5_官方普通股過濾"
    )

    OFFICIAL_LABEL = (
        "v1.2.8官方普通股重新排名Q5"
    )

    old_zip = Path(
        v129_module.V128_ZIP
    )

    if not old_zip.is_file():
        raise FileNotFoundError(
            "找不到 v1.2.8 原始 ZIP："
            + str(old_zip)
        )

    actual_sha = v129_module.sha256_file(
        old_zip
    )

    expected_sha = (
        v129_module.V128_ZIP_SHA256
    )

    if actual_sha != expected_sha:
        raise RuntimeError(
            "v1.2.8 ZIP SHA256 不符。"
            f"\n預期：{expected_sha}"
            f"\n實際：{actual_sha}"
        )

    with zipfile.ZipFile(
        old_zip,
        "r",
    ) as zf:

        member = (
            v129_module.find_member_by_basename(
                zf.namelist(),
                "12_H10已成熟逐筆實際淨報酬.csv",
            )
        )

        old = pd.read_csv(
            zf.open(member),
            dtype="string",
            encoding="utf-8-sig",
            low_memory=False,
        )

    required = [
        "訊號口徑",
        "訊號日期",
        "股票代號",
        "H",
        "毛報酬",
        "基準成本淨報酬",
        "壓力成本淨報酬",
    ]

    missing_cols = [
        col for col in required
        if col not in old.columns
    ]

    if missing_cols:
        raise RuntimeError(
            "v1.2.8 原始 H10 CSV 缺少欄位："
            + repr(missing_cols)
        )

    old["訊號口徑"] = (
        old["訊號口徑"]
        .astype("string")
        .str.strip()
    )

    old_legacy = old.loc[
        old["訊號口徑"].eq(LEGACY_LABEL)
    ].copy()

    old_official = old.loc[
        old["訊號口徑"].eq(OFFICIAL_LABEL)
    ].copy()

    if (
        len(old) != 747
        or len(old_legacy) != 375
        or len(old_official) != 372
    ):
        distribution = (
            old["訊號口徑"]
            .value_counts(dropna=False)
            .to_dict()
        )

        raise RuntimeError(
            "v1.2.8 原始 H10 口徑拆分與固定成果不符。"
            f"\n合計：{len(old)}"
            f"\n原凍結 Q5：{len(old_legacy)}"
            f"\n官方普通股 Q5：{len(old_official)}"
            f"\n口徑分布：{distribution}"
        )

    # v1.3.0 的 Q5 是官方普通股重新排名口徑。
    new = new_schedule.loc[
        pd.to_numeric(
            new_schedule["H"],
            errors="coerce",
        ).eq(10)
        & new_schedule["觀察狀態"].eq(
            "MATURED_RETURN_COMPUTED"
        )
    ].copy()

    # 將新排程的 Q5 標籤對齊 v1.2.8 官方口徑。
    new["訊號口徑"] = OFFICIAL_LABEL

    if len(new) != 372:
        raise RuntimeError(
            "v1.3.0 官方普通股 Q5 H10 成熟筆數不是 372。"
            f"\n實際：{len(new)}"
        )

    keys = [
        "訊號口徑",
        "訊號日期",
        "股票代號",
        "H",
    ]

    compare_cols = [
        "毛報酬",
        "基準成本淨報酬",
        "壓力成本淨報酬",
    ]

    for frame in (old_official, new):

        for col in keys:
            frame[col] = (
                frame[col]
                .astype("string")
                .str.strip()
            )

        frame["訊號日期"] = (
            pd.to_datetime(
                frame["訊號日期"],
                errors="coerce",
            )
            .dt.strftime("%Y-%m-%d")
        )

        frame["股票代號"] = (
            frame["股票代號"]
            .str.replace(
                r"\.0$",
                "",
                regex=True,
            )
            .str.strip()
        )

    if old_official.duplicated(keys).any():
        raise RuntimeError(
            "v1.2.8 官方 Q5 H10 出現重複訊號鍵。"
        )

    if new.duplicated(keys).any():
        raise RuntimeError(
            "v1.3.0 Q5 H10 出現重複訊號鍵。"
        )

    merged = old_official[
        keys + compare_cols
    ].merge(
        new[
            keys + compare_cols
        ],
        on=keys,
        how="outer",
        suffixes=("_v128", "_v130"),
        indicator=True,
    )

    if (
        len(merged) != 372
        or not merged["_merge"].eq("both").all()
    ):
        left_only = int(
            merged["_merge"].eq("left_only").sum()
        )

        right_only = int(
            merged["_merge"].eq("right_only").sum()
        )

        raise RuntimeError(
            "v1.3.0 與 v1.2.8 官方普通股 Q5 H10 "
            "訊號鍵不一致。"
            f"\n舊版缺少：{right_only}"
            f"\n新版缺少：{left_only}"
        )

    max_diffs = {}

    rows = [
        {
            "核對項目": "v1.2.8 舊版兩種口徑合計",
            "預期": 747,
            "實際": len(old),
            "核對結果": "PASS",
        },
        {
            "核對項目": "v1.2.8 原凍結 Q5 口徑",
            "預期": 375,
            "實際": len(old_legacy),
            "核對結果": "PASS",
        },
        {
            "核對項目": "v1.2.8 官方普通股重新排名 Q5",
            "預期": 372,
            "實際": len(old_official),
            "核對結果": "PASS",
        },
        {
            "核對項目": "v1.3.0 官方普通股 Q5 H10",
            "預期": 372,
            "實際": len(new),
            "核對結果": "PASS",
        },
        {
            "核對項目": "H10 訊號鍵集合",
            "預期": "372 筆完全相同",
            "實際": "372 筆完全相同",
            "核對結果": "PASS",
        },
    ]

    for col in compare_cols:

        a = pd.to_numeric(
            merged[f"{col}_v128"],
            errors="coerce",
        )

        b = pd.to_numeric(
            merged[f"{col}_v130"],
            errors="coerce",
        )

        diff = (a - b).abs()

        max_diff = (
            float(diff.max())
            if diff.notna().any()
            else 0.0
        )

        max_diffs[col] = max_diff

        if not np.allclose(
            a.to_numpy(dtype=float),
            b.to_numpy(dtype=float),
            rtol=0,
            atol=1e-12,
            equal_nan=True,
        ):
            raise RuntimeError(
                f"H10 報酬逐筆不一致：{col}；"
                f"最大差異={max_diff}"
            )

        rows.append({
            "核對項目": f"{col}最大絕對差異",
            "預期": "<= 1e-12",
            "實際": max_diff,
            "核對結果": "PASS",
        })

    result = pd.DataFrame(rows)

    if not result["核對結果"].eq("PASS").all():
        raise RuntimeError(
            "H10 官方 Q5 相容性核對未全部通過。"
        )

    print("[PASS] v1.2.8 舊版 H10 合計 747 筆。")
    print("[PASS] 原凍結 Q5：375 筆。")
    print("[PASS] 官方普通股重排名 Q5：372 筆。")
    print("[PASS] v1.3.0 官方 Q5 H10：372 筆。")
    print("[PASS] 訊號鍵及三種報酬逐筆一致。")

    return result


print("\n[H10 1/2] 修正舊版比較範圍。")

h10_qa = verify_q5_h10_official_subset(
    v129,
    L["q5_compat_schedule"],
)

print(
    h10_qa.to_string(
        index=False,
        max_rows=20,
    )
)


# =============================================================================
# 04. 修正並保存完整 v1.3.0 程式原文
# =============================================================================

print("\n[SOURCE] 封存修正後完整 v1.3.0 程式。")

target_program = (
    out_dir / program_filename
)

source_text = None

if target_program.is_file():
    source_text = target_program.read_text(
        encoding="utf-8-sig"
    )

# 從 Colab 歷史取得完整主程式。
if not source_text:

    try:
        shell = get_ipython()

        history = (
            shell.history_manager.input_hist_raw
        )

        for candidate in reversed(history):

            if not isinstance(candidate, str):
                continue

            if (
                "SSO v1.3.0" in candidate
                and "def run_v130():" in candidate
                and "load_pinned_v129_safe" in candidate
                and "v129.verify_h10_against_v128" in candidate
                and len(candidate) > 20000
            ):
                source_text = candidate
                break

    except Exception:
        pass

if not source_text:
    raise RuntimeError(
        "無法從 Colab 執行紀錄取得完整 v1.3.0 程式原文。"
        "\n已完成的分析仍保留在目前 runtime，"
        "但停止封存及上傳不完整程式。"
    )


# 修正 helper：只核對 v1.2.8 官方普通股 Q5 的 372 筆。
helper_source = r'''
def verify_q5_h10_official_subset(v129_module, new_schedule):
    """
    v1.2.8 H10 舊版：
      原凍結 Q5 = 375
      官方普通股重新排名 Q5 = 372
      合計 = 747

    v1.3.0 Q1-Q5 的 Q5 是官方普通股重新排名，
    因此只與 372 筆比較。
    """
    LEGACY_LABEL = "v1.2.7原凍結Q5_官方普通股過濾"
    OFFICIAL_LABEL = "v1.2.8官方普通股重新排名Q5"

    old_zip = Path(v129_module.V128_ZIP)

    actual_sha = v129_module.sha256_file(old_zip)

    if actual_sha != v129_module.V128_ZIP_SHA256:
        raise RuntimeError("v1.2.8 ZIP SHA256 不符。")

    with zipfile.ZipFile(old_zip, "r") as zf:
        member = v129_module.find_member_by_basename(
            zf.namelist(),
            "12_H10已成熟逐筆實際淨報酬.csv",
        )

        old = pd.read_csv(
            zf.open(member),
            dtype="string",
            encoding="utf-8-sig",
            low_memory=False,
        )

    old["訊號口徑"] = (
        old["訊號口徑"].astype("string").str.strip()
    )

    old_legacy = old.loc[
        old["訊號口徑"].eq(LEGACY_LABEL)
    ].copy()

    old_official = old.loc[
        old["訊號口徑"].eq(OFFICIAL_LABEL)
    ].copy()

    if (
        len(old) != 747
        or len(old_legacy) != 375
        or len(old_official) != 372
    ):
        raise RuntimeError("v1.2.8 H10 舊版口徑筆數不符。")

    new = new_schedule.loc[
        pd.to_numeric(new_schedule["H"], errors="coerce").eq(10)
        & new_schedule["觀察狀態"].eq("MATURED_RETURN_COMPUTED")
    ].copy()

    new["訊號口徑"] = OFFICIAL_LABEL

    if len(new) != 372:
        raise RuntimeError(
            f"v1.3.0 官方普通股 Q5 H10 筆數不符：{len(new)}"
        )

    keys = ["訊號口徑", "訊號日期", "股票代號", "H"]

    compare_cols = [
        "毛報酬",
        "基準成本淨報酬",
        "壓力成本淨報酬",
    ]

    for frame in (old_official, new):
        for col in keys:
            frame[col] = (
                frame[col].astype("string").str.strip()
            )

        frame["股票代號"] = (
            frame["股票代號"]
            .str.replace(r"\.0$", "", regex=True)
        )

        frame["訊號日期"] = (
            pd.to_datetime(
                frame["訊號日期"], errors="coerce"
            ).dt.strftime("%Y-%m-%d")
        )

    merged = old_official[keys + compare_cols].merge(
        new[keys + compare_cols],
        on=keys,
        how="outer",
        suffixes=("_v128", "_v130"),
        indicator=True,
    )

    if (
        len(merged) != 372
        or not merged["_merge"].eq("both").all()
    ):
        raise RuntimeError(
            "H10 官方普通股 Q5 新舊版訊號鍵不一致。"
        )

    rows = [
        {
            "核對項目": "v1.2.8兩種口徑合計",
            "預期": 747,
            "實際": 747,
            "核對結果": "PASS",
        },
        {
            "核對項目": "v1.2.8原凍結Q5",
            "預期": 375,
            "實際": 375,
            "核對結果": "PASS",
        },
        {
            "核對項目": "v1.2.8官方普通股Q5",
            "預期": 372,
            "實際": 372,
            "核對結果": "PASS",
        },
        {
            "核對項目": "v1.3.0官方普通股Q5",
            "預期": 372,
            "實際": len(new),
            "核對結果": "PASS",
        },
    ]

    for col in compare_cols:
        a = pd.to_numeric(
            merged[f"{col}_v128"], errors="coerce"
        )
        b = pd.to_numeric(
            merged[f"{col}_v130"], errors="coerce"
        )

        diff = (a - b).abs()
        maximum = float(diff.max()) if diff.notna().any() else 0.0

        if not np.allclose(
            a.to_numpy(dtype=float),
            b.to_numpy(dtype=float),
            rtol=0,
            atol=1e-12,
            equal_nan=True,
        ):
            raise RuntimeError(
                f"H10 報酬不一致：{col}；最大差異={maximum}"
            )

        rows.append({
            "核對項目": f"{col}最大絕對差異",
            "預期": "<= 1e-12",
            "實際": maximum,
            "核對結果": "PASS",
        })

    return pd.DataFrame(rows)
'''

# 如果原程式尚未修正，替換唯一錯誤呼叫。
if "def verify_q5_h10_official_subset(" not in source_text:

    pattern = re.compile(
        r"h10_qa\s*=\s*\(\s*"
        r"v129\.verify_h10_against_v128\s*\(\s*"
        r"q5_compat_schedule\s*\)\s*\)",
        flags=re.DOTALL,
    )

    source_text, replace_count = pattern.subn(
        "h10_qa = verify_q5_h10_official_subset("
        "v129, q5_compat_schedule)",
        source_text,
        count=1,
    )

    if replace_count != 1:
        raise RuntimeError(
            "無法唯一定位 v1.3.0 原 H10 核對呼叫。"
        )

    marker = "\ndef run_v130():"

    if source_text.count(marker) != 1:
        raise RuntimeError(
            "無法唯一定位 v1.3.0 主流程。"
        )

    source_text = source_text.replace(
        marker,
        "\n\n" + helper_source.strip() + "\n\n" + marker,
        1,
    )

# 文字說明一併校正。
source_text = source_text.replace(
    "Q5 H10 與 v1.2.8 官方普通股口徑 372 筆逐筆核對：PASS（舊版兩口徑合計 747 筆）",
    "Q5 H10 與 v1.2.8 官方普通股口徑 372 筆逐筆核對：PASS（舊版兩口徑合計 747 筆）",
)

# 語法檢查。
compile(
    source_text,
    str(target_program),
    "exec",
)

target_program.write_text(
    source_text,
    encoding="utf-8",
)

print("[PASS] 完整 v1.3.0 程式原文已修正並通過語法檢查。")
print("[PASS] 後續重跑時不會再將 372 筆錯誤比對 747 筆。")


# =============================================================================
# 05. 接續原 v1.3.0 四週期摘要
# =============================================================================

print("\n[CONTINUE 1/5] 建立 Q1-Q5 跨週期摘要。")

horizon_summary = (
    v129.summarize_by_horizon(schedule)
)

daily_horizon_summary = (
    v129.summarize_by_signal_date(schedule)
)

stock_horizon_summary = (
    v129.summarize_stock_horizon(matured)
)

cross_horizon_summary = (
    v129.build_cross_horizon_stock_table(
        stock_horizon_summary
    )
)

print("[PASS] Q1-Q5 H1/H3/H5/H10 摘要完成。")


# =============================================================================
# 06. 接續 Drive 歷史行情及證券分類來源盤點
# =============================================================================

print("\n[CONTINUE 2/5] 唯讀盤點 Drive 歷史來源。")

(
    history_inventory,
    zip_member_inventory,
    history_summary,
    history_ohlc,
    history_classification,
) = scan_history_files(
    ROOT,
    date_parser=v126.parse_date_series,
)

print("[PASS] 歷史來源盤點完成。")


# =============================================================================
# 07. 正式行情期間、來源清單
# =============================================================================

official_source = sources["official"].copy()
v469_source = sources["v469"].copy()

official_dates = pd.to_datetime(
    official_source["date"],
    errors="coerce",
)

v469_dates = pd.to_datetime(
    v469_source["date"],
    errors="coerce",
)

official_min = official_dates.min()
official_max = official_dates.max()

v469_min = v469_dates.min()
v469_max = v469_dates.max()

source_manifest = (
    sources["source_manifest"].copy()
    if "source_manifest" in sources
    else pd.DataFrame()
)

append_manifest = pd.DataFrame([
    {
        "來源": "v1.5.1官方全市場行情增量",
        "路徑": overlay_info.get(
            "zip_path",
            str(v128.V151_ZIP),
        ),
        "SHA256": overlay_info.get(
            "zip_sha256",
            "",
        ),
        "讀取列數": len(new_rows),
        "起日": "2026-11-09",
        "迄日": "2026-11-10",
    },
    {
        "來源": "v1.4.1官方ISIN CFI普通股分類",
        "路徑": ordinary_info.get(
            "zip_path",
            str(v128.V141_ZIP),
        ),
        "SHA256": ordinary_info.get(
            "zip_sha256",
            "",
        ),
        "讀取列數": len(ordinary_frame),
        "起日": "2026-11-10",
        "迄日": "2026-11-10",
    },
    {
        "來源": "v1.2.9固定程式",
        "路徑": V129_SCRIPT_PATH,
        "SHA256": V129_SCRIPT_GIT_BLOB_SHA1,
        "讀取列數": None,
        "起日": None,
        "迄日": None,
    },
])

source_manifest = pd.concat(
    [
        source_manifest,
        append_manifest,
    ],
    ignore_index=True,
)

source_periods = pd.DataFrame([
    {
        "來源": "v4.69已驗證特徵行情來源",
        "資料起日": (
            str(v469_min.date())
            if pd.notna(v469_min) else ""
        ),
        "資料迄日": (
            str(v469_max.date())
            if pd.notna(v469_max) else ""
        ),
        "資料列數": len(v469_source),
        "用途": "歷史區段及特徵重建",
    },
    {
        "來源": "官方行情基礎資料及增量合併",
        "資料起日": (
            str(official_min.date())
            if pd.notna(official_min) else ""
        ),
        "資料迄日": (
            str(official_max.date())
            if pd.notna(official_max) else ""
        ),
        "資料列數": len(official_source),
        "用途": "實際進出場OHLC及交易日曆",
    },
    {
        "來源": "官方ISIN CFI普通股分類",
        "資料起日": "2026-11-10",
        "資料迄日": "2026-11-10",
        "資料列數": len(ordinary_frame),
        "用途": "2026-11-10當日普通股universe",
    },
])


# =============================================================================
# 08. 報告資料
# =============================================================================

status_distribution = {
    str(k): int(v)
    for k, v in schedule["觀察狀態"]
    .value_counts(dropna=False)
    .items()
}

horizon_rows = [
    json_safe(row)
    for row in horizon_summary.to_dict("records")
]

qa_failures = int(
    qa["核對結果"].ne("PASS").sum()
)

report = {
    "版本": VERSION,
    "執行時間台北": datetime.now(TAIPEI).isoformat(),
    "執行識別碼": run_id_original + "_H10FIX",
    "研究週期": list(HORIZONS),

    "官方普通股市場代號鍵數": int(len(ordinary_frame)),
    "官方普通股唯一代號數": int(len(ordinary_codes)),
    "官方普通股分類基準日": "2026-11-10",
    "官方普通股分類方法": "ISIN CFI前兩碼ES",

    "v126原始摘要核對項目": int(len(qa)),
    "v126原始摘要核對失敗": qa_failures,
    "v127原九日Q5訊號數": int(legacy_first_nine_n),

    "Q1至Q5明細列數": int(len(quintile_detail)),
    "Q1至Q5分組列數": {
        str(row["Q分組"]): int(row["股票日筆數"])
        for row in group_counts.to_dict("records")
    },

    "多週期排程列數": int(len(schedule)),
    "成熟淨報酬列數": int(len(matured)),
    "觀察狀態分布": status_distribution,

    "H10舊版相容性": "PASS",
    "H10核對項目數": int(len(h10_qa)),
    "H10舊版兩口徑合計成熟筆數": 747,
    "H10舊版原凍結Q5成熟筆數": 375,
    "H10舊版官方普通股Q5成熟筆數": 372,
    "H10本次官方普通股Q5成熟筆數": 372,
    "H10本次比較口徑": (
        "只與v1.2.8官方普通股重新排名Q5逐筆比較"
    ),

    "凍結候選檔數": int(len(frozen_codes)),
    "凍結候選代號": frozen_codes,

    "歷史掃描": history_summary,
    "歷史OHLC候選數": int(len(history_ohlc)),
    "歷史證券分類候選數": int(len(history_classification)),

    "官方行情實際涵蓋": {
        "起日": (
            str(official_min.date())
            if pd.notna(official_min) else None
        ),
        "迄日": (
            str(official_max.date())
            if pd.notna(official_max) else None
        ),
        "資料列數": int(len(official_source)),
    },

    "v469實際涵蓋": {
        "起日": (
            str(v469_min.date())
            if pd.notna(v469_min) else None
        ),
        "迄日": (
            str(v469_max.date())
            if pd.notna(v469_max) else None
        ),
        "資料列數": int(len(v469_source)),
    },

    "各週期摘要": horizon_rows,

    "研究限制": [
        "Q1-Q5是同期橫斷面分組，不代表未來獲利機率。",
        "H1/H3/H5/H10可能使用重疊持有期間，不是獨立樣本。",
        "尚未成熟的觀察不計算報酬。",
        "2026-11-10官方普通股分類不得回套至更早歷史日期。",
        "歷史OHLC文件存在，不等於已取得逐日歷史證券分類。",
        "長期回測仍須確認公司行動、價格區段及證券存續狀態。",
        "本次研究候選不構成投資推薦。",
    ],

    "資料安全": {
        "Drive原始來源唯讀": True,
        "未刪除或搬移來源": True,
        "未覆寫既有結果": True,
        "未呼叫官方API": True,
        "未重新訓練": True,
        "未修改GitHub_main": True,
    },
}


# =============================================================================
# 09. 保存成果
# =============================================================================

print("\n[CONTINUE 3/5] 保存 Q1-Q5 分析成果。")

csv_write(
    quintile_detail,
    out_dir / "01_官方普通股Q1至Q5完整分組明細.csv",
)

csv_write(
    group_counts,
    out_dir / "02_Q1至Q5整體分組數量.csv",
)

csv_write(
    daily_group_counts,
    out_dir / "03_Q1至Q5每日分組數量.csv",
)

schedule.to_csv(
    out_dir / "04_Q1至Q5_H1H3H5H10完整觀察排程.csv.gz",
    index=False,
    encoding="utf-8-sig",
    compression="gzip",
)

matured.to_csv(
    out_dir / "05_Q1至Q5已成熟實際淨報酬.csv.gz",
    index=False,
    encoding="utf-8-sig",
    compression="gzip",
)

csv_write(
    horizon_summary,
    out_dir / "06_Q1至Q5各週期整體績效摘要.csv",
)

csv_write(
    daily_horizon_summary,
    out_dir / "07_Q1至Q5各訊號日週期成熟概況.csv",
)

csv_write(
    stock_horizon_summary,
    out_dir / "08_Q1至Q5個股各週期成熟報酬彙總.csv",
)

csv_write(
    cross_horizon_summary,
    out_dir / "09_Q1至Q5個股跨週期表現總表.csv",
)

csv_write(
    h10_qa,
    out_dir / "10_Q5_H10與v128官方普通股372筆逐筆相容性核對.csv",
)

csv_write(
    frozen_candidates,
    out_dir / "11_v129凍結12檔研究候選核驗.csv",
)

csv_write(
    source_manifest,
    out_dir / "12_正式行情及分類來源清單.csv",
)

csv_write(
    source_periods,
    out_dir / "13_已使用行情來源實際涵蓋期間.csv",
)

csv_write(
    history_inventory,
    out_dir / "14_Drive歷史行情及證券分類候選檔案盤點.csv",
)

csv_write(
    zip_member_inventory,
    out_dir / "15_ZIP內部歷史資料候選成員盤點.csv",
)

csv_write(
    history_ohlc,
    out_dir / "16_歷史OHLC候選來源.csv",
)

csv_write(
    history_classification,
    out_dir / "17_歷史證券分類候選來源.csv",
)

json_write(
    report,
    out_dir / "18_v130研究結論.json",
)


# =============================================================================
# 10. 執行摘要
# =============================================================================

summary_lines = [
    "=" * 104,
    VERSION,
    "=" * 104,
    f"執行時間：{report['執行時間台北']}",
    "",
    "一、固定來源驗證",
    f"v1.2.9固定程式Blob SHA1：{V129_SCRIPT_GIT_BLOB_SHA1}",
    f"v1.2.6原始摘要核對：{len(qa):,}項",
    f"v1.2.6核對失敗：{qa_failures:,}項",
    f"v1.2.7原九日Q5：{legacy_first_nine_n:,}筆",
    "",
    "二、官方普通股範圍",
    f"官方普通股市場－代號鍵：{len(ordinary_frame):,}",
    f"官方普通股唯一代號：{len(ordinary_codes):,}",
    "分類基準日：2026-11-10",
    "分類方法：ISIN CFI前兩碼ES",
    "",
    "三、Q1-Q5同期分組",
    f"有效特徵列數：{len(quintile_detail):,}",
]

for row in group_counts.to_dict("records"):
    summary_lines.append(
        f"{row['Q分組']}："
        f"{int(row['股票日筆數']):,}股票日；"
        f"{int(row['訊號日期數']):,}個訊號日期；"
        f"百分位範圍 "
        f"{row['最低百分位']:.4f}至{row['最高百分位']:.4f}"
    )

summary_lines += [
    "",
    "四、四週期實際淨報酬",
    f"完整配對排程：{len(schedule):,}筆",
    f"已成熟且完成淨報酬：{len(matured):,}筆",
    "",
]

for row in horizon_summary.to_dict("records"):
    summary_lines.append(
        f"{row.get('訊號口徑', '')} | "
        f"H={row.get('H', '')} | "
        f"訊號={row.get('訊號總筆數', 0)} | "
        f"成熟={row.get('已成熟且完成報酬筆數', 0)} | "
        f"基準平均={row.get('基準成本平均淨報酬', np.nan)} | "
        f"壓力平均={row.get('壓力成本平均淨報酬', np.nan)}"
    )

summary_lines += [
    "",
    "五、H10舊版相容性口徑校正",
    "v1.2.8舊版兩種Q5口徑合計：747筆。",
    "其中v1.2.7原凍結Q5：375筆。",
    "其中v1.2.8官方普通股重新排名Q5：372筆。",
    "v1.3.0 Q1-Q5只採官方普通股重新排名，因此只與372筆逐筆比較。",
    "H10官方普通股Q5訊號鍵及三種報酬均通過核對。",
    "",
    "六、v1.2.9凍結研究候選",
    f"固定候選數：{len(frozen_codes):,}",
    "、".join(frozen_codes),
    "",
    "七、歷史來源盤點",
    f"Drive資料夾數：{history_summary['資料夾數']:,}",
    f"Drive檔案數：{history_summary['全部檔案數']:,}",
    f"相關候選檔：{history_summary['相關候選檔案數']:,}",
    f"歷史OHLC候選：{len(history_ohlc):,}",
    f"歷史證券分類候選：{len(history_classification):,}",
    "",
    f"目前已使用官方行情：{official_min.date()}至{official_max.date()}",
    f"v4.69已驗證特徵行情：{v469_min.date()}至{v469_max.date()}",
    "",
    "八、重要研究限制",
    "2026-11-10單日官方普通股分類，不得回套到過去日期。",
    "有歷史OHLC不代表已取得逐日歷史普通股分類。",
    "歷史分類來源仍須確認CFI、有效期間及證券存續狀態。",
    "跨週期報酬可能重疊，不得直接當成獨立樣本。",
    "短期正報酬不等於可重複獲利。",
    "本次結果不直接產生十檔投資推薦。",
    "",
    "九、資料安全",
    "Drive原始來源唯讀。",
    "未刪除、搬移、覆寫來源。",
    "未呼叫官方API。",
    "未重新訓練。",
    "GitHub僅新增獨立audit branch。",
    "GitHub main SHA前後一致。",
    "=" * 104,
]

(out_dir / "19_執行摘要.txt").write_text(
    "\n".join(summary_lines) + "\n",
    encoding="utf-8-sig",
)


# =============================================================================
# 11. README
# =============================================================================

readme_lines = [
    "# SSO v1.3.0｜同期 Q1-Q5 分組對照暨長期資料來源盤點",
    "",
    f"- 執行時間：`{report['執行時間台北']}`",
    f"- 官方普通股市場－代號鍵：`{len(ordinary_frame):,}`",
    f"- Q1-Q5明細：`{len(quintile_detail):,}`",
    f"- 多週期排程：`{len(schedule):,}`",
    f"- 已成熟淨報酬：`{len(matured):,}`",
    f"- 冷凍候選：`{len(frozen_codes):,}`",
    f"- 歷史OHLC候選：`{len(history_ohlc):,}`",
    f"- 歷史分類候選：`{len(history_classification):,}`",
    "",
    "Q1-Q5使用同一每日排名。",
    "Q5與v1.2.8官方普通股重新排名定義一致。",
    "H10與v1.2.8官方普通股Q5的372筆逐筆核對。",
    "v1.2.8舊版747筆為兩種口徑合計：375+372。",
    "不重新訓練，不呼叫官方行情API。",
    "Drive原始資料唯讀。",
    "GitHub不修改main。",
    "",
    "## 檔案 SHA256",
]

for path in sorted(out_dir.iterdir()):
    if path.is_file():
        readme_lines.append(
            f"- `{path.name}` — "
            f"{path.stat().st_size:,} bytes — "
            f"`{sha256_file(path)}`"
        )

(out_dir / "README_稽核說明.md").write_text(
    "\n".join(readme_lines) + "\n",
    encoding="utf-8",
)


# =============================================================================
# 12. ZIP 及 CRC
# =============================================================================

print("\n[CONTINUE 4/5] 建立 ZIP 並驗證 CRC。")

with zipfile.ZipFile(
    zip_path,
    "w",
    compression=zipfile.ZIP_DEFLATED,
    compresslevel=6,
) as archive:

    for path in sorted(out_dir.iterdir()):
        if path.is_file():
            archive.write(
                path,
                arcname=path.name,
            )

with zipfile.ZipFile(zip_path, "r") as archive:
    bad = archive.testzip()

if bad:
    raise RuntimeError(
        "ZIP CRC 驗證失敗：" + bad
    )

zip_sha = sha256_file(zip_path)

print("[PASS] ZIP CRC。")
print("[PASS] ZIP SHA256：", zip_sha)


# =============================================================================
# 13. GitHub 獨立 audit branch
# =============================================================================

print("\n[CONTINUE 5/5] 上傳 GitHub audit branch。")

token = find_github_token(v127)

github_result = upload_github_v130(
    out_dir=out_dir,
    zip_path=zip_path,
    run_id=run_id_original + "_H10FIX",
    token=token,
    report=report,
)

if (
    github_result["main_before"]
    != github_result["main_after"]
):
    raise RuntimeError(
        "GitHub main SHA 前後不一致。"
    )

verification_path = (
    ROOT
    / f"{out_dir.name}_GitHub上傳核驗.json"
)

verification = {
    "版本": VERSION,
    "原執行識別碼": run_id_original,
    "修正識別碼": run_id_original + "_H10FIX",
    "輸出資料夾": str(out_dir),
    "ZIP": str(zip_path),
    "ZIP_SHA256": zip_sha,
    "ZIP_bytes": zip_path.stat().st_size,
    "程式": str(target_program),
    "程式_SHA256": sha256_file(target_program),
    "GitHub": github_result,

    "H10口徑": {
        "舊版兩種口徑合計": 747,
        "舊版原凍結Q5": 375,
        "舊版官方普通股Q5": 372,
        "本次官方普通股Q5": 372,
        "逐筆核對": "PASS",
    },

    "驗證": {
        "v129_source_blob_sha1": V129_SCRIPT_GIT_BLOB_SHA1,
        "v126_original_QA": qa_failures == 0,
        "Q5_definition_matches_v128": True,
        "H10_official_Q5_372_rows_compatible": True,
        "github_blob_readback": True,
        "main_sha_unchanged": (
            github_result["main_before"]
            == github_result["main_after"]
        ),
        "drive_source_readonly": True,
    },
}

json_write(
    verification,
    verification_path,
)


# =============================================================================
# 14. 最終結果
# =============================================================================

print("\n" + "=" * 104)
print("[PASS] SSO v1.3.0 中斷成果接續完成")
print("=" * 104)

print("原執行識別碼：", run_id_original)
print("輸出資料夾：", out_dir)
print("修正後完整程式：", target_program)
print("ZIP：", zip_path)
print("ZIP SHA256：", zip_sha)

print("\nQ1-Q5 分組：")
print(
    group_counts.to_string(
        index=False,
        max_rows=10,
    )
)

print("\n各週期摘要：")
print(
    horizon_summary.to_string(
        index=False,
        max_rows=100,
    )
)

print("\nH10 相容性核對：")
print(
    h10_qa.to_string(
        index=False,
        max_rows=20,
    )
)

print("\n凍結候選：", frozen_codes)

print("歷史OHLC候選數：", len(history_ohlc))
print("歷史證券分類候選數：", len(history_classification))

print("\nGitHub branch：", github_result["branch"])
print("GitHub commit：", github_result["commit"])
print("GitHub資料夾：", github_result["folder_url"])
print("GitHub ZIP：", github_result["zip_url"])
print("GitHub commit URL：", github_result["commit_url"])

print("\nmain before：", github_result["main_before"])
print("main after：", github_result["main_after"])

print("\n核驗紀錄：", verification_path)

print("[PASS] v1.2.8 H10 747 = 375 + 372。")
print("[PASS] v1.3.0 官方普通股 Q5 H10 372 筆逐筆相容。")
print("[PASS] Q1-Q5 四週期摘要。")
print("[PASS] 歷史資料來源盤點。")
print("[PASS] ZIP CRC 及 SHA256。")
print("[PASS] GitHub 每檔 Git blob SHA read-back。")
print("[PASS] GitHub main SHA 前後一致。")
print("[PASS] Drive 原始資料唯讀。")
print("=" * 104)

RESULT = {
    "output_dir": str(out_dir),
    "program_path": str(target_program),
    "zip_path": str(zip_path),
    "zip_sha256": zip_sha,
    "github": github_result,
    "report": report,
    "horizon_summary": horizon_summary,
    "group_counts": group_counts,
    "history_summary": history_summary,
    "frozen_candidates": frozen_codes,
    "h10_qa": h10_qa,
}