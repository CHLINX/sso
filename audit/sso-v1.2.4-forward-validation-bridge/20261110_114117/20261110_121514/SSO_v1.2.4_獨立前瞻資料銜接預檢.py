#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""SSO v1.2.4｜2026-10-23 後獨立前瞻驗證資料銜接預檢

目的：
1. 固定承接 v1.2.3 已封存的 238 檔訓練候選，不重選股票、不調整條件。
2. 唯讀檢查 v4.69 歷史影子 OHLC、v4.17 官方行情主檔、官方行情增量檔的來源與日期覆蓋。
3. 以最近 20 個共同交易日比對 v4.69 影子 OHLC 與 v4.17 官方 OHLC 的比例是否逐股穩定，確認能否無誤銜接。
4. 核對 238 檔候選的近期 20 根有效 K 棒、價格研究區段、現行名稱及增量行情覆蓋。
5. 只產生資料銜接預檢報告；本版不產生交易訊號、不回測、不選股、不重訓。
6. 結果、ZIP、程式及 README 在同一次執行中上傳 CHLINX/sso 獨立 audit branch。

安全：
- Drive 所有來源唯讀；不改寫、不刪除、不移動、不覆寫來源。
- 不呼叫 TWSE／TPEx 官方 API。
- 不更新 GitHub main；只新增獨立 audit branch。
- GitHub 使用環境變數或 Colab Secret 的既有 GITHUB_TOKEN，不輸出 Token。

Colab：使用通用 Python 上傳執行器 v2 上傳本檔並執行。
"""
from __future__ import annotations

import os
import re
import csv
import json
import math
import gzip
import time
import uuid
import shutil
import hashlib
import zipfile
import base64
import tempfile
import subprocess
from pathlib import Path
from datetime import datetime
from urllib.parse import quote
from zoneinfo import ZoneInfo
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

# -----------------------------------------------------------------------------
# 固定設定
# -----------------------------------------------------------------------------
ROOT = Path('/content/drive/MyDrive/看股最速適')
REPO_OWNER = 'CHLINX'
REPO_NAME = 'sso'
REPO_URL = 'https://github.com/CHLINX/sso.git'
DEFAULT_BRANCH = 'main'
TAIPEI = ZoneInfo('Asia/Taipei')

V469_PATH = (
    ROOT / '看股最速適_v4.84.9.8_正式來源重建與逐筆配對_20261104_013113'
    / 'v4.69'
    / '01_v231補行日日期排序與已驗證特徵重建候選.csv.gz'
)
V469_SHA256 = '7debd5cde9d47a42091f7e8a0dec3b5c50808e28c6f1f6c59a55ddaaeb182dd8'

A_PATH = (
    ROOT / '看股最速適_官方行情完整證券代號重建_v4_16_20261031_131511'
    / '官方行情總表_完整證券代號_v4.17.csv'
)
A_SHA256_EXPECTED = '25f736d96b8d81008a0325375837b5a663d06fdfc597b5b19793a8c003ac3444'
STATE_DIR = ROOT / 'SSO_Market_Data_Continuation_TWSE_TPEX_v1'
DELTA_PATH = STATE_DIR / '行情增量資料.csv.gz'
STATE_PATH = STATE_DIR / '資料庫來源與更新狀態.json'

V123_PREFIX = '看股最速適_v1_2_3_HAC重算與多重檢定稽核_'
V123_CANDIDATE_FILE = '01_238檔HAC重算與多重比較校正.csv'
EXPECTED_CANDIDATES = 238
V469_END_EXPECTED = pd.Timestamp('2026-10-22')
FORWARD_START = pd.Timestamp('2026-10-23')
OVERLAP_LOOKBACK_START = pd.Timestamp('2026-06-01')
OVERLAP_BARS = 20
RATIO_REL_TOL = 1e-5
MIN_VALID_OVERLAP_BARS = 20
MIN_DAILY_ROWS_PER_MARKET = 300

# -----------------------------------------------------------------------------
# 日誌與通用工具
# -----------------------------------------------------------------------------
def log(msg: str) -> None:
    print(f"[{datetime.now(TAIPEI).strftime('%H:%M:%S')}] {msg}", flush=True)


def stamp() -> str:
    return datetime.now(TAIPEI).strftime('%Y%m%d_%H%M%S')


def sha256_file(path: Path, chunk_size: int = 8 * 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open('rb') as f:
        for block in iter(lambda: f.read(chunk_size), b''):
            h.update(block)
    return h.hexdigest()


def fmt_bytes(n: int) -> str:
    if n >= 1024 ** 3:
        return f'{n / 1024**3:.3f} GB'
    if n >= 1024 ** 2:
        return f'{n / 1024**2:.2f} MB'
    return f'{n / 1024:.1f} KB'


def normalize_header(x: Any) -> str:
    s = str(x or '').strip().lower().replace('\ufeff', '')
    s = s.replace('　', '').replace(' ', '').replace('（', '(').replace('）', ')')
    s = s.replace('／', '/').replace('－', '-').replace('＋', '+')
    return re.sub(r'[^a-z0-9\u4e00-\u9fff()+/.-]', '', s)


def find_col(headers: Sequence[str], aliases: Sequence[str], required: bool = True) -> Optional[str]:
    norm_map = {normalize_header(h): h for h in headers}
    for a in aliases:
        k = normalize_header(a)
        if k in norm_map:
            return norm_map[k]
    # Conservative contains fallback for long aliases only.
    for a in aliases:
        k = normalize_header(a)
        if len(k) >= 3:
            for nk, original in norm_map.items():
                if k in nk:
                    return original
    if required:
        raise RuntimeError(f'缺少必要欄位；候選={list(aliases)}；實際={list(headers)}')
    return None


def normalize_market_series(s: pd.Series) -> pd.Series:
    x = s.astype('string').fillna('').str.strip().str.upper()
    out = pd.Series(pd.NA, index=s.index, dtype='string')
    tw = x.str.contains('TWSE|上市|證交所|集中市場|LISTED', regex=True, na=False) | x.isin(['1', 'L'])
    ot = x.str.contains('TPEX|TPEx|OTC|上櫃|櫃買|櫃檯買賣', regex=True, na=False) | x.isin(['2', 'O'])
    out.loc[tw] = 'TWSE'
    out.loc[ot] = 'TPEx'
    return out


def parse_date_series(s: pd.Series) -> pd.Series:
    raw = s.astype('string').fillna('').str.strip().str.replace(r'\.0$', '', regex=True)
    compact = raw.str.replace(r'[-/.]', '', regex=True)
    out = pd.to_datetime(compact.where(compact.str.fullmatch(r'\d{8}')), format='%Y%m%d', errors='coerce')
    roc_mask = compact.str.fullmatch(r'\d{7}') & out.isna()
    if roc_mask.any():
        roc = compact[roc_mask]
        y = pd.to_numeric(roc.str.slice(0, 3), errors='coerce') + 1911
        m = roc.str.slice(3, 5)
        d = roc.str.slice(5, 7)
        ymd = y.astype('Int64').astype('string') + m + d
        out.loc[roc_mask] = pd.to_datetime(ymd, format='%Y%m%d', errors='coerce')
    other = out.isna() & raw.ne('')
    if other.any():
        out.loc[other] = pd.to_datetime(raw.loc[other], errors='coerce')
    return out.dt.normalize()


def clean_code_series(s: pd.Series) -> pd.Series:
    return (s.astype('string').fillna('').str.strip().str.replace(r'\.0$', '', regex=True))


def numeric_series(s: pd.Series) -> pd.Series:
    x = s.astype('string').fillna('').str.strip().str.replace(',', '', regex=False)
    x = x.replace({'': pd.NA, '--': pd.NA, '---': pd.NA, '----': pd.NA, '-': pd.NA, 'X': pd.NA, 'N/A': pd.NA})
    return pd.to_numeric(x, errors='coerce')

# -----------------------------------------------------------------------------
# 來源定位與檔案欄位
# -----------------------------------------------------------------------------
def find_v123_folder() -> Path:
    folders = [p for p in ROOT.glob(V123_PREFIX + '*') if p.is_dir()]
    valid = []
    for p in folders:
        candidate = p / V123_CANDIDATE_FILE
        meta = p / '04_HAC重算與多重檢定稽核.json'
        if candidate.is_file() and meta.is_file():
            try:
                obj = json.loads(meta.read_text(encoding='utf-8-sig'))
                if int(obj.get('候選家族數', -1)) == EXPECTED_CANDIDATES:
                    valid.append(p)
            except Exception:
                continue
    if not valid:
        raise RuntimeError('找不到完整的 v1.2.3 238 檔稽核結果資料夾。')
    return max(valid, key=lambda p: p.name)


def source_mapping(path: Path, kind: str) -> Dict[str, Optional[str]]:
    header = pd.read_csv(path, nrows=0, compression='gzip' if path.suffix == '.gz' else None, encoding='utf-8-sig').columns.tolist()
    if kind == 'v469':
        aliases = {
            'market': ['市場', '市場別', '交易市場', 'market'],
            'date': ['日期', '交易日期', '交易日', 'date'],
            'code': ['股票代號', '證券代號', '股票代碼', '證券代碼', 'symbol', 'code'],
            'name': ['股票名稱', '證券名稱', '名稱', '公司名稱', 'name'],
            'open': ['影子開盤價'], 'high': ['影子最高價'], 'low': ['影子最低價'], 'close': ['影子收盤價'],
            'segment': ['價格研究區段', '價格區段', 'price_segment'],
            'ohlc_valid': ['OHLC有效', 'ohlc_valid'],
        }
    else:
        aliases = {
            'market': ['市場', '市場別', '市場類別', '市場類型', '交易市場', 'market', 'market_type'],
            'date': ['日期', '交易日期', '交易日', '資料日期', 'date', 'trade_date'],
            'code': ['完整證券代號', '股票代號', '證券代號', '股票代碼', '證券代碼', 'symbol', 'code'],
            'name': ['官方名稱', '股票名稱', '證券名稱', '名稱', '公司名稱', 'name', 'company_name'],
            'open': ['開盤價', '開盤', 'open'], 'high': ['最高價', '最高', 'high'],
            'low': ['最低價', '最低', 'low'], 'close': ['收盤價', '收盤', 'close'],
            'volume': ['原始成交量_不作指標', '成交股數', '成交量', 'volume'],
            'official_note': ['官方備註', '備註', '附註', 'official_note', 'source_note'],
        }
    return {k: find_col(header, a, required=(k not in ['name', 'segment', 'ohlc_valid', 'volume', 'official_note'])) for k, a in aliases.items()}


def read_recent(path: Path, mapping: Dict[str, Optional[str]], start: pd.Timestamp,
                end: Optional[pd.Timestamp], label: str, chunksize: int = 250_000) -> pd.DataFrame:
    usecols = list(dict.fromkeys([v for v in mapping.values() if v]))
    compression = 'gzip' if path.suffix.lower() == '.gz' else None
    pieces: List[pd.DataFrame] = []
    total = 0
    reader = pd.read_csv(path, usecols=usecols, dtype='string', encoding='utf-8-sig',
                         compression=compression, chunksize=chunksize, low_memory=False)
    for chunk_no, chunk in enumerate(reader, 1):
        total += len(chunk)
        d = parse_date_series(chunk[mapping['date']])
        mask = d.ge(start)
        if end is not None:
            mask &= d.le(end)
        if not mask.any():
            if chunk_no % 8 == 0:
                log(f'[{label}] 已掃描 {total:,} 列')
            continue
        sub = pd.DataFrame(index=chunk.index[mask])
        sub['date'] = d.loc[mask]
        sub['market'] = normalize_market_series(chunk.loc[mask, mapping['market']])
        sub['code'] = clean_code_series(chunk.loc[mask, mapping['code']])
        sub['name'] = (chunk.loc[mask, mapping['name']].astype('string').fillna('').str.strip()
                       if mapping.get('name') else '')
        for field in ('open', 'high', 'low', 'close'):
            sub[field] = numeric_series(chunk.loc[mask, mapping[field]])
        if mapping.get('volume'):
            sub['volume'] = numeric_series(chunk.loc[mask, mapping['volume']])
        else:
            sub['volume'] = np.nan
        if mapping.get('segment'):
            sub['segment'] = chunk.loc[mask, mapping['segment']].astype('string').fillna('').astype(str)
        else:
            sub['segment'] = ''
        if mapping.get('ohlc_valid'):
            sub['ohlc_valid_source'] = chunk.loc[mask, mapping['ohlc_valid']].astype('string').fillna('').astype(str)
        else:
            sub['ohlc_valid_source'] = ''
        if mapping.get('official_note'):
            sub['official_note'] = chunk.loc[mask, mapping['official_note']].astype('string').fillna('').astype(str)
        else:
            sub['official_note'] = ''
        sub = sub.loc[sub['market'].isin(['TWSE', 'TPEx']) & sub['code'].ne('') & sub['date'].notna()]
        pieces.append(sub.reset_index(drop=True))
        if chunk_no % 8 == 0:
            log(f'[{label}] 已掃描 {total:,} 列；命中近期香港資料 {sum(len(x) for x in pieces):,} 列')
    if not pieces:
        return pd.DataFrame(columns=['date','market','code','name','open','high','low','close','volume','segment','ohlc_valid_source','official_note'])
    result = pd.concat(pieces, ignore_index=True)
    return result


def load_delta(path: Path, mapping: Dict[str, Optional[str]]) -> pd.DataFrame:
    compression = 'gzip'
    raw = pd.read_csv(path, dtype='string', encoding='utf-8-sig', compression=compression, low_memory=False)
    d = parse_date_series(raw[mapping['date']])
    out = pd.DataFrame()
    out['date'] = d
    out['market'] = normalize_market_series(raw[mapping['market']])
    out['code'] = clean_code_series(raw[mapping['code']])
    out['name'] = raw[mapping['name']].astype('string').fillna('').str.strip() if mapping.get('name') else ''
    for f in ('open','high','low','close'):
        out[f] = numeric_series(raw[mapping[f]])
    out['volume'] = numeric_series(raw[mapping['volume']]) if mapping.get('volume') else np.nan
    out['segment'] = ''
    out['ohlc_valid_source'] = ''
    out['official_note'] = raw[mapping['official_note']].astype('string').fillna('').astype(str) if mapping.get('official_note') else ''
    out = out.loc[out['market'].isin(['TWSE','TPEx']) & out['code'].ne('') & out['date'].notna()].reset_index(drop=True)
    return out

# -----------------------------------------------------------------------------
# 核心稽核
# -----------------------------------------------------------------------------
def main_audit(v123_dir: Path, out_dir: Path) -> Dict[str, Any]:
    if not V469_PATH.is_file() or not A_PATH.is_file() or not STATE_PATH.is_file() or not DELTA_PATH.is_file():
        missing = [str(p) for p in (V469_PATH, A_PATH, STATE_PATH, DELTA_PATH) if not p.is_file()]
        raise FileNotFoundError('缺少前瞻驗證所需來源：\n' + '\n'.join(missing))

    log('[01/09] 驗證 v4.69 SHA256……')
    v469_sha = sha256_file(V469_PATH)
    if v469_sha != V469_SHA256:
        raise RuntimeError(f'v4.69 SHA256 不符：{v469_sha}')
    log('[PASS] v4.69 SHA256 相符。')

    log('[02/09] 核對 v4.17 官方主檔與增量資料中繼資料……')
    state = json.loads(STATE_PATH.read_text(encoding='utf-8-sig'))
    if state.get('storage_mode') != 'BASE_A_PLUS_APPEND_ONLY_DELTA':
        raise RuntimeError('行情資料庫不是 A + append-only delta 結構。')
    if Path(state.get('original_source_database', '')).resolve() != A_PATH.resolve():
        raise RuntimeError('增量資料庫中繼資料的 A 來源路徑不符。')
    if state.get('original_source_sha256') != A_SHA256_EXPECTED:
        raise RuntimeError('增量資料庫記錄的 v4.17 SHA256 不符。')
    a_stat = A_PATH.stat()
    if 'base_source_size_bytes' in state and a_stat.st_size != int(state['base_source_size_bytes']):
        raise RuntimeError('v4.17 檔案大小與增量資料庫初始化狀態不符。')
    if 'base_source_mtime_ns' in state and a_stat.st_mtime_ns != int(state['base_source_mtime_ns']):
        raise RuntimeError('v4.17 修改時間與增量資料庫初始化狀態不符。')
    delta_stat = DELTA_PATH.stat()
    if 'delta_size_bytes' in state and delta_stat.st_size != int(state['delta_size_bytes']):
        raise RuntimeError('增量檔大小與狀態中繼資料不符。')

    log('[03/09] 讀取 238 檔凍結候選……')
    cand_path = v123_dir / V123_CANDIDATE_FILE
    cand = pd.read_csv(cand_path, dtype='string', encoding='utf-8-sig', low_memory=False)
    mcol = find_col(cand.columns, ['市場'])
    ccol = find_col(cand.columns, ['股票代號','證券代號'])
    candidates = cand[[mcol, ccol]].copy()
    candidates.columns = ['market','code']
    candidates['market'] = normalize_market_series(candidates['market'])
    candidates['code'] = clean_code_series(candidates['code'])
    candidates = candidates.drop_duplicates().reset_index(drop=True)
    if len(candidates) != EXPECTED_CANDIDATES:
        raise RuntimeError(f'凍結候選數不符：預期 {EXPECTED_CANDIDATES}，實際 {len(candidates)}')
    log(f'[PASS] 凍結候選 {len(candidates)} 檔，完全沿用 v1.2.3；不重新挑選。')

    log('[04/09] 讀取增量行情並驗證自然鍵……')
    a_map = source_mapping(A_PATH, 'official')
    delta_map = source_mapping(DELTA_PATH, 'official')
    delta = load_delta(DELTA_PATH, delta_map)
    raw_delta_count = len(delta)
    delta_dups = int(delta.duplicated(['market','code','date']).sum())
    if delta_dups:
        raise RuntimeError(f'增量行情自然鍵重複 {delta_dups:,} 列。')
    state_rows = state.get('delta_rows')
    if state_rows is not None and int(state_rows) != raw_delta_count:
        raise RuntimeError(f'增量行情列數與 state 不符：{raw_delta_count:,}/{state_rows:,}')
    delta_latest = delta.groupby('market')['date'].max().dropna().to_dict()
    state_latest = state.get('last_date_by_market', {})
    for market, latest in delta_latest.items():
        expected = state_latest.get(market) or state_latest.get('TPEX' if market == 'TPEx' else 'TWSE')
        if expected:
            expected_dt = pd.to_datetime(expected)
            if pd.Timestamp(latest).normalize() != expected_dt.normalize():
                raise RuntimeError(f'{market} 增量最晚日與 state 不符：{latest} / {expected}')
    log(f'[PASS] 增量行情 {raw_delta_count:,} 列；最後日期 { {k: str(pd.Timestamp(v).date()) for k,v in delta_latest.items()} }。')

    log('[05/09] 讀取 v4.69 最近價格區段及 v4.17 重疊行情……')
    vmap = source_mapping(V469_PATH, 'v469')
    v469 = read_recent(V469_PATH, vmap, OVERLAP_LOOKBACK_START, V469_END_EXPECTED, 'v4.69')
    # 與 v1.2.2 一致：排除星期六補行日；逐股挑選最近20根有效K棒，
    # 不把停牌／無效 OHLC 日期誤當成價格K棒。
    if not v469.empty:
        v469 = v469.loc[v469['date'].dt.weekday.lt(5)].copy()
    if v469.empty:
        raise RuntimeError('v4.69 近期香港行情為空。')
    v469_dates = sorted(pd.Timestamp(x).normalize() for x in v469['date'].dropna().unique())
    if not v469_dates or v469_dates[-1] != V469_END_EXPECTED:
        raise RuntimeError(f'v4.69 最晚日期不是 2026-10-22：{v469_dates[-1] if v469_dates else None}')

    # A 只讀取最近幾個月；固定主檔全檔不複製、不修改。
    a_recent = read_recent(A_PATH, a_map, OVERLAP_LOOKBACK_START, None, 'v4.17')
    # 以每一檔證券最近20根有效且價格邏輯正確的K棒為比較視窗。
    v469_ohlc = v469[['open','high','low','close']].apply(pd.to_numeric, errors='coerce')
    v469_valid_mask = (
        v469_ohlc.gt(0).all(axis=1)
        & v469_ohlc['low'].le(v469_ohlc[['open','close']].min(axis=1))
        & v469_ohlc['high'].ge(v469_ohlc[['open','close']].max(axis=1))
        & v469_ohlc['low'].le(v469_ohlc['high'])
    )
    if 'ohlc_valid_source' in v469.columns:
        source_valid = v469['ohlc_valid_source'].astype('string').str.strip().str.lower()
        false_source = source_valid.isin(['false','0','no','n','否','無效'])
        v469_valid_mask &= ~false_source
    v469_valid = v469.loc[v469_valid_mask].copy()
    v469_valid = v469_valid.sort_values(['market','code','date'])
    hist20 = (v469_valid.groupby(['market','code'], group_keys=False, observed=True)
              .tail(OVERLAP_BARS).copy())
    overlap_dates = set(hist20['date'].unique())
    official_overlap = a_recent.loc[a_recent['date'].isin(overlap_dates)].copy()
    for label, frame in [('v4.69', hist20), ('v4.17', official_overlap)]:
        dups = int(frame.duplicated(['market','code','date']).sum())
        if dups:
            raise RuntimeError(f'{label} 最近 20 日自然鍵重複 {dups:,} 列。')

    v_price_cols = {'open':'open','high':'high','low':'low','close':'close'}
    joined = hist20.merge(official_overlap, on=['market','code','date'], how='inner', suffixes=('_shadow','_official'), validate='one_to_one')
    if joined.empty:
        raise RuntimeError('v4.69 與 v4.17 最近 20 個交易日完全沒有共同自然鍵。')
    ratio_frames = []
    for field in ('open','high','low','close'):
        left = pd.to_numeric(joined[f'{field}_shadow'], errors='coerce')
        right = pd.to_numeric(joined[f'{field}_official'], errors='coerce')
        ratio = left / right
        good = left.gt(0) & right.gt(0) & np.isfinite(ratio)
        tmp = joined.loc[good, ['market','code','date']].copy()
        tmp['field'] = field
        tmp['ratio'] = ratio.loc[good].astype(float).to_numpy()
        ratio_frames.append(tmp)
    ratios = pd.concat(ratio_frames, ignore_index=True) if ratio_frames else pd.DataFrame()
    if ratios.empty:
        raise RuntimeError('最近 20 日沒有有效 OHLC 比例可比較。')
    ratio_summary = (
        ratios.groupby(['market','code'], observed=True)
        .agg(
            OHLC比例觀測數=('ratio','size'),
            OHLC比例中位數=('ratio','median'),
            OHLC比例最小值=('ratio','min'),
            OHLC比例最大值=('ratio','max'),
            OHLC比例標準差=('ratio','std'),
            比對交易日數=('date','nunique'),
        ).reset_index()
    )
    ratio_summary['比例最大相對偏差'] = np.maximum(
        np.abs(ratio_summary['OHLC比例最小值'] / ratio_summary['OHLC比例中位數'] - 1),
        np.abs(ratio_summary['OHLC比例最大值'] / ratio_summary['OHLC比例中位數'] - 1),
    )
    hist_summary = (
        hist20.groupby(['market','code'], observed=True)
        .agg(最近20根K棒數=('date','nunique'), 最近20根價格區段數=('segment','nunique'),
             最近20根價格區段完整=('segment',lambda x: int(x.astype(str).str.strip().ne('').all())),
             最近20根有效OHLC列數=('close','count'))
        .reset_index()
    )
    ratio_summary = hist_summary.merge(ratio_summary, on=['market','code'], how='left')
    ratio_summary['價格尺度穩定'] = (
        (ratio_summary['最近20根K棒數'] >= OVERLAP_BARS)
        & (ratio_summary['最近20根有效OHLC列數'] >= OVERLAP_BARS)
        & (ratio_summary['OHLC比例觀測數'] >= OVERLAP_BARS * 4)
        & (ratio_summary['比對交易日數'] >= OVERLAP_BARS)
        & (ratio_summary['最近20根價格區段數'] == 1)
        & (ratio_summary['最近20根價格區段完整'] == 1)
        & (ratio_summary['比例最大相對偏差'] <= RATIO_REL_TOL)
    ).fillna(False)

    log('[06/09] 核對 238 檔候選最近 20 根K棒、價格區段及公司名稱……')
    latest_official = pd.concat([a_recent, delta], ignore_index=True)
    combined_dups = int(latest_official.duplicated(['market','code','date']).sum())
    if combined_dups:
        raise RuntimeError(f'A/v4.17 + 增量檔出現重複自然鍵 {combined_dups:,} 列；禁止自動去重。')
    latest_official = latest_official.sort_values('date').copy()
    latest_names = (
        latest_official.sort_values('date')
        .groupby(['market','code'], observed=True, as_index=False)
        .tail(1)[['market','code','name','date']]
        .rename(columns={'name':'現行可得名稱','date':'最新官方行情日期'})
    )
    cand_report = candidates.merge(ratio_summary, on=['market','code'], how='left').merge(latest_names, on=['market','code'], how='left')
    cand_report['價格尺度穩定'] = cand_report['價格尺度穩定'].fillna(False).astype(bool)

    log('[07/09] 核對 2026-10-23 後全市場逐日行情覆蓋……')
    future = latest_official.loc[latest_official['date'].ge(FORWARD_START)].copy()
    future = future.loc[future['date'].dt.weekday.lt(5)].copy()  # 與 v1.2.2 一致，排除星期六補行日
    future_dups = int(future.duplicated(['market','code','date']).sum())
    if future_dups:
        raise RuntimeError(f'2026-10-23 後 A+delta 自然鍵重複 {future_dups:,} 列。')
    future_ohlc = future[['open','high','low','close']].apply(pd.to_numeric, errors='coerce')
    future['OHLC有效'] = (
        future_ohlc.gt(0).all(axis=1)
        & future_ohlc['low'].le(future_ohlc[['open','close']].min(axis=1))
        & future_ohlc['high'].ge(future_ohlc[['open','close']].max(axis=1))
        & future_ohlc['low'].le(future_ohlc['high'])
    )
    daily = (
        future.groupby(['date','market'], observed=True)
        .agg(
            行情列數=('code','size'),
            不重複代號數=('code','nunique'),
            有效OHLC列數=('OHLC有效','sum'),
            有名稱列數=('name',lambda x: int(x.fillna('').astype(str).str.strip().ne('').sum())),
        ).reset_index()
    )
    daily['市場日原始列覆蓋初步通過'] = daily['行情列數'] >= MIN_DAILY_ROWS_PER_MARKET
    daily['市場日有效OHLC覆蓋初步通過'] = daily['有效OHLC列數'] >= MIN_DAILY_ROWS_PER_MARKET
    # 這是觀察到日期中的雙市場同日覆蓋，不等同官方交易日曆完整性認證。
    date_market = daily.pivot(index='date', columns='market', values='有效OHLC列數').reset_index()
    for market in ('TWSE','TPEx'):
        if market not in date_market.columns:
            date_market[market] = 0
    date_market = date_market.rename(columns={'TWSE':'TWSE有效OHLC列數','TPEx':'TPEx有效OHLC列數'})
    date_market['雙市場同日均有有效OHLC'] = (
        date_market['TWSE有效OHLC列數'].fillna(0).ge(MIN_DAILY_ROWS_PER_MARKET)
        & date_market['TPEx有效OHLC列數'].fillna(0).ge(MIN_DAILY_ROWS_PER_MARKET)
    )

    # 候選在新行情期間可見的列數；不代表交易訊號或獲利。
    cand_keys = candidates.assign(_key=candidates['market'].astype(str)+'|'+candidates['code'].astype(str))['_key'].tolist()
    future['_key'] = future['market'].astype(str)+'|'+future['code'].astype(str)
    cand_future = future.loc[future['_key'].isin(set(cand_keys)) & future['OHLC有效']].groupby(['market','code'], observed=True).agg(
        新行情有效列數=('date','size'),
        新行情起日=('date','min'),
        新行情迄日=('date','max'),
    ).reset_index()
    cand_report = cand_report.merge(cand_future, on=['market','code'], how='left')
    cand_report['新行情有效列數'] = cand_report['新行情有效列數'].fillna(0).astype(int)
    cand_report['候選資料銜接可用'] = (
        cand_report['價格尺度穩定']
        & cand_report['現行可得名稱'].fillna('').ne('')
        & cand_report['最新官方行情日期'].notna()
        & cand_report['新行情有效列數'].gt(0)
    )
    cand_report = cand_report.sort_values(['候選資料銜接可用','market','code'], ascending=[False,True,True]).reset_index(drop=True)
    candidate_ready = int(cand_report['候選資料銜接可用'].sum())

    # -------------------------------------------------------------------------
    # 輸出報告
    # -------------------------------------------------------------------------
    log('[08/09] 整理資料銜接結論與報告……')
    out_dir.mkdir(parents=True, exist_ok=False)
    # 逐股尺度比率核對
    ratio_out = ratio_summary.sort_values(['價格尺度穩定','market','code'], ascending=[False,True,True])
    ratio_out.to_csv(out_dir/'02_v469對v417最近20日OHLC比例核對.csv', index=False, encoding='utf-8-sig')
    cand_report.to_csv(out_dir/'03_238檔候選近期資料銜接核對.csv', index=False, encoding='utf-8-sig')
    daily.to_csv(out_dir/'04_2026-10-23後雙市場逐日行情覆蓋.csv', index=False, encoding='utf-8-sig')
    date_market.to_csv(out_dir/'05_雙市場同日有效OHLC覆蓋.csv', index=False, encoding='utf-8-sig')

    source_inventory = pd.DataFrame([
        {'來源':'v4.69歷史影子行情','路徑':str(V469_PATH),'檔案大小bytes':V469_PATH.stat().st_size,'SHA256':v469_sha,'讀取資料列':len(v469),'起日':str(v469['date'].min().date()),'迄日':str(v469['date'].max().date())},
        {'來源':'v4.17官方行情主檔','路徑':str(A_PATH),'檔案大小bytes':a_stat.st_size,'SHA256':state.get('original_source_sha256'),'讀取近期香港列':len(a_recent),'起日':str(a_recent['date'].min().date()) if len(a_recent) else '', '迄日':str(a_recent['date'].max().date()) if len(a_recent) else ''},
        {'來源':'官方行情增量檔','路徑':str(DELTA_PATH),'檔案大小bytes':delta_stat.st_size,'SHA256':sha256_file(DELTA_PATH),'讀取列':len(delta),'起日':str(delta['date'].min().date()) if len(delta) else '', '迄日':str(delta['date'].max().date()) if len(delta) else ''},
        {'來源':'v1.2.3凍結238候選','路徑':str(cand_path),'檔案大小bytes':cand_path.stat().st_size,'SHA256':sha256_file(cand_path),'讀取候選數':len(candidates),'起日':'','迄日':''},
    ])
    source_inventory.to_csv(out_dir/'01_來源檔案與日期範圍.csv', index=False, encoding='utf-8-sig')

    active_keys = future[['market','code']].drop_duplicates()
    active_ratio = active_keys.merge(ratio_summary[['market','code','價格尺度穩定']], on=['market','code'], how='left')
    active_ratio['價格尺度穩定'] = active_ratio['價格尺度穩定'].fillna(False).astype(bool)
    stable_universe = int(active_ratio['價格尺度穩定'].sum())
    total_overlap_codes = len(active_ratio)
    stable_share = stable_universe / total_overlap_codes if total_overlap_codes else 0.0
    active_candidate_count = int(cand_future[['market','code']].drop_duplicates().shape[0]) if len(cand_future) else 0
    candidate_forward_share = active_candidate_count / len(candidates) if len(candidates) else 0.0
    # 分母固定使用凍結的238檔，不讓缺少新行情的股票從分母中消失。
    candidate_ready_share = candidate_ready / len(candidates) if len(candidates) else 0.0
    both_market_days = int(date_market['雙市場同日均有有效OHLC'].sum())
    total_future_days = len(date_market)
    ready = (
        active_candidate_count > 0
        and candidate_forward_share >= 0.95
        and candidate_ready_share >= 0.95
        and stable_share >= 0.95
        and total_future_days > 0
        and both_market_days == total_future_days
    )
    status = 'BRIDGE_READY' if ready else 'BRIDGE_NOT_READY_REQUIRES_REVIEW'
    state_dates = state.get('last_date_by_market', {})
    report = {
        '版本':'SSO_v1.2.4_獨立前瞻資料銜接預檢',
        '執行時間台北':datetime.now(TAIPEI).isoformat(),
        '狀態':status,
        'v1.2.3來源資料夾':v123_dir.name,
        'v1.2.3凍結候選數':len(candidates),
        'v4.69_SHA256':v469_sha,
        'v4.69_最後交易日':str(v469['date'].max().date()),
        'v4.17_SHA256_state':state.get('original_source_sha256'),
        'v4.17檔案大小bytes':a_stat.st_size,
        '增量檔列數':len(delta),
        '增量檔_SHA256':sha256_file(DELTA_PATH),
        '增量檔state最後日期':state_dates,
        '重疊檢查基準':f'每檔證券最近{OVERLAP_BARS}根有效K棒',
        '重疊日期範圍起':str(hist20['date'].min().date()),
        '重疊日期範圍迄':str(hist20['date'].max().date()),
        '重疊共同自然鍵數':len(joined),
        'OHLC比例穩定股票數':stable_universe,
        'OHLC比例核對股票數':total_overlap_codes,
        'OHLC比例穩定活躍證券數':stable_universe,
        'OHLC比例核對活躍證券數':total_overlap_codes,
        'OHLC比例穩定活躍證券占比':stable_share,
        'OHLC比例相對容許差':RATIO_REL_TOL,
        '238候選前瞻期間有有效OHLC數':active_candidate_count,
        '238候選前瞻有效OHLC占比':candidate_forward_share,
        '238候選銜接可用數':candidate_ready,
        '238候選銜接可用占比':candidate_ready_share,
        '238候選總數':EXPECTED_CANDIDATES,
        '2026-10-23後雙市場同日有有效OHLC日期數':both_market_days,
        '2026-10-23後觀察到的資料日期數':total_future_days,
        '2026-10-23後最新行情日期':str(future['date'].max().date()) if len(future) else None,
        '未執行項目':['未計算前瞻range_pos20_rank','未產生新訊號','未計算新H=10交易結果','未選股','未回測','未重訓'],
        '研究限制':[
            '本版只檢查 v4.69 影子OHLC與v4.17官方OHLC在最近20個交易日是否存在逐股穩定比例，不會因比例不穩定而強行拼接。',
            '價格尺度穩定是資料銜接必要條件，不代表公司行動、停牌、漲跌停成交可行性已完整核驗。',
            '現行公司名稱來自官方行情主檔可得欄位；不等同逐日歷史證券類別完整認證。',
            '雙市場同日覆蓋只針對觀察到至少一市場有列的日期；本版未獨立載入官方交易日曆，不能證明兩市場均無資料的日期不存在。',
            '本版不使用 v1.2.2 原測試期結果重選 238 檔候選。',
        ],
        '安全限制':['Drive來源唯讀','不呼叫官方API','不修改、不刪除、不移動、不覆寫既有資料','GitHub只建立獨立audit branch，不更新main'],
    }
    (out_dir/'06_前瞻資料銜接結論.json').write_text(json.dumps(report,ensure_ascii=False,indent=2,default=str),encoding='utf-8')

    summary_lines = [
        '='*96,
        'SSO v1.2.4｜2026-10-23後獨立前瞻驗證資料銜接預檢',
        '='*96,
        f"執行時間：{report['執行時間台北']}",
        f"預檢狀態：{status}",
        f"v1.2.3凍結候選：{EXPECTED_CANDIDATES}檔（不重新挑選）",
        f"v4.69歷史價格最後日：{v469['date'].max().date()}",
        f"官方行情增量最後日期：{state_dates}",
        f"每檔最近20根有效K棒比例核對共同自然鍵：{len(joined):,}",
        f"前瞻活躍證券價格尺度穩定：{stable_universe:,}/{total_overlap_codes:,}（{stable_share:.2%}）",
        f"238檔候選有前瞻有效OHLC：{active_candidate_count}（{candidate_forward_share:.2%}）；其中資料銜接可用：{candidate_ready}（{candidate_ready_share:.2%}）",
        f"觀察到的資料日期雙市場同日有效OHLC覆蓋：{both_market_days}/{total_future_days}",
        f"前瞻行情最新日：{report['2026-10-23後最新行情日期']}",
        '',
        '本版僅完成資料來源與價格尺度預檢，未產生交易訊號或淨報酬。',
        '只有 BRIDGE_READY 才能進入下一步計算 2026-10-23 後的前瞻訊號；否則先處理逐股價格尺度／日期缺口。',
        'Drive來源全程唯讀；不呼叫官方API；不改動既有資料。',
        '='*96,
    ]
    (out_dir/'07_執行摘要.txt').write_text('\n'.join(summary_lines)+'\n',encoding='utf-8-sig')
    return report

# -----------------------------------------------------------------------------
# ZIP / GitHub audit upload
# -----------------------------------------------------------------------------
def create_zip(out_dir: Path) -> Path:
    zip_path = out_dir.with_suffix('.zip')
    with zipfile.ZipFile(zip_path,'w',compression=zipfile.ZIP_DEFLATED,compresslevel=6) as z:
        for f in sorted(out_dir.iterdir()):
            if f.is_file():
                z.write(f,arcname=f.name)
    with zipfile.ZipFile(zip_path,'r') as z:
        bad = z.testzip()
        if bad:
            raise RuntimeError(f'ZIP CRC失敗：{bad}')
    log(f'[PASS] ZIP CRC：{zip_path.name} | {fmt_bytes(zip_path.stat().st_size)}')
    return zip_path


def find_token() -> str:
    token = os.environ.get('GITHUB_TOKEN','').strip() or os.environ.get('GH_TOKEN','').strip()
    if not token:
        try:
            from google.colab import userdata  # type: ignore
            token = (userdata.get('GITHUB_TOKEN') or '').strip()
        except Exception:
            token = ''
    if not token:
        raise RuntimeError('找不到 GITHUB_TOKEN；請確認既有 Colab Secret 已設定，不要將 Token 貼到程式碼或對話。')
    return token


def run_git(args: List[str], cwd: Optional[Path], env: Dict[str,str], input_bytes: Optional[bytes]=None) -> str:
    p = subprocess.run(['git',*args],cwd=str(cwd) if cwd else None,env=env,input=input_bytes,
                       stdout=subprocess.PIPE,stderr=subprocess.PIPE,check=False)
    if p.returncode:
        raise RuntimeError(f"git {' '.join(args[:5])} 失敗 code={p.returncode}\n{p.stderr.decode('utf-8','replace')[-5000:]}")
    return p.stdout.decode('utf-8','replace').strip()


def upload_github(out_dir: Path, zip_path: Path, script_path: Path, report: Dict[str,Any], run_stamp: str) -> Dict[str,Any]:
    token = find_token()
    upload_files = [p for p in sorted(out_dir.iterdir()) if p.is_file()] + [zip_path]
    too_large = [(p.name,p.stat().st_size) for p in upload_files if p.stat().st_size >= 100*1024*1024]
    if too_large:
        raise RuntimeError('單檔達 GitHub 100MB 限制：'+repr(too_large))
    v123_stamp = re.search(r'(\d{8}_\d{6})$', report.get('v1.2.3來源資料夾',''))
    source_stamp = v123_stamp.group(1) if v123_stamp else run_stamp
    branch = f"audit/sso-v1.2.4-forward-bridge-{source_stamp}-gitpush-{run_stamp[-6:]}-{uuid.uuid4().hex[:4]}"
    artifact_dir = f"audit/sso-v1.2.4-forward-validation-bridge/{source_stamp}/{run_stamp}"
    readme = [
        '# SSO v1.2.4｜獨立前瞻資料銜接預檢','',
        f"- 狀態：`{report.get('狀態')}`",
        f"- v1.2.3候選來源：`{report.get('v1.2.3來源資料夾')}`",
        f"- v4.69 SHA256：`{report.get('v4.69_SHA256')}`",
        '- 本版只做資料銜接與價格比例預檢，不計算前瞻交易報酬。',
        '- Google Drive來源唯讀；只建立獨立audit branch，不更新main。','',
        '## 檔案',
    ]
    for p in upload_files:
        readme.append(f"- `{p.name}` — {p.stat().st_size:,} bytes — SHA256 `{sha256_file(p)}`")
    readme_bytes = ('\n'.join(readme)+'\n').encode('utf-8')
    with tempfile.TemporaryDirectory(prefix='sso_v124_gitpush_') as td:
        temp=Path(td); bare=temp/'repo.git'; askpass=temp/'askpass.sh'
        askpass.write_text("#!/bin/sh\ncase \"$1\" in *Username*|*username*) echo x-access-token;; *Password*|*password*) printf '%s\\n' \"$GITHUB_TOKEN\";; *) echo;; esac\n",encoding='utf-8')
        askpass.chmod(0o700)
        env=os.environ.copy(); env.update({
            'GITHUB_TOKEN':token,'GIT_ASKPASS':str(askpass),'GIT_TERMINAL_PROMPT':'0',
            'GIT_CONFIG_NOSYSTEM':'1','GIT_AUTHOR_NAME':'SSO Audit Uploader',
            'GIT_AUTHOR_EMAIL':'sso-audit-uploader@users.noreply.github.com',
            'GIT_COMMITTER_NAME':'SSO Audit Uploader','GIT_COMMITTER_EMAIL':'sso-audit-uploader@users.noreply.github.com',
        })
        log('[GITHUB 01/06] 初始化暫存 Git bare repository……')
        run_git(['init','--bare',str(bare)],None,env)
        run_git(['remote','add','origin',REPO_URL],bare,env)
        log('[GITHUB 02/06] 從 main 取得 shallow partial history……')
        run_git(['-c','protocol.version=2','fetch','--depth=1','--filter=blob:none','origin',f'refs/heads/{DEFAULT_BRANCH}'],bare,env)
        main_sha=run_git(['rev-parse','FETCH_HEAD^{commit}'],bare,env)
        run_git(['read-tree',main_sha],bare,env)
        log(f'[PASS] main基準SHA：{main_sha}')
        log('[GITHUB 03/06] 將報告、ZIP及程式加入 Git objects……')
        items=[(p,f'{artifact_dir}/{p.name}',None) for p in upload_files]
        items.append((None,f'{artifact_dir}/README_GitHub上傳索引.md',readme_bytes))
        for i,(local,remote,inline) in enumerate(items,1):
            if local is not None:
                blob=run_git(['hash-object','-w',str(local)],bare,env)
                name=local.name; size=local.stat().st_size
            else:
                blob=run_git(['hash-object','-w','--stdin'],bare,env,inline)
                name='README_GitHub上傳索引.md'; size=len(inline or b'')
            run_git(['update-index','--add','--cacheinfo','100644',blob,remote],bare,env)
            log(f'[ADD {i}/{len(items)}] {name} | {fmt_bytes(size)}')
        log('[GITHUB 04/06] 建立 audit commit……')
        tree=run_git(['write-tree'],bare,env)
        commit=run_git(['-c','user.name=SSO Audit Uploader','-c','user.email=sso-audit-uploader@users.noreply.github.com',
                        'commit-tree',tree,'-p',main_sha,'-m',f'Add SSO v1.2.4 forward-data bridge audit ({source_stamp})'],bare,env)
        log('[GITHUB 05/06] 推送獨立 audit branch……')
        run_git(['push','origin',f'{commit}:refs/heads/{branch}'],bare,env)
        log('[GITHUB 06/06] 驗證遠端 audit branch 與 main……')
        remote=run_git(['ls-remote','origin',f'refs/heads/{branch}'],bare,env).split()
        if not remote or remote[0]!=commit:
            raise RuntimeError('GitHub遠端audit branch SHA驗證失敗。')
        after=run_git(['ls-remote','origin',f'refs/heads/{DEFAULT_BRANCH}'],bare,env).split()
        main_after=after[0] if after else ''
        if main_after!=main_sha:
            raise RuntimeError(f'main SHA 前後不同：{main_sha} -> {main_after}；本程式沒有對main push。')
        env['GITHUB_TOKEN']=''
    return {
        'branch':branch,'commit':commit,'main_before':main_sha,'main_after':main_after,
        'folder_url':f'https://github.com/{REPO_OWNER}/{REPO_NAME}/tree/{branch}/{artifact_dir}',
        'zip_url':f'https://github.com/{REPO_OWNER}/{REPO_NAME}/blob/{branch}/{artifact_dir}/{quote(zip_path.name)}',
        'commit_url':f'https://github.com/{REPO_OWNER}/{REPO_NAME}/commit/{commit}',
        'uploaded_files':len(items),
    }

# -----------------------------------------------------------------------------
# 執行入口
# -----------------------------------------------------------------------------
def main() -> int:
    print('='*104,flush=True)
    log('SSO v1.2.4｜2026-10-23後獨立前瞻驗證資料銜接預檢')
    log('[安全] Drive來源唯讀；不呼叫官方API；不修改、不刪除、不移動、不覆寫。')
    log('[安全] GitHub只建立獨立audit branch，不更新main。')

    if not ROOT.is_dir():
        try:
            from google.colab import drive  # type: ignore
            drive.mount('/content/drive',force_remount=False)
        except Exception as e:
            raise RuntimeError(f'無法掛載Google Drive：{e}')
    if not ROOT.is_dir():
        raise RuntimeError(f'找不到Drive根目錄：{ROOT}')

    v123_dir=find_v123_folder()
    log(f'[SOURCE] v1.2.3候選結果：{v123_dir}')
    run_stamp=stamp()
    out_dir=ROOT/f'看股最速適_v1_2_4_獨立前瞻資料銜接預檢_{run_stamp}'
    report=main_audit(v123_dir,out_dir)
    report_path=out_dir/'06_前瞻資料銜接結論.json'
    log('[PASS] 資料銜接報告已產生。')
    print((out_dir/'07_執行摘要.txt').read_text(encoding='utf-8-sig'),flush=True)

    # 將程式本身存入輸出資料夾，便於可重現性；不修改上傳原檔。
    try:
        script_src=Path(__file__).resolve()
        if script_src.is_file():
            shutil.copy2(script_src,out_dir/script_src.name)
    except Exception:
        pass
    zip_path=create_zip(out_dir)
    log('[09/09] 上傳本次結果至GitHub audit branch……')
    upload=upload_github(out_dir,zip_path,Path(__file__).resolve(),report,run_stamp)
    report['GitHub上傳']=upload
    # JSON/summary is already in ZIP; upload index is in GitHub README. Do not mutate Drive output after archive.
    print('='*104,flush=True)
    print('[PASS] GitHub上傳完成',flush=True)
    print('Repository：CHLINX/sso',flush=True)
    print('Audit branch：',upload['branch'],flush=True)
    print('Commit：',upload['commit'],flush=True)
    print('上傳檔案數：',upload['uploaded_files'],flush=True)
    print('Drive資料夾：',out_dir,flush=True)
    print('GitHub資料夾：',upload['folder_url'],flush=True)
    print('GitHub ZIP：',upload['zip_url'],flush=True)
    print('GitHub Commit：',upload['commit_url'],flush=True)
    print('main SHA：',upload['main_before'],flush=True)
    print('[PASS] main未更新；Drive來源唯讀。',flush=True)
    print('='*104,flush=True)
    return 0


if __name__=='__main__':
    raise SystemExit(main())
