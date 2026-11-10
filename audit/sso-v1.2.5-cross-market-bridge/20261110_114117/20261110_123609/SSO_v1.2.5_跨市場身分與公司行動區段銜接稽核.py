#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""SSO v1.2.5｜跨市場身分與公司行動區段銜接稽核

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


def self_test() -> None:
    """快速自測：日期、動態市場欄位及跨市場 code+date 配對。"""
    markets = normalize_market_series(pd.Series(['上市', '上櫃', 'TWSE', 'TPEX']))
    assert markets.tolist() == ['TWSE', 'TPEx', 'TWSE', 'TPEx']
    dates = parse_date_series(pd.Series(['2026-10-22', '115/11/06']))
    assert dates.iloc[0] == pd.Timestamp('2026-10-22')
    assert dates.iloc[1] == pd.Timestamp('2026-11-06')

    shadow = pd.DataFrame([{
        'code': '1795', 'date': pd.Timestamp('2026-10-22'),
        'market': 'TPEx', 'segment': 'SEG-A',
        'open': 100.0, 'high': 102.0, 'low': 99.0, 'close': 101.0
    }])
    official = pd.DataFrame([{
        'code': '1795', 'date': pd.Timestamp('2026-10-22'),
        'market': 'TWSE', 'segment': '',
        'open': 100.0, 'high': 102.0, 'low': 99.0, 'close': 101.0
    }])
    paired = shadow.merge(
        official, on=['code', 'date'], how='inner',
        suffixes=('_shadow', '_official'), validate='one_to_one'
    )
    assert len(paired) == 1
    row = next(paired.itertuples(index=False))
    assert getattr(row, 'market_shadow') == 'TPEx'
    assert getattr(row, 'market_official') == 'TWSE'
    assert getattr(row, 'segment_shadow') == 'SEG-A'
    ratios = [
        getattr(row, f'{f}_shadow') / getattr(row, f'{f}_official')
        for f in ('open', 'high', 'low', 'close')
    ]
    assert max(ratios) - min(ratios) < RATIO_REL_TOL
    log('[SELF-TEST] PASS：日期解析、跨市場代號+日期配對、欄位後綴及OHLC同尺度檢查。')


def main_audit(v123_dir: Path, out_dir: Path) -> Dict[str, Any]:
    """
    v1.2.5 修正重點：
    - 證券身分以代號為主，市場別為日期變動欄位；跨市場移轉時不再用舊市場欄位阻斷配對。
    - 比對 v4.69 / v4.17 時使用 code + date，並保留兩側市場欄位供稽核。
    - 價格尺度檢查拆成「最近歷史視窗」與「最新重疊日的 OHLC 一致比例」。
    - 不因歷史窗口內曾發生除息而直接判定未來價格不能銜接；記錄已知公司行動並輸出最新日尺度因子。
    - 依 v4.69 最後一個價格研究區段估算暖機根數；不跨區段湊足20根。
    - 本版仍只做資料銜接稽核，不產生新訊號、不計算未來淨報酬。
    """
    self_test()
    if not V469_PATH.is_file() or not A_PATH.is_file() or not STATE_PATH.is_file() or not DELTA_PATH.is_file():
        missing = [str(p) for p in (V469_PATH, A_PATH, STATE_PATH, DELTA_PATH) if not p.is_file()]
        raise FileNotFoundError('缺少前瞻驗證所需來源：\n' + '\n'.join(missing))

    # 1. 固定來源指紋與狀態核對
    log('[01/10] 驗證 v4.69 SHA256……')
    v469_sha = sha256_file(V469_PATH)
    if v469_sha != V469_SHA256:
        raise RuntimeError(f'v4.69 SHA256 不符：{v469_sha}')
    log('[PASS] v4.69 SHA256 相符。')

    log('[02/10] 核對 v4.17 官方主檔與增量資料中繼資料……')
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

    # 2. 凍結候選，禁止按測試期報酬調整
    log('[03/10] 讀取 v1.2.3 凍結的238檔候選……')
    cand_path = v123_dir / V123_CANDIDATE_FILE
    cand = pd.read_csv(cand_path, dtype='string', encoding='utf-8-sig', low_memory=False)
    mcol = find_col(cand.columns, ['市場'])
    ccol = find_col(cand.columns, ['股票代號','證券代號'])
    candidates = cand[[mcol, ccol]].copy()
    candidates.columns = ['候選來源市場','code']
    candidates['候選來源市場'] = normalize_market_series(candidates['候選來源市場'])
    candidates['code'] = clean_code_series(candidates['code'])
    candidates = candidates.drop_duplicates().reset_index(drop=True)
    if len(candidates) != EXPECTED_CANDIDATES:
        raise RuntimeError(f'凍結候選數不符：預期 {EXPECTED_CANDIDATES}，實際 {len(candidates)}')
    duplicate_codes = int(candidates['code'].duplicated(keep=False).sum())
    candidate_codes = set(candidates['code'].dropna().astype(str))
    if candidates['code'].isna().any() or candidates['code'].eq('').any():
        raise RuntimeError('凍結候選存在空白股票代號，停止銜接。')
    if candidates['候選來源市場'].isna().any():
        raise RuntimeError('凍結候選存在無法辨識的來源市場欄位，停止銜接。')
    if len(candidate_codes) != EXPECTED_CANDIDATES:
        raise RuntimeError(f'凍結候選代號非一對一：候選列數={len(candidates)}，唯一代號數={len(candidate_codes)}；不自動合併。')
    log(f'[PASS] 凍結候選 {len(candidates)} 檔／{len(candidate_codes)} 個唯一代號；不重新挑選。')

    # 3. 讀取增量；自然鍵以市場+代號+日期檢查，市場轉換保留為時變欄位
    log('[04/10] 讀取增量行情並驗證自然鍵……')
    a_map = source_mapping(A_PATH, 'official')
    delta_map = source_mapping(DELTA_PATH, 'official')
    delta = load_delta(DELTA_PATH, delta_map)
    raw_delta_count = len(delta)
    delta_dups = int(delta.duplicated(['market','code','date']).sum())
    if delta_dups:
        raise RuntimeError(f'增量行情自然鍵（市場+代號+日期）重複 {delta_dups:,} 列。')
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

    # 4. 讀取歷史影子價與官方價；只保留238檔候選代號，市場不作為證券身分鍵
    log('[05/10] 讀取 v4.69／v4.17 候選代號近期行情（跨市場代號銜接）……')
    vmap = source_mapping(V469_PATH, 'v469')
    v469_all = read_recent(V469_PATH, vmap, OVERLAP_LOOKBACK_START, V469_END_EXPECTED, 'v4.69')
    v469_all = v469_all.loc[
        v469_all['code'].astype(str).isin(candidate_codes)
        & v469_all['date'].dt.weekday.lt(5)
    ].copy()
    if v469_all.empty:
        raise RuntimeError('v4.69 凍結候選近期行情為空。')
    v469_dates = sorted(pd.Timestamp(x).normalize() for x in v469_all['date'].dropna().unique())
    if not v469_dates or v469_dates[-1] != V469_END_EXPECTED:
        raise RuntimeError(f'v4.69 最晚日期不是 2026-10-22：{v469_dates[-1] if v469_dates else None}')

    a_recent_all = read_recent(A_PATH, a_map, OVERLAP_LOOKBACK_START, None, 'v4.17')
    # 只留下凍結候選代號；之後可用 code+date 識別跨市場移轉
    a_recent = a_recent_all.loc[a_recent_all['code'].astype(str).isin(candidate_codes)].copy()
    delta_candidate = delta.loc[delta['code'].astype(str).isin(candidate_codes)].copy()
    official_candidate = pd.concat([a_recent, delta_candidate], ignore_index=True)
    # 與 v4.69 的工作日序列口徑一致；不讓歷史星期六補行日
    # 參與 code-date 歧義判定或 20 根 K 棒銜接。
    official_candidate = official_candidate.loc[
        official_candidate['date'].notna()
        & official_candidate['date'].dt.weekday.lt(5)
    ].copy()

    # 全來源精確自然鍵不可重複
    official_mkt_dups = int(official_candidate.duplicated(['market','code','date']).sum())
    if official_mkt_dups:
        raise RuntimeError(f'v4.17 + 增量候選行情市場自然鍵重複 {official_mkt_dups:,} 列。')

    # 跨市場同代號同日期若同時存在兩筆，視為歧義，不自動挑一筆
    official_code_date_dups = official_candidate.duplicated(['code','date'], keep=False)
    ambiguous_official = set(
        official_candidate.loc[official_code_date_dups, 'code'].astype(str).unique()
    )
    v469_code_date_dups = v469_all.duplicated(['code','date'], keep=False)
    ambiguous_v469 = set(v469_all.loc[v469_code_date_dups, 'code'].astype(str).unique())
    ambiguous_codes = ambiguous_official | ambiguous_v469

    # 5. OHLC有效性；最近20根以證券代號排序，不按歷史市場拆成兩檔
    def valid_ohlc(frame: pd.DataFrame) -> pd.Series:
        x = frame[['open','high','low','close']].apply(pd.to_numeric, errors='coerce')
        mask = (
            x.gt(0).all(axis=1)
            & x['low'].le(x[['open','close']].min(axis=1))
            & x['high'].ge(x[['open','close']].max(axis=1))
            & x['low'].le(x['high'])
        )
        if 'ohlc_valid_source' in frame.columns:
            sv = frame['ohlc_valid_source'].astype('string').fillna('').str.strip().str.lower()
            mask &= ~sv.isin(['false','0','no','n','否','無效'])
        return mask.fillna(False)

    v469_all['OHLC有效'] = valid_ohlc(v469_all)
    official_candidate['OHLC有效'] = valid_ohlc(official_candidate)

    # 有重複 code/date 的代號不參與自動銜接；仍列出供人工核對
    v469_usable = v469_all.loc[
        v469_all['OHLC有效']
        & ~v469_all['code'].astype(str).isin(ambiguous_codes)
    ].copy()
    official_usable = official_candidate.loc[
        official_candidate['OHLC有效']
        & ~official_candidate['code'].astype(str).isin(ambiguous_codes)
    ].copy()

    # 最近20根有效K棒：只按 code，不因 market 變動切斷證券身分
    v469_usable = v469_usable.sort_values(['code','date','market'], kind='mergesort')
    hist20 = (
        v469_usable.groupby('code', group_keys=False, observed=True)
        .tail(OVERLAP_BARS).copy()
    )
    # 每檔若同日重複仍應被視為不明確；前面已排除 ambiguous_codes
    hist20 = hist20.sort_values(['code','date'], kind='mergesort')

    official_overlap = official_usable.loc[
        official_usable['date'].le(V469_END_EXPECTED)
        & official_usable['code'].isin(set(hist20['code'].astype(str)))
    ].copy()
    joined = hist20.merge(
        official_overlap,
        on=['code','date'],
        how='inner',
        suffixes=('_shadow','_official'),
        validate='one_to_one'
    )
    if joined.empty:
        raise RuntimeError('v4.69 與 v4.17 使用 code+date 跨市場配對後，仍沒有共同自然鍵。')

    # 6. 逐日、逐欄計算 shadow / official 比例；市場差異保留，不作 join 條件
    ratio_rows = []
    for row in joined.itertuples(index=False):
        item = {
            'code': str(row.code),
            'date': pd.Timestamp(row.date).normalize(),
            'shadow_market': getattr(row, 'market_shadow'),
            'official_market': getattr(row, 'market_official'),
            'segment': str(getattr(row, 'segment_shadow')),
        }
        field_ratios = []
        for field in ('open','high','low','close'):
            left = float(getattr(row, f'{field}_shadow'))
            right = float(getattr(row, f'{field}_official'))
            ratio = left / right if right > 0 else np.nan
            item[f'{field}_ratio'] = ratio
            if np.isfinite(ratio) and ratio > 0:
                field_ratios.append(ratio)
        if field_ratios:
            item['日OHLC比例中位數'] = float(np.median(field_ratios))
            item['日OHLC比例最大相對偏差'] = float(
                max(abs(x / np.median(field_ratios) - 1.0) for x in field_ratios)
            )
        else:
            item['日OHLC比例中位數'] = np.nan
            item['日OHLC比例最大相對偏差'] = np.nan
        item['當日市場欄位不同'] = bool(item['shadow_market'] != item['official_market'])
        ratio_rows.append(item)

    ratio_daily = pd.DataFrame(ratio_rows).sort_values(['code','date']).reset_index(drop=True)
    ratio_summary_rows = []
    for code, g in ratio_daily.groupby('code', sort=False):
        g = g.sort_values('date')
        last = g.iloc[-1]
        last_factor = float(last['日OHLC比例中位數'])
        last_spread = float(last['日OHLC比例最大相對偏差'])
        last3 = g.tail(3)
        last3_ratios = last3['日OHLC比例中位數'].dropna().to_numpy(dtype=float)
        last3_spread = (
            float(np.max(np.abs(last3_ratios / last_factor - 1.0)))
            if len(last3_ratios) and np.isfinite(last_factor) and last_factor > 0 else np.nan
        )
        ratio_summary_rows.append({
            'code': code,
            '共同比對日期數': int(g['date'].nunique()),
            '比對最早日': str(g['date'].min().date()),
            '比對最晚日': str(g['date'].max().date()),
            '歷史視窗比例中位數': float(g['日OHLC比例中位數'].median()),
            '歷史視窗比例最小值': float(g['日OHLC比例中位數'].min()),
            '歷史視窗比例最大值': float(g['日OHLC比例中位數'].max()),
            '最新重疊日': str(pd.Timestamp(last['date']).date()),
            '最新重疊日等於v4.69最後日': bool(pd.Timestamp(last['date']).normalize() == V469_END_EXPECTED),
            '最新重疊日市場（v4.69）': last['shadow_market'],
            '最新重疊日市場（v4.17）': last['official_market'],
            '最新日OHLC比例中位數': last_factor,
            '最新日OHLC四欄最大相對偏差': last_spread,
            '最新三個重疊日對最新尺度最大偏差': last3_spread,
            '歷史視窗內比例不同日數': int((g['日OHLC比例最大相對偏差'] > RATIO_REL_TOL).sum())
                + int((np.abs(g['日OHLC比例中位數'] / last_factor - 1.0) > RATIO_REL_TOL).sum()),
            '最新市場欄位不同': bool(last['shadow_market'] != last['official_market']),
            '最新尺度可銜接': bool(
                np.isfinite(last_factor) and last_factor > 0
                and np.isfinite(last_spread) and last_spread <= RATIO_REL_TOL
                and pd.Timestamp(last['date']).normalize() == V469_END_EXPECTED
            ),
        })
    ratio_summary = pd.DataFrame(ratio_summary_rows)

    # 7. 最新價格區段暖機統計；只用最新區段，不跨段湊足20根
    segment_rows = []
    v469_valid_sorted = v469_usable.sort_values(['code','date'], kind='mergesort')
    for code, g in v469_valid_sorted.groupby('code', sort=False):
        g = g.sort_values('date').reset_index(drop=True)
        if len(g) == 0:
            continue
        latest = g.iloc[-1]
        latest_segment = str(latest.get('segment') or '').strip()
        if not latest_segment:
            current_segment_bars = 0
            segment_start = None
            segment_complete = False
        else:
            same = g['segment'].astype(str).str.strip().eq(latest_segment)
            idx = np.flatnonzero(same.to_numpy())
            # 最新價格研究區段的連續尾段；避免同名 segment 在前段重用造成誤計
            last_idx = len(g) - 1
            first_idx = last_idx
            while first_idx > 0 and str(g.loc[first_idx - 1, 'segment']).strip() == latest_segment:
                first_idx -= 1
            current_segment_bars = int(last_idx - first_idx + 1)
            segment_start = str(pd.Timestamp(g.loc[first_idx, 'date']).date())
            segment_complete = True
        segment_rows.append({
            'code': code,
            'v4.69最後日期': str(pd.Timestamp(latest['date']).date()),
            'v4.69最後市場': latest['market'],
            '最新價格研究區段': latest_segment,
            '最新區段起日': segment_start,
            '最新區段有效K棒數': current_segment_bars,
            '最新區段標籤完整': segment_complete,
        })
    segment_summary = pd.DataFrame(segment_rows)

    # 8. 前瞻官方行情：以 code 為證券身分，市場別允許隨日期變化
    log('[06/10] 讀取前瞻官方行情並按代號跨市場銜接……')
    all_official = pd.concat([a_recent_all, delta], ignore_index=True)
    # 全來源精確市場自然鍵不能重複
    all_official_dups = int(all_official.duplicated(['market','code','date']).sum())
    if all_official_dups:
        raise RuntimeError(f'A/v4.17 + 增量檔市場自然鍵重複 {all_official_dups:,} 列。')
    all_official = all_official.loc[
        all_official['date'].ge(FORWARD_START)
        & all_official['date'].dt.weekday.lt(5)
    ].copy()
    all_official['OHLC有效'] = valid_ohlc(all_official)
    candidate_future = all_official.loc[
        all_official['code'].astype(str).isin(candidate_codes)
        & all_official['OHLC有效']
    ].copy()

    future_code_date_dups = candidate_future.duplicated(['code','date'], keep=False)
    ambiguous_future = set(candidate_future.loc[future_code_date_dups,'code'].astype(str).unique())

    # 逐股合併候選靜態資訊、最新市場、前瞻行數、暖機
    candidate_rows = []
    seg_map = {r['code']: r for r in segment_rows}
    ratio_map = {r['code']: r for r in ratio_summary_rows}
    for cand_row in candidates.to_dict('records'):
        code = str(cand_row['code'])
        cfuture = candidate_future.loc[candidate_future['code'].astype(str).eq(code)].sort_values(['date','market'])
        n_future = int(cfuture['date'].nunique())
        first_future = str(cfuture['date'].min().date()) if n_future else ''
        last_future = str(cfuture['date'].max().date()) if n_future else ''
        latest_future_market = str(cfuture.iloc[-1]['market']) if n_future else ''
        latest_official_rows = official_candidate.loc[official_candidate['code'].astype(str).eq(code)].sort_values(['date','market'])
        latest_official_market = str(latest_official_rows.iloc[-1]['market']) if len(latest_official_rows) else ''
        latest_v469_market = str(seg_map.get(code, {}).get('v4.69最後市場',''))
        segment_info = seg_map.get(code, {})
        ratio_info = ratio_map.get(code, {})
        base_bars = int(segment_info.get('最新區段有效K棒數',0) or 0)
        total_bars = base_bars + n_future
        ready_date = ''
        if total_bars >= OVERLAP_BARS and base_bars < OVERLAP_BARS:
            need = OVERLAP_BARS - base_bars
            valid_future_dates = sorted(cfuture['date'].drop_duplicates())
            if len(valid_future_dates) >= need:
                ready_date = str(pd.Timestamp(valid_future_dates[need - 1]).date())
        elif base_bars >= OVERLAP_BARS and n_future:
            ready_date = first_future

        ambiguous = code in ambiguous_codes or code in ambiguous_future
        latest_scale_ok = bool(ratio_info.get('最新尺度可銜接',False))
        if ambiguous:
            status = 'REVIEW_AMBIGUOUS_CODE_DATE'
        elif not latest_scale_ok:
            status = 'REVIEW_LATEST_PRICE_SCALE'
        elif n_future == 0:
            status = 'NO_FUTURE_OHLC'
        elif total_bars < OVERLAP_BARS:
            status = 'WAITING_FOR_20_VALID_KBARS'
        else:
            status = 'READY_FOR_FORWARD_FEATURE_CALCULATION'

        market_sequence = []
        if len(latest_official_rows):
            market_sequence = list(dict.fromkeys(latest_official_rows.sort_values('date')['market'].astype(str).tolist()))
        market_changed = bool(
            cand_row['候選來源市場'] and latest_official_market
            and str(cand_row['候選來源市場']) != latest_official_market
        )
        candidate_rows.append({
            '候選來源市場':cand_row['候選來源市場'],
            '股票代號':code,
            'v4.69最後市場':latest_v469_market,
            'v4.17及增量最新市場':latest_official_market,
            '前瞻最新市場':latest_future_market,
            '市場序列':'>'.join(market_sequence),
            '候選來源市場與現行市場不同':market_changed,
            '跨市場轉換已恢復代號銜接':bool(market_changed and n_future > 0 and latest_scale_ok),
            'v4.69最新日期':segment_info.get('v4.69最後日期',''),
            'v4.69最新區段起日':segment_info.get('最新區段起日',''),
            'v4.69最新區段有效K棒數':base_bars,
            '最新重疊日':ratio_info.get('最新重疊日',''),
            '最新日OHLC比例中位數':ratio_info.get('最新日OHLC比例中位數',np.nan),
            '前瞻OHLC需乘價格尺度因子':ratio_info.get('最新日OHLC比例中位數',np.nan),
            '最新日OHLC四欄最大相對偏差':ratio_info.get('最新日OHLC四欄最大相對偏差',np.nan),
            '歷史視窗比例不同日數':ratio_info.get('歷史視窗內比例不同日數',np.nan),
            '前瞻有效OHLC列數':n_future,
            '前瞻有效OHLC起日':first_future,
            '前瞻有效OHLC迄日':last_future,
            '最新區段加前瞻有效K棒數':total_bars,
            '距20根K棒尚缺':max(0, OVERLAP_BARS - total_bars),
            '預計首次可計算range_pos20日期':ready_date,
            '代號日期鍵歧義':ambiguous,
            '候選資料狀態':status,
        })

    candidate_report = pd.DataFrame(candidate_rows).sort_values(
        ['候選資料狀態','候選來源市場','股票代號']
    ).reset_index(drop=True)

    # 市場轉換明細：將同代號在來源期間內的市場欄位變化明列
    migration_rows = []
    for code, g in official_candidate.sort_values('date').groupby('code', sort=False):
        g = g.sort_values('date')
        markets = g[['date','market']].drop_duplicates().reset_index(drop=True)
        prev = None
        for row in markets.itertuples(index=False):
            current = str(row.market)
            if prev is not None and current != prev:
                migration_rows.append({
                    '股票代號':code,
                    '市場切換日期':str(pd.Timestamp(row.date).date()),
                    '前市場':prev,
                    '新市場':current,
                    '資料來源':'v4.17 + append-only official delta',
                })
            prev = current
    migration_report = pd.DataFrame(migration_rows)

    # 市場/日期全域行情覆蓋，仍以市場+代號+日期自然鍵定義
    log('[07/10] 核對前瞻雙市場逐日覆蓋……')
    future = all_official.copy()
    future_ohlc = future[['open','high','low','close']].apply(pd.to_numeric, errors='coerce')
    future['OHLC有效'] = (
        future_ohlc.gt(0).all(axis=1)
        & future_ohlc['low'].le(future_ohlc[['open','close']].min(axis=1))
        & future_ohlc['high'].ge(future_ohlc[['open','close']].max(axis=1))
        & future_ohlc['low'].le(future_ohlc['high'])
    )
    daily = (
        future.groupby(['date','market'], observed=True)
        .agg(行情列數=('code','size'),不重複代號數=('code','nunique'),
             有效OHLC列數=('OHLC有效','sum'),
             有名稱列數=('name',lambda x:int(x.fillna('').astype(str).str.strip().ne('').sum())))
        .reset_index()
    )
    daily['市場日原始列覆蓋初步通過'] = daily['行情列數'] >= MIN_DAILY_ROWS_PER_MARKET
    daily['市場日有效OHLC覆蓋初步通過'] = daily['有效OHLC列數'] >= MIN_DAILY_ROWS_PER_MARKET
    date_market = daily.pivot(index='date',columns='market',values='有效OHLC列數').reset_index()
    for market in ('TWSE','TPEx'):
        if market not in date_market.columns:
            date_market[market] = 0
    date_market = date_market.rename(columns={'TWSE':'TWSE有效OHLC列數','TPEx':'TPEx有效OHLC列數'})
    date_market['雙市場同日均有有效OHLC'] = (
        date_market['TWSE有效OHLC列數'].fillna(0).ge(MIN_DAILY_ROWS_PER_MARKET)
        & date_market['TPEx有效OHLC列數'].fillna(0).ge(MIN_DAILY_ROWS_PER_MARKET)
    )

    # 9. 證據索引（官方公告/市場資料）。僅供解讀，不自動改寫或校正任何行情。
    event_evidence = pd.DataFrame([
        {'代號':'1795','事件類型':'櫃轉市','事件日期':'2019-12-16','說明':'美時上市；歷史候選市場欄位可能仍保留舊市場。','來源URL':'https://www.twse.com.tw/company/newlisting?response=html'},
        {'代號':'3563','事件類型':'櫃轉市','事件日期':'2019-04-02','說明':'牧德上市。','來源URL':'https://www.twse.com.tw/pcversion/staticFiles/product/publication/twse60/P17.pdf'},
        {'代號':'4736','事件類型':'櫃轉市','事件日期':'2023-12-22','說明':'泰博開始上市買賣。','來源URL':'https://www.twse.com.tw/staticFiles/news/news/tsecnews/ff8080818c3d4c4f018c8bc2962801ec.pdf'},
        {'代號':'6426','事件類型':'櫃轉市','事件日期':'2021-03-24','說明':'統新開始上市買賣。','來源URL':'https://www.twse.com.tw/rwd/zh/company/applylisting?response=html'},
        {'代號':'6438','事件類型':'櫃轉市','事件日期':'2021-01-19','說明':'迅得開始上市買賣。','來源URL':'https://www.twse.com.tw/rwd/zh/company/applylisting?response=html'},
        {'代號':'8908','事件類型':'櫃轉市','事件日期':'2026-10-16','說明':'欣雄自該日起終止櫃買並於TWSE上市。','來源URL':'https://dsp.tpex.org.tw/web/announcement/announcement_detail.php?content_file=MTE1MDA3NjkzOTEuaHRtbA%3D%3D&content_number=MTE1MDA3NjkzOTE%3D'},
        {'代號':'5301','事件類型':'減資換發新股','事件日期':'2026-10-14','說明':'寶得利新股開始上櫃買賣；原股最後交易日2026-10-06，應依新價格區段暖機。','來源URL':'https://goodinfo.tw/tw/StockAnnounceDetail.asp?CLAIM_TIME=2026%2F08%2F14+17%3A22%3A59&STOCK_ID=5301'},
        {'代號':'6171','事件類型':'除息','事件日期':'2026-10-02','說明':'大城地產每股現金股利2元。','來源URL':'https://m.esunsec.com.tw/news/instant-detail.aspx?id=%7B51408E23-FA9E-4313-AA17-0FFD88609B7C%7D'},
        {'代號':'8936','事件類型':'除息','事件日期':'2026-10-22','說明':'國統每股現金股利3元。','來源URL':'https://goodinfo.tw/tw/StockAnnounceDetail.asp?CLAIM_TIME=2026%2F09%2F10+18%3A04%3A57&STOCK_ID=8936'},
        {'代號':'1808','事件類型':'除息','事件日期':'2026-09-23','說明':'潤隆每股現金股利1.5元。','來源URL':'https://goodinfo.tw/tw/StockAnnounceDetail.asp?CLAIM_TIME=2026%2F08%2F10+14%3A08%3A00&STOCK_ID=1808'},
    ])

    # 輸出
    log('[08/10] 輸出跨市場、公司行動與暖機稽核報告……')
    out_dir.mkdir(parents=True, exist_ok=False)
    source_inventory = pd.DataFrame([
        {'來源':'v4.69歷史影子行情','路徑':str(V469_PATH),'檔案大小bytes':V469_PATH.stat().st_size,'SHA256':v469_sha,'讀取候選近期列':len(v469_all),'起日':str(v469_all['date'].min().date()),'迄日':str(v469_all['date'].max().date())},
        {'來源':'v4.17官方行情主檔','路徑':str(A_PATH),'檔案大小bytes':a_stat.st_size,'SHA256':state.get('original_source_sha256'),'讀取近期香港全市場列':len(a_recent_all),'讀取候選列':len(a_recent),'起日':str(a_recent_all['date'].min().date()) if len(a_recent_all) else '', '迄日':str(a_recent_all['date'].max().date()) if len(a_recent_all) else ''},
        {'來源':'官方行情增量檔','路徑':str(DELTA_PATH),'檔案大小bytes':delta_stat.st_size,'SHA256':sha256_file(DELTA_PATH),'全檔讀取列':len(delta),'候選代號列':len(delta_candidate),'起日':str(delta['date'].min().date()) if len(delta) else '', '迄日':str(delta['date'].max().date()) if len(delta) else ''},
        {'來源':'v1.2.3凍結238候選','路徑':str(cand_path),'檔案大小bytes':cand_path.stat().st_size,'SHA256':sha256_file(cand_path),'讀取候選數':len(candidates),'起日':'','迄日':''},
    ])
    source_inventory.to_csv(out_dir/'01_來源檔案與日期範圍.csv',index=False,encoding='utf-8-sig')
    candidate_report.to_csv(out_dir/'02_238檔候選跨市場資料銜接.csv',index=False,encoding='utf-8-sig')
    ratio_daily.sort_values(['code','date']).to_csv(out_dir/'03_v469對v417逐日OHLC比例與市場別.csv',index=False,encoding='utf-8-sig')
    migration_report.to_csv(out_dir/'04_證券市場轉換事件.csv',index=False,encoding='utf-8-sig')
    daily.to_csv(out_dir/'05_前瞻雙市場逐日行情覆蓋.csv',index=False,encoding='utf-8-sig')
    date_market.to_csv(out_dir/'06_雙市場同日OHLC覆蓋.csv',index=False,encoding='utf-8-sig')
    event_evidence.to_csv(out_dir/'07_已知公司行動與市場事件證據.csv',index=False,encoding='utf-8-sig')

    status_counts = candidate_report['候選資料狀態'].value_counts(dropna=False).to_dict()
    ready_count = int((candidate_report['候選資料狀態']=='READY_FOR_FORWARD_FEATURE_CALCULATION').sum())
    waiting_count = int((candidate_report['候選資料狀態']=='WAITING_FOR_20_VALID_KBARS').sum())
    review_count = int(candidate_report['候選資料狀態'].astype(str).str.startswith('REVIEW').sum())
    no_future_count = int((candidate_report['候選資料狀態']=='NO_FUTURE_OHLC').sum())
    market_changed_count = int(candidate_report['候選來源市場與現行市場不同'].sum())
    market_recovered_count = int(candidate_report['跨市場轉換已恢復代號銜接'].sum())
    total_future_days = int(date_market.shape[0])
    both_days = int(date_market['雙市場同日均有有效OHLC'].sum())
    latest_forward = str(all_official.loc[all_official['date'].ge(FORWARD_START),'date'].max().date()) if len(all_official.loc[all_official['date'].ge(FORWARD_START)]) else None

    # 本版不宣稱全市場Q5橫斷面完成；只判定238檔候選的逐股資料可否銜接
    if review_count > 0 or no_future_count > 0:
        overall_status = 'CANDIDATE_BRIDGE_PARTIAL_REQUIRES_REVIEW'
    elif waiting_count > 0:
        overall_status = 'CANDIDATE_BRIDGE_READY_WITH_WARMUP_PENDING'
    else:
        overall_status = 'CANDIDATE_BRIDGE_READY_FULL_MARKET_Q5_STILL_UNVERIFIED'

    report = {
        '版本':'SSO_v1.2.5_跨市場身分與公司行動區段銜接稽核',
        '執行時間台北':datetime.now(TAIPEI).isoformat(),
        '整體狀態':overall_status,
        'v1.2.3來源資料夾':v123_dir.name,
        'v1.2.3凍結候選數':len(candidates),
        '凍結候選代號數':len(candidate_codes),
        '候選重複代號數':duplicate_codes,
        'v4.69_SHA256':v469_sha,
        'v4.69最後日':str(v469_all['date'].max().date()),
        'v4.17_SHA256_state':state.get('original_source_sha256'),
        '增量列數':len(delta),
        '增量最後日期':state.get('last_date_by_market',{}),
        'v4.69與v4.17最近20根逐股共同比對列數':len(joined),
        '跨市場代號日期配對數':int(joined[['code','date']].drop_duplicates().shape[0]),
        'v4.69或官方code-date歧義代號數':len(ambiguous_codes),
        '跨市場轉換候選數':market_changed_count,
        '跨市場轉換後恢復前瞻行情數':market_recovered_count,
        '238候選前瞻有效OHLC數':int((candidate_report['前瞻有效OHLC列數']>0).sum()),
        '238候選有前瞻有效OHLC占比':float((candidate_report['前瞻有效OHLC列數']>0).mean()),
        '可立即計算前瞻range_pos20候選數':ready_count,
        '需等待20根有效K棒候選數':waiting_count,
        '待價格尺度或自然鍵複核候選數':review_count,
        '沒有前瞻有效OHLC候選數':no_future_count,
        '前瞻最新行情日':latest_forward,
        '觀察日期雙市場OHLC均覆蓋':f'{both_days}/{total_future_days}',
        '候選資料狀態分布':{str(k):int(v) for k,v in status_counts.items()},
        '前瞻尺度規則':'只使用最後共同重疊日四個OHLC欄位的同一比例作為未來指標價格尺度因子；原始官方行情不被改寫；淨報酬必須使用未縮放的官方實際價格。',
        '市場身分規則':'以代號+日期作為證券身分配對；市場別保留為逐日變動欄位。歷史及前瞻銜接排除星期六補行日，避免與v4.69基準序列不一致。若同一代號同日跨市場出現多筆，標示歧義，不自動挑選。',
        '價格區段規則':'暖機只計算v4.69最後一個非空價格研究區段的有效K棒，不跨價格區段湊足20根。',
        '已知事件參考':'另附已知櫃轉市、現金除息及減資換股事件索引；只供核對，不自動改價或回寫來源。',
        '未執行項目':['未計算新的range_pos20_rank','未產生新選股訊號','未計算新H=10淨報酬','未回測','未重訓','未重新排名238檔候選'],
        '研究限制':[
            '本版僅確認候選證券代號、日期、逐日市場轉換及最新價格尺度銜接；未證明整個全市場橫斷面Q5因子排名已完成。',
            '最新日OHLC比例需四個欄位同一正比例；比例因子只供延續指標價格尺度，不可用於淨報酬或交易成交價。',
            '歷史視窗中不同日的比例變化可能來自現金除息、減資、還原口徑或其他價格事件；本版列出事件證據，不會自行替代官方價格。',
            '跨市場轉換以證券代號及日期配對，若發現同代號同日期多市場重複則停止該代號自動銜接。',
            '本版使用v1.2.3凍結的238檔，不以任何前瞻報酬重新篩選或排名。',
        ],
        '安全限制':['Drive來源唯讀','不呼叫官方API','不修改、不刪除、不移動、不覆寫v1.2.2/v1.2.3/v1.2.4或行情來源','GitHub只建立獨立audit branch，不更新main'],
    }
    (out_dir/'08_跨市場與公司行動銜接結論.json').write_text(json.dumps(report,ensure_ascii=False,indent=2,default=str),encoding='utf-8')

    summary_lines = [
        '='*100,
        'SSO v1.2.5｜跨市場身分與公司行動區段銜接稽核',
        '='*100,
        f"執行時間：{report['執行時間台北']}",
        f"整體狀態：{overall_status}",
        f"v1.2.3凍結候選：{len(candidates)}檔／{len(candidate_codes)}個代號（不重新挑選）",
        f"v4.69最後日：{report['v4.69最後日']}",
        f"增量行情最後日期：{state.get('last_date_by_market',{})}",
        f"跨市場code+date共同配對：{len(joined):,}列",
        f"市場欄位與目前市場不同：{market_changed_count}檔；已恢復前瞻資料：{market_recovered_count}檔",
        f"候選有前瞻有效OHLC：{int((candidate_report['前瞻有效OHLC列數']>0).sum())}/{len(candidates)}",
        f"可立即計算range_pos20：{ready_count}檔",
        f"需暖機：{waiting_count}檔；尺度/自然鍵複核：{review_count}檔；無新行情：{no_future_count}檔",
        f"前瞻最新行情日：{latest_forward}",
        '',
        '本版沒有計算任何新交易訊號或淨報酬；也沒有重新挑選候選。',
        '市場轉換不再被視為不同證券；市場別仍保留為逐日變動欄位。',
        '未來指標可使用最新日OHLC同比例尺度因子；真實淨報酬只使用官方未縮放價格。',
        '全市場Q5橫斷面準備度仍需另外驗證，未宣稱可直接推薦股票。',
        'Drive原始資料全程唯讀；GitHub僅新增獨立audit branch，不更新main。',
        '='*100,
    ]
    (out_dir/'09_執行摘要.txt').write_text('\n'.join(summary_lines)+'\n',encoding='utf-8-sig')

    return report

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
    branch = f"audit/sso-v1.2.5-cross-market-bridge-{source_stamp}-gitpush-{run_stamp[-6:]}-{uuid.uuid4().hex[:4]}"
    artifact_dir = f"audit/sso-v1.2.5-cross-market-bridge/{source_stamp}/{run_stamp}"
    readme = [
        '# SSO v1.2.5｜跨市場身分與公司行動區段銜接稽核','',
        f"- 狀態：`{report.get('整體狀態')}`",
        f"- v1.2.3候選來源：`{report.get('v1.2.3來源資料夾')}`",
        f"- v4.69 SHA256：`{report.get('v4.69_SHA256')}`",
        '- 本版只做資料銜接與價格比例預檢，不計算前瞻交易報酬。',
        '- Google Drive來源唯讀；只建立獨立audit branch，不更新main。','',
        '## 檔案',
    ]
    for p in upload_files:
        readme.append(f"- `{p.name}` — {p.stat().st_size:,} bytes — SHA256 `{sha256_file(p)}`")
    readme_bytes = ('\n'.join(readme)+'\n').encode('utf-8')
    with tempfile.TemporaryDirectory(prefix='sso_v125_gitpush_') as td:
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
                        'commit-tree',tree,'-p',main_sha,'-m',f'Add SSO v1.2.5 cross-market identity and action bridge audit ({source_stamp})'],bare,env)
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
    log('SSO v1.2.5｜跨市場身分與公司行動區段銜接稽核')
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
    out_dir=ROOT/f'看股最速適_v1_2_5_跨市場身分與公司行動區段銜接稽核_{run_stamp}'
    report=main_audit(v123_dir,out_dir)
    log('[PASS] 跨市場與公司行動銜接報告已產生。')
    print((out_dir/'09_執行摘要.txt').read_text(encoding='utf-8-sig'),flush=True)

    # 將程式本身存入輸出資料夾，便於可重現性；不修改上傳原檔。
    try:
        script_src=Path(__file__).resolve()
        if script_src.is_file():
            shutil.copy2(script_src,out_dir/script_src.name)
    except Exception:
        pass
    zip_path=create_zip(out_dir)
    log('[09/10] 上傳本次結果至GitHub audit branch……')
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
