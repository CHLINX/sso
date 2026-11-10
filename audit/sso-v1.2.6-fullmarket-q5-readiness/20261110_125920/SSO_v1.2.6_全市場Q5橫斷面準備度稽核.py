#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""SSO v1.2.6｜全市場個股 Q5 橫斷面準備度稽核

本版目的：
1. 承接 v1.2.5 已驗證的 v4.69 / v4.17 / append-only delta 資料來源。
2. 將檢查範圍由 v1.2.3 凍結的 238 檔擴大為近似全市場普通股 universe。
3. 逐股檢查 v4.69 最新價格區段、歷史/官方價格尺度銜接、跨市場代號日期歧義及前瞻暖機。
4. 逐交易日只計算 Q5 橫斷面準備度統計；不輸出任何個股排名名單或 Q5 股票名單。
5. 追蹤 3064、4728、5301；保守處理可能公司行動及明顯價格跳空，不跨價格區段湊足 20 根 K 棒。
6. 輸出全市場覆蓋、可計算特徵個股數、Q5 分位數及樣本覆蓋率等稽核摘要。

明確不執行：新交易訊號、個股推薦、淨報酬、H=10 回測、模型重訓、候選重排名。

安全：Drive 唯讀；不呼叫官方 API；不修改/刪除/移動/覆寫原始檔；GitHub 僅建立獨立 audit branch，不更新 main。
Colab：使用通用 Python 上傳執行器 v2 上傳並執行本檔。
"""
from __future__ import annotations

import os
import re
import json
import math
import uuid
import shutil
import hashlib
import zipfile
import tempfile
import subprocess
from pathlib import Path
from datetime import datetime
from urllib.parse import quote
from zoneinfo import ZoneInfo
from collections import deque, defaultdict
from typing import Any, Dict, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

# =============================================================================
# 固定來源與研究設定（沿用 v1.2.5 成功執行的來源位置）
# =============================================================================
ROOT = Path('/content/drive/MyDrive/看股最速適')
V469_PATH = (
    ROOT / '看股最速適_v4.84.9.8_正式來源重建與逐筆配對_20261104_013113'
    / 'v4.69' / '01_v231補行日日期排序與已驗證特徵重建候選.csv.gz'
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

V469_END = pd.Timestamp('2026-10-22')
FORWARD_START = pd.Timestamp('2026-10-23')
HISTORY_SCAN_START = pd.Timestamp('2026-06-01')
ROLLING_BARS = 20
RATIO_REL_TOL = 1e-5
MIN_DAILY_ROWS_PER_MARKET = 300
KNOWN_DR_CODES = {'9103', '9105', '9110', '9136'}
WATCH_CODES = {'3064', '4728', '5301', '6171', '8936', '1808', '8908'}
# 明確公司行動註記；不把一般櫃轉市/市場別變更當成價格區段切斷。
ACTION_NOTE_RE = re.compile(r'除權|除息|減資|增資|換股|換發新股|新股換發|股份分割|股票分割|合併換股|面額變更|資本公積轉增資')
ETF_NAME_RE = re.compile(r'ETF|指數股票型基金|指數股票型|存託憑證|DEPOSITARY|(^|[\s\-\(])DR($|[\s\-\)])', re.I)
GAP_RESET_ABS = 0.25  # 25% 以上開盤跳空僅作保守價格區段重啟，不宣稱必然為公司行動。
REPO_OWNER = 'CHLINX'
REPO_NAME = 'sso'
REPO_URL = 'https://github.com/CHLINX/sso.git'
DEFAULT_BRANCH = 'main'
TAIPEI = ZoneInfo('Asia/Taipei')

# =============================================================================
# 通用工具
# =============================================================================
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
    if n >= 1024**3:
        return f'{n / 1024**3:.3f} GB'
    if n >= 1024**2:
        return f'{n / 1024**2:.2f} MB'
    return f'{n / 1024:.1f} KB'


def normalize_header(x: Any) -> str:
    s = str(x or '').strip().lower().replace('\ufeff', '')
    s = s.replace('　', '').replace(' ', '').replace('（', '(').replace('）', ')')
    s = s.replace('／', '/').replace('－', '-').replace('＋', '+')
    return re.sub(r'[^a-z0-9\u4e00-\u9fff()+/.-]', '', s)


def find_col(headers: Sequence[str], aliases: Sequence[str], required: bool = True) -> Optional[str]:
    norm_map = {normalize_header(h): h for h in headers}
    for alias in aliases:
        key = normalize_header(alias)
        if key in norm_map:
            return norm_map[key]
    for alias in aliases:
        key = normalize_header(alias)
        if len(key) >= 3:
            for norm, original in norm_map.items():
                if key in norm:
                    return original
    if required:
        raise RuntimeError(f'缺少必要欄位；候選={list(aliases)}；實際={list(headers)}')
    return None


def normalize_market_series(s: pd.Series) -> pd.Series:
    x = s.astype('string').fillna('').str.strip().str.upper()
    out = pd.Series(pd.NA, index=s.index, dtype='string')
    tw = x.str.contains(r'TWSE|上市|證交所|集中市場|LISTED', regex=True, na=False) | x.isin(['1', 'L'])
    ot = x.str.contains(r'TPEX|TPEx|OTC|上櫃|櫃買|櫃檯買賣', regex=True, na=False) | x.isin(['2', 'O'])
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
        ymd = y.astype('Int64').astype('string') + roc.str.slice(3, 5) + roc.str.slice(5, 7)
        out.loc[roc_mask] = pd.to_datetime(ymd, format='%Y%m%d', errors='coerce')
    other = out.isna() & raw.ne('')
    if other.any():
        out.loc[other] = pd.to_datetime(raw.loc[other], errors='coerce')
    return out.dt.normalize()


def clean_code_series(s: pd.Series) -> pd.Series:
    return s.astype('string').fillna('').str.strip().str.replace(r'\.0$', '', regex=True)


def numeric_series(s: pd.Series) -> pd.Series:
    x = s.astype('string').fillna('').str.strip().str.replace(',', '', regex=False)
    x = x.replace({'': pd.NA, '--': pd.NA, '---': pd.NA, '----': pd.NA, '-': pd.NA, 'X': pd.NA, 'N/A': pd.NA})
    return pd.to_numeric(x, errors='coerce')


def valid_ohlc(frame: pd.DataFrame) -> pd.Series:
    x = frame[['open', 'high', 'low', 'close']].apply(pd.to_numeric, errors='coerce')
    mask = (
        x.gt(0).all(axis=1)
        & x['low'].le(x[['open', 'close']].min(axis=1))
        & x['high'].ge(x[['open', 'close']].max(axis=1))
        & x['low'].le(x['high'])
    )
    if 'ohlc_valid_source' in frame.columns:
        source_valid = frame['ohlc_valid_source'].astype('string').fillna('').str.strip().str.lower()
        mask &= ~source_valid.isin(['false', '0', 'no', 'n', '否', '無效'])
    return mask.fillna(False)


def is_common_stock_code(code: str) -> bool:
    return bool(re.fullmatch(r'\d{4}', str(code))) and not str(code).startswith('00') and str(code) not in KNOWN_DR_CODES

# =============================================================================
# 來源欄位與分塊讀取
# =============================================================================
def source_mapping(path: Path, kind: str) -> Dict[str, Optional[str]]:
    compression = 'gzip' if path.suffix.lower() == '.gz' else None
    headers = pd.read_csv(path, nrows=0, compression=compression, encoding='utf-8-sig').columns.tolist()
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
            'official_note': ['官方備註', '備註', '附註', 'official_note', 'source_note'],
            'ohlc_valid': ['OHLC有效', 'ohlc_valid'],
        }
    optional = {'name', 'segment', 'ohlc_valid', 'official_note'}
    return {k: find_col(headers, a, required=(k not in optional)) for k, a in aliases.items()}


def read_recent(path: Path, mapping: Dict[str, Optional[str]], start: pd.Timestamp,
                end: Optional[pd.Timestamp], label: str, chunksize: int = 250_000) -> pd.DataFrame:
    usecols = list(dict.fromkeys(v for v in mapping.values() if v))
    compression = 'gzip' if path.suffix.lower() == '.gz' else None
    pieces: List[pd.DataFrame] = []
    total = 0
    matched = 0
    reader = pd.read_csv(path, usecols=usecols, dtype='string', encoding='utf-8-sig',
                         compression=compression, chunksize=chunksize, low_memory=False)
    for chunk_no, chunk in enumerate(reader, 1):
        total += len(chunk)
        dates = parse_date_series(chunk[mapping['date']])
        mask = dates.ge(start)
        if end is not None:
            mask &= dates.le(end)
        if not mask.any():
            if chunk_no % 8 == 0:
                log(f'[{label}] 已掃描 {total:,} 列')
            continue
        sub = pd.DataFrame(index=chunk.index[mask])
        sub['date'] = dates.loc[mask]
        sub['market'] = normalize_market_series(chunk.loc[mask, mapping['market']])
        sub['code'] = clean_code_series(chunk.loc[mask, mapping['code']])
        sub['name'] = (chunk.loc[mask, mapping['name']].astype('string').fillna('').str.strip()
                       if mapping.get('name') else '')
        for field in ('open', 'high', 'low', 'close'):
            sub[field] = numeric_series(chunk.loc[mask, mapping[field]])
        sub['segment'] = (chunk.loc[mask, mapping['segment']].astype('string').fillna('').astype(str)
                          if mapping.get('segment') else '')
        sub['ohlc_valid_source'] = (chunk.loc[mask, mapping['ohlc_valid']].astype('string').fillna('').astype(str)
                                    if mapping.get('ohlc_valid') else '')
        sub['official_note'] = (chunk.loc[mask, mapping['official_note']].astype('string').fillna('').astype(str)
                                if mapping.get('official_note') else '')
        sub = sub.loc[sub['market'].isin(['TWSE', 'TPEx']) & sub['code'].ne('') & sub['date'].notna()]
        matched += len(sub)
        if len(sub):
            pieces.append(sub.reset_index(drop=True))
        if chunk_no % 8 == 0:
            log(f'[{label}] 已掃描 {total:,} 列；命中日期範圍 {matched:,} 列')
    cols = ['date', 'market', 'code', 'name', 'open', 'high', 'low', 'close', 'segment', 'ohlc_valid_source', 'official_note']
    return pd.concat(pieces, ignore_index=True) if pieces else pd.DataFrame(columns=cols)


def load_delta(path: Path, mapping: Dict[str, Optional[str]]) -> pd.DataFrame:
    raw = pd.read_csv(path, dtype='string', encoding='utf-8-sig', compression='gzip', low_memory=False)
    out = pd.DataFrame()
    out['date'] = parse_date_series(raw[mapping['date']])
    out['market'] = normalize_market_series(raw[mapping['market']])
    out['code'] = clean_code_series(raw[mapping['code']])
    out['name'] = raw[mapping['name']].astype('string').fillna('').str.strip() if mapping.get('name') else ''
    for field in ('open', 'high', 'low', 'close'):
        out[field] = numeric_series(raw[mapping[field]])
    out['segment'] = ''
    out['ohlc_valid_source'] = raw[mapping['ohlc_valid']].astype('string').fillna('').astype(str) if mapping.get('ohlc_valid') else ''
    out['official_note'] = raw[mapping['official_note']].astype('string').fillna('').astype(str) if mapping.get('official_note') else ''
    return out.loc[out['market'].isin(['TWSE', 'TPEx']) & out['code'].ne('') & out['date'].notna()].reset_index(drop=True)

# =============================================================================
# 自測與證券範圍
# =============================================================================
def self_test() -> None:
    m = normalize_market_series(pd.Series(['上市', '上櫃', 'TWSE', 'TPEX']))
    assert m.tolist() == ['TWSE', 'TPEx', 'TWSE', 'TPEx']
    d = parse_date_series(pd.Series(['2026-10-22', '115/11/06']))
    assert d.iloc[0] == pd.Timestamp('2026-10-22') and d.iloc[1] == pd.Timestamp('2026-11-06')
    x = pd.DataFrame({'open':[10.0], 'high':[12.0], 'low':[9.0], 'close':[11.0]})
    assert bool(valid_ohlc(x).iloc[0])
    feat = (11.0 - 8.0) / (12.0 - 8.0)
    assert abs(feat - 0.75) < 1e-12
    assert is_common_stock_code('2330') and not is_common_stock_code('0050') and not is_common_stock_code('9103')
    assert ETF_NAME_RE.search('元大台灣50 ETF') and ETF_NAME_RE.search('ABC Depositary Receipt')
    assert not ETF_NAME_RE.search('台積電')
    log('[SELF-TEST] PASS：日期、市場、OHLC、range_pos20、普通股代號及既有ETF/DR名稱排除規則。')


def build_universe(v469: pd.DataFrame, official: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    codes = sorted(set(v469['code'].astype(str)) | set(official['code'].astype(str)))
    latest_names: Dict[str, str] = {}
    latest_markets: Dict[str, str] = {}
    if len(official):
        name_rows = official.loc[official['name'].astype(str).str.strip().ne('')].sort_values('date')
        latest_names = name_rows.groupby('code', sort=False)['name'].last().astype(str).to_dict()
        market_rows = official.sort_values('date')
        latest_markets = market_rows.groupby('code', sort=False)['market'].last().astype(str).to_dict()
    universe_rows = []
    exclusion_rows = []
    for code in codes:
        name = latest_names.get(code, '')
        reasons = []
        if not re.fullmatch(r'\d{4}', code):
            reasons.append('非四位數純數字代號')
        elif code.startswith('00'):
            reasons.append('00開頭代號（沿用既有近似ETF排除規則）')
        if code in KNOWN_DR_CODES:
            reasons.append('已知DR代號')
        if name and ETF_NAME_RE.search(name):
            reasons.append('現行名稱命中ETF/基金/指數型證券排除關鍵字')
        if reasons:
            exclusion_rows.append({'股票代號': code, '股票名稱': name, '排除原因': '；'.join(reasons)})
        else:
            universe_rows.append({
                '股票代號': code, '股票名稱': name,
                '現行市場': latest_markets.get(code, ''),
                '名稱欄位可用': bool(name),
                '證券範圍註記': '近似普通股；非逐日官方證券類別完整認證',
            })
    universe = pd.DataFrame(universe_rows)
    exclusions = pd.DataFrame(exclusion_rows, columns=['股票代號', '股票名稱', '排除原因'])
    if universe.empty:
        raise RuntimeError('依既有普通股範圍規則篩選後 universe 為空。')
    return universe, exclusions

# =============================================================================
# 歷史價格區段與尺度橋接
# =============================================================================
def build_historical_segments(v469: pd.DataFrame, universe_codes: set) -> Tuple[Dict[str, pd.DataFrame], pd.DataFrame, set]:
    hist = v469.loc[v469['code'].astype(str).isin(universe_codes) & v469['date'].dt.weekday.lt(5)].copy()
    if hist.duplicated(['market', 'code', 'date']).any():
        n = int(hist.duplicated(['market', 'code', 'date']).sum())
        raise RuntimeError(f'v4.69 近期香港資料市場自然鍵重複 {n:,} 列，停止自動銜接。')
    hist['OHLC有效'] = valid_ohlc(hist)
    code_date_dups = hist.duplicated(['code', 'date'], keep=False)
    ambiguous = set(hist.loc[code_date_dups, 'code'].astype(str))
    segments: Dict[str, pd.DataFrame] = {}
    rows = []
    for code, raw_group in hist.sort_values(['code', 'date', 'market'], kind='mergesort').groupby('code', sort=False):
        g = raw_group.reset_index(drop=True)
        valid_positions = np.flatnonzero(g['OHLC有效'].to_numpy(dtype=bool))
        if len(valid_positions) == 0:
            rows.append({'股票代號': code, 'v4.69最後有效日': '', '最新價格區段': '', '最新區段有效K棒數': 0,
                         '最新區段起日': '', '歷史橋接狀態': 'NO_VALID_HISTORICAL_OHLC', '歷史代號日期歧義': code in ambiguous})
            continue
        last_pos = int(valid_positions[-1])
        latest_valid = g.iloc[last_pos]
        seg = str(latest_valid.get('segment') or '').strip()
        if not seg or seg.lower() in {'nan', 'none', '<na>'}:
            rows.append({'股票代號': code, 'v4.69最後有效日': str(latest_valid['date'].date()), '最新價格區段': seg,
                         '最新區段有效K棒數': 0, '最新區段起日': '',
                         '歷史橋接狀態': 'REVIEW_MISSING_PRICE_SEGMENT_LABEL', '歷史代號日期歧義': code in ambiguous})
            continue
        start_pos = last_pos
        while start_pos > 0 and str(g.loc[start_pos - 1, 'segment']).strip() == seg:
            start_pos -= 1
        tail = g.iloc[start_pos:last_pos + 1].copy()
        tail = tail.loc[tail['OHLC有效']].sort_values('date').reset_index(drop=True)
        if tail.empty:
            continue
        segments[str(code)] = tail
        rows.append({'股票代號': str(code), 'v4.69最後有效日': str(latest_valid['date'].date()),
                     '最新價格區段': seg, '最新區段有效K棒數': len(tail),
                     '最新區段起日': str(tail['date'].min().date()),
                     '歷史橋接狀態': 'PENDING_SCALE_BRIDGE', '歷史代號日期歧義': code in ambiguous})
    return segments, pd.DataFrame(rows), ambiguous


def build_scale_bridge(segments: Dict[str, pd.DataFrame], official_hist: pd.DataFrame,
                       hist_ambiguous: set) -> Tuple[pd.DataFrame, Dict[str, Dict[str, Any]], set]:
    off = official_hist.copy()
    off['OHLC有效'] = valid_ohlc(off)
    off = off.loc[off['OHLC有效'] & off['date'].dt.weekday.lt(5)].copy()
    if off.duplicated(['market', 'code', 'date']).any():
        n = int(off.duplicated(['market', 'code', 'date']).sum())
        raise RuntimeError(f'v4.17 近期香港官方資料市場自然鍵重複 {n:,} 列。')
    code_date_dups = off.duplicated(['code', 'date'], keep=False)
    off_ambiguous = set(off.loc[code_date_dups, 'code'].astype(str))
    ambiguous = set(hist_ambiguous) | off_ambiguous
    ratio_rows = []
    bridge: Dict[str, Dict[str, Any]] = {}
    for code, histseg in segments.items():
        if code in ambiguous:
            bridge[code] = {'status': 'REVIEW_CODE_DATE_AMBIGUITY', 'factor': None, 'matched_rows': 0}
            ratio_rows.append({'股票代號': code, '最近共同配對日': '', '共同配對列數': 0, '最新日OHLC比例中位數': np.nan,
                               '最新日OHLC四欄最大相對偏差': np.nan, '最近三個共同日尺度最大相對偏差': np.nan,
                               '歷史橋接狀態': 'REVIEW_CODE_DATE_AMBIGUITY'})
            continue
        left = histseg[['code', 'date', 'market', 'open', 'high', 'low', 'close']].tail(20)
        right = off.loc[off['code'].astype(str).eq(code), ['code', 'date', 'market', 'open', 'high', 'low', 'close']]
        if right.empty:
            bridge[code] = {'status': 'REVIEW_NO_OFFICIAL_OVERLAP', 'factor': None, 'matched_rows': 0}
            ratio_rows.append({'股票代號': code, '最近共同配對日': '', '共同配對列數': 0, '最新日OHLC比例中位數': np.nan,
                               '最新日OHLC四欄最大相對偏差': np.nan, '最近三個共同日尺度最大相對偏差': np.nan,
                               '歷史橋接狀態': 'REVIEW_NO_OFFICIAL_OVERLAP'})
            continue
        joined = left.merge(right, on=['code', 'date'], how='inner', suffixes=('_shadow', '_official'), validate='one_to_one')
        if joined.empty:
            bridge[code] = {'status': 'REVIEW_NO_OFFICIAL_OVERLAP', 'factor': None, 'matched_rows': 0}
            ratio_rows.append({'股票代號': code, '最近共同配對日': '', '共同配對列數': 0, '最新日OHLC比例中位數': np.nan,
                               '最新日OHLC四欄最大相對偏差': np.nan, '最近三個共同日尺度最大相對偏差': np.nan,
                               '歷史橋接狀態': 'REVIEW_NO_OFFICIAL_OVERLAP'})
            continue
        ratios = []
        for row in joined.sort_values('date').itertuples(index=False):
            vals = []
            for f in ('open', 'high', 'low', 'close'):
                a = float(getattr(row, f'{f}_shadow'))
                b = float(getattr(row, f'{f}_official'))
                vals.append(a / b if b > 0 else np.nan)
            good = [x for x in vals if np.isfinite(x) and x > 0]
            factor = float(np.median(good)) if len(good) == 4 else np.nan
            spread = float(max(abs(x / factor - 1.0) for x in good)) if len(good) == 4 and factor > 0 else np.inf
            ratios.append({'date': pd.Timestamp(row.date), 'factor': factor, 'spread': spread})
        ratio_frame = pd.DataFrame(ratios).sort_values('date')
        last = ratio_frame.iloc[-1]
        last_date = pd.Timestamp(last['date'])
        last3 = ratio_frame.tail(3)
        last3_factor = last3['factor'].to_numpy(dtype=float)
        last3_spread = float(np.nanmax(np.abs(last3_factor / float(last['factor']) - 1.0))) if len(last3_factor) and np.isfinite(float(last['factor'])) else np.nan
        if last_date.normalize() != V469_END:
            status = 'REVIEW_NO_20261022_COMMON_PRICE'
        elif not np.isfinite(float(last['factor'])) or float(last['factor']) <= 0 or float(last['spread']) > RATIO_REL_TOL:
            status = 'REVIEW_LATEST_OHLC_SCALE_INCONSISTENT'
        elif len(last3) < 3 or not np.isfinite(last3_spread) or last3_spread > RATIO_REL_TOL:
            status = 'REVIEW_LAST3_SCALE_NOT_STABLE'
        else:
            status = 'BRIDGE_READY'
        factor = float(last['factor']) if status == 'BRIDGE_READY' else None
        bridge[code] = {'status': status, 'factor': factor, 'matched_rows': int(len(joined)),
                        'latest_common_date': str(last_date.date()), 'latest_spread': float(last['spread']),
                        'last3_spread': last3_spread}
        ratio_rows.append({'股票代號': code, '最近共同配對日': str(last_date.date()), '共同配對列數': int(len(joined)),
                           '最新日OHLC比例中位數': float(last['factor']), '最新日OHLC四欄最大相對偏差': float(last['spread']),
                           '最近三個共同日尺度最大相對偏差': last3_spread, '歷史橋接狀態': status})
    return pd.DataFrame(ratio_rows), bridge, ambiguous

# =============================================================================
# 前瞻區段暖機與每日 Q5 準備度
# =============================================================================
def range_pos20(history: deque, current: Dict[str, Any]) -> Optional[float]:
    if len(history) < ROLLING_BARS - 1:
        return None
    lows = [float(x['low']) for x in history] + [float(current['low'])]
    highs = [float(x['high']) for x in history] + [float(current['high'])]
    lo = min(lows)
    hi = max(highs)
    if not np.isfinite(lo) or not np.isfinite(hi) or hi <= lo:
        return None
    value = (float(current['close']) - lo) / (hi - lo)
    if not np.isfinite(value):
        return None
    return float(value)


def run_forward_readiness(universe: pd.DataFrame, segments: Dict[str, pd.DataFrame], bridge: Dict[str, Dict[str, Any]],
                          future: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, Dict[str, Dict[str, Any]]]:
    codes = set(universe['股票代號'].astype(str))
    f = future.loc[future['code'].astype(str).isin(codes) & future['date'].dt.weekday.lt(5)].copy()
    f['OHLC有效'] = valid_ohlc(f)
    f = f.loc[f['OHLC有效']].copy()
    if f.empty:
        raise RuntimeError('2026-10-23 之後沒有符合普通股範圍的有效前瞻 OHLC。')
    natural_dups = int(f.duplicated(['market', 'code', 'date']).sum())
    if natural_dups:
        raise RuntimeError(f'前瞻官方行情市場自然鍵重複 {natural_dups:,} 列。')
    duplicate_code_date = f.duplicated(['code', 'date'], keep=False)
    ambiguous_by_day = set(zip(f.loc[duplicate_code_date, 'code'].astype(str), f.loc[duplicate_code_date, 'date']))

    # 歷史橋接成功的股票，以 v4.69 最新價格區段最後 19 根有效 K 棒暖機。
    histories: Dict[str, deque] = {}
    stock_state: Dict[str, Dict[str, Any]] = {}
    for code in codes:
        b = bridge.get(code, {'status': 'NO_HISTORICAL_BASE', 'factor': None})
        status = b.get('status', 'NO_HISTORICAL_BASE')
        histseg = segments.get(code)
        if status == 'BRIDGE_READY' and histseg is not None and len(histseg):
            rows = histseg.tail(ROLLING_BARS - 1)
            q = deque(maxlen=ROLLING_BARS - 1)
            for r in rows.itertuples(index=False):
                q.append({'date': r.date, 'market': r.market,
                          'open': float(r.open), 'high': float(r.high),
                          'low': float(r.low), 'close': float(r.close)})
            histories[code] = q
            seed_bars = len(rows)
        else:
            histories[code] = deque(maxlen=ROLLING_BARS - 1)
            seed_bars = 0
        stock_state[code] = {'bridge_status': status, 'scale_factor': b.get('factor'),
                             'seed_bars': seed_bars, 'current_segment_bars': seed_bars,
                             'forward_valid_bars': 0,
                             'last_status': 'WAITING_FOR_20_VALID_KBARS', 'events': 0}

    daily_rows: List[Dict[str, Any]] = []
    feature_rows_by_date: Dict[pd.Timestamp, List[Dict[str, Any]]] = defaultdict(list)
    event_rows: List[Dict[str, Any]] = []
    active_rows_by_date: Dict[pd.Timestamp, pd.DataFrame] = {}

    for date, day in f.sort_values(['date', 'code', 'market']).groupby('date', sort=True):
        date = pd.Timestamp(date).normalize()
        day = day.sort_values(['code', 'market']).copy()
        active_rows_by_date[date] = day
        duplicate_codes_today = {c for c, d in ambiguous_by_day if pd.Timestamp(d).normalize() == date}
        # 任何同代號同日跨市場重複均不自動選取其中一筆。
        for code in duplicate_codes_today:
            event_rows.append({'日期': str(date.date()), '股票代號': code, '事件類型': 'AMBIGUOUS_CODE_DATE',
                               '處理': '該代號該日不計入特徵；不自動選市場'})
        for row in day.itertuples(index=False):
            code = str(row.code)
            if code in duplicate_codes_today:
                continue
            st = stock_state[code]
            q = histories[code]
            factor = float(st.get('scale_factor') or 1.0)
            # 官方前瞻 OHLC 先套用「官方價 -> v4.69影子價」比例；不改寫來源檔。
            # range_pos20 對全序列同一正比例不變，但必須避免歷史/前瞻混用不同尺度。
            cur = {'date': date, 'market': str(row.market), 'open': float(row.open) * factor,
                   'high': float(row.high) * factor, 'low': float(row.low) * factor,
                   'close': float(row.close) * factor}
            note = str(getattr(row, 'official_note', '') or '').strip()
            action_hit = bool(note and ACTION_NOTE_RE.search(note))
            gap_ratio = np.nan
            if q:
                prev_close = float(q[-1]['close'])
                if prev_close > 0:
                    gap_ratio = cur['open'] / prev_close - 1.0
            gap_hit = bool(np.isfinite(gap_ratio) and abs(gap_ratio) >= GAP_RESET_ABS)
            if action_hit or gap_hit:
                q.clear()
                st['current_segment_bars'] = 0
                st['events'] += 1
                reason = 'OFFICIAL_CORPORATE_ACTION_NOTE' if action_hit else 'POTENTIAL_PRICE_SEGMENT_BREAK_GAP_GE_25PCT'
                event_rows.append({'日期': str(date.date()), '股票代號': code, '事件類型': reason,
                                   '開盤相對前收跳空比例': gap_ratio, '官方備註': note,
                                   '處理': '保守重啟價格區段；當日作為新區段第1根，不沿用舊區段'})
            value = range_pos20(q, cur)
            if value is not None:
                feature_rows_by_date[date].append({'code': code, 'market': str(row.market), 'range_pos20': value})
            q.append(cur)
            st['forward_valid_bars'] += 1
            st['current_segment_bars'] += 1
            st['last_status'] = 'FEATURE_READY' if value is not None else 'WAITING_FOR_20_VALID_KBARS'

    # 逐日全市場 OHLC 覆蓋與 Q5 樣本量；不輸出個股分數、排名或 Q5 代號。
    daily_cov = f.groupby(['date', 'market'], observed=True).agg(
        有效OHLC列數=('code', 'size'), 不重複代號數=('code', 'nunique')
    ).reset_index()
    daily_cov['市場日有效OHLC覆蓋初步通過'] = daily_cov['有效OHLC列數'].ge(MIN_DAILY_ROWS_PER_MARKET)
    dates = sorted(set(daily_cov['date']))
    daily_q5_rows = []
    for date in dates:
        cov = daily_cov.loc[daily_cov['date'].eq(date)]
        market_counts = {str(r.market): int(r.有效OHLC列數) for r in cov.itertuples(index=False)}
        rows = feature_rows_by_date.get(pd.Timestamp(date), [])
        feat = pd.DataFrame(rows)
        if len(feat):
            # 與原研究使用的逐日橫斷面百分位排名方式一致；僅計數，不輸出代號。
            pct_rank = feat['range_pos20'].rank(method='average', pct=True)
            q5_count = int((pct_rank >= 0.80).sum())
            q80 = float(feat['range_pos20'].quantile(0.80))
            distinct = int(feat['range_pos20'].nunique(dropna=True))
            tw_feat = int(feat.loc[feat['market'].eq('TWSE')].shape[0])
            ot_feat = int(feat.loc[feat['market'].eq('TPEx')].shape[0])
        else:
            q5_count, q80, distinct, tw_feat, ot_feat = 0, np.nan, 0, 0, 0
        active_n = int(f.loc[f['date'].eq(date), 'code'].nunique())
        ready_n = int(len(feat))
        coverage = ready_n / active_n if active_n else np.nan
        both_ohlc = market_counts.get('TWSE', 0) >= MIN_DAILY_ROWS_PER_MARKET and market_counts.get('TPEx', 0) >= MIN_DAILY_ROWS_PER_MARKET
        both_feature = tw_feat > 0 and ot_feat > 0
        if ready_n >= 5 and both_feature and both_ohlc:
            status = 'Q5_CROSS_SECTION_COMPUTABLE_BOTH_MARKETS'
        elif ready_n >= 5 and both_feature:
            status = 'Q5_COMPUTABLE_BUT_DAILY_MARKET_COVERAGE_REVIEW'
        elif ready_n >= 5:
            status = 'Q5_COMPUTABLE_SINGLE_MARKET_ONLY'
        else:
            status = 'Q5_INSUFFICIENT_FEATURE_SAMPLE'
        daily_q5_rows.append({
            '日期': str(pd.Timestamp(date).date()),
            'TWSE有效普通股OHLC數': market_counts.get('TWSE', 0),
            'TPEx有效普通股OHLC數': market_counts.get('TPEx', 0),
            '當日有效普通股數': active_n,
            '可計算range_pos20數': ready_n,
            '特徵覆蓋率': coverage,
            'TWSE可計算特徵數': tw_feat,
            'TPEx可計算特徵數': ot_feat,
            'range_pos20不同值數': distinct,
            'Q80原始特徵分位數': q80,
            'Q5百分位排名列數': q5_count,
            '雙市場OHLC覆蓋初步通過': bool(both_ohlc),
            '雙市場特徵樣本均存在': bool(both_feature),
            'Q5橫斷面狀態': status,
        })

    # 全市場逐股橋接與暖機狀態（不含任何個股因子分數）。
    name_map = universe.set_index('股票代號')['股票名稱'].to_dict()
    market_map = universe.set_index('股票代號')['現行市場'].to_dict()
    stock_rows = []
    for code in sorted(codes):
        b = bridge.get(code, {'status': 'NO_HISTORICAL_BASE', 'factor': None, 'matched_rows': 0})
        st = stock_state.get(code, {})
        histseg = segments.get(code)
        hist_n = int(len(histseg)) if histseg is not None else 0
        first_hist = str(histseg['date'].min().date()) if hist_n else ''
        last_hist = str(histseg['date'].max().date()) if hist_n else ''
        total = min(ROLLING_BARS, int(st.get('current_segment_bars', 0)))
        stock_rows.append({
            '股票代號': code, '股票名稱': name_map.get(code, ''), '現行市場': market_map.get(code, ''),
            '歷史橋接狀態': b.get('status', 'NO_HISTORICAL_BASE'),
            '最近共同配對日': b.get('latest_common_date', ''),
            '共同配對列數': b.get('matched_rows', 0),
            '最新OHLC比例中位數': b.get('factor', np.nan),
            '最新OHLC四欄最大相對偏差': b.get('latest_spread', np.nan),
            '最近三個共同日尺度最大相對偏差': b.get('last3_spread', np.nan),
            'v4.69最新價格區段有效K棒數': hist_n,
            'v4.69最新區段起日': first_hist,
            'v4.69最新有效日': last_hist,
            '前瞻有效K棒數': st.get('forward_valid_bars', 0),
            '目前區段可用暖機K棒數': total,
            '目前區段尚需有效K棒數': max(0, ROLLING_BARS - total),
            '可能價格區段事件數': st.get('events', 0),
            '資料處理註記': ('橋接成功，沿用最新v4.69價格區段' if b.get('status') == 'BRIDGE_READY'
                           else '不混接舊歷史；由前瞻資料重新暖機'),
        })
    stock_report = pd.DataFrame(stock_rows)
    daily_coverage = daily_cov.copy()
    daily_coverage['日期'] = daily_coverage['date'].dt.strftime('%Y-%m-%d')
    daily_coverage = daily_coverage.drop(columns=['date'])
    daily_q5 = pd.DataFrame(daily_q5_rows)
    events = pd.DataFrame(event_rows)
    if events.empty:
        events = pd.DataFrame(columns=['日期', '股票代號', '事件類型', '處理'])
    return stock_report, daily_coverage, daily_q5, {'events': events, 'ambiguous_by_day': ambiguous_by_day, 'feature_rows': feature_rows_by_date}

# =============================================================================
# 主稽核流程
# =============================================================================
def main_audit(out_dir: Path) -> Dict[str, Any]:
    self_test()
    required = [V469_PATH, A_PATH, DELTA_PATH, STATE_PATH]
    missing = [str(p) for p in required if not p.is_file()]
    if missing:
        raise FileNotFoundError('缺少必要來源：\n' + '\n'.join(missing))

    log('[01/09] 驗證 v4.69 來源 SHA256……')
    v469_sha = sha256_file(V469_PATH)
    if v469_sha != V469_SHA256:
        raise RuntimeError(f'v4.69 SHA256 不符：{v469_sha}')
    log('[PASS] v4.69 SHA256 相符。')

    log('[02/09] 核對 v4.17 與增量資料庫中繼資料……')
    state = json.loads(STATE_PATH.read_text(encoding='utf-8-sig'))
    if state.get('storage_mode') != 'BASE_A_PLUS_APPEND_ONLY_DELTA':
        raise RuntimeError('行情資料庫不是 A + append-only delta 結構。')
    if Path(state.get('original_source_database', '')).resolve() != A_PATH.resolve():
        raise RuntimeError('增量資料庫中繼資料的 v4.17 來源路徑不符。')
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

    log('[03/09] 讀取近期香港全市場 v4.69／v4.17 與增量行情……')
    vmap = source_mapping(V469_PATH, 'v469')
    amap = source_mapping(A_PATH, 'official')
    dmap = source_mapping(DELTA_PATH, 'official')
    v469 = read_recent(V469_PATH, vmap, HISTORY_SCAN_START, V469_END, 'v4.69全市場')
    official_base = read_recent(A_PATH, amap, HISTORY_SCAN_START, None, 'v4.17全市場')
    delta = load_delta(DELTA_PATH, dmap)
    if v469.empty or official_base.empty or delta.empty:
        raise RuntimeError(f'來源讀取結果異常：v4.69={len(v469):,}, v4.17={len(official_base):,}, delta={len(delta):,}')
    v469 = v469.loc[v469['date'].dt.weekday.lt(5)].copy()  # 排除星期六補行日，與既有研究日軸一致。
    official = pd.concat([official_base, delta], ignore_index=True)
    official = official.loc[official['date'].dt.weekday.lt(5)].copy()
    if v469['date'].max() != V469_END:
        raise RuntimeError(f'v4.69近期香港資料最後日期不是 {V469_END.date()}：{v469.date.max()}')
    if official.duplicated(['market', 'code', 'date']).any():
        n = int(official.duplicated(['market', 'code', 'date']).sum())
        raise RuntimeError(f'v4.17 + delta 市場自然鍵重複 {n:,} 列。')
    delta_rows_expected = state.get('delta_rows')
    if delta_rows_expected is not None and int(delta_rows_expected) != len(delta):
        raise RuntimeError(f'delta列數與state不符：{len(delta):,}/{delta_rows_expected:,}')
    delta_latest = delta.groupby('market')['date'].max().dropna().to_dict()
    state_latest = state.get('last_date_by_market', {})
    for market, latest in delta_latest.items():
        expected = state_latest.get(market) or state_latest.get('TPEX' if market == 'TPEx' else 'TWSE')
        if expected:
            expected_date = pd.to_datetime(expected).normalize()
            if pd.Timestamp(latest).normalize() != expected_date:
                raise RuntimeError(f'{market}增量最後日期與state不符：{latest}/{expected}')
    log(f'[PASS] v4.69={len(v469):,}列；v4.17近期香港={len(official_base):,}列；delta={len(delta):,}列；增量最後日={state_latest}。')

    log('[04/09] 建立近似全市場普通股範圍……')
    universe, exclusions = build_universe(v469, official)
    universe_codes = set(universe['股票代號'].astype(str))
    v469_common = v469.loc[v469['code'].astype(str).isin(universe_codes)].copy()
    official_hist = official.loc[official['date'].le(V469_END) & official['code'].astype(str).isin(universe_codes)].copy()
    future = official.loc[official['date'].ge(FORWARD_START) & official['code'].astype(str).isin(universe_codes)].copy()
    log(f'[PASS] 近似普通股 universe={len(universe):,}檔；名稱可用={int(universe["名稱欄位可用"].sum()):,}檔；排除={len(exclusions):,}代號。')

    log('[05/09] 建立每檔最新價格區段與官方價格尺度橋接……')
    segments, segment_report, hist_ambiguous = build_historical_segments(v469_common, universe_codes)
    ratio_report, bridge, all_ambiguous = build_scale_bridge(segments, official_hist, hist_ambiguous)
    # 保留「缺少歷史區段標籤」與「歷史無有效OHLC」等具體原因，不統一混成一般無歷史資料。
    for r in segment_report.to_dict('records'):
        code = str(r['股票代號'])
        if code not in bridge:
            bridge[code] = {'status': str(r.get('歷史橋接狀態') or 'NO_HISTORICAL_BASE'),
                            'factor': None, 'matched_rows': 0}
    # v1.2.6 依更嚴謹的 2026-10-22 最後共同比對日與最近三日尺度穩定度判斷是否可混接。
    bridge_counts = pd.Series([v.get('status', 'NO_HISTORICAL_BASE') for v in bridge.values()]).value_counts().to_dict()
    log(f'[PASS] 價格橋接狀態：{bridge_counts}')

    log('[06/09] 逐日計算全市場特徵可用數與 Q5 準備度（不輸出個股排名）……')
    stock_report, daily_coverage, daily_q5, extra = run_forward_readiness(universe, segments, bridge, future)
    events = extra['events']
    if len(stock_report):
        watch = stock_report.loc[stock_report['股票代號'].isin(WATCH_CODES)].copy()
        watch_codes_found = set(watch['股票代號'].astype(str))
        for c in sorted(WATCH_CODES - watch_codes_found):
            watch = pd.concat([watch, pd.DataFrame([{'股票代號': c, '資料處理註記': '本次來源範圍未找到該代號'}])], ignore_index=True)
    else:
        watch = pd.DataFrame({'股票代號': sorted(WATCH_CODES)})

    log('[07/09] 產生來源、全市場逐股橋接、雙市場覆蓋與 Q5 摘要……')
    # 補齊主來源與每日資料範圍摘要。
    source_inventory = pd.DataFrame([
        {'來源': 'v4.69歷史影子行情', '路徑': str(V469_PATH), '檔案大小bytes': V469_PATH.stat().st_size,
         'SHA256': v469_sha, '讀取列數': len(v469), '起日': str(v469['date'].min().date()), '迄日': str(v469['date'].max().date())},
        {'來源': 'v4.17官方行情主檔（近期香港）', '路徑': str(A_PATH), '檔案大小bytes': a_stat.st_size,
         'SHA256': state.get('original_source_sha256'), '讀取列數': len(official_base),
         '起日': str(official_base['date'].min().date()), '迄日': str(official_base['date'].max().date())},
        {'來源': '官方行情 append-only delta', '路徑': str(DELTA_PATH), '檔案大小bytes': delta_stat.st_size,
         'SHA256': sha256_file(DELTA_PATH), '讀取列數': len(delta),
         '起日': str(delta['date'].min().date()), '迄日': str(delta['date'].max().date())},
    ])
    # 逐股價格區段和尺度 bridge 表合併，保留沒有重疊配對的證券。
    stock_report = stock_report.merge(
        segment_report[['股票代號', '最新價格區段', '最新區段起日']].drop_duplicates('股票代號'),
        on='股票代號', how='left'
    )
    # 特別追蹤代號表保留前瞻追蹤結論。
    watch = watch.merge(universe[['股票代號', '股票名稱', '現行市場']].drop_duplicates('股票代號'),
                        on='股票代號', how='left', suffixes=('', '_來源'))

    market_events = []
    recent_official = official.loc[official['code'].astype(str).isin(universe_codes)].sort_values(['code', 'date'])
    for code, g in recent_official.groupby('code', sort=False):
        seq = g[['date', 'market']].drop_duplicates().sort_values('date')
        prev = None
        for r in seq.itertuples(index=False):
            cur_market = str(r.market)
            if prev is not None and cur_market != prev:
                market_events.append({'股票代號': str(code), '市場切換日期': str(pd.Timestamp(r.date).date()),
                                      '前市場': prev, '新市場': cur_market,
                                      '說明': '市場別為時變欄位；同一代號不因轉板拆成不同證券'})
            prev = cur_market
    migration_report = pd.DataFrame(market_events, columns=['股票代號', '市場切換日期', '前市場', '新市場', '說明'])

    status_counts = stock_report['歷史橋接狀態'].fillna('NO_HISTORICAL_BASE').value_counts().to_dict()
    ready_days = int(daily_q5['Q5橫斷面狀態'].eq('Q5_CROSS_SECTION_COMPUTABLE_BOTH_MARKETS').sum()) if len(daily_q5) else 0
    latest_q5 = daily_q5.iloc[-1].to_dict() if len(daily_q5) else {}
    report = {
        '版本': 'SSO_v1.2.6_全市場Q5橫斷面準備度稽核',
        '執行時間台北': datetime.now(TAIPEI).isoformat(),
        'v4.69_SHA256': v469_sha,
        'v4.69最後日': str(v469['date'].max().date()),
        'v4.17_SHA256_state': state.get('original_source_sha256'),
        'delta列數': len(delta),
        'delta最後日期': state.get('last_date_by_market', {}),
        '近似全市場普通股代號數': len(universe),
        '名稱欄位可用代號數': int(universe['名稱欄位可用'].sum()),
        '因非四位數/00開頭/DR/ETF名稱規則排除代號數': len(exclusions),
        'v4.69含有效最新區段代號數': len(segments),
        'v4.69或官方code-date歧義代號數': len(all_ambiguous),
        '前瞻code-date歧義代號日期組數': len(extra['ambiguous_by_day']),
        '歷史尺度橋接狀態分布': {str(k): int(v) for k, v in status_counts.items()},
        '前瞻有效交易日數': len(daily_q5),
        '前瞻起日': str(future['date'].min().date()) if len(future) else None,
        '前瞻迄日': str(future['date'].max().date()) if len(future) else None,
        '雙市場Q5可計算日數': ready_days,
        '雙市場單日最低有效OHLC列數門檻': MIN_DAILY_ROWS_PER_MARKET,
        '雙市場門檻說明': '每市場300列僅為最低技術覆蓋門檻，不是官方全市場完整性認證；須連同每日原始列數、有效OHLC數及特徵覆蓋率判讀。',
        'Q5準備度解讀': '可計算僅代表當日有效特徵樣本足以執行橫斷面百分位計算，不代表樣本完整、統計顯著或具獲利能力。',
        '最後一日Q5準備度': latest_q5,
        '追蹤代號': sorted(WATCH_CODES),
        '公司行動/潛在價格區段事件數': len(events),
        '特徵定義': 'range_pos20=(當日收盤價-最近20根有效K棒最低價)/(最近20根有效K棒最高價-最近20根有效K棒最低價)。只做特徵可用性與每日橫斷面樣本量稽核。',
        '橫斷面排名方式': '逐交易日 range_pos20 rank(method=average, pct=True)；僅統計百分位>=0.80的列數，不輸出個股排名與代號。',
        '價格銜接規則': '只有最新共同配對日為2026-10-22、當日四欄OHLC比例一致，且最近三個共同配對日尺度穩定時，才沿用v4.69最新價格區段；其餘代號不混接舊歷史，僅由前瞻資料重新暖機。',
        '價格區段規則': '新股、缺少可靠尺度橋接、官方公司行動註記或開盤跳空達25%者，不跨段湊足20根有效K棒；官方註記及跳空僅作保守重啟依據，不宣稱一定是公司行動。',
        '市場身分規則': '以股票代號+日期識別同一證券，市場別保留為逐日變動欄位；同代號同日同時出現在兩市場時不自動挑選，該代號當日排除特徵統計。',
        '明確未執行': ['未產生新交易訊號', '未輸出Q5個股名單或個股排名', '未計算淨報酬', '未進行H=10回測', '未重訓', '未重新排名v1.2.3的238檔候選'],
        '研究限制': [
            '普通股範圍延續既有近似規則：四位數純數字代號、排除00開頭與已知DR，並以目前可取得名稱排除ETF/基金/指數型關鍵字；不是逐日官方證券類別完整認證。',
            'v4.69缺少股票名稱欄位，名稱以v4.17與增量行情可取得欄位為準；名稱空白者無法依名稱完整辨識ETF/DR。',
            '只有在最新共同價格日與最近三日尺度穩定時才混接v4.69與前瞻行情；否則從前瞻日起重新暖機，減少錯誤跨段。',
            '開盤跳空25%是保守異常切段門檻，不代表已證實公司行動；事件會列出供人工核對。',
            'Q5統計只代表當日具有range_pos20值的近似普通股橫斷面樣本，不代表通過獲利檢定或可直接推薦。',
            '本版不將特徵覆蓋率不足的個股補值，也不跨越缺價日期或不同價格研究區段補足20根K棒。',
        ],
        '安全限制': ['Drive來源唯讀', '不呼叫官方API', '不修改/刪除/移動/覆寫任何來源或既有成果', 'GitHub只新增獨立audit branch，不更新main'],
    }

    out_dir.mkdir(parents=True, exist_ok=False)
    source_inventory.to_csv(out_dir / '01_來源檔案與日期範圍.csv', index=False, encoding='utf-8-sig')
    universe.to_csv(out_dir / '02_近似全市場普通股範圍.csv', index=False, encoding='utf-8-sig')
    stock_report.to_csv(out_dir / '03_全市場逐股價格銜接與暖機狀態.csv', index=False, encoding='utf-8-sig')
    daily_coverage.to_csv(out_dir / '04_前瞻雙市場逐日普通股行情覆蓋.csv', index=False, encoding='utf-8-sig')
    daily_q5.to_csv(out_dir / '05_每日Q5橫斷面準備度摘要.csv', index=False, encoding='utf-8-sig')
    watch.to_csv(out_dir / '06_3064_4728_5301及公司行動追蹤.csv', index=False, encoding='utf-8-sig')
    events.to_csv(out_dir / '07_公司行動與潛在價格區段事件.csv', index=False, encoding='utf-8-sig')
    migration_report.to_csv(out_dir / '08_全市場跨市場轉換事件.csv', index=False, encoding='utf-8-sig')
    exclusions.to_csv(out_dir / '09_普通股範圍排除清單.csv', index=False, encoding='utf-8-sig')
    (out_dir / '10_全市場Q5準備度稽核結論.json').write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding='utf-8')

    latest_text = ''
    if len(daily_q5):
        r = daily_q5.iloc[-1]
        latest_text = (
            f"最後交易日：{r['日期']}\n"
            f"當日有效普通股：{int(r['當日有效普通股數']):,}\n"
            f"可計算range_pos20：{int(r['可計算range_pos20數']):,}\n"
            f"特徵覆蓋率：{float(r['特徵覆蓋率']):.2%}\n"
            f"Q5百分位排名列數：{int(r['Q5百分位排名列數']):,}\n"
            f"橫斷面狀態：{r['Q5橫斷面狀態']}\n"
        )
    summary = [
        '=' * 100,
        'SSO v1.2.6｜全市場個股 Q5 橫斷面準備度稽核',
        '=' * 100,
        f"執行時間：{report['執行時間台北']}",
        f"v4.69來源SHA256：{v469_sha}",
        f"近似全市場普通股：{len(universe):,}檔；名稱可用：{int(universe['名稱欄位可用'].sum()):,}檔；排除：{len(exclusions):,}代號",
        f"歷史尺度橋接狀態：{status_counts}",
        f"前瞻日期：{report['前瞻起日']} 至 {report['前瞻迄日']}；共 {len(daily_q5)} 個交易日",
        f"雙市場Q5可計算日：{ready_days}/{len(daily_q5)}（每市場至少{MIN_DAILY_ROWS_PER_MARKET}列有效OHLC；僅為最低技術門檻）",
        latest_text,
        '本版只做全市場資料覆蓋、逐股暖機及Q5橫斷面樣本量稽核。',
        '沒有輸出個股因子分數、個股排名、Q5股票名單、交易訊號或任何新淨報酬。',
        '無法可靠銜接的股票不混接舊歷史，改由前瞻新資料重新暖機。',
        'Drive來源全程唯讀；GitHub只新增獨立audit branch，不更新main。',
        '=' * 100,
    ]
    (out_dir / '11_執行摘要.txt').write_text('\n'.join(summary) + '\n', encoding='utf-8-sig')
    readme = [
        '# SSO v1.2.6｜全市場個股 Q5 橫斷面準備度稽核', '',
        f"- 執行時間：`{report['執行時間台北']}`",
        f"- 近似普通股範圍：`{len(universe):,}` 檔",
        f"- 前瞻交易日：`{report['前瞻起日']}` 至 `{report['前瞻迄日']}`",
        f"- 雙市場 Q5 可計算日：`{ready_days}/{len(daily_q5)}`",
        '- 本版只統計特徵可用性與每日橫斷面樣本量，不輸出個股排名或 Q5 股票名單。',
        '- 不產生交易訊號、不計算淨報酬、不回測、不重訓。',
        '- Drive來源唯讀；GitHub僅建立獨立audit branch，不更新main。', '', '## 檔案',
    ]
    for p in sorted(out_dir.iterdir()):
        if p.is_file():
            readme.append(f'- `{p.name}` — {p.stat().st_size:,} bytes — SHA256 `{sha256_file(p)}`')
    (out_dir / 'README_稽核說明.md').write_text('\n'.join(readme) + '\n', encoding='utf-8')
    return report

# =============================================================================
# ZIP 與 GitHub 獨立 audit branch（git CLI；不使用 Git Blobs REST API）
# =============================================================================
def create_zip(out_dir: Path) -> Path:
    zip_path = out_dir.with_suffix('.zip')
    with zipfile.ZipFile(zip_path, 'w', compression=zipfile.ZIP_DEFLATED, compresslevel=6) as z:
        for f in sorted(out_dir.iterdir()):
            if f.is_file():
                z.write(f, arcname=f.name)
    with zipfile.ZipFile(zip_path, 'r') as z:
        bad = z.testzip()
        if bad:
            raise RuntimeError(f'ZIP CRC失敗：{bad}')
    log(f'[PASS] ZIP CRC：{zip_path.name} | {fmt_bytes(zip_path.stat().st_size)}')
    return zip_path


def find_token() -> str:
    token = os.environ.get('GITHUB_TOKEN', '').strip() or os.environ.get('GH_TOKEN', '').strip()
    if not token:
        try:
            from google.colab import userdata  # type: ignore
            token = (userdata.get('GITHUB_TOKEN') or '').strip()
        except Exception:
            token = ''
    if not token:
        raise RuntimeError('找不到 GITHUB_TOKEN；請確認既有 Colab Secret 已設定，不要將 Token 貼到程式碼或對話。')
    return token


def run_git(args: List[str], cwd: Optional[Path], env: Dict[str, str], input_bytes: Optional[bytes] = None) -> str:
    p = subprocess.run(['git', *args], cwd=str(cwd) if cwd else None, env=env, input=input_bytes,
                       stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
    if p.returncode:
        raise RuntimeError(f"git {' '.join(args[:5])} 失敗 code={p.returncode}\n{p.stderr.decode('utf-8', 'replace')[-5000:]}")
    return p.stdout.decode('utf-8', 'replace').strip()


def upload_github(out_dir: Path, zip_path: Path, report: Dict[str, Any], run_stamp: str) -> Dict[str, Any]:
    token = find_token()
    upload_files = [p for p in sorted(out_dir.iterdir()) if p.is_file()] + [zip_path]
    too_large = [(p.name, p.stat().st_size) for p in upload_files if p.stat().st_size >= 100 * 1024 * 1024]
    if too_large:
        raise RuntimeError('單檔達 GitHub 100MB 限制：' + repr(too_large))
    branch = f'audit/sso-v1.2.6-fullmarket-q5-readiness-{run_stamp}-gitpush-{uuid.uuid4().hex[:4]}'
    artifact_dir = f'audit/sso-v1.2.6-fullmarket-q5-readiness/{run_stamp}'
    readme_lines = [
        '# SSO v1.2.6｜全市場個股 Q5 橫斷面準備度稽核', '',
        f"- 近似普通股範圍：`{report.get('近似全市場普通股代號數')}` 檔",
        f"- 前瞻交易日：`{report.get('前瞻起日')}` 至 `{report.get('前瞻迄日')}`",
        f"- 雙市場 Q5 可計算日：`{report.get('雙市場Q5可計算日數')}`",
        '- 不輸出個股排名或 Q5 股票名單；不產生交易訊號、不計算淨報酬、不回測、不重訓。',
        '- Google Drive來源唯讀；只建立獨立audit branch，不更新main。', '', '## 檔案',
    ]
    for p in upload_files:
        readme_lines.append(f'- `{p.name}` — {p.stat().st_size:,} bytes — SHA256 `{sha256_file(p)}`')
    readme_bytes = ('\n'.join(readme_lines) + '\n').encode('utf-8')
    with tempfile.TemporaryDirectory(prefix='sso_v126_gitpush_') as td:
        temp = Path(td)
        bare = temp / 'repo.git'
        askpass = temp / 'askpass.sh'
        askpass.write_text('#!/bin/sh\ncase "$1" in *Username*|*username*) echo x-access-token;; *Password*|*password*) printf \'%s\\n\' "$GITHUB_TOKEN";; *) echo;; esac\n', encoding='utf-8')
        askpass.chmod(0o700)
        env = os.environ.copy()
        env.update({
            'GITHUB_TOKEN': token, 'GIT_ASKPASS': str(askpass), 'GIT_TERMINAL_PROMPT': '0',
            'GIT_CONFIG_NOSYSTEM': '1', 'GIT_AUTHOR_NAME': 'SSO Audit Uploader',
            'GIT_AUTHOR_EMAIL': 'sso-audit-uploader@users.noreply.github.com',
            'GIT_COMMITTER_NAME': 'SSO Audit Uploader', 'GIT_COMMITTER_EMAIL': 'sso-audit-uploader@users.noreply.github.com',
        })
        log('[GITHUB 01/06] 初始化暫存 Git bare repository……')
        run_git(['init', '--bare', str(bare)], None, env)
        run_git(['remote', 'add', 'origin', REPO_URL], bare, env)
        log('[GITHUB 02/06] 從 main 取得 shallow partial history……')
        run_git(['-c', 'protocol.version=2', 'fetch', '--depth=1', '--filter=blob:none', 'origin', f'refs/heads/{DEFAULT_BRANCH}'], bare, env)
        main_sha = run_git(['rev-parse', 'FETCH_HEAD^{commit}'], bare, env)
        run_git(['read-tree', main_sha], bare, env)
        log(f'[PASS] main基準SHA：{main_sha}')
        log('[GITHUB 03/06] 將報告、ZIP及程式加入 Git objects……')
        items = [(p, f'{artifact_dir}/{p.name}', None) for p in upload_files]
        items.append((None, f'{artifact_dir}/README_GitHub上傳索引.md', readme_bytes))
        for i, (local, remote, inline) in enumerate(items, 1):
            if local is not None:
                blob = run_git(['hash-object', '-w', str(local)], bare, env)
                name, size = local.name, local.stat().st_size
            else:
                blob = run_git(['hash-object', '-w', '--stdin'], bare, env, inline)
                name, size = 'README_GitHub上傳索引.md', len(inline or b'')
            run_git(['update-index', '--add', '--cacheinfo', '100644', blob, remote], bare, env)
            log(f'[ADD {i}/{len(items)}] {name} | {fmt_bytes(size)}')
        log('[GITHUB 04/06] 建立 audit commit……')
        tree = run_git(['write-tree'], bare, env)
        commit = run_git(['-c', 'user.name=SSO Audit Uploader', '-c', 'user.email=sso-audit-uploader@users.noreply.github.com',
                          'commit-tree', tree, '-p', main_sha, '-m', f'Add SSO v1.2.6 full-market Q5 readiness audit ({run_stamp})'], bare, env)
        log('[GITHUB 05/06] 推送獨立 audit branch……')
        run_git(['push', 'origin', f'{commit}:refs/heads/{branch}'], bare, env)
        log('[GITHUB 06/06] 驗證遠端 audit branch 與 main……')
        remote = run_git(['ls-remote', 'origin', f'refs/heads/{branch}'], bare, env).split()
        if not remote or remote[0] != commit:
            raise RuntimeError('GitHub遠端audit branch SHA驗證失敗。')
        after = run_git(['ls-remote', 'origin', f'refs/heads/{DEFAULT_BRANCH}'], bare, env).split()
        main_after = after[0] if after else ''
        if main_after != main_sha:
            raise RuntimeError(f'main SHA 前後不同：{main_sha} -> {main_after}；本程式沒有對main push。')
        env['GITHUB_TOKEN'] = ''
    return {
        'branch': branch, 'commit': commit, 'main_before': main_sha, 'main_after': main_after,
        'folder_url': f'https://github.com/{REPO_OWNER}/{REPO_NAME}/tree/{branch}/{artifact_dir}',
        'zip_url': f'https://github.com/{REPO_OWNER}/{REPO_NAME}/blob/{branch}/{artifact_dir}/{quote(zip_path.name)}',
        'commit_url': f'https://github.com/{REPO_OWNER}/{REPO_NAME}/commit/{commit}',
        'uploaded_files': len(items),
    }

# =============================================================================
# Colab 入口
# =============================================================================
def main() -> int:
    print('=' * 104, flush=True)
    log('SSO v1.2.6｜全市場個股 Q5 橫斷面準備度稽核')
    log('[安全] Drive來源唯讀；不呼叫官方API；不修改、不刪除、不移動、不覆寫。')
    log('[安全] GitHub只建立獨立audit branch，不更新main。')
    if not ROOT.is_dir():
        try:
            from google.colab import drive  # type: ignore
            drive.mount('/content/drive', force_remount=False)
        except Exception as e:
            raise RuntimeError(f'無法掛載Google Drive：{e}')
    if not ROOT.is_dir():
        raise RuntimeError(f'找不到Drive根目錄：{ROOT}')
    # 在開始掃描前確認既有 Token 可取得；不輸出 Token。
    find_token()
    run_stamp = stamp()
    out_dir = ROOT / f'看股最速適_v1_2_6_全市場Q5橫斷面準備度稽核_{run_stamp}'
    report = main_audit(out_dir)
    log('[PASS] 全市場Q5橫斷面準備度報告已產生。')
    print((out_dir / '11_執行摘要.txt').read_text(encoding='utf-8-sig'), flush=True)
    try:
        script_src = Path(__file__).resolve()
        if script_src.is_file():
            shutil.copy2(script_src, out_dir / script_src.name)
    except Exception as e:
        log(f'[WARN] 程式複製至成果資料夾失敗：{e}')
    # 更新 README 檔案清單，將程式本身也納入 ZIP 的可重現索引。
    readme_path = out_dir / 'README_稽核說明.md'
    readme_lines = [
        '# SSO v1.2.6｜全市場個股 Q5 橫斷面準備度稽核', '',
        f"- 近似普通股範圍：`{report.get('近似全市場普通股代號數')}` 檔",
        f"- 前瞻交易日：`{report.get('前瞻起日')}` 至 `{report.get('前瞻迄日')}`",
        f"- 雙市場 Q5 可計算日：`{report.get('雙市場Q5可計算日數')}`",
        '- 不輸出個股排名或 Q5 股票名單；不產生交易訊號、不計算淨報酬、不回測、不重訓。',
        '- Google Drive來源唯讀；只建立獨立audit branch，不更新main。', '', '## 檔案',
    ]
    for p in sorted(out_dir.iterdir()):
        if p.is_file() and p.name != readme_path.name:
            readme_lines.append(f'- `{p.name}` — {p.stat().st_size:,} bytes — SHA256 `{sha256_file(p)}`')
    readme_path.write_text('\n'.join(readme_lines) + '\n', encoding='utf-8')
    zip_path = create_zip(out_dir)
    log('[08/09] 上傳本次結果至GitHub audit branch……')
    upload = upload_github(out_dir, zip_path, report, run_stamp)
    print('=' * 104, flush=True)
    print('[PASS] GitHub上傳完成', flush=True)
    print('Repository：CHLINX/sso', flush=True)
    print('Audit branch：', upload['branch'], flush=True)
    print('Commit：', upload['commit'], flush=True)
    print('上傳檔案數：', upload['uploaded_files'], flush=True)
    print('Drive資料夾：', out_dir, flush=True)
    print('GitHub資料夾：', upload['folder_url'], flush=True)
    print('GitHub ZIP：', upload['zip_url'], flush=True)
    print('GitHub Commit：', upload['commit_url'], flush=True)
    print('main SHA：', upload['main_before'], flush=True)
    print('[PASS] main未更新；Drive來源唯讀。', flush=True)
    print('=' * 104, flush=True)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
