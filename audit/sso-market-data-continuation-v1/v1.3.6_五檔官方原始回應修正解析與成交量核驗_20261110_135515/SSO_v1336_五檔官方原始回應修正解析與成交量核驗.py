#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SSO_Market_Data_Continuation_TWSE_TPEX
v1.3.6｜五檔官方原始回應修正解析與成交量核驗

本版目的：
1. 以 v1.3.4 找到的單一來源檔「12筆OHLCV異常_校正後分類.csv」為追溯中心。
2. 核對來源檔 SHA256、完整欄位、五檔指定日期原始列及每一欄原值。
3. 在 v1.2.3 專屬資料夾及其 ZIP 內，搜尋可能產生該 CSV 的程式、Notebook、說明、manifest、紀錄及上游檔案。
4. 重新列出 v1.3.4 曾回報的解析錯誤，逐檔記錄錯誤與解析方式。
5. 將官方來源 ZIP 內可辨識的五檔同日資料列出確切成員路徑，避免只留彙總值。

安全界線：
- Google Drive 僅讀取指定 v1.2.3 資料夾，不掃描整個 Drive。
- 不呼叫 TWSE / TPEx 官方行情 API。
- 不改寫、搬移或刪除來源檔；不更新正式資料庫。
- 所有輸出只寫入 /content。
- GitHub 只新增獨立 audit branch；以 main 當下 SHA 為 parent，並核對 main 前後不變。

Colab：
  %run /content/SSO_v1336_五檔官方原始回應修正解析與成交量核驗.py
"""
from __future__ import annotations

import os
import re
import io
import sys
import csv
import json
import gzip
import time
import base64
import hashlib
import zipfile
import tempfile
import shutil
import inspect
from pathlib import Path
from datetime import datetime, date
from urllib.parse import quote

import pandas as pd
import requests

# =============================================================================
# 1. 固定參數
# =============================================================================
PROJECT_ROOT = Path('/content/drive/MyDrive/看股最速適')
V123_NAME = 'v1.2.3_稽核結論校正與證據去循環_20261109_200337'
TARGET_DATE = '2026-11-09'
TARGET_CODES = ['3085', '4305', '4406', '4530', '6512']
TARGET_SOURCE_NAME = '12筆OHLCV異常_校正後分類.csv'
EXPECTED_TARGET_SOURCE_SHA256 = '5f164a2c26279713aa81df5161763fdc480e7018697329d4828e328d6bfc446e'
EXPECTED_OFFICIAL_ZIP_SHA256 = '48fbd3a78edcde0efbb4b39f7dc24197d55047fbac00b3b7f9a41f586d32480d'
OFFICIAL_ZIP_NAME = 'v1.3.3_官方普通股範圍與逐日行情交叉驗證_20261109_125127.zip'
OFFICIAL_RAW_URL = (
    'https://raw.githubusercontent.com/CHLINX/sso/'
    'audit/sso-market-data-continuation-v1-v133-upload-20261109_125725/'
    'audit/sso-market-data-continuation-v1/'
    'v1.3.3_%E5%AE%98%E6%96%B9%E6%99%AE%E9%80%9A%E8%82%A1%E7%AF%84%E5%9C%8D%E8%88%87%E9%80%90%E6%97%A5%E8%A1%8C%E6%83%85%E4%BA%A4%E5%8F%89%E9%A9%97%E8%AD%89_20261109_125127/'
    'v1.3.3_%E5%AE%98%E6%96%B9%E6%99%AE%E9%80%9A%E8%82%A1%E7%AF%84%E5%9C%8D%E8%88%87%E9%80%90%E6%97%A5%E8%A1%8C%E6%83%85%E4%BA%A4%E5%8F%89%E9%A9%97%E8%AD%89_20261109_125127.zip'
)
MAX_FILE_BYTES = 100 * 1024 * 1024
MAX_TEXT_BYTES = 20 * 1024 * 1024
MAX_SNIPPETS_PER_FILE = 80
STAMP = datetime.now().strftime('%Y%m%d_%H%M%S')
OUT_DIR = Path(tempfile.mkdtemp(prefix=f'sso_v1336_五檔官方原始回應核驗_{STAMP}_', dir='/content'))
OUT_DIR.mkdir(parents=True, exist_ok=True)
print('[OUTPUT]', OUT_DIR)

# =============================================================================
# 2. 通用函式
# =============================================================================
def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


def norm_header(value) -> str:
    return re.sub(r'\s+', '', str(value or '').replace('\ufeff', '')).lower()


def norm_code(value) -> str:
    s = str(value or '').strip().upper()
    if s.lower() in ('', 'nan', 'none', 'null'):
        return ''
    if re.fullmatch(r'\d+\.0+', s):
        s = s.split('.')[0]
    return s


def norm_market(value) -> str:
    s = norm_header(value).upper()
    if any(x in s for x in ('上櫃', '櫃買', 'TPEX', 'OTC')):
        return '上櫃'
    if any(x in s for x in ('上市', 'TWSE', 'TSE')):
        return '上市'
    return ''


def market_from_path(value) -> str:
    s = str(value or '').upper()
    if any(x in s for x in ('上櫃', '櫃買', 'TPEX', 'OTC')):
        return '上櫃'
    if any(x in s for x in ('上市', 'TWSE', 'TSE')):
        return '上市'
    return ''


def parse_date_from_path(value) -> str:
    """從官方每日原始回應檔名擷取日期；只作該日 API 檔案的日期鍵，不改寫原始資料。"""
    text = str(value or '')
    m = re.search(r'(20\d{2}-\d{2}-\d{2})', text)
    if not m:
        return ''
    return parse_date(m.group(1))


def parse_date(value) -> str:
    s = str(value or '').strip()
    if not s or s.lower() in ('nan', 'none', 'null'):
        return ''
    s = s.replace('年', '/').replace('月', '/').replace('日', '').replace('-', '/').replace('.', '/')
    m = re.search(r'(?<!\d)(20\d{2})/(\d{1,2})/(\d{1,2})(?!\d)', s)
    if m:
        try:
            return date(int(m.group(1)), int(m.group(2)), int(m.group(3))).isoformat()
        except Exception:
            return ''
    m = re.search(r'(?<!\d)(\d{2,3})/(\d{1,2})/(\d{1,2})(?!\d)', s)
    if m:
        try:
            return date(int(m.group(1)) + 1911, int(m.group(2)), int(m.group(3))).isoformat()
        except Exception:
            return ''
    digits = re.sub(r'\D', '', s)
    if len(digits) == 8 and digits.startswith(('19', '20')):
        try:
            return date(int(digits[:4]), int(digits[4:6]), int(digits[6:8])).isoformat()
        except Exception:
            return ''
    if len(digits) == 7:
        try:
            return date(int(digits[:3]) + 1911, int(digits[3:5]), int(digits[5:7])).isoformat()
        except Exception:
            return ''
    return ''


def to_number(value):
    s = str(value or '').strip()
    if not s or s.lower() in ('nan', 'none', 'null', '--', '-'):
        return None
    s = s.replace(',', '')
    s = re.sub(r'[^\d.\-]', '', s)
    try:
        return float(s)
    except Exception:
        return None


def find_col(columns, aliases):
    mapping = {norm_header(c): c for c in columns}
    for alias in aliases:
        key = norm_header(alias)
        if key in mapping:
            return mapping[key]
    return None


CODE_ALIASES = [
    '代號_norm', '股票代號', '證券代號', '公司代號', '代號', '代碼', '證券代碼', '股票代碼',
    'stock_code', 'security_code', 'code', 'SecuritiesCompanyCode', 'symbol'
]
DATE_ALIASES = [
    '日期_norm', '交易日期', '交易日', '資料日期', '行情日期', '年月日', '日期', 'date',
    'trading_date', 'trade_date', 'TradeDate'
]
MARKET_ALIASES = ['市場_norm', '市場別', '交易市場', '市場', 'market', 'market_type', 'market_code']
VOLUME_ALIASES = [
    '成交股數_norm', '成交股數', '成交量_norm', '成交股數(股)', '成交量', '總成交股數',
    '交易量', '成交數量', 'volume', 'trade_volume', 'trading_volume', 'TradeVolume',
    'TradingVolume', 'shares_traded'
]
TEXT_EXTS = {'.py', '.ipynb', '.txt', '.md', '.html', '.htm', '.json', '.jsonl', '.log', '.yaml', '.yml', '.csv'}
DATA_EXTS = ('.csv', '.csv.gz', '.json', '.json.gz')
TEXT_TERMS = [TARGET_SOURCE_NAME, EXPECTED_TARGET_SOURCE_SHA256, '12筆OHLCV異常', '校正後分類', '16檔行情缺漏_校正後逐檔結果.csv']


def read_csv_bytes(data: bytes, source_label: str, parse_log: list):
    errors = []
    for encoding in ('utf-8-sig', 'utf-8', 'cp950', 'big5'):
        for engine in ('c', 'python'):
            try:
                kwargs = {
                    'dtype': str,
                    'encoding': encoding,
                    'keep_default_na': False,
                    'engine': engine,
                }
                if engine == 'c':
                    kwargs['low_memory'] = False
                df = pd.read_csv(io.BytesIO(data), **kwargs)
                parse_log.append({
                    '來源': source_label, '狀態': 'PASS', '編碼': encoding,
                    'Parser': engine, '列數': len(df), '欄數': len(df.columns),
                    '錯誤': ''
                })
                return df
            except Exception as e:
                errors.append(f'{encoding}/{engine}: {type(e).__name__}: {e}')
    parse_log.append({
        '來源': source_label, '狀態': 'FAIL', '編碼': '', 'Parser': '',
        '列數': '', '欄數': '', '錯誤': ' || '.join(errors)
    })
    raise RuntimeError(f'CSV 解析失敗：{source_label}：' + ' || '.join(errors))


def decode_text(data: bytes):
    for enc in ('utf-8-sig', 'utf-8', 'cp950', 'big5'):
        try:
            return data.decode(enc), enc
        except UnicodeDecodeError:
            pass
    return data.decode('utf-8', errors='replace'), 'utf-8-replace'


def write_csv(rows, path: Path):
    pd.DataFrame(rows).to_csv(path, index=False, encoding='utf-8-sig')


def json_dump(obj, path: Path):
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2, default=str), encoding='utf-8')

# =============================================================================
# 3. 定位專屬來源與官方 ZIP（不掃描整個 Drive）
# =============================================================================
if not PROJECT_ROOT.exists():
    try:
        from google.colab import drive
        drive.mount('/content/drive')
    except Exception as e:
        raise FileNotFoundError(f'Google Drive 未掛載且自動掛載失敗：{e}')
if not PROJECT_ROOT.exists():
    raise FileNotFoundError(f'找不到專案目錄：{PROJECT_ROOT}')

v123_matches = []
for p in PROJECT_ROOT.iterdir():
    if p.is_dir() and p.name == V123_NAME:
        v123_matches.append(p)
if not v123_matches:
    for parent in PROJECT_ROOT.iterdir():
        if parent.is_dir():
            try:
                for p in parent.iterdir():
                    if p.is_dir() and p.name == V123_NAME:
                        v123_matches.append(p)
            except PermissionError:
                pass
if len(v123_matches) != 1:
    raise RuntimeError(f'v1.2.3 專屬資料夾應唯一，實際找到 {len(v123_matches)} 個：{v123_matches}')
V123_DIR = v123_matches[0]
print('[PASS] v1.2.3 專屬資料夾：', V123_DIR)

official_candidates = []
for child in Path('/content').iterdir():
    if child.is_dir() and child.name.startswith(('sso_v133_source_recovered_', 'sso_v1334_official_source_recovered_')):
        p = child / OFFICIAL_ZIP_NAME
        if p.is_file():
            official_candidates.append(p)
OFFICIAL_ZIP = None
for p in official_candidates:
    try:
        if sha256_file(p) == EXPECTED_OFFICIAL_ZIP_SHA256:
            OFFICIAL_ZIP = p
            break
    except OSError:
        pass
if OFFICIAL_ZIP is None:
    print('[INFO] /content 找不到已驗證官方 ZIP，從既有 GitHub Raw 來源取回；不呼叫官方行情 API。')
    recovery_dir = Path('/content') / f'sso_v1336_official_source_recovered_{STAMP}'
    recovery_dir.mkdir(parents=True, exist_ok=True)
    OFFICIAL_ZIP = recovery_dir / OFFICIAL_ZIP_NAME
    last_error = None
    for attempt in range(1, 6):
        try:
            response = requests.get(OFFICIAL_RAW_URL, timeout=(20, 120))
            response.raise_for_status()
            payload = response.content
            actual = sha256_bytes(payload)
            if actual != EXPECTED_OFFICIAL_ZIP_SHA256:
                raise RuntimeError(f'官方來源 ZIP SHA256 不符：{actual}')
            OFFICIAL_ZIP.write_bytes(payload)
            break
        except Exception as e:
            last_error = repr(e)
            if attempt < 5:
                print(f'[RETRY] GitHub Raw {attempt}/5：{last_error}')
                time.sleep(min(attempt * 3, 12))
    else:
        raise RuntimeError(f'無法取回既有 GitHub 官方來源 ZIP：{last_error}')

if sha256_file(OFFICIAL_ZIP) != EXPECTED_OFFICIAL_ZIP_SHA256:
    raise RuntimeError('官方來源 ZIP SHA256 最終驗證失敗。')
with zipfile.ZipFile(OFFICIAL_ZIP, 'r') as zf:
    bad = zf.testzip()
    if bad:
        raise RuntimeError(f'官方來源 ZIP CRC 失敗：{bad}')
    official_members = zf.infolist()
print('[PASS] 官方來源 ZIP：', OFFICIAL_ZIP)
print('[PASS] 官方來源 ZIP SHA256：', EXPECTED_OFFICIAL_ZIP_SHA256)
print('[PASS] 官方來源 ZIP CRC；成員數：', len(official_members))

# =============================================================================
# 4. Source inventory + exact target CSV inspection + error logs
# =============================================================================
source_files = sorted([p for p in V123_DIR.rglob('*') if p.is_file()], key=lambda p: str(p).lower())
print('[SOURCE] v1.2.3 專屬資料夾檔案總數：', len(source_files))
source_inventory = []
parse_log = []
parse_errors = []
target_csv_matches = []
target_csv_rows = []
target_csv_fields = []
text_hits = []
source_zip_members = []
source_skipped = []


def inspect_target_frame(df: pd.DataFrame, source_label: str, file_sha: str, row_base=2):
    cols = list(df.columns)
    code_col = find_col(cols, CODE_ALIASES)
    date_col = find_col(cols, DATE_ALIASES)
    market_col = find_col(cols, MARKET_ALIASES)
    volume_col = find_col(cols, VOLUME_ALIASES)
    target_csv_fields.append({
        '來源': source_label, '欄數': len(cols), '欄位順序JSON': json.dumps([str(c) for c in cols], ensure_ascii=False),
        '代號欄': str(code_col or ''), '日期欄': str(date_col or ''),
        '市場欄': str(market_col or ''), '成交量欄': str(volume_col or ''),
        '來源SHA256': file_sha
    })
    if code_col is None:
        parse_errors.append({'證據層':'v1.2.3-target-csv','來源':source_label,'錯誤':'找不到可辨識的股票代號欄位'})
        return
    for i, (_, row) in enumerate(df.iterrows(), start=row_base):
        code = norm_code(row.get(code_col, ''))
        if code not in TARGET_CODES:
            continue
        raw_date = str(row.get(date_col, '')) if date_col else ''
        parsed_date = parse_date(raw_date) if date_col else ''
        if date_col and parsed_date != TARGET_DATE:
            continue
        row_dict = {str(k): str(v) for k, v in row.to_dict().items()}
        target_csv_rows.append({
            '證據層':'v1.2.3-target-csv', '代號':code,
            '日期原值':raw_date, '日期解析':parsed_date,
            '市場欄原名':str(market_col or ''), '市場原值':str(row.get(market_col, '')) if market_col else '',
            '成交量欄原名':str(volume_col or ''), '成交量原值':str(row.get(volume_col, '')) if volume_col else '',
            '成交量數值':to_number(row.get(volume_col, '')) if volume_col else None,
            '來源':source_label, '來源列號':i, '來源SHA256':file_sha,
            '完整原始列JSON':json.dumps(row_dict, ensure_ascii=False)
        })


def search_text_content(text: str, origin: str, sha: str):
    lines = text.splitlines()
    hit_indexes = [i for i, line in enumerate(lines) if any(term.lower() in line.lower() for term in TEXT_TERMS)]
    for i in hit_indexes[:MAX_SNIPPETS_PER_FILE]:
        line = lines[i]
        lo, hi = max(0, i - 3), min(len(lines), i + 4)
        text_hits.append({
            '來源': origin, 'SHA256': sha, '行號': i + 1,
            '命中詞': ';'.join(t for t in TEXT_TERMS if t.lower() in line.lower()),
            '前後文起訖行': f'{lo + 1}-{hi}',
            '上下文': '\n'.join(f'{j + 1}: {lines[j]}' for j in range(lo, hi))[:12000],
            '原文': line[:3000]
        })


def inspect_data_payload(payload: bytes, logical_name: str, origin: str, outer_sha: str, member_sha: str, layer: str):
    lower = logical_name.lower()
    raw = payload
    if lower.endswith('.gz'):
        raw = gzip.decompress(payload)
    inner_sha = sha256_bytes(raw)
    if lower.endswith('.csv') or lower.endswith('.csv.gz'):
        df = read_csv_bytes(raw, origin, parse_log)
        if Path(logical_name).name.lower() == TARGET_SOURCE_NAME.lower():
            target_csv_matches.append({
                '來源':origin, 'bytes':len(payload), '外層SHA256':outer_sha,
                '成員或解壓SHA256':inner_sha, '來源層':layer
            })
            inspect_target_frame(df, origin, inner_sha)
        else:
            cols = list(df.columns)
            code_col = find_col(cols, CODE_ALIASES)
            date_col = find_col(cols, DATE_ALIASES)
            market_col = find_col(cols, MARKET_ALIASES)
            volume_col = find_col(cols, VOLUME_ALIASES)
            if code_col is not None:
                for rowno, (_, row) in enumerate(df.iterrows(), start=2):
                    code = norm_code(row.get(code_col, ''))
                    if code not in TARGET_CODES:
                        continue
                    rawdate = str(row.get(date_col, '')) if date_col else ''
                    parsed_date = parse_date(rawdate) if date_col else parse_date_from_path(origin)
                    if date_col and not parsed_date:
                        parsed_date = parse_date_from_path(origin)
                    if parsed_date and parsed_date != TARGET_DATE:
                        continue
                    market_raw = str(row.get(market_col, '')) if market_col else ''
                    target_csv_rows.append({
                        '證據層':layer, '代號':code, '日期原值':rawdate, '日期解析':parsed_date,
                        '市場欄原名':str(market_col or ''), '市場原值':market_raw,
                        '解析市場':norm_market(market_raw),
                        '路徑推定市場':market_from_path(origin),
                        '有效市場':norm_market(market_raw) or market_from_path(origin),
                        '成交量欄原名':str(volume_col or ''),
                        '成交量原值':str(row.get(volume_col, '')) if volume_col else '',
                        '成交量數值':to_number(row.get(volume_col, '')) if volume_col else None,
                        '來源':origin, '來源列號':rowno,
                        '外層SHA256':outer_sha, '內層SHA256':inner_sha,
                        '完整原始列JSON':json.dumps({str(k):str(v) for k,v in row.to_dict().items()},ensure_ascii=False)
                    })
    elif lower.endswith('.json') or lower.endswith('.json.gz'):
        obj, enc = json.loads(raw.decode('utf-8-sig')), 'utf-8-sig'
        def walk(x, suffix=''):
            if isinstance(x, list):
                # 官方原始 JSON 的 tables 常是「table 物件陣列」，每個物件各自含 fields/data。
                # 先遞迴處理這類容器，不能把 table 容器本身誤當成行情資料列 DataFrame。
                if x and all(isinstance(item, dict) for item in x) and any(
                    isinstance(item.get('fields'), list) and isinstance(item.get('data'), list)
                    for item in x
                ):
                    for j, child in enumerate(x):
                        walk(child, f'{suffix}.table[{j}]')
                    return
                if x and isinstance(x[0], dict):
                    try:
                        df = pd.DataFrame(x)
                        cols = list(df.columns)
                        code_col = find_col(cols, CODE_ALIASES)
                        date_col = find_col(cols, DATE_ALIASES)
                        market_col = find_col(cols, MARKET_ALIASES)
                        volume_col = find_col(cols, VOLUME_ALIASES)
                        if code_col:
                            for rowno, (_, row) in enumerate(df.iterrows(), start=2):
                                code = norm_code(row.get(code_col,''))
                                d0 = str(row.get(date_col,'')) if date_col else ''
                                d1 = parse_date(d0) if date_col else parse_date_from_path(origin)
                                if date_col and not d1:
                                    d1 = parse_date_from_path(origin)
                                if code in TARGET_CODES and d1 == TARGET_DATE:
                                    mr = str(row.get(market_col,'')) if market_col else ''
                                    target_csv_rows.append({
                                        '證據層':layer,'代號':code,'日期原值':d0,'日期解析':d1,
                                        '市場欄原名':str(market_col or ''),'市場原值':mr,
                                        '解析市場':norm_market(mr),'路徑推定市場':market_from_path(origin),
                                        '有效市場':norm_market(mr) or market_from_path(origin),
                                        '成交量欄原名':str(volume_col or ''),'成交量原值':str(row.get(volume_col,'')) if volume_col else '',
                                        '成交量數值':to_number(row.get(volume_col,'')) if volume_col else None,
                                        '來源':origin+suffix,'來源列號':rowno,'外層SHA256':outer_sha,'內層SHA256':inner_sha,
                                        '完整原始列JSON':json.dumps({str(k):str(v) for k,v in row.to_dict().items()},ensure_ascii=False)
                                    })
                    except Exception as e:
                        parse_errors.append({'證據層':layer,'來源':origin+suffix,'錯誤':f'JSON list table parse: {repr(e)}'})
                else:
                    for j, child in enumerate(x):
                        if isinstance(child,(dict,list)):
                            walk(child,f'{suffix}[{j}]')
                return
            if isinstance(x,dict):
                fields, rows = x.get('fields'), x.get('data')
                if isinstance(fields,list) and isinstance(rows,list):
                    try:
                        walk([dict(zip(fields, r)) if not isinstance(r,dict) else r for r in rows],suffix+'.fields-data')
                    except Exception as e:
                        parse_errors.append({'證據層':layer,'來源':origin+suffix,'錯誤':f'JSON fields/data parse: {repr(e)}'})
                for key in ('data','result','records','items','rows','payload','tables'):
                    child=x.get(key)
                    if isinstance(child,(dict,list)) and not (key=='data' and isinstance(fields,list)):
                        walk(child,suffix+'.'+key)
        walk(obj)
    else:
        text, enc = decode_text(raw)
        search_text_content(text, origin, inner_sha)

# Inventory and inspect v1.2.3 files.
for path in source_files:
    rel = str(path.relative_to(V123_DIR))
    try:
        size = path.stat().st_size
        outer_sha = sha256_file(path)
        source_inventory.append({'來源檔':rel,'bytes':size,'SHA256':outer_sha,'副檔名':''.join(path.suffixes)})
        if size > MAX_FILE_BYTES:
            source_skipped.append({'來源':rel,'原因':f'超過100MB：{size}'})
            continue
        raw = path.read_bytes()
        lower = path.name.lower()
        if lower.endswith('.zip'):
            with zipfile.ZipFile(io.BytesIO(raw),'r') as zf:
                bad=zf.testzip()
                if bad:
                    parse_errors.append({'證據層':'v1.2.3','來源':rel,'錯誤':f'ZIP CRC失敗：{bad}'})
                    continue
                for info in zf.infolist():
                    if info.is_dir():
                        continue
                    member=info.filename
                    mlow=member.lower()
                    if info.file_size > MAX_FILE_BYTES:
                        source_skipped.append({'來源':f'{rel}::{member}','原因':f'ZIP member 超過100MB：{info.file_size}'})
                        continue
                    payload=zf.read(info)
                    member_sha=sha256_bytes(payload)
                    source_zip_members.append({'外層來源':rel,'外層SHA256':outer_sha,'ZIP內檔案':member,
                                               'bytes':info.file_size,'ZIP CRC32':f'{info.CRC:08x}','成員SHA256':member_sha})
                    if Path(member).name.lower()==TARGET_SOURCE_NAME.lower() or mlow.endswith(DATA_EXTS):
                        inspect_data_payload(payload,member,f'{rel}::{member}',outer_sha,member_sha,'v1.2.3-zip-member')
                    if Path(member).suffix.lower() in TEXT_EXTS and info.file_size <= MAX_TEXT_BYTES:
                        text, enc=decode_text(payload)
                        search_text_content(text,f'{rel}::{member}',member_sha)
        elif lower.endswith(DATA_EXTS) or path.name.lower()==TARGET_SOURCE_NAME.lower():
            inspect_data_payload(raw,path.name,rel,outer_sha,outer_sha,'v1.2.3-file')
        if path.suffix.lower() in TEXT_EXTS and size <= MAX_TEXT_BYTES:
            text, enc=decode_text(raw)
            search_text_content(text,rel,outer_sha)
    except Exception as e:
        parse_errors.append({'證據層':'v1.2.3','來源':rel,'錯誤':f'{type(e).__name__}: {e}'})

# Explicitly collect exact target CSV files that may not have been caught by suffix handling.
for path in source_files:
    if path.name.lower()==TARGET_SOURCE_NAME.lower() and not any(x['來源'].endswith(path.name) for x in target_csv_matches):
        try:
            data=path.read_bytes(); digest=sha256_bytes(data)
            df=read_csv_bytes(data,str(path),parse_log)
            target_csv_matches.append({'來源':str(path.relative_to(V123_DIR)),'bytes':len(data),'外層SHA256':digest,
                                       '成員或解壓SHA256':digest,'來源層':'v1.2.3-file-direct-fallback'})
            inspect_target_frame(df,str(path.relative_to(V123_DIR)),digest)
        except Exception as e:
            parse_errors.append({'證據層':'v1.2.3-target-csv','來源':str(path),'錯誤':repr(e)})

# Search official ZIP members; preserve exact member path for all matched target rows.
official_inventory=[]
official_matches=[]
official_member_parse=[]
official_raw_payload_inventory=[]
official_outer_sha=sha256_file(OFFICIAL_ZIP)
with zipfile.ZipFile(OFFICIAL_ZIP,'r') as zf:
    for info in zf.infolist():
        if info.is_dir():
            continue
        member=info.filename; payload=zf.read(info); msha=sha256_bytes(payload)
        official_inventory.append({'ZIP內檔案':member,'bytes':info.file_size,'CRC32':f'{info.CRC:08x}','成員SHA256':msha})
        # 將目標日期的官方原始回應原封不動複製到成果包，供離線複核；不呼叫 API。
        if info.file_size <= MAX_FILE_BYTES and '/官方原始回應/' in member and member.lower().endswith(('.json.gz', '.json')) and parse_date_from_path(member) == TARGET_DATE:
            raw_dir = OUT_DIR / '官方原始回應_20261109'
            raw_dir.mkdir(parents=True, exist_ok=True)
            raw_copy = raw_dir / Path(member).name
            raw_copy.write_bytes(payload)
            official_raw_payload_inventory.append({
                'ZIP內路徑': member, '成果包複本路徑': str(raw_copy.relative_to(OUT_DIR)),
                'bytes': info.file_size, 'ZIP CRC32': f'{info.CRC:08x}',
                '壓縮成員SHA256': msha, '路徑日期': parse_date_from_path(member),
                '路徑市場': market_from_path(member)
            })
        before=len(target_csv_rows); before_parse=len(parse_errors)
        if info.file_size > MAX_FILE_BYTES:
            official_member_parse.append({'ZIP內檔案':member,'狀態':'SKIPPED','原因':f'超過100MB：{info.file_size}'})
            continue
        if member.lower().endswith(DATA_EXTS):
            try:
                inspect_data_payload(payload,member,f'{OFFICIAL_ZIP.name}::{member}',official_outer_sha,msha,'v1.3.3-official-zip')
                official_member_parse.append({'ZIP內檔案':member,'狀態':'PASS','新增命中列數':len(target_csv_rows)-before,
                                               '新增解析錯誤數':len(parse_errors)-before_parse})
            except Exception as e:
                parse_errors.append({'證據層':'official','來源':member,'錯誤':f'{type(e).__name__}: {e}'})
                official_member_parse.append({'ZIP內檔案':member,'狀態':'FAIL','錯誤':repr(e)})
        elif Path(member).suffix.lower() in TEXT_EXTS and info.file_size <= MAX_TEXT_BYTES:
            try:
                txt,enc=decode_text(payload); search_text_content(txt,f'{OFFICIAL_ZIP.name}::{member}',msha)
                official_member_parse.append({'ZIP內檔案':member,'狀態':'TEXT_SCANNED','編碼':enc})
            except Exception as e:
                parse_errors.append({'證據層':'official-text','來源':member,'錯誤':repr(e)})
                official_member_parse.append({'ZIP內檔案':member,'狀態':'FAIL','錯誤':repr(e)})
        else:
            official_member_parse.append({'ZIP內檔案':member,'狀態':'NOT_TABLE_OR_TEXT'})

# =============================================================================
# 5. Cross-check: the target report CSV is one container, not five independent origins.
# =============================================================================
expected_sha_matches = [x for x in target_csv_matches if x.get('成員或解壓SHA256') == EXPECTED_TARGET_SOURCE_SHA256]
unique_origin = sorted({(x.get('來源'),x.get('成員或解壓SHA256')) for x in target_csv_matches})

# 僅統計「檔名完全相符、SHA 已核對」的目標 CSV 本身，不混入其他相關報告。
exact_target_rows = [x for x in target_csv_rows if x.get('證據層') == 'v1.2.3-target-csv']
related_v123_rows = [x for x in target_csv_rows if str(x.get('證據層','')).startswith('v1.2.3') and x.get('證據層') != 'v1.2.3-target-csv']

def summarize_rows_by_code(rows, code_field='代號', source_field='來源', volume_field='成交量數值', date_field='日期解析'):
    result=[]
    for code in TARGET_CODES:
        subset=[x for x in rows if x.get(code_field)==code and (not x.get(date_field) or x.get(date_field)==TARGET_DATE)]
        volumes=sorted({x.get(volume_field) for x in subset if x.get(volume_field) is not None})
        result.append({
            '代號':code,'命中列數':len(subset),'成交量集合':json.dumps(volumes,ensure_ascii=False),
            '來源檔集合':json.dumps(sorted({x.get(source_field,'') for x in subset}),ensure_ascii=False),
            '來源列號集合':json.dumps(sorted({x.get('來源列號') for x in subset}),ensure_ascii=False),
            '有效市場集合':json.dumps(sorted({x.get('有效市場') or x.get('解析市場') or x.get('路徑推定市場') or x.get('市場原值','') for x in subset}),ensure_ascii=False),
            '不同列內容Hash數':len({sha256_bytes(x.get('完整原始列JSON','').encode('utf-8')) for x in subset if x.get('完整原始列JSON')})
        })
    return result

target_summary = summarize_rows_by_code(exact_target_rows)
related_summary = summarize_rows_by_code(related_v123_rows)

# 官方資料嚴格分層：原始回應 JSON 與 ZIP 內衍生 CSV 不混為一談。
official_all_rows = [x for x in target_csv_rows if x.get('證據層') == 'v1.3.3-official-zip']
official_raw_rows = [x for x in official_all_rows if '/官方原始回應/' in str(x.get('來源','')) and re.search(r'\.json(?:\.gz)?(?:$|[.\[])', str(x.get('來源','')).lower()) is not None]
official_nonraw_rows = [x for x in official_all_rows if x not in official_raw_rows]
official_raw_summary = summarize_rows_by_code(official_raw_rows)
official_nonraw_summary = summarize_rows_by_code(official_nonraw_rows)

raw_status=[]
for code in TARGET_CODES:
    subset=[x for x in official_raw_rows if x.get('代號')==code and x.get('日期解析')==TARGET_DATE]
    vols=sorted({x.get('成交量數值') for x in subset if x.get('成交量數值') is not None})
    markets=sorted({x.get('有效市場') or x.get('路徑推定市場') or x.get('解析市場') or '' for x in subset})
    if not subset:
        status='NO_MATCH_IN_TARGET_DATE_RAW_RESPONSE_NOT_EQUIVALENT_TO_ZERO'
    elif not vols:
        status='RAW_RESPONSE_ROW_FOUND_BUT_VOLUME_FIELD_UNRESOLVED'
    elif len(vols) > 1:
        status='RAW_RESPONSE_CONFLICTING_VOLUME_VALUES'
    elif vols == [0.0]:
        status='RAW_OFFICIAL_RESPONSE_ZERO_VOLUME_ROW_FOUND'
    elif vols and vols[0] > 0:
        status='RAW_OFFICIAL_RESPONSE_POSITIVE_VOLUME_ROW_FOUND'
    else:
        status='RAW_OFFICIAL_RESPONSE_OTHER_VOLUME_VALUE_REVIEW'
    raw_status.append({'代號':code,'原始回應命中列數':len(subset),'原始回應成交量集合':json.dumps(vols,ensure_ascii=False),
                       '原始回應市場集合':json.dumps(markets,ensure_ascii=False),'原始回應檔案':json.dumps(sorted({x.get('來源','') for x in subset}),ensure_ascii=False),
                       '判讀狀態':status})

# Search findings by file; distinguish explicit generator references from incidental target data.
reference_hits = [x for x in text_hits if TARGET_SOURCE_NAME.lower() in x.get('原文','').lower() or
                  EXPECTED_TARGET_SOURCE_SHA256.lower() in x.get('原文','').lower()]
# 僅將明確提及輸出檔名或其 SHA 的程式/說明文字列為生成程式候選；不是已證明的生成關係。
explicit_generator_refs=[x for x in reference_hits if Path(str(x.get('來源','')).split('::')[-1]).suffix.lower() in {'.py','.ipynb','.html','.txt','.md','.log'}]

# =============================================================================
# 6. Write complete audit files
# =============================================================================
write_csv(source_inventory,OUT_DIR/'v1.2.3來源檔案清冊.csv')
write_csv(source_zip_members,OUT_DIR/'v1.2.3內含ZIP成員清冊.csv')
write_csv(target_csv_matches,OUT_DIR/'目標校正分類CSV定位與SHA256.csv')
write_csv(target_csv_fields,OUT_DIR/'目標CSV欄位盤點.csv')
write_csv(target_csv_rows,OUT_DIR/'五檔所有命中列完整欄位.csv')
write_csv(target_summary,OUT_DIR/'五檔精確目標CSV逐筆摘要.csv')
write_csv(related_summary,OUT_DIR/'五檔其他v1.2.3相關報告逐筆摘要.csv')
write_csv(text_hits,OUT_DIR/'上游檔名與雜湊文字搜尋命中.csv')
write_csv(reference_hits,OUT_DIR/'目標CSV明確來源引用命中.csv')
write_csv(explicit_generator_refs,OUT_DIR/'可能生成程式引用及上下文.csv')
write_csv(parse_log,OUT_DIR/'CSV解析嘗試與結果.csv')
write_csv(parse_errors,OUT_DIR/'來源解析錯誤逐筆清單.csv')
write_csv(source_skipped,OUT_DIR/'來源檔案略過清單.csv')
write_csv(official_inventory,OUT_DIR/'v1.3.3官方ZIP成員清冊.csv')
write_csv(official_member_parse,OUT_DIR/'官方ZIP逐成員解析狀態.csv')
write_csv(official_raw_rows,OUT_DIR/'官方原始API回應五檔逐筆原始列.csv')
write_csv(official_raw_summary,OUT_DIR/'官方原始API回應五檔逐筆摘要.csv')
write_csv(official_nonraw_summary,OUT_DIR/'官方ZIP非原始JSON成員五檔逐筆摘要.csv')
write_csv(raw_status,OUT_DIR/'五檔原始官方回應判讀狀態.csv')
write_csv(official_raw_payload_inventory,OUT_DIR/'目標日期官方原始回應封存清冊.csv')

# Generator references are candidates only; they are not treated as proven code lineage.
summary={
    '版本':'v1.3.6_五檔官方原始回應修正解析與成交量核驗',
    '執行時間':STAMP,'目標日期':TARGET_DATE,'目標代號':TARGET_CODES,
    'v1.2.3資料夾':str(V123_DIR),'v1.2.3來源檔案總數':len(source_files),
    '目標CSV檔名':TARGET_SOURCE_NAME,'目標CSV預期SHA256':EXPECTED_TARGET_SOURCE_SHA256,
    '目標CSV定位數':len(target_csv_matches),'目標CSV SHA256完全相符數':len(expected_sha_matches),
    '目標CSV來源容器與SHA組合數':len(unique_origin),
    '目標CSV來源容器與SHA':unique_origin,
    '五檔精確目標CSV命中列':target_summary,
    '五檔其他v1.2.3相關報告命中列':related_summary,
    '五檔官方原始API回應命中列':official_raw_summary,
    '五檔官方ZIP非原始JSON成員命中列':official_nonraw_summary,
    '五檔原始官方回應判讀狀態':raw_status,
    '目標日期官方原始回應封存清冊':official_raw_payload_inventory,
    '官方ZIP成員數':len(official_inventory),
    '官方ZIP資料表/文字成員解析狀態數':sum(1 for x in official_member_parse if x.get('狀態') in ('PASS','TEXT_SCANNED','FAIL')),
    '明確提及目標CSV檔名或SHA的文字引用數':len(reference_hits),
    '可能生成程式引用數':len(explicit_generator_refs),
    '可能生成程式引用':explicit_generator_refs,
    '解析錯誤數':len(parse_errors),
    '解析錯誤摘要':parse_errors,
    '重要限制':'本版僅在官方原始 JSON/JSON.GZ 成員成功解析且同時匹配代號及 2026-11-09 日期時，才將列標示為原始回應證據。ZIP 內彙整 CSV 屬衍生報告，不可代替原始回應。無原始列不等同於成交量為零。',
    '官方原始JSON回應解析錯誤數':sum(1 for e in parse_errors if e.get('證據層')=='v1.3.3-official-zip' and '/官方原始回應/' in str(e.get('來源',''))),
    '官方行情API呼叫':False,'Drive來源寫入':False,'Drive來源刪除':False,'正式資料庫更新':False,
    '狀態':'RAW_OFFICIAL_RESPONSE_REVIEW_REQUIRED' if any(x['判讀狀態'] not in ('RAW_OFFICIAL_RESPONSE_ZERO_VOLUME_ROW_FOUND','RAW_OFFICIAL_RESPONSE_POSITIVE_VOLUME_ROW_FOUND') for x in raw_status) or parse_errors else 'RAW_OFFICIAL_RESPONSE_PARSED_REVIEW_VALUES'
}
json_dump(summary,OUT_DIR/'v1.3.6上游生成來源追溯摘要.json')

readme=f'''SSO_Market_Data_Continuation_TWSE_TPEX v1.3.6｜五檔官方原始回應修正解析與成交量核驗

執行時間：{STAMP}
目標日期：{TARGET_DATE}
目標代號：{', '.join(TARGET_CODES)}
目標來源檔：{TARGET_SOURCE_NAME}
目標來源檔預期 SHA256：{EXPECTED_TARGET_SOURCE_SHA256}

本版修正 v1.3.5 的 JSON 原始回應解析錯誤：原程式在 inspect_data_payload() 內使用 pd 作為日期區域變數，遮蔽 pandas 別名 pd，導致官方 JSON.GZ 內 tables 無法建立 DataFrame。本版改用 parsed_date，並以原始回應檔名日期作為日期欄缺失時的日期鍵。

本版將官方原始回應 JSON/JSON.GZ 與官方 ZIP 內的彙整 CSV、公司主檔等非原始 JSON 成員分開統計；只有原始回應檔內實際找到代號及 2026-11-09 日期紀錄，才作為原始回應證據。未找到原始列不等於零成交量。另封存 2026-11-09 上市及上櫃原始回應成員（若 ZIP 有提供），並輸出五檔所有命中列、來源雜湊、解析錯誤及生成程式引用上下文。

注意：目標校正分類 CSV 與其他相關報告 CSV 分開統計。報告間相同成交量並非獨立原始證據；仍需追到生成程式及其原始輸入，才能建立完整上游證據鏈。

安全界線：Google Drive 僅讀取指定 v1.2.3 資料夾；不呼叫 TWSE/TPEx 官方行情 API；不寫入或刪除 Drive；不更新正式資料庫。成果上傳獨立 GitHub audit branch，不修改 main。
'''
(OUT_DIR/'README_操作與判讀.txt').write_text(readme,encoding='utf-8')

print('\n'+'='*88)
print('v1.3.6｜五檔官方原始回應修正解析與成交量核驗')
print('='*88)
print('[目標CSV定位數]',len(target_csv_matches))
print('[目標CSV SHA256完全相符數]',len(expected_sha_matches))
print('[來源容器與SHA組合數]',len(unique_origin))
print('[目標CSV五檔列數]',sum(x['命中列數'] for x in target_summary))
print('[明確上游檔名/SHA引用數]',len(reference_hits))
print('[可能生成程式引用數]',len(explicit_generator_refs))
print('[官方ZIP成員數]',len(official_inventory))
print('[解析錯誤數]',len(parse_errors))
if parse_errors:
    print('\n[解析錯誤逐筆]')
    for i,e in enumerate(parse_errors,1):
        print(f"{i}. [{e.get('證據層','')}] {e.get('來源','')} :: {e.get('錯誤','')}")
print('\n[五檔目標CSV摘要]')
print(pd.DataFrame(target_summary).to_string(index=False))
print('\n[五檔其他 v1.2.3 相關報告摘要]')
print(pd.DataFrame(related_summary).to_string(index=False))
print('\n[五檔官方原始 API 回應摘要]')
print(pd.DataFrame(official_raw_summary).to_string(index=False))
print('\n[五檔原始官方回應判讀狀態]')
print(pd.DataFrame(raw_status).to_string(index=False))
print('\n[官方原始回應封存數]', len(official_raw_payload_inventory))

# =============================================================================
# 7. Program self-capture: __file__ plus explicit /content candidate search
# =============================================================================
script_source_path=None
possible=[]
try:
    f=globals().get('__file__')
    if f:
        possible.append(Path(f))
except Exception:
    pass
try:
    f=inspect.getsourcefile(inspect.currentframe())
    if f:
        possible.append(Path(f))
except Exception:
    pass
for base in {Path('/content'),Path.cwd()}:
    try:
        for p in base.iterdir():
            if p.is_file() and ('v1336' in p.name.lower() or 'v1335' in p.name.lower()) and p.suffix.lower()=='.py':
                possible.append(p)
    except Exception:
        pass
for p in dict.fromkeys(possible):
    try:
        # 包含目前執行中的 .py 本身；不能因為它等於 __file__ 就排除。
        if p.is_file():
            script_source_path=p
            break
    except Exception:
        pass

script_copy=OUT_DIR/'SSO_v1336_五檔官方原始回應修正解析與成交量核驗.py'
if script_source_path:
    shutil.copy2(script_source_path,script_copy)
    print('[PASS] 程式來源封存：',script_source_path)
else:
    # Notebook cell 執行時沒有 __file__：嘗試從 IPython 輸入歷史取得本次完整 cell 原始碼。
    history_source=''
    try:
        from IPython import get_ipython
        ip=get_ipython()
        history=getattr(getattr(ip,'history_manager',None),'input_hist_raw',[]) if ip else []
        for cell in reversed(history):
            if isinstance(cell,str) and 'def inspect_data_payload' in cell and 'EXPECTED_OFFICIAL_ZIP_SHA256' in cell and 'v1.3.6' in cell:
                history_source=cell
                break
    except Exception:
        history_source=''
    if history_source:
        script_copy.write_text(history_source,encoding='utf-8')
        print('[PASS] 從 Notebook 輸入歷史封存完整程式原始碼。')
    else:
        note=(
            '本次執行環境未提供可讀取的原始 .py 檔路徑，且 Notebook 輸入歷史未取得完整程式原始碼。\n'
            '請保留本次下載的完整程式檔：SSO_v1336_五檔官方原始回應修正解析與成交量核驗.py\n'
            '本次輸出 ZIP 含完整分析報告，但不宣稱內含執行程式原檔。\n'
        )
        (OUT_DIR/'程式原檔封存狀態.txt').write_text(note,encoding='utf-8')
        print('[WARN] 無法封存執行程式原檔；結果包會明確標示。')

# =============================================================================
# 8. Create result ZIP
# =============================================================================
ZIP_PATH=OUT_DIR.with_suffix('.zip')
with zipfile.ZipFile(ZIP_PATH,'w',compression=zipfile.ZIP_DEFLATED,compresslevel=6) as zf:
    for p in sorted(OUT_DIR.rglob('*')):
        if p.is_file():
            zf.write(p,arcname=p.relative_to(OUT_DIR.parent))
zip_sha=sha256_file(ZIP_PATH)
zip_size=ZIP_PATH.stat().st_size
with zipfile.ZipFile(ZIP_PATH,'r') as zf:
    bad=zf.testzip()
    if bad:
        raise RuntimeError(f'本次成果 ZIP CRC 失敗：{bad}')
    zip_members=len(zf.namelist())
print('\n[ZIP PASS]',ZIP_PATH)
print('[ZIP SIZE]',zip_size)
print('[ZIP SHA256]',zip_sha)
print('[ZIP FILE COUNT]',zip_members)

# =============================================================================
# 9. GitHub audit branch: upload result ZIP + source script + README; main unchanged
# =============================================================================
token=os.environ.get('GITHUB_TOKEN','').strip()
if not token:
    try:
        from google.colab import userdata
        token=userdata.get('GITHUB_TOKEN') or ''
    except Exception:
        token=''
if not token:
    raise RuntimeError(f'找不到 GITHUB_TOKEN；成果 ZIP 已留在 {ZIP_PATH}')

OWNER,REPO='CHLINX','sso'
API=f'https://api.github.com/repos/{OWNER}/{REPO}'
gh=requests.Session()
gh.headers.update({
    'Authorization':f'Bearer {token}',
    'Accept':'application/vnd.github+json',
    'X-GitHub-Api-Version':'2022-11-28',
    'User-Agent':'SSO-v1336-source-generation-lineage-audit'
})

def gh_call(method,url,payload=None,params=None):
    last_error=None
    for attempt in range(1,6):
        try:
            response=gh.request(method,url,json=payload,params=params,timeout=120)
            if response.status_code in (200,201):
                return response.json()
            body=response.text[:2000]
            if response.status_code not in (502,503,504) and 'timeout' not in body.lower():
                raise RuntimeError(f'GitHub HTTP {response.status_code}: {body}')
            last_error=body
        except Exception as e:
            last_error=repr(e)
            if attempt==5:
                break
        print(f'[GITHUB RETRY] {attempt}/5')
        time.sleep(min(attempt*3,15))
    raise RuntimeError(f'GitHub API 最終失敗：{last_error}')

repo_info=gh_call('GET',API)
default_branch=repo_info.get('default_branch','main')
main_before_ref=gh_call('GET',f"{API}/git/ref/heads/{quote(default_branch,safe='')}")
main_before=main_before_ref['object']['sha']
base_commit=gh_call('GET',f'{API}/git/commits/{main_before}')
base_tree=base_commit['tree']['sha']
print('[MAIN BEFORE]',default_branch,main_before)

branch=f'audit/sso-market-data-continuation-v1-v1336-target-csv-lineage-{STAMP}'
repo_folder=f'audit/sso-market-data-continuation-v1/v1.3.6_五檔官方原始回應修正解析與成交量核驗_{STAMP}'
files_to_upload=[ZIP_PATH,OUT_DIR/'README_操作與判讀.txt']
script_copy=OUT_DIR/'SSO_v1336_五檔官方原始回應修正解析與成交量核驗.py'
if script_copy.is_file():
    files_to_upload.append(script_copy)
else:
    print('[WARN] GitHub 將上傳成果 ZIP 與 README；本次執行環境沒有可封存的原始 .py 檔。')

entries=[]
expected_uploads={}
for file_path in files_to_upload:
    payload=file_path.read_bytes()
    blob=gh_call('POST',f'{API}/git/blobs',{
        'content':base64.b64encode(payload).decode('ascii'),
        'encoding':'base64'
    })
    repo_path=f'{repo_folder}/{file_path.name}'
    entries.append({'path':repo_path,'mode':'100644','type':'blob','sha':blob['sha']})
    expected_uploads[repo_path]=len(payload)

tree=gh_call('POST',f'{API}/git/trees',{'base_tree':base_tree,'tree':entries})
commit=gh_call('POST',f'{API}/git/commits',{
    'message':'SSO v1.3.6 v1.3.6 raw official response parsing and volume verification',
    'tree':tree['sha'],'parents':[main_before]
})
gh_call('POST',f'{API}/git/refs',{'ref':f'refs/heads/{branch}','sha':commit['sha']})

for entry in entries:
    remote=gh_call('GET',f"{API}/contents/{quote(entry['path'],safe='/')}",params={'ref':branch})
    if remote.get('sha')!=entry['sha'] or remote.get('size')!=expected_uploads[entry['path']]:
        raise RuntimeError(f"GitHub 回讀驗證失敗：{entry['path']}")
main_after_ref=gh_call('GET',f"{API}/git/ref/heads/{quote(default_branch,safe='')}")
main_after=main_after_ref['object']['sha']
if main_after!=main_before:
    raise RuntimeError(f'main SHA 發生變化：{main_before} -> {main_after}')

commit_url=f'https://github.com/{OWNER}/{REPO}/commit/{commit["sha"]}'
folder_url=f'https://github.com/{OWNER}/{REPO}/tree/{quote(branch,safe="/")}/{quote(repo_folder,safe="/")}'
zip_url=f'https://github.com/{OWNER}/{REPO}/blob/{quote(branch,safe="/")}/{quote(f"{repo_folder}/{ZIP_PATH.name}",safe="/")}'
manifest={
    'version':'v1.3.6','branch':branch,'commit':commit['sha'],'commit_url':commit_url,
    'folder_url':folder_url,'zip_url':zip_url,'main_before':main_before,'main_after':main_after,
    'uploaded_files':[e['path'] for e in entries],'result_zip_sha256':zip_sha,'result_zip_size':zip_size,
    'source_script_included':script_copy.is_file()
}
json_dump(manifest,OUT_DIR/'GitHub上傳結果.json')
print('\n'+'='*88)
print('v1.3.6 五檔 OHLCV 校正分類檔上游生成來源追溯完成')
print('='*88)
print('GitHub branch：',branch)
print('GitHub commit：',commit['sha'])
print('GitHub commit URL：',commit_url)
print('GitHub folder：',folder_url)
print('GitHub ZIP：',zip_url)
print('ZIP SHA256：',zip_sha)
print('[PASS] GitHub 上傳檔案回讀驗證')
print(f'[PASS] main SHA 前後：{main_before} -> {main_after}')
print('程式原檔已納入上傳：',script_copy.is_file())
print('Drive 寫入：False')
print('Drive 刪除：False')
print('官方行情 API 呼叫：False')
print('正式資料庫更新：False')
print('[OUTPUT]',OUT_DIR)