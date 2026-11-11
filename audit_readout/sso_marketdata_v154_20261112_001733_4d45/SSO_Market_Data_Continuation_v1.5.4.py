from __future__ import annotations
import os
import re
import csv
import io
import json
import gzip
import time
import uuid
import base64
import hashlib
import zipfile
import statistics
from pathlib import Path
from datetime import datetime, date, timedelta, time as dt_time
from zoneinfo import ZoneInfo
from collections import defaultdict
import requests
import pandas as pd

VERSION = 'SSO_Market_Data_Continuation_TWSE_TPEX_v1.5.4'
ROOT = Path('/content/drive/MyDrive/看股最速適/SSO_Market_Data_Continuation_TWSE_TPEX_v1')
REPO = 'CHLINX/sso'
TZ = ZoneInfo('Asia/Taipei')
MARKET_CLOSE_CUTOFF = dt_time(17, 0)
CORRECTION_MARKET = 'TPEX'
CORRECTION_DATE = date(2026, 11, 11)
MIN_ROWS_ABSOLUTE = 500
MAX_ROWS_ABSOLUTE = 3000
MIN_COUNT_RATIO = 0.55
MAX_COUNT_RATIO = 1.6
MAX_ZERO_VOLUME_RATIO = 0.25
MAX_MISSING_OHLC_RATIO = 0.25
MAX_INVALID_PRICE_RATIO = 0.01
MIN_CODE_LENGTH = 2
MAX_CODE_LENGTH = 16

def mount_drive():
    if not Path('/content/drive/MyDrive').exists():
        from google.colab import drive
        drive.mount('/content/drive', force_remount=False)
    ROOT.mkdir(parents=True, exist_ok=True)

def log(msg, logs):
    line = f"[{datetime.now(TZ).strftime('%H:%M:%S')}] {msg}"
    print(line, flush=True)
    logs.append(line)

def norm(x):
    return re.sub('[\\s\\u3000_\\-（）()【】\\[\\]／/：:、,.%]+', '', str(x).strip().lower())

def market_norm(x):
    s = str(x).strip().upper()
    if s in {'TWSE', '上市', '集中市場'}:
        return 'TWSE'
    if s in {'TPEX', 'TPEx', '上櫃', '櫃買'}:
        return 'TPEX'
    return s

def parse_date(x):
    s = str(x or '').strip()
    if not s:
        return None
    m = re.fullmatch('(\\d{2,3})[/-]?(\\d{1,2})[/-]?(\\d{1,2})', s)
    if m:
        y, mo, da = map(int, m.groups())
        if y < 1911:
            y += 1911
        try:
            return date(y, mo, da)
        except Exception:
            return None
    try:
        return pd.to_datetime(s, errors='raise').date()
    except Exception:
        return None

def safe_num(x, integer=False):
    s = str(x or '').strip().replace(',', '').replace('＋', '+').replace('－', '-')
    if s in {'', '-', '--', '---', 'X', 'x', 'N/A', 'nan', 'None'}:
        return None
    try:
        v = float(s)
        if not pd.notna(v):
            return None
        if integer:
            return int(round(v))
        return v
    except Exception:
        return None

def find_col(columns, aliases):
    lookup = {norm(c): c for c in columns}
    for a in aliases:
        k = norm(a)
        if k in lookup:
            return lookup[k]
    return None

def read_header(path):
    raw = Path(path).read_bytes()
    for enc in ('utf-8-sig', 'cp950', 'big5', 'gb18030'):
        try:
            return next(csv.reader(io.StringIO(raw.decode(enc))))
        except Exception:
            pass
    raise RuntimeError(f'無法讀取欄位表頭：{path}')

ALIASES = {
    'trade_date': ['trade_date', 'date', '日期', '交易日期', '資料日期', '年月日'],
    'market': ['market', '市場', '市場別', '交易市場'],
    'instrument_code': ['instrument_code', 'code', 'stock_id', 'stock_code', '證券代號', '證券代碼', '代號', '股票代號'],
    'instrument_name': ['instrument_name', 'name', 'stock_name', '證券名稱', '名稱', '股票名稱'],
    'open': ['open', 'opening_price', 'open_price', '開盤', '開盤價', '開盤價格'],
    'high': ['high', 'highest_price', 'high_price', '最高', '最高價', '最高價格'],
    'low': ['low', 'lowest_price', 'low_price', '最低', '最低價', '最低價格'],
    'close': ['close', 'closing_price', 'close_price', '收盤', '收盤價', '收盤價格'],
    'volume': ['volume', 'trade_volume', 'trading_volume', '成交股數', '成交量'],
    'trade_value': ['trade_value', 'traded_value', 'turnover', 'amount', '成交金額', '成交金額元', '成交金額(元)'],
    'price_change': ['price_change', 'change', '漲跌', '漲跌價差'],
    'transactions': ['transactions', 'transaction_count', 'transaction', '成交筆數', '筆數', '交易筆數'],
    'source': ['source', 'source_archive_member', '來源', '資料來源']
}

def validate_schema(columns, label, standard=True):
    required = ['trade_date', 'open', 'high', 'low', 'close', 'volume']
    if standard:
        required += ['market', 'instrument_code']
    missing = []
    for key in required:
        if find_col(columns, ALIASES[key]) is None:
            missing.append(key)
    if missing:
        raise RuntimeError(f'{label} 欄位格式不相容，缺少欄位={missing}；現有欄位={columns}')

def record_to_target(record, columns):
    result = {}
    for col in columns:
        value = ''
        for key, aliases in ALIASES.items():
            if norm(col) in {norm(a) for a in aliases}:
                value = record.get(key, '')
                break
        result[col] = '' if value is None else value
    return result

def write_csv(path, rows, columns):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('w', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=list(columns), extrasaction='ignore')
        writer.writeheader()
        for row in rows:
            writer.writerow(record_to_target(row, columns))

def version_dirs():
    dirs = [p for p in ROOT.iterdir() if p.is_dir() and re.match('v1\\.5\\.\\d+_', p.name)]
    return sorted(dirs, key=lambda p: p.name)

def read_previous_dates(dirs):
    latest = {'TWSE': None, 'TPEX': None}
    for d in dirs:
        files = list(d.glob('**/全市場標準化增量_*.csv'))
        for p in files:
            try:
                df = pd.read_csv(p, dtype=str, encoding='utf-8-sig', usecols=lambda c: norm(c) in {'tradedate', 'date', '日期', '交易日期', '資料日期', 'market', '市場', '市場別', '交易市場'}, keep_default_na=False)
                dc = find_col(df.columns, ALIASES['trade_date'])
                mc = find_col(df.columns, ALIASES['market'])
                if not dc or not mc:
                    continue
                for mk, group in df.groupby(mc):
                    market = market_norm(mk)
                    if market not in latest:
                        continue
                    dates = [parse_date(v) for v in group[dc]]
                    dates = [x for x in dates if x is not None]
                    if dates:
                        last = max(dates)
                        if latest[market] is None or last > latest[market]:
                            latest[market] = last
            except Exception as e:
                print(f'[DATE READ WARN] {p.name}: {e}')
    if any((x is None for x in latest.values())):
        raise RuntimeError(f'無法辨識兩市場最後行情日期：{latest}')
    return latest

def load_baseline_and_schema(dirs):
    baseline_values = {'TWSE': [], 'TPEX': []}
    standard_columns = None
    overlay_columns = {'TWSE': None, 'TPEX': None}
    for d in dirs:
        if not d.name.startswith('v1.5.1_'):
            continue
        stat_files = list(d.glob('**/新增九個交易日_逐日雙市場統計.csv'))
        for p in stat_files:
            df = pd.read_csv(p, dtype=str, encoding='utf-8-sig')
            mc = find_col(df.columns, ALIASES['market'])
            rc = find_col(df.columns, ['rows', 'row_count', '筆數', '資料筆數'])
            if mc and rc:
                for _, row in df.iterrows():
                    mk = market_norm(row[mc])
                    n = safe_num(row[rc], integer=True)
                    if mk in baseline_values and n is not None and (n >= MIN_ROWS_ABSOLUTE):
                        baseline_values[mk].append(n)
        if any(baseline_values.values()):
            break
    for d in reversed(dirs):
        std_files = list(d.glob('**/全市場標準化增量_*.csv'))
        if standard_columns is None and std_files:
            standard_columns = read_header(std_files[-1])
        overlay_dir = d / 'overlay' / '官方歷史行情'
        if overlay_dir.exists():
            for market, pattern in (('TWSE', '*_TWSE.csv'), ('TPEX', '*_TPEx.csv')):
                if overlay_columns[market] is None:
                    files = list(overlay_dir.glob(pattern))
                    if files:
                        overlay_columns[market] = read_header(files[0])
        if standard_columns is not None and all(overlay_columns.values()):
            break
    if standard_columns is None:
        raise RuntimeError('找不到既有全市場標準化增量欄位格式。')
    for market in ('TWSE', 'TPEX'):
        if overlay_columns[market] is None:
            overlay_columns[market] = list(standard_columns)
    validate_schema(standard_columns, '既有全市場標準化增量', standard=True)
    for market in ('TWSE', 'TPEX'):
        validate_schema(overlay_columns[market], f'{market} 個股覆蓋檔', standard=True)
    baseline = {}
    for market in ('TWSE', 'TPEX'):
        values = baseline_values[market]
        baseline[market] = int(statistics.median(values)) if values else 1400 if market == 'TWSE' else 1020
    return (baseline, standard_columns, overlay_columns)

def correction_already_exists(dirs):
    for d in dirs:
        p = d / 'corrections' / '官方歷史行情' / 'TPEX' / '20261111.csv'
        if p.exists() and p.stat().st_size > 20:
            return True
        manifests = list(d.glob('**/v1.5.*_修正替代規則.json'))
        for m in manifests:
            try:
                obj = json.loads(m.read_text(encoding='utf-8'))
                if obj.get('correction_pass') is True:
                    return True
            except Exception:
                pass
    return False

def make_session():
    s = requests.Session()
    s.headers.update({'User-Agent': 'Mozilla/5.0 (compatible; SSO-Market-Data/1.5.4)', 'Accept': 'application/json,text/plain,*/*'})
    return s

def extract_tables(payload):
    result = []
    if not isinstance(payload, dict):
        return result
    tables = payload.get('tables')
    if isinstance(tables, list):
        for t in tables:
            if isinstance(t, dict) and isinstance(t.get('fields'), list) and isinstance(t.get('data'), list):
                result.append(t)
    if isinstance(payload.get('fields'), list) and isinstance(payload.get('data'), list):
        result.append(payload)
    data_obj = payload.get('data')
    if isinstance(data_obj, dict) and isinstance(data_obj.get('fields'), list) and isinstance(data_obj.get('data'), list):
        result.append(data_obj)
    return result

def parse_payload(payload, market, day):
    field_aliases = {'code': ALIASES['instrument_code'], 'name': ALIASES['instrument_name'], 'open': ALIASES['open'], 'high': ALIASES['high'], 'low': ALIASES['low'], 'close': ALIASES['close'], 'volume': ALIASES['volume'], 'trade_value': ALIASES['trade_value'], 'transactions': ALIASES['transactions'], 'change': ALIASES['price_change'], 'sign': ['漲跌(+/-)', '漲跌符號', '漲跌記號']}
    required = ['code', 'open', 'high', 'low', 'close', 'volume']
    candidates = []
    for table in extract_tables(payload):
        fields = table['fields']
        mapping = {k: find_col(fields, a) for k, a in field_aliases.items()}
        if all((mapping.get(k) is not None for k in required)):
            candidates.append((table, mapping))
    if not candidates:
        if not any((t.get('data') for t in extract_tables(payload))):
            return ([], 'NO_DATA', '官方端點沒有行情列', '')
        raise RuntimeError(f'{market} 找不到完整 OHLC/成交量欄位表')
    candidates.sort(key=lambda x: len(x[0].get('data', [])), reverse=True)
    table, mapping = candidates[0]
    fields = table['fields']
    indexes = {k: fields.index(v) if v in fields else None for k, v in mapping.items()}
    records = []
    for row in table['data']:
        if not isinstance(row, (list, tuple)):
            continue

        def cell(k):
            i = indexes.get(k)
            if i is not None and i < len(row):
                return row[i]
            return ''
        code = re.sub('\\s+', '', str(cell('code') or '')).strip()
        if not code:
            continue
        if not re.fullmatch('[0-9A-Za-z.\\-]{2,16}', code):
            continue
        change = safe_num(cell('change'))
        if indexes.get('sign') is not None and change is not None:
            sign = str(cell('sign')).strip()
            if sign == '-':
                change = -abs(change)
            elif sign == '+':
                change = abs(change)
        records.append({'trade_date': day.isoformat(), 'market': market, 'instrument_code': code, 'instrument_name': str(cell('name')).strip(), 'open': safe_num(cell('open')), 'high': safe_num(cell('high')), 'low': safe_num(cell('low')), 'close': safe_num(cell('close')), 'volume': safe_num(cell('volume'), integer=True), 'trade_value': safe_num(cell('trade_value'), integer=True), 'price_change': change, 'transactions': safe_num(cell('transactions'), integer=True), 'source': 'TWSE_MI_INDEX' if market == 'TWSE' else 'TPEX_afterTrading_otc_type_EW'})
    title = str(table.get('title', table.get('note', '')))
    return (records, 'PARSED', '', title)

def validate_download(records, market, day, expected_rows):
    errors = []
    n = len(records)
    codes = [r['instrument_code'] for r in records]
    unique_codes = len(set(codes))
    duplicate_count = n - unique_codes
    if n < MIN_ROWS_ABSOLUTE:
        errors.append(f'資料筆數低於 {MIN_ROWS_ABSOLUTE}：{n}')
    if n > MAX_ROWS_ABSOLUTE:
        errors.append(f'資料筆數超過 {MAX_ROWS_ABSOLUTE}：{n}')
    if expected_rows:
        lower = expected_rows * MIN_COUNT_RATIO
        upper = expected_rows * MAX_COUNT_RATIO
        if not lower <= n <= upper:
            errors.append(f'資料筆數偏離既有母體：本次={n}；既有中位數={expected_rows}；允許={lower:.0f}~{upper:.0f}')
    if duplicate_count > 0:
        errors.append(f'重複代號列數={duplicate_count}')
    if not records:
        errors.append('沒有可寫入行情列')
    zero_volume = sum((r['volume'] == 0 for r in records if r['volume'] is not None))
    missing_volume = sum((r['volume'] is None for r in records))
    missing_ohlc_rows = sum((any((r[k] is None for k in ('open', 'high', 'low', 'close'))) for r in records))
    zero_ratio = zero_volume / n if n else 0
    missing_ohlc_ratio = missing_ohlc_rows / n if n else 0
    if missing_volume:
        errors.append(f'成交量無法解析列數={missing_volume}')
    if zero_ratio > MAX_ZERO_VOLUME_RATIO:
        errors.append(f'零成交量比例異常：{zero_ratio:.1%}')
    if missing_ohlc_ratio > MAX_MISSING_OHLC_RATIO:
        errors.append(f'OHLC 缺值比例異常：{missing_ohlc_ratio:.1%}')
    bad_price = 0
    complete_price = 0
    for r in records:
        vals = [r[k] for k in ('open', 'high', 'low', 'close')]
        if any((v is None for v in vals)):
            continue
        o, h, l, c = vals
        complete_price += 1
        if min(vals) < 0 or l > h or (not l <= o <= h) or (not l <= c <= h):
            bad_price += 1
    bad_price_ratio = bad_price / complete_price if complete_price else 0
    if bad_price_ratio > MAX_INVALID_PRICE_RATIO:
        errors.append(f'OHLC 價格關係異常：{bad_price}/{complete_price} ({bad_price_ratio:.2%})')
    wrong_date = sum((r['trade_date'] != day.isoformat() for r in records))
    wrong_market = sum((r['market'] != market for r in records))
    if wrong_date:
        errors.append(f'交易日期不符列數={wrong_date}')
    if wrong_market:
        errors.append(f'市場代碼不符列數={wrong_market}')
    stats = {'rows': n, 'unique_codes': unique_codes, 'duplicate_rows': duplicate_count, 'zero_volume_rows': zero_volume, 'zero_volume_ratio': round(zero_ratio, 6), 'missing_volume_rows': missing_volume, 'missing_ohlc_rows': missing_ohlc_rows, 'missing_ohlc_ratio': round(missing_ohlc_ratio, 6), 'complete_ohlc_rows': complete_price, 'invalid_ohlc_rows': bad_price, 'invalid_ohlc_ratio': round(bad_price_ratio, 6), 'expected_rows': expected_rows, 'count_ratio': round(n / expected_rows, 6) if expected_rows else None, 'validation_errors': errors}
    return (len(errors) == 0, stats)

def download_and_validate(session, market, day, expected_rows, out_dir, logs):
    if market == 'TWSE':
        url = 'https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX'
        params = {'date': day.strftime('%Y%m%d'), 'type': 'ALLBUT0999', 'response': 'json'}
        query_type = 'ALLBUT0999'
    else:
        url = 'https://www.tpex.org.tw/www/zh-tw/afterTrading/otc'
        params = {'date': day.strftime('%Y/%m/%d'), 'type': 'EW', 'response': 'json'}
        query_type = 'EW'
    meta = {'trade_date': day.isoformat(), 'market': market, 'query_type': query_type, 'url': url, 'params': params, 'http_status': None, 'response_sha256': '', 'raw_file': '', 'table_title': '', 'status': '', 'validation': {}}
    try:
        response = session.get(url, params=params, timeout=(20, 90))
        meta['http_status'] = response.status_code
        raw = response.content
        meta['response_sha256'] = hashlib.sha256(raw).hexdigest()
        raw_dir = Path(out_dir) / 'raw' / market
        raw_dir.mkdir(parents=True, exist_ok=True)
        raw_path = raw_dir / f"{day.strftime('%Y%m%d')}.json.gz"
        raw_path.write_bytes(gzip.compress(raw, compresslevel=6, mtime=0))
        meta['raw_file'] = str(raw_path.relative_to(out_dir)).replace('\\', '/')
        response.raise_for_status()
        content_type = response.headers.get('Content-Type', '')
        if 'html' in content_type.lower() or raw.lstrip().lower().startswith((b'<html', b'<!doctype html')):
            raise RuntimeError('官方回應為 HTML，不是 JSON 行情')
        payload = response.json()
        records, parse_status, parse_error, title = parse_payload(payload, market, day)
        meta['table_title'] = title
        if parse_status == 'NO_DATA':
            meta['status'] = 'WAITING_FOR_OFFICIAL_DATA'
            meta['error'] = f'{parse_error}；原始回應已保存，未推定休市，未寫入增量，後續執行將重新查詢。'
            meta['validation'] = {'rows': 0, 'validation_errors': []}
            log(f'[WAITING FOR OFFICIAL DATA] {market} {day}：官方回應目前沒有行情列；保留原始回應，不視為品質拒絕。', logs)
            return ([], meta)
        passed, stats = validate_download(records, market, day, expected_rows)
        meta['validation'] = stats
        if not passed:
            meta['status'] = 'REJECTED'
            meta['error'] = '; '.join(stats['validation_errors'])
            log(f"[REJECTED] {market} {day}：{meta['error']}", logs)
            return ([], meta)
        meta['status'] = 'PASS'
        meta['error'] = ''
        log(f"[DOWNLOAD + CHECK PASS] {market} {day}：{stats['rows']:,} 筆；唯一代號={stats['unique_codes']:,}；零成交量={stats['zero_volume_ratio']:.1%}；OHLC缺值={stats['missing_ohlc_ratio']:.1%}", logs)
        return (records, meta)
    except Exception as e:
        meta['status'] = 'ERROR'
        meta['error'] = f'{type(e).__name__}: {e}'
        log(f"[DOWNLOAD ERROR] {market} {day}：{meta['error']}", logs)
        return ([], meta)

def write_json(path, obj):
    Path(path).write_text(json.dumps(obj, ensure_ascii=False, indent=2, default=str), encoding='utf-8')

def save_current_source(out_dir, logs):
    try:
        history = get_ipython().history_manager.input_hist_raw
        for cell in reversed(history):
            if VERSION in cell and 'def run_v154' in cell and (len(cell) > 10000):
                (Path(out_dir) / 'SSO_Market_Data_Continuation_v1.5.4.py').write_text(cell, encoding='utf-8')
                log('[SOURCE PASS] 本次完整程式碼已封存', logs)
                return
    except Exception as e:
        log(f'[SOURCE WARN] {e}', logs)

def make_zip(out_dir, zip_path):
    with zipfile.ZipFile(zip_path, 'w', compression=zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
        for p in sorted(Path(out_dir).rglob('*')):
            if p.is_file() and p != zip_path:
                zf.write(p, p.relative_to(out_dir).as_posix())

def github_upload(files, run_id, logs):
    token = os.environ.get('GITHUB_TOKEN', '').strip()
    if not token:
        try:
            from google.colab import userdata
            token = str(userdata.get('GITHUB_TOKEN') or '').strip()
        except Exception:
            token = ''
    if not token:
        raise RuntimeError('找不到 GITHUB_TOKEN')
    api = 'https://api.github.com'
    session = requests.Session()
    session.headers.update({'Authorization': f'Bearer {token}', 'Accept': 'application/vnd.github+json', 'X-GitHub-Api-Version': '2022-11-28'})

    def req(method, path, **kwargs):
        r = session.request(method, api + path, timeout=(20, 120), **kwargs)
        if not r.ok:
            raise RuntimeError(f'GitHub API HTTP {r.status_code}: {r.text[:1200]}')
        return r.json() if r.content else {}
    owner, repo = REPO.split('/')
    main_ref = req('GET', f'/repos/{owner}/{repo}/git/ref/heads/main')
    main_sha = main_ref['object']['sha']
    main_commit = req('GET', f'/repos/{owner}/{repo}/git/commits/{main_sha}')
    base_tree = main_commit['tree']['sha']
    branch = f'audit/sso-marketdata-v154-{run_id}'
    prefix = f'audit_readout/sso_marketdata_v154_{run_id}'
    entries = []
    expected = []
    for p in files:
        p = Path(p)
        data = p.read_bytes()
        local_blob_sha = hashlib.sha1(b'blob ' + str(len(data)).encode() + b'\x00' + data).hexdigest()
        blob = req('POST', f'/repos/{owner}/{repo}/git/blobs', json={'content': base64.b64encode(data).decode('ascii'), 'encoding': 'base64'})
        if blob['sha'] != local_blob_sha:
            raise RuntimeError(f'本機與 GitHub blob SHA 不一致：{p.name}')
        path = f'{prefix}/{p.name}'
        entries.append({'path': path, 'mode': '100644', 'type': 'blob', 'sha': blob['sha']})
        expected.append({'path': path, 'sha': blob['sha'], 'sha256': hashlib.sha256(data).hexdigest(), 'bytes': len(data)})
    tree = req('POST', f'/repos/{owner}/{repo}/git/trees', json={'base_tree': base_tree, 'tree': entries})
    commit = req('POST', f'/repos/{owner}/{repo}/git/commits', json={'message': f'SSO market data v1.5.4 validated incremental update {run_id}', 'tree': tree['sha'], 'parents': [main_sha]})
    req('POST', f'/repos/{owner}/{repo}/git/refs', json={'ref': f'refs/heads/{branch}', 'sha': commit['sha']})
    for item in expected:
        blob = req('GET', f"/repos/{owner}/{repo}/git/blobs/{item['sha']}")
        remote_data = base64.b64decode(blob['content'])
        remote_sha256 = hashlib.sha256(remote_data).hexdigest()
        if remote_sha256 != item['sha256']:
            raise RuntimeError(f"GitHub 回讀 SHA256 不一致：{item['path']}")
    main_after = req('GET', f'/repos/{owner}/{repo}/git/ref/heads/main')['object']['sha']
    main_unchanged = main_sha == main_after
    return {'branch': branch, 'commit_sha': commit['sha'], 'main_before': main_sha, 'main_after': main_after, 'main_unchanged': main_unchanged, 'file_count': len(expected), 'blob_readback_pass': True, 'url': f'https://github.com/{REPO}/tree/{branch}/{prefix}'}

def run_v154():
    mount_drive()
    logs = []
    now = datetime.now(TZ)
    today = now.date()
    same_day_download_allowed = now.time() >= MARKET_CLOSE_CUTOFF
    target_end_date = today if same_day_download_allowed else today - timedelta(days=1)
    run_id = now.strftime('%Y%m%d_%H%M%S') + '_' + uuid.uuid4().hex[:4]
    log(f'啟動：{VERSION}', logs)
    log(f'台灣時間：{now.isoformat()}；當日行情查詢門檻：17:00；本次允許查詢至：{target_end_date}。', logs)
    if not same_day_download_allowed:
        log(f'[SAME-DAY GATE] 尚未到 17:00，跳過 {today} 當日行情查詢；不把收盤前的空回應當成缺漏或休市。', logs)
    dirs = version_dirs()
    latest = read_previous_dates(dirs)
    baseline, standard_cols, overlay_cols = load_baseline_and_schema(dirs)
    correction_done = correction_already_exists(dirs)
    out_name = f'v1.5.4_官方行情即時驗證與增量續接_{run_id}'
    out_dir = ROOT / out_name
    out_dir.mkdir(parents=True, exist_ok=False)
    log(f'資料根目錄：{ROOT}', logs)
    log(f"TWSE 最後行情日：{latest['TWSE']}", logs)
    log(f"TPEx 最後行情日：{latest['TPEX']}", logs)
    log(f"歷史合理筆數基準：TWSE={baseline['TWSE']}；TPEX={baseline['TPEX']}", logs)
    log('TPEx 查詢參數固定使用 type=EW；與既有不含權證、牛熊證的行情母體接合。', logs)
    log('下載後立即驗證，通過才可寫入；未通過者只保存原始回應。', logs)
    correction_records = []
    correction_meta = None
    correction_pass = correction_done
    if correction_done:
        log('[CORRECTION SKIP] 2026-11-11 TPEx 已有通過的既有 v1.5.x 修正層；沿用，不重複下載。', logs)
    else:
        log('[CORRECTION] 重取 2026-11-11 TPEx，以 type=EW 修正 v1.5.2 的 type=AL 資料。', logs)
        session = make_session()
        correction_records, correction_meta = download_and_validate(session, CORRECTION_MARKET, CORRECTION_DATE, baseline['TPEX'], out_dir, logs)
        correction_pass = correction_meta.get('status') == 'PASS'
        if correction_pass:
            correction_dir = out_dir / 'corrections' / '官方歷史行情' / 'TPEX'
            correction_dir.mkdir(parents=True, exist_ok=True)
            write_csv(correction_dir / '20261111.csv', correction_records, overlay_cols['TPEX'])
            write_csv(out_dir / '修正_TPEX_20261111.csv', correction_records, standard_cols)
            log(f'[CORRECTION PASS] TPEx 2026-11-11 修正行情 {len(correction_records):,} 筆。', logs)
        else:
            log('[CORRECTION BLOCK] 已知錯誤日期的修正未通過；本次不往後下載，以免建立在錯誤銜接狀態上。', logs)
    forward_records = []
    api_meta = []
    daily_summary = []
    rejected_count = 0
    download_error_count = 0
    waiting_official_data_count = 0
    unresolved_weekday_no_data = []
    if correction_pass:
        session = make_session()
        start_by_market = {market: last_day + timedelta(days=1) for market, last_day in latest.items()}
        cursor = min(start_by_market.values())
        while cursor <= target_end_date:
            for market in ('TWSE', 'TPEX'):
                if cursor < start_by_market[market]:
                    continue
                records, meta = download_and_validate(session, market, cursor, baseline[market], out_dir, logs)
                api_meta.append(meta)
                if meta['status'] == 'PASS':
                    forward_records.extend(records)
                    v = meta['validation']
                    daily_summary.append({'trade_date': cursor.isoformat(), 'market': market, 'rows': v['rows'], 'unique_codes': v['unique_codes'], 'zero_volume_rows': v['zero_volume_rows'], 'zero_volume_ratio': v['zero_volume_ratio'], 'missing_ohlc_rows': v['missing_ohlc_rows'], 'missing_ohlc_ratio': v['missing_ohlc_ratio'], 'duplicate_rows': v['duplicate_rows'], 'status': 'APPENDED'})
                elif meta['status'] == 'WAITING_FOR_OFFICIAL_DATA':
                    waiting_official_data_count += 1
                    if cursor.weekday() < 5:
                        unresolved_weekday_no_data.append({'trade_date': cursor.isoformat(), 'market': market, 'status': 'WAITING_FOR_OFFICIAL_DATA_NOT_CONFIRMED_CLOSED', 'raw_file': meta.get('raw_file', ''), 'response_sha256': meta.get('response_sha256', '')})
                    daily_summary.append({'trade_date': cursor.isoformat(), 'market': market, 'rows': 0, 'unique_codes': 0, 'zero_volume_rows': 0, 'zero_volume_ratio': 0, 'missing_ohlc_rows': 0, 'missing_ohlc_ratio': 0, 'duplicate_rows': 0, 'status': 'WAITING_FOR_OFFICIAL_DATA'})
                else:
                    if meta['status'] == 'REJECTED':
                        rejected_count += 1
                    else:
                        download_error_count += 1
                    daily_summary.append({'trade_date': cursor.isoformat(), 'market': market, 'rows': 0, 'unique_codes': 0, 'zero_volume_rows': 0, 'zero_volume_ratio': 0, 'missing_ohlc_rows': 0, 'missing_ohlc_ratio': 0, 'duplicate_rows': 0, 'status': meta['status']})
                time.sleep(0.3)
            cursor += timedelta(days=1)
    forward_dates = sorted({r['trade_date'] for r in forward_records})
    if forward_dates:
        std_path = out_dir / f"全市場標準化增量_{forward_dates[0].replace('-', '')}_{forward_dates[-1].replace('-', '')}.csv"
    else:
        std_path = out_dir / f'全市場標準化增量_無新增_{run_id}.csv'
    write_csv(std_path, forward_records, standard_cols)
    groups = defaultdict(list)
    for r in forward_records:
        groups[r['market'], r['instrument_code']].append(r)
    overlay_root = out_dir / 'overlay' / '官方歷史行情'
    overlay_index = []
    for (market, code), rows in sorted(groups.items()):
        suffix = 'TWSE' if market == 'TWSE' else 'TPEx'
        filename = f'{code}_{suffix}.csv'
        write_csv(overlay_root / filename, rows, overlay_cols[market])
        dates = sorted((r['trade_date'] for r in rows))
        overlay_index.append({'instrument_code': code, 'market': market, 'overlay_file': f'官方歷史行情/{filename}', 'new_rows': len(rows), 'first_new_date': dates[0], 'last_new_date': dates[-1]})
    correction_manifest = {'version': VERSION, 'correction_date': CORRECTION_DATE.isoformat(), 'market': 'TPEX', 'previous_v1_5_2_query_type': 'AL', 'correct_query_type': 'EW', 'correction_pass': bool(correction_pass), 'old_v1_5_2_file_modified': False, 'replacement_rule': '分析 2026-11-11 TPEx 行情時，以本次 corrections/官方歷史行情/TPEX/20261111.csv 替代 v1.5.2 同日期 TPEx 全部資料列；不可將兩者相加。', 'reason': 'v1.5.2 使用 type=AL，資料母體包含權證及牛熊證；本次改用 type=EW 與既有 v1.5.1 母體一致。', 'correction_rows': len(correction_records), 'correction_sha256': hashlib.sha256(json.dumps(correction_records, ensure_ascii=False, sort_keys=True, default=str).encode('utf-8')).hexdigest() if correction_records else None}
    write_json(out_dir / 'v1.5.4_修正替代規則.json', correction_manifest)
    updated_latest = {m: latest[m] for m in latest}
    accepted_records = correction_records + forward_records
    for r in accepted_records:
        d = parse_date(r['trade_date'])
        market = r['market']
        if d and (updated_latest[market] is None or d > updated_latest[market]):
            updated_latest[market] = d
    state = {'version': VERSION, 'updated_at_taipei': now.isoformat(), 'market_last_date': {m: d.isoformat() for m, d in updated_latest.items()}, 'correction_20261111_tpex_pass': bool(correction_pass), 'tpex_type': 'EW'}
    write_json(out_dir / 'latest_market_dates.json', state)
    if correction_meta:
        api_meta.insert(0, correction_meta)
        if correction_meta['status'] == 'PASS':
            v = correction_meta['validation']
            daily_summary.insert(0, {'trade_date': CORRECTION_DATE.isoformat(), 'market': 'TPEX', 'rows': v['rows'], 'unique_codes': v['unique_codes'], 'zero_volume_rows': v['zero_volume_rows'], 'zero_volume_ratio': v['zero_volume_ratio'], 'missing_ohlc_rows': v['missing_ohlc_rows'], 'missing_ohlc_ratio': v['missing_ohlc_ratio'], 'duplicate_rows': v['duplicate_rows'], 'status': 'CORRECTION_REPLACEMENT'})
    pd.DataFrame(daily_summary).to_csv(out_dir / '逐日雙市場行情驗證統計.csv', index=False, encoding='utf-8-sig')
    pd.DataFrame(api_meta).to_csv(out_dir / '官方API下載及即時驗證索引.csv', index=False, encoding='utf-8-sig')
    pd.DataFrame(overlay_index).to_csv(out_dir / '個股增量覆蓋檔索引.csv', index=False, encoding='utf-8-sig')
    if not correction_pass:
        status = 'BLOCKED_CORRECTION_FAILED'
    elif rejected_count or download_error_count or unresolved_weekday_no_data:
        status = 'PARTIAL'
    elif not accepted_records:
        status = 'NO_NEW_ROWS'
    else:
        status = 'SUCCESS'
    report = {'version': VERSION, 'run_id': run_id, 'run_time_taipei': now.isoformat(), 'market_close_gate': {'timezone': 'Asia/Taipei', 'cutoff_local': '17:00', 'run_time_local': now.isoformat(), 'today': today.isoformat(), 'same_day_download_allowed': same_day_download_allowed, 'target_end_date': target_end_date.isoformat(), 'rule': '台灣時間 17:00 前不查詢當日行情；17:00 起才允許查詢當日。'}, 'status': status, 'previous_market_last_date': {m: d.isoformat() for m, d in latest.items()}, 'baseline_daily_rows_v151': baseline, 'tpex_query_type': 'EW', 'correction': correction_manifest, 'forward_new_rows': len(forward_records), 'correction_new_rows': len(correction_records), 'forward_new_instruments': len(groups), 'forward_trade_dates': forward_dates, 'market_last_date_after_update': state['market_last_date'], 'api_requests': len(api_meta), 'rejected_download_count': rejected_count, 'download_error_count': download_error_count, 'waiting_official_data_count': waiting_official_data_count, 'unresolved_weekday_no_data': unresolved_weekday_no_data, 'immediate_validation': {'enabled': True, 'before_write': True, 'checks': ['HTTP 狀態及 JSON 回應', '行情資料表欄位', '最低及最高列數', '相對於 v1.5.1 母體筆數', '證券代號格式', '唯一代號及重複代號', '成交量數值及零成交量比例', 'OHLC 缺值比例', 'OHLC 價格高低關係', '日期及市場一致性'], 'failed_data_written_to_increment': False, 'raw_response_preserved': True}, 'preservation': {'v2_8_original_zip_modified': False, 'v1_4_2_original_zip_modified': False, 'v1_5_1_original_files_modified': False, 'v1_5_2_original_files_modified': False, 'zero_volume_rows_deleted': False, 'missing_ohlc_rows_deleted': False, 'duplicate_rows_removed': False, 'old_tpex_al_data_physically_deleted': False, 'replacement_layer_created': bool(correction_pass)}, 'scope': '僅針對已知錯誤的 2026-11-11 TPEx 建立一次替代層；除此之外，各市場均從既有最後行情日的下一日起向前增量。'}
    write_json(out_dir / 'v1.5.4_官方增量行情續接報告.json', report)
    readme = f"# {VERSION}\n\n## 執行狀態\n\n{status}\n\n## 既有行情基準\n\n- TWSE 最後行情日：{latest['TWSE']}\n- TPEx 最後行情日：{latest['TPEX']}\n- TWSE 每日母體基準：{baseline['TWSE']:,}\n- TPEx 每日母體基準：{baseline['TPEX']:,}\n\n## TPEx 行情範圍修正\n\nv1.5.2 使用 type=AL，會納入權證及牛熊證，\n造成 2026-11-11 TPEx 取得 13,752 筆，\n與既有 v1.5.1 每日約 1,020 筆的行情母體不一致。\n\nv1.5.4 改用 type=EW。\n\n修正檔位置：\n\n`corrections/官方歷史行情/TPEX/20261111.csv`\n\n使用規則：\n\n2026-11-11 TPEx 資料以本次修正檔替代 v1.5.2 同日 TPEx 全部資料列，\n不得將兩份資料相加。\n\n原 v1.5.2 ZIP 與 CSV 不修改、不刪除。\n\n## 當日行情查詢時間門檻\n\n- 時區：Asia/Taipei。\n- 台灣時間 17:00 前，不查詢當日行情；本次最多查詢至前一日。\n- 台灣時間 17:00 起，才允許查詢當日行情。\n- 官方 API 回應空資料時，狀態列為 `WAITING_FOR_OFFICIAL_DATA`，不列為 `REJECTED`，也不據此推定休市。\n- 空回應的 gzip 原始回應及 SHA256 會保留；該日期不會寫入增量，也不會推進最後行情日，後續執行會重新查詢。\n\n## 即時檢查\n\n每次官方 API 下載後立即執行：\n\n- HTTP/JSON 回應檢查\n- 行情欄位檢查\n- 列數及母體範圍檢查\n- 代號及重複鍵檢查\n- 成交量與零成交量比例\n- OHLC 缺值比例\n- OHLC 價格關係\n- 市場及日期檢查\n\n通過才會寫入增量資料。\n\n未通過資料只保存 gzip 原始回應及錯誤原因，不會混入標準化行情。\n\n## 新增行情\n\n- 新增前瞻行情：{len(forward_records):,} 列\n- 修正行情：{len(correction_records):,} 列\n- 新增個股／市場檔：{len(groups):,}\n\n## 原始資料保護\n\n- v2.8 歷史底稿不修改\n- v1.4.2 不修改\n- v1.5.1 不修改\n- v1.5.2 不修改\n- 不回頭掃描十年歷史\n- 不重訓、不回測、不選股\n\n## 官方端點\n\nTWSE:\n`https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX`\n\nTPEx:\n`https://www.tpex.org.tw/www/zh-tw/afterTrading/otc`\n\nTPEx type:\n`EW`（排除權證及牛熊證）\n\n## 分析資料銜接\n\n分析時使用：\n\n1. v2.8 原始歷史行情\n2. v1.5.1 原有 overlay\n3. v1.5.2 既有增量，但排除 2026-11-11 TPEx 舊資料列\n4. 本次 v1.5.4 修正層\n5. 本次 v1.5.4 前瞻增量 overlay\n\n修正層優先於被替代的原始日期／市場資料。\n"
    (out_dir / 'README.md').write_text(readme, encoding='utf-8')
    save_current_source(out_dir, logs)
    (out_dir / 'run.log').write_text('\n'.join(logs) + '\n', encoding='utf-8')
    zip_path = out_dir / f'{out_name}.zip'
    make_zip(out_dir, zip_path)
    zip_sha256 = hashlib.sha256(zip_path.read_bytes()).hexdigest()
    log(f'[ZIP PASS] {zip_path.name}；bytes={zip_path.stat().st_size:,}；SHA256={zip_sha256}', logs)
    with zipfile.ZipFile(zip_path) as zf:
        bad_member = zf.testzip()
        if bad_member:
            raise RuntimeError(f'ZIP CRC 失敗：{bad_member}')
    log('[ZIP CRC PASS] 封存完整性檢查通過。', logs)
    (out_dir / 'run.log').write_text('\n'.join(logs) + '\n', encoding='utf-8')
    upload_files = [zip_path, out_dir / 'README.md', out_dir / 'v1.5.4_官方增量行情續接報告.json', out_dir / 'v1.5.4_修正替代規則.json', out_dir / '逐日雙市場行情驗證統計.csv', out_dir / '官方API下載及即時驗證索引.csv', out_dir / '個股增量覆蓋檔索引.csv', out_dir / 'latest_market_dates.json', out_dir / 'run.log']
    upload_files.append(std_path)
    corrected_csv = out_dir / '修正_TPEX_20261111.csv'
    if corrected_csv.exists():
        upload_files.append(corrected_csv)
    source_path = out_dir / 'SSO_Market_Data_Continuation_v1.5.4.py'
    if source_path.exists():
        upload_files.append(source_path)
    github_result = None
    try:
        github_result = github_upload(upload_files, run_id, logs)
        write_json(out_dir / 'GitHub上傳結果.json', github_result)
        log(f"[GITHUB PASS] branch={github_result['branch']}", logs)
        log(f"[GITHUB READBACK PASS] files={github_result['file_count']}", logs)
        log(f"[GITHUB MAIN] unchanged={github_result['main_unchanged']}", logs)
        log(f"[GITHUB URL] {github_result['url']}", logs)
    except Exception as e:
        github_result = {'status': 'FAILED', 'error': f'{type(e).__name__}: {e}'}
        write_json(out_dir / 'GitHub上傳結果.json', github_result)
        log(f"[GITHUB ERROR] {github_result['error']}", logs)
    (out_dir / 'run.log').write_text('\n'.join(logs) + '\n', encoding='utf-8')
    print('\n' + '=' * 88)
    print(f'{VERSION}：{status}')
    print(f"已知 TPEx 2026-11-11 修正：{('PASS' if correction_pass else 'FAILED')}")
    print(f'前瞻新增行情：{len(forward_records):,} 列')
    print(f'修正行情：{len(correction_records):,} 列')
    print(f'新增個股／市場檔：{len(groups):,}')
    print(f'TPEx 查詢參數：EW')
    print(f'品質檢查拒絕的下載批次：{rejected_count}')
    print(f'下載／解析錯誤批次：{download_error_count}')
    print(f'待官方資料的空回應批次：{waiting_official_data_count}')
    print(f'未確認休市的平日空回應：{len(unresolved_weekday_no_data)}')
    print(f'當日查詢門檻：台灣時間 17:00；本次查詢截止日：{target_end_date}')
    print(f"TWSE 最後行情日：{state['market_last_date']['TWSE']}")
    print(f"TPEx 最後行情日：{state['market_last_date']['TPEX']}")
    print(f'輸出資料夾：{out_dir}')
    print(f'ZIP：{zip_path}')
    if github_result and github_result.get('branch'):
        print(f"GitHub branch：{github_result['branch']}")
        print(f"GitHub URL：{github_result['url']}")
        print(f"GitHub main 未變更：{github_result['main_unchanged']}")
    else:
        print(f'GitHub：{github_result}')
    print('=' * 88)
    return report

RESULT_V154 = run_v154()