"""Daily Dukascopy ticks -> BarReplay bin_v1 -> Hugging Face. UTC only."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import datetime as dt
import email.utils
import hashlib
import json
import lzma
import math
import os
from pathlib import Path
import random
import re
import struct
import time
import urllib.request
import urllib.error

UTC = dt.timezone.utc
FEED = 'https://datafeed.dukascopy.com/datafeed'
MAX_BODY = 64 * 1024 * 1024
MAX_RAW = 256 * 1024 * 1024


def hour_url(symbol, day, hour):
    # Dukascopy months are zero-based: April is 03, not 04.
    return f'{FEED}/{symbol}/{day.year}/{day.month-1:02}/{day.day:02}/{hour:02}h_ticks.bi5'


def decode_hour(body, day, hour, divisor):
    if not body:
        return []
    dec = lzma.LZMADecompressor()
    raw = dec.decompress(body, max_length=MAX_RAW + 1)
    if len(raw) > MAX_RAW or not dec.eof or dec.unused_data or len(raw) % 20:
        raise ValueError('Invalid/truncated/oversized BI5 payload')
    base = int(dt.datetime(day.year, day.month, day.day, hour, tzinfo=UTC).timestamp()) * 1000
    rows = []
    previous = -1
    for ms, ask_i, bid_i, ask_v, bid_v in struct.iter_unpack('>3i2f', raw):
        if not 0 <= ms < 3600000 or ms < previous:
            raise ValueError('Invalid or unsorted tick time')
        if bid_i <= 0 or ask_i < bid_i or not all(math.isfinite(v) and v >= 0 for v in (ask_v, bid_v)):
            raise ValueError('Invalid price/volume')
        previous = ms
        rows.append((base + ms, bid_i / divisor, ask_i / divisor))
    return rows


def encode_block(rows):
    if not rows:
        return b''
    n = len(rows)
    if n > 20000000:
        raise ValueError('Block too large for BarReplay')
    # MT5 little-endian: int32 count, int64 UTC milliseconds[], float64 bids[], asks[].
    return (struct.pack('<i', n) + struct.pack(f'<{n}q', *(r[0] for r in rows))
            + struct.pack(f'<{n}d', *(r[1] for r in rows))
            + struct.pack(f'<{n}d', *(r[2] for r in rows)))


def retry_delay(value, attempt):
    try:
        return min(300, max(0, float(value)))
    except (ValueError, TypeError):
        try:
            return min(300, max(0, email.utils.parsedate_to_datetime(value).timestamp() - time.time()))
        except (ValueError, TypeError, AttributeError):
            return min(60, 2 ** (attempt + 1)) + random.random()


def fetch_hour(symbol, day, hour, cfg, cache):
    url = hour_url(symbol, day, hour)
    path = cache / symbol / day.isoformat() / f'{hour:02}.bi5'
    divisor = cfg['instruments'][symbol]['price_divisor']
    if path.exists():
        try:
            return {'hour': hour, 'state': 'cached', 'rows': decode_hour(path.read_bytes(), day, hour, divisor)}
        except Exception:
            path.unlink()  # Corrupt local cache is not reused.
    last = 'not attempted'
    status = None
    for attempt in range(cfg['attempts']):
        retry_after = None
        try:
            req = urllib.request.Request(url, headers={'User-Agent': 'BarReplayDaily/1.0'})
            try:
                response = urllib.request.urlopen(req, timeout=cfg['timeout'])
            except urllib.error.HTTPError as exc:
                response = exc
            with response as r:
                status = r.code
                retry_after = r.headers.get('Retry-After')
                content_type = r.headers.get('Content-Type', '')
                if status == 200:
                    body = r.read(MAX_BODY + 1)
                    if len(body) > MAX_BODY or any(t in content_type.lower() for t in ('text/html', 'application/json')):
                        raise ValueError('Unexpected content type or oversized response')
                    rows = decode_hour(body, day, hour, divisor)
                    path.parent.mkdir(parents=True, exist_ok=True)
                    part = path.with_suffix('.part')
                    part.write_bytes(body)
                    part.replace(path)
                    return {'hour': hour, 'state': 'ok' if rows else 'empty_200', 'rows': rows}
                if status == 404 and cfg.get('allow_404_empty', False):
                    return {'hour': hour, 'state': 'empty_404_assumed', 'rows': []}
                last = f'HTTP {status}; type={content_type}'
                # 202 is unresolved, NOT an empty hour or successful download.
                if status not in (202, 408, 425, 429) and not 500 <= status <= 599:
                    break
        except Exception as exc:
            last = f'{type(exc).__name__}: {exc}'
        if attempt + 1 < cfg['attempts']:
            time.sleep(retry_delay(retry_after, attempt))
    return {'hour': hour, 'state': 'failed', 'status': status, 'error': last, 'url': url}


def build_day(symbol, day, cfg, cache, output):
    def work(h):
        return fetch_hour(symbol, day, h, cfg, cache)
    target = output / symbol / f'{symbol}_{day}.BIN'
    # Never accidentally publish a stale file from an earlier local run.
    target.unlink(missing_ok=True)
    report = {'symbol': symbol, 'date': str(day), 'hours': [], 'ticks': 0, 'upload_eligible': False}
    target.parent.mkdir(parents=True, exist_ok=True)
    part = target.with_suffix('.part')
    previous = -1
    try:
        with part.open('wb') as f, ThreadPoolExecutor(max_workers=cfg['workers']) as pool:
            # map preserves hourly ordering, with bounded per-day input (24 hours).
            for result in pool.map(work, range(24)):
                rows = result.pop('rows', [])
                report['hours'].append(result)
                if rows:
                    if rows[0][0] < previous:
                        raise ValueError('Non-monotonic daily data')
                    previous = rows[-1][0]
                    f.write(encode_block(rows))
                    report['ticks'] += len(rows)
        if any(h['state'] == 'failed' for h in report['hours']):
            report['result'] = 'failed_hours_no_publish'
        elif not report['ticks']:
            report['result'] = 'no_ticks_no_publish'
        else:
            part.replace(target)
            report['upload_eligible'] = True
            report['result'] = 'ready'
            report['bytes'] = target.stat().st_size
            with target.open('rb') as source:
                report['sha256'] = hashlib.file_digest(source, 'sha256').hexdigest()
    finally:
        part.unlink(missing_ok=True)
    return target, report


def parse_index(text):
    lines = text.lstrip('\ufeff').splitlines()
    if not lines or lines[0] != 'BRIDX1':
        raise ValueError('Unexpected index format; refusing to overwrite')
    symbols, entries = {}, {}
    for line in lines[1:]:
        if not line.strip():
            continue
        a = line.split('|')
        if a[0] == 'SYM' and len(a) == 5:
            symbols[a[1]] = line
        elif a[0] == 'FILE' and len(a) == 4:
            entries[a[1]] = line
        else:
            raise ValueError('Unknown index entry; refusing destructive rewrite')
    return symbols, entries


def publish(api, repo, cfg, ready, dry_run=False):
    from huggingface_hub import CommitOperationAdd, hf_hub_download
    snapshot = api.repo_info(repo, repo_type='dataset').sha
    # Never fetch an old revision of index.txt from a floating local cache.
    existing = api.list_repo_files(repo, repo_type='dataset', revision=snapshot)
    if 'index.txt' in existing:
        text = Path(hf_hub_download(repo, 'index.txt', repo_type='dataset', revision=snapshot, token=api.token)).read_text('utf-8-sig')
        symbols, entries = parse_index(text)
    else:
        symbols, entries = {}, {}
    operations = []
    for path, report in ready:
        symbol, date = report['symbol'], report['date']
        remote = f'{symbol}/{symbol}_{date}.BIN'
        symbols.setdefault(symbol, f"SYM|{symbol}|{symbol}|0|{cfg['instruments'][symbol]['digits']}")
        entries[remote] = f"FILE|{remote}|{date}|{path.stat().st_size}"
        operations.append(CommitOperationAdd(path_in_repo=remote, path_or_fileobj=str(path)))
        meta = json.dumps(report, indent=2).encode()
        operations.append(CommitOperationAdd(path_in_repo=f'audit/{symbol}/{date}.json', path_or_fileobj=meta))
    if not operations:
        return None
    index = ['BRIDX1']
    for symbol in sorted(symbols):
        index.append(symbols[symbol])
        index.extend(entries[p] for p in sorted(entries) if p.split('/')[0] == symbol)
    if any(p.split('/')[0] not in symbols for p in entries):
        raise ValueError('Index has files with no SYM declaration')
    operations.append(CommitOperationAdd(path_in_repo='index.txt', path_or_fileobj=('\n'.join(index)+'\n').encode()))
    if dry_run:
        return 'dry-run: no commit'
    # Atomic commit: BIN + audit + index. Fail on conflict; never silently lose another writer's index changes.
    return api.create_commit(repo, repo_type='dataset', operations=operations,
                             parent_commit=snapshot, commit_message='Add validated daily ticks and index')


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--config', default='config.json')
    p.add_argument('--symbols', default='')
    p.add_argument('--start', help='UTC YYYY-MM-DD, inclusive')
    p.add_argument('--end', help='UTC YYYY-MM-DD, inclusive')
    p.add_argument('--upload', action='store_true')
    p.add_argument('--replace-existing', action='store_true')
    args = p.parse_args()
    cfg = json.loads(Path(args.config).read_text())
    if not 1 <= cfg['workers'] <= 8 or not 1 <= cfg['attempts'] <= 8 or not 1 <= cfg['timeout'] <= 120:
        p.error('workers 1..8, attempts 1..8, timeout 1..120 required')
    for s, item in cfg['instruments'].items():
        if not re.fullmatch('[A-Z0-9]+', s) or not math.isfinite(item['price_divisor']) or item['price_divisor'] <= 0:
            p.error('Invalid instrument configuration')
    symbols = [s.strip().upper() for s in args.symbols.split(',') if s.strip()] or list(cfg['instruments'])
    if any(s not in cfg['instruments'] for s in symbols):
        p.error('Unknown symbol; configure price_divisor first')
    yesterday = dt.datetime.now(UTC).date() - dt.timedelta(days=1)
    end = dt.date.fromisoformat(args.end) if args.end else yesterday
    start = dt.date.fromisoformat(args.start) if args.start else end - dt.timedelta(days=cfg['lookback_days']-1)
    if start > end or end > yesterday or (end-start).days > 30:
        p.error('Use completed UTC days only; maximum 31 days per run')
    api = None
    remote_files = set()
    if args.upload:
        from huggingface_hub import HfApi
        token = os.environ.get('HF_TOKEN')
        if not token:
            p.error('Set HF_TOKEN as a secret (write permission for target dataset)')
        api = HfApi(token=token)
        remote_files = set(api.list_repo_files(cfg['repository'], repo_type='dataset'))
    reports = []
    failed = False
    Path('reports').mkdir(exist_ok=True)
    try:
        for symbol in symbols:
            day = start
            while day <= end:
                remote = f'{symbol}/{symbol}_{day}.BIN'
                if remote in remote_files and not args.replace_existing:
                    report = {'symbol': symbol, 'date': str(day), 'result': 'already_remote'}
                else:
                    path, report = build_day(symbol, day, cfg, Path('cache'), Path('output'))
                    if report['upload_eligible'] and api:
                        publish(api, cfg['repository'], cfg, [(path, report)])
                        remote_files.add(remote)
                        report['result'] = 'uploaded'
                    failed |= report['result'] == 'failed_hours_no_publish'
                reports.append(report)
                print(symbol, day, report['result'], 'ticks=', report.get('ticks', 0), flush=True)
                day += dt.timedelta(days=1)
    finally:
        Path('reports/run.json').write_text(json.dumps(reports, indent=2))
    return 1 if failed else 0


if __name__ == '__main__':
    raise SystemExit(main())
