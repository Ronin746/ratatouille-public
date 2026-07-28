"""
generate_historical_csvs.py — Genera CSV storici dello screener per N giorni passati.

Scarica ~1 anno di OHLCV per tutti i ticker una volta sola, poi per ogni
giorno di trading storico ricalcola tutti i 7 fattori (esattamente come fa
lo screener live) usando solo i dati disponibili fino a quella data.
Salva un file screen_results_YYYYMMDD.csv per ogni giorno in Data/archive/.

NON modifica nessun file del codice esistente. Usa indicators.py e scorer.py
in sola lettura via import.

Usage:
    python generate_historical_csvs.py              # ultimi 63 trading days
    python generate_historical_csvs.py --days 90   # ultimi 90 trading days
    python generate_historical_csvs.py --dry-run   # mostra i giorni senza scrivere
"""

import argparse
import os
import sys
import time
import logging
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import yfinance as yf

# ── Path setup — trova il root del progetto ───────────────────────────────────
SCRIPT_DIR  = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(SCRIPT_DIR)
DATA_DIR    = os.path.join(PROJECT_DIR, 'Data')
ARCHIVE_DIR = os.path.join(DATA_DIR, 'archive')

sys.path.insert(0, SCRIPT_DIR)

# Import delle funzioni esistenti (sola lettura — non modificate)
from indicators import (
    calc_price_performance,
    calc_bullish_candles,
    calc_ma_alignment,
    calc_trend_consistency,
    calc_volatility,
    calc_volume,
    calc_relative_strength,
)
from scorer import calculate_scores, calculate_short_scores
try:
    from config import WEIGHTS, SHORT_WEIGHTS, BENCHMARK_TICKER
except ImportError:
    BENCHMARK_TICKER = 'SPY'
    WEIGHTS = {
        'price_performance': 0.25,
        'bullish_candles':   0.15,
        'ma_alignment':      0.15,
        'trend_consistency': 0.15,
        'volatility':        0.10,
        'volume':            0.10,
        'relative_strength': 0.10,
    }
    SHORT_WEIGHTS = WEIGHTS.copy()

logging.basicConfig(level=logging.WARNING)
logging.getLogger('yfinance').setLevel(logging.CRITICAL)

# ── yfinance download settings (same as data_fetcher.py) ─────────────────────
import importlib.metadata as _im
try:
    _yf_ver = tuple(int(x) for x in _im.version('yfinance').split('.')[:3])
except Exception:
    _yf_ver = (0, 0, 0)

_DL_KWARGS = dict(
    group_by='ticker',
    threads=False,
    progress=False,
    auto_adjust=True,
    repair=False,
    timeout=30,
)
try:
    import inspect as _ins
    if 'multi_level_index' in _ins.signature(yf.download).parameters:
        _DL_KWARGS['multi_level_index'] = True
except Exception:
    pass


# ── Helpers ───────────────────────────────────────────────────────────────────

def find_latest_csv():
    """Restituisce il percorso dell'ultimo screen_results_*.csv in Data/."""
    files = sorted([
        f for f in os.listdir(DATA_DIR)
        if f.startswith('screen_results_') and f.endswith('.csv')
        and 'archive' not in f
    ])
    if not files:
        raise FileNotFoundError(f'Nessun screen_results_*.csv in {DATA_DIR}')
    return os.path.join(DATA_DIR, files[-1])


def load_tickers_and_sectors(csv_path):
    df = pd.read_csv(csv_path, index_col=0)
    tickers = [str(t) for t in df.index if t and str(t).strip()]
    sector_map = {}
    if 'Sector' in df.columns:
        sector_map = {str(k): str(v) for k, v in df['Sector'].items()
                      if v and str(v) != 'nan'}
    return tickers, sector_map


def trading_days_back(n):
    """Restituisce gli ultimi n giorni di trading (escluso oggi)."""
    days = []
    d = datetime.today().date() - timedelta(days=1)
    while len(days) < n:
        if d.weekday() < 5:   # lun-ven
            days.append(d)
        d -= timedelta(days=1)
    return sorted(days)


def download_all_ohlcv(tickers, period='1y', batch_size=400):
    """
    Scarica OHLCV per tutti i ticker in batch.
    Restituisce un dict {ticker: DataFrame(OHLCV)} con dati completi.
    """
    clean = [t for t in tickers
             if t and len(t) <= 6 and not t.startswith('^') and '/' not in t]
    clean = list(dict.fromkeys(clean))

    # Aggiungi benchmark
    if BENCHMARK_TICKER not in clean:
        clean = [BENCHMARK_TICKER] + clean

    total = (len(clean) + batch_size - 1) // batch_size
    print(f'  {len(clean):,} ticker in {total} batch da {batch_size}...')

    all_frames = {}
    price_cols = {'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume'}

    for i in range(0, len(clean), batch_size):
        batch = clean[i: i + batch_size]
        bn    = i // batch_size + 1
        print(f'    batch {bn}/{total} ({len(batch)} ticker)… ', end='', flush=True)

        for attempt in range(3):
            try:
                raw = yf.download(batch, period=period, interval='1d', **_DL_KWARGS)
                if raw.empty:
                    print('vuoto')
                    break

                # Normalizza MultiIndex
                if isinstance(raw.columns, pd.MultiIndex):
                    l0 = set(raw.columns.get_level_values(0))
                    if l0 <= price_cols:
                        raw = raw.swaplevel(axis=1).sort_index(axis=1)

                # Single-ticker batch
                if len(batch) == 1 and not isinstance(raw.columns, pd.MultiIndex):
                    raw.columns = pd.MultiIndex.from_product([[batch[0]], raw.columns])

                # Estrai per ticker
                ok = 0
                for t in batch:
                    try:
                        if isinstance(raw.columns, pd.MultiIndex):
                            l0u = set(raw.columns.get_level_values(0))
                            if t in l0u:
                                df = raw[t].copy()
                            else:
                                df = raw.xs(t, axis=1, level=1).copy()
                        else:
                            df = raw.copy()

                        # Rinomina Adj Close → Close
                        if 'Adj Close' in df.columns and 'Close' not in df.columns:
                            df = df.rename(columns={'Adj Close': 'Close'})
                        elif 'Adj Close' in df.columns:
                            df = df.drop(columns=['Adj Close'])

                        if not df.empty and 'Close' in df.columns:
                            all_frames[t] = df
                            ok += 1
                    except Exception:
                        pass

                print(f'ok ({ok} validi)')
                break

            except Exception as e:
                if attempt < 2:
                    time.sleep(5 * (attempt + 1))
                else:
                    print(f'errore: {e}')

        time.sleep(0.3)

    return all_frames


# ── Calcolo indicatori per singolo ticker a data specifica ────────────────────

def compute_ticker_row(ticker, ohlcv, bench_ohlcv, target_date):
    """
    Calcola tutti gli indicatori per un ticker usando solo dati <= target_date.
    Restituisce un dict con tutti i campi del CSV, oppure None se dati insufficienti.
    """
    df = ohlcv.get(ticker)
    if df is None or df.empty:
        return None

    # Taglia ai dati disponibili fino a target_date
    df_cut = df[df.index.date <= target_date].copy()
    if len(df_cut) < 50:
        return None

    bench = bench_ohlcv
    if bench is None or bench.empty:
        return None
    bench_cut = bench[bench.index.date <= target_date].copy()

    row = {}

    # Factor 1: Price Performance
    pp = calc_price_performance(df_cut)
    if pp is None:
        return None
    row.update(pp)

    # Factor 2: Bullish Candles
    bc = calc_bullish_candles(df_cut)
    if bc is None:
        return None
    row.update(bc)

    # Factor 3: MA Alignment
    ma = calc_ma_alignment(df_cut)
    if ma is None:
        return None
    row['ma_aligned']        = ma['aligned']
    row['ma_positive_slopes'] = ma['positive_slopes']
    row['ma10'] = ma['ma10']
    row['ma20'] = ma['ma20']
    row['ma30'] = ma['ma30']
    row['ma50'] = ma['ma50']

    # Factor 4: Trend Consistency
    tc = calc_trend_consistency(df_cut)
    if tc is None:
        return None
    row.update(tc)

    # Factor 5: Volatility
    vl = calc_volatility(df_cut)
    if vl is None:
        return None
    row['atr_stability'] = vl['atr_stability']
    row['adr_pct']       = vl['adr_pct']
    row['atr_pct']       = vl['atr_pct']

    # Factor 6: Volume
    vm = calc_volume(df_cut)
    if vm is None:
        return None
    row.update(vm)

    # Factor 7: Relative Strength
    rs = calc_relative_strength(df_cut, bench_cut)
    if rs is None:
        return None
    row.update(rs)

    return row


def compute_day_csv(target_date, ohlcv_all, sector_map, tickers):
    """
    Genera il DataFrame completo (tutti i ticker) per target_date.
    Ritorna il DataFrame pronto per essere salvato come CSV.
    """
    rows = {}
    bench_ohlcv = ohlcv_all.get(BENCHMARK_TICKER)

    for ticker in tickers:
        r = compute_ticker_row(ticker, ohlcv_all, bench_ohlcv, target_date)
        if r is not None:
            rows[ticker] = r

    if len(rows) < 200:
        print(f'  ⚠  {target_date}: solo {len(rows)} ticker validi — skip')
        return None

    df = pd.DataFrame.from_dict(rows, orient='index')
    df.index.name = 'Ticker'

    # Calcola score long e short
    df = calculate_scores(df, WEIGHTS)
    df = calculate_short_scores(df, SHORT_WEIGHTS)

    # Aggiungi settore
    df['Sector'] = df.index.map(lambda t: sector_map.get(t, ''))

    return df


def save_csv(df, date):
    """Salva il DataFrame in Data/archive/screen_results_YYYYMMDD.csv."""
    os.makedirs(ARCHIVE_DIR, exist_ok=True)
    fname = f"screen_results_{date.strftime('%Y%m%d')}.csv"
    path  = os.path.join(ARCHIVE_DIR, fname)
    df.to_csv(path)
    return path


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description='Genera CSV storici dello screener')
    parser.add_argument('--days',    type=int, default=63,
                        help='Numero di giorni di trading da rigenerare (default 63)')
    parser.add_argument('--dry-run', action='store_true',
                        help='Mostra i giorni senza scrivere nulla')
    args = parser.parse_args()

    print()
    print('══════════════════════════════════════════════════════')
    print('  generate_historical_csvs.py')
    print('══════════════════════════════════════════════════════')
    print()

    # Trova il CSV più recente per la lista ticker
    csv_path = find_latest_csv()
    print(f'  Ticker da: {os.path.basename(csv_path)}')
    tickers, sector_map = load_tickers_and_sectors(csv_path)
    print(f'  {len(tickers):,} ticker caricati')

    # Giorni da generare
    days_list = trading_days_back(args.days)
    print(f'  Giorni da generare: {len(days_list)}  ({days_list[0]} → {days_list[-1]})')

    # Controlla quali CSV esistono già
    existing = set()
    if os.path.isdir(ARCHIVE_DIR):
        for f in os.listdir(ARCHIVE_DIR):
            if f.startswith('screen_results_') and f.endswith('.csv'):
                try:
                    d = datetime.strptime(f.replace('screen_results_', '').replace('.csv', ''), '%Y%m%d').date()
                    existing.add(d)
                except ValueError:
                    pass

    to_generate = [d for d in days_list if d not in existing]
    print(f'  Già esistenti:      {len(existing)}')
    print(f'  Da generare:        {len(to_generate)}')

    if not to_generate:
        print('\n  ✅  Niente da fare — tutti i CSV sono già presenti.')
        return

    if args.dry_run:
        print('\n  [dry-run] Giorni che verrebbero generati:')
        for d in to_generate:
            print(f'    {d}')
        return

    print()
    print('  Scaricando OHLCV (1 anno, tutti i ticker)…')
    print('  Questo richiede 10-20 minuti.')
    t0 = time.time()
    ohlcv_all = download_all_ohlcv(tickers, period='1y')
    elapsed = time.time() - t0
    print(f'  Download completato in {elapsed:.0f}s — {len(ohlcv_all):,} ticker scaricati')

    if BENCHMARK_TICKER not in ohlcv_all:
        print(f'  ✗  Benchmark {BENCHMARK_TICKER} non scaricato — impossibile continuare')
        sys.exit(1)

    print()
    print(f'  Calcolando indicatori per {len(to_generate)} giorni…')
    saved = 0
    failed = 0

    for i, target_date in enumerate(to_generate, 1):
        print(f'  [{i:3d}/{len(to_generate)}] {target_date}… ', end='', flush=True)
        t1 = time.time()

        df = compute_day_csv(target_date, ohlcv_all, sector_map, tickers)
        if df is None:
            failed += 1
            continue

        path = save_csv(df, target_date)
        elapsed_day = time.time() - t1
        print(f'{len(df):,} ticker  →  {os.path.basename(path)}  ({elapsed_day:.1f}s)')
        saved += 1

    print()
    print('══════════════════════════════════════════════════════')
    print(f'  ✅  Completato: {saved} CSV salvati in Data/archive/')
    if failed:
        print(f'  ⚠   {failed} giorni saltati (dati insufficienti)')
    print(f'  📁  {ARCHIVE_DIR}')
    print('══════════════════════════════════════════════════════')
    print()
    print('  Prossimo passo: python Screener/backfill_market_history.py')
    print('  (rileverà automaticamente i CSV storici e userà il 7-factor reale)')
    print()


if __name__ == '__main__':
    main()
