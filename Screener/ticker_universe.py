
import logging
import os
import json
import time

logger = logging.getLogger(__name__)

HERE = os.path.dirname(os.path.abspath(__file__))

# ── Exchange config ───────────────────────────────────────────────────────
EXCHANGES = {
    "LSE":  (".L",  "London Stock Exchange"),
    "GER":  (".DE", "XETRA"),
    "MUN":  (".MU", "Gettex/Munich"),
    "MIL":  (".MI", "Euronext Milan"),
    "AMS":  (".AS", "Euronext Amsterdam"),
    "EBS":  (".SW", "SIX Swiss Exchange"),
    "ISE":  (".IR", "Euronext Dublin"),
    "TOR":  (".TO", "Toronto Stock Exchange"),
}

_CACHE_DIR = os.path.join(os.path.dirname(HERE), 'Data', 'tickers')
_CACHE_MAX_AGE_DAYS = 7


# ── Yahoo Finance screener via yfinance ───────────────────────────────────

def _yf_fetch_exchange(exchange_code: str) -> list[str]:
    """
    Fetch ALL equity tickers for a given exchange using yfinance.screen().
    Paginates through ALL results (no cap). Cached weekly so cost is minimal.
    """
    import yfinance as yf

    tickers = []
    offset = 0
    batch_size = 250

    query = yf.EquityQuery('eq', ['exchange', exchange_code])

    while True:
        try:
            res = yf.screen(query, size=batch_size, offset=offset,
                            sortField='intradaymarketcap', sortAsc=False)
            quotes = res.get('quotes', [])
            total = res.get('total', 0)

            if not quotes:
                break

            for q in quotes:
                sym = q.get('symbol', '')
                if not sym:
                    continue
                # Filter: only real stocks
                name = q.get('longName') or q.get('shortName') or ''
                base = sym.split('.')[0]
                # Skip numeric-heavy codes (warrants/rights like Z53508)
                if not base or not base[0].isalpha():
                    continue
                # Skip if >50% digits (e.g. A2A is fine, Z53508 is not)
                digit_ratio = sum(c.isdigit() for c in base) / len(base)
                if digit_ratio > 0.5:
                    continue
                # Skip unnamed instruments
                if not name:
                    continue
                tickers.append(sym)

            offset += batch_size
            if offset >= total:
                break

            time.sleep(0.2)

        except Exception as e:
            logger.warning("yf.screen %s offset=%d error: %s", exchange_code, offset, e)
            break

    return tickers


def fetch_all_exchange_tickers(exchange_code: str) -> list[str]:
    """
    Fetch all equity tickers for an exchange.
    Uses cache (Data/tickers/) to avoid re-fetching daily.
    """
    os.makedirs(_CACHE_DIR, exist_ok=True)
    cache_file = os.path.join(_CACHE_DIR, f"{exchange_code}.json")

    # Check cache
    if os.path.exists(cache_file):
        age_days = (time.time() - os.path.getmtime(cache_file)) / 86400
        if age_days < _CACHE_MAX_AGE_DAYS:
            with open(cache_file, 'r') as f:
                cached = json.load(f)
            logger.info("  %s: %d tickers (cache, %.1f days old)",
                       exchange_code, len(cached), age_days)
            return cached

    # Fetch from Yahoo via yfinance (no cap — paginates through everything)
    logger.info("  Fetching %s tickers from Yahoo Finance...", exchange_code)
    tickers = _yf_fetch_exchange(exchange_code)

    if tickers:
        with open(cache_file, 'w') as f:
            json.dump(tickers, f)
        logger.info("  %s: %d tickers fetched and cached.", exchange_code, len(tickers))
        return tickers

    # Stale cache fallback
    if os.path.exists(cache_file):
        with open(cache_file, 'r') as f:
            cached = json.load(f)
        logger.info("  %s: %d tickers (stale cache fallback)", exchange_code, len(cached))
        return cached

    logger.warning("  %s: no tickers found.", exchange_code)
    return []


# ── US tickers ────────────────────────────────────────────────────────────

def get_us_tickers():
    """Fetches US tickers from comprehensive GitHub source (~8000)."""
    import requests
    try:
        url = "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/all/all_tickers.txt"
        response = requests.get(url, timeout=15)
        if response.status_code == 200:
            raw = [t.strip() for t in response.text.split('\n') if t.strip()]
            tickers = [t for t in raw if '.' not in t]
            logger.info("Fetched %d clean US tickers.", len(tickers))
            return tickers
    except Exception as e:
        logger.error("Error fetching US tickers: %s", e)

    logger.warning("Using fallback US list.")
    return ["AAPL", "MSFT", "NVDA", "GOOGL", "AMZN", "META", "TSLA", "AVGO",
            "JPM", "V", "MA", "LLY", "UNH", "XOM", "JNJ", "PG", "HD", "COST"]


# ── Public API ────────────────────────────────────────────────────────────

def get_us_ca_tickers():
    """Returns US + Canada (TSX) tickers — both close at 20:00 UTC."""
    us = get_us_tickers()
    ca = fetch_all_exchange_tickers("TOR")
    combined = us + ca
    logger.info("US+CA Universe: %d US + %d CA = %d total.",
                len(us), len(ca), len(combined))
    return combined


def get_eu_tickers():
    """
    Returns ALL listed equity tickers from EU exchanges via yfinance screener.
    Cached weekly in Data/tickers/.
    """
    eu_exchanges = ["LSE", "GER", "MUN", "MIL", "AMS", "EBS", "ISE"]
    all_tickers = []

    for exch in eu_exchanges:
        tickers = fetch_all_exchange_tickers(exch)
        all_tickers.extend(tickers)

    # Deduplicate
    all_tickers = list(dict.fromkeys(all_tickers))
    logger.info("EU Universe: %d total tickers.", len(all_tickers))
    return all_tickers


def get_all_tickers():
    """Returns the full US-only ticker list (backward compatible)."""
    tickers = get_us_tickers()
    logger.info("Total Market Universe: %d US tickers.", len(tickers))
    return tickers


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print("=== EU ===")
    eu = get_eu_tickers()
    print(f"  EU Total: {len(eu)}")
    for code in ["LSE", "GER", "MUN", "MIL", "AMS", "EBS", "ISE"]:
        suffix = EXCHANGES[code][0]
        count = len([t for t in eu if t.endswith(suffix)])
        print(f"  {EXCHANGES[code][1]} ({suffix}): {count}")

    print("\n=== US + CA ===")
    us_ca = get_us_ca_tickers()
    print(f"  US+CA Total: {len(us_ca)}")
