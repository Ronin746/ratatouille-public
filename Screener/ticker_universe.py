
import requests
import logging

logger = logging.getLogger(__name__)


def get_us_tickers():
    """
    Fetches US tickers from a comprehensive GitHub source.
    Filters out any ticker containing '.' (ADRs, preferred shares, etc.)
    to keep only clean US-listed symbols.
    """
    try:
        url = "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/all/all_tickers.txt"
        response = requests.get(url, timeout=15)
        if response.status_code == 200:
            raw = [t.strip() for t in response.text.split('\n') if t.strip()]
            # STRICT: remove any ticker with a dot (e.g. BRK.A, BF.B, etc.)
            tickers = [t for t in raw if '.' not in t]
            logger.info("Fetched %d clean US tickers (no dots).", len(tickers))
            return tickers
    except Exception as e:
        logger.error("Error fetching US tickers: %s", e)

    # Fallback — small curated list
    logger.warning("Using fallback US list.")
    return [
        "AAPL", "MSFT", "NVDA", "GOOGL", "AMZN", "META", "TSLA", "AVGO",
        "JPM", "V", "MA", "LLY", "UNH", "XOM", "JNJ", "PG", "HD", "COST",
        "SPY", "QQQ"
    ]


def get_all_tickers():
    """Returns the full US-only ticker list (no European tickers)."""
    tickers = get_us_tickers()
    logger.info("Total Market Universe: %d US tickers.", len(tickers))
    return tickers


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    all_t = get_all_tickers()
    print(f"Total Tickers: {len(all_t)}")
    # Verify no dots
    dotted = [t for t in all_t if '.' in t]
    print(f"Tickers with dots (should be 0): {len(dotted)}")
    if dotted:
        print("Examples:", dotted[:10])
