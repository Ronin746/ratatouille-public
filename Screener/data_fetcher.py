
import yfinance as yf
import pandas as pd
import logging
import time

try:
    from config import ALL_TICKERS, BENCHMARK_TICKER
except ImportError:
    # Fallback for testing independently
    ALL_TICKERS = ["AAPL", "MSFT"]
    BENCHMARK_TICKER = "SPY"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Suppress yfinance internal error logging (e.g. "possibly delisted")
logging.getLogger("yfinance").setLevel(logging.CRITICAL)

# ── yfinance version detection ──────────────────────────────────────────────
import importlib.metadata as _im
try:
    _yf_version = tuple(int(x) for x in _im.version("yfinance").split(".")[:3])
except Exception:
    _yf_version = (0, 0, 0)

logger.info("yfinance version detected: %s", _yf_version)

# yfinance >= 1.x changed group_by default to 'column' → (Price, Ticker) MultiIndex.
# We force group_by='ticker' → (Ticker, Price) so data[ticker] still works.
# repair=True was introduced in 0.2.x but in yfinance 1.x it can overwrite High/Low
# with NaN for many tickers — so we disable it.
_DOWNLOAD_KWARGS = dict(
    group_by="ticker",
    threads=False,
    progress=False,
    auto_adjust=True,
    repair=False,           # ← KEY FIX: repair=True corrupts H/L in yfinance 1.x
    timeout=30,
)

# Pass multi_level_index only if yfinance supports it (>= 0.2.x)
try:
    import inspect as _inspect
    if "multi_level_index" in _inspect.signature(yf.download).parameters:
        _DOWNLOAD_KWARGS["multi_level_index"] = True
except Exception:
    pass


def _normalize_ticker_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure the per-ticker DataFrame always has expected column names.
    Handles yfinance returning 'Adj Close' instead of / alongside 'Close'.
    """
    if df is None or df.empty:
        return df
    # If only 'Adj Close' exists (no 'Close'), rename it
    if "Adj Close" in df.columns and "Close" not in df.columns:
        df = df.rename(columns={"Adj Close": "Close"})
    # Drop 'Adj Close' duplicate when both exist
    elif "Adj Close" in df.columns and "Close" in df.columns:
        df = df.drop(columns=["Adj Close"])
    return df


def _normalize_batch(data: pd.DataFrame, batch: list) -> pd.DataFrame:
    """
    Normalize the MultiIndex structure of a yfinance batch download result.
    Shared between main loop and retry loop.
    """
    price_cols = {"Open", "High", "Low", "Close", "Adj Close", "Volume"}

    if isinstance(data.columns, pd.MultiIndex):
        l0_unique = set(data.columns.get_level_values(0).unique())
        if l0_unique <= price_cols:
            logger.warning("Detected (Price, Ticker) MultiIndex — swapping levels.")
            data = data.swaplevel(axis=1).sort_index(axis=1)

    # Single-ticker batch: ensure MultiIndex
    if len(batch) == 1 and not isinstance(data.columns, pd.MultiIndex):
        data.columns = pd.MultiIndex.from_product([[batch[0]], data.columns])

    return data


def fetch_data(tickers: list[str] | tuple[str, ...] = ALL_TICKERS, period: str = "1y", interval: str = "1d") -> pd.DataFrame:
    """
    Fetch historical OHLCV data for the given tickers via yfinance batch download.

    Compatible with yfinance 0.2.x and 1.x:
    - Forces group_by='ticker' so columns are (Ticker, PriceType)
    - Disables repair=True which corrupts High/Low in yfinance >= 1.0
    - Handles both (Ticker, Price) and (Price, Ticker) MultiIndex structures
    - Failed batches are retried in smaller batches of 10 tickers
    """
    logger.info("Downloading data for %d tickers in batches... [yfinance %s]", len(tickers), _yf_version)

    # Adding benchmark to the list if not present
    unique_tickers = list(set(tickers + [BENCHMARK_TICKER]))

    # Batch Processing — 50 tickers to prevent DNS exhaustion on macOS
    BATCH_SIZE = 50
    all_data = []
    failed_tickers = []  # Tickers from batches that failed entirely

    total_batches = (len(unique_tickers) + BATCH_SIZE - 1) // BATCH_SIZE

    for i in range(0, len(unique_tickers), BATCH_SIZE):
        batch = unique_tickers[i:i + BATCH_SIZE]
        logger.info("Fetching batch %d/%d (%d tickers)...", i // BATCH_SIZE + 1, total_batches, len(batch))

        try:
            data = yf.download(
                batch,
                period=period,
                interval=interval,
                **_DOWNLOAD_KWARGS
            )

            if data.empty:
                logger.warning("Batch %d returned empty — queuing for retry.", i // BATCH_SIZE + 1)
                failed_tickers.extend(batch)
                continue

            data = _normalize_batch(data, batch)
            all_data.append(data)

            # Small sleep to let DNS/Network stack recover on macOS
            time.sleep(0.5)

        except (ValueError, KeyError, ConnectionError) as e:
            logger.warning("Batch %d network/data reject: %s — queuing %d tickers.", i // BATCH_SIZE + 1, e, len(batch))
            failed_tickers.extend(batch)
            continue
        except Exception as e:
            logger.warning("Batch %d unexpected failure: %s — queuing %d tickers.", i // BATCH_SIZE + 1, e, len(batch))
            failed_tickers.extend(batch)
            continue

    # ── RETRY: failed tickers in mini-batches of 10 ──────────────────────────
    if failed_tickers:
        RETRY_BATCH_SIZE = 10
        # Remove duplicates (benchmark might be in the list twice)
        failed_tickers = list(set(failed_tickers))
        total_retry = (len(failed_tickers) + RETRY_BATCH_SIZE - 1) // RETRY_BATCH_SIZE
        logger.info("Retrying %d tickers in %d mini-batches of %d...", len(failed_tickers), total_retry, RETRY_BATCH_SIZE)

        for j in range(0, len(failed_tickers), RETRY_BATCH_SIZE):
            retry_batch = failed_tickers[j:j + RETRY_BATCH_SIZE]
            retry_num = j // RETRY_BATCH_SIZE + 1
            logger.info("Retry %d/%d: %s...", retry_num, total_retry, retry_batch)

            try:
                data = yf.download(
                    retry_batch,
                    period=period,
                    interval=interval,
                    **_DOWNLOAD_KWARGS
                )

                if data.empty:
                    logger.warning("Retry %d still empty — skipping.", retry_num)
                    continue

                data = _normalize_batch(data, retry_batch)
                all_data.append(data)
                logger.info("Retry %d OK — recovered %d tickers.", retry_num, len(retry_batch))

                time.sleep(1.0)  # Longer pause between retries

            except (ValueError, KeyError, ConnectionError) as e:
                logger.warning("Retry %d failed (network/data): %s — tickers permanently skipped.", retry_num, e)
                continue
            except Exception as e:
                logger.warning("Retry %d failed (unexpected): %s — tickers permanently skipped.", retry_num, e)
                continue

    if not all_data:
        logger.error("No data fetched from any batch.")
        return pd.DataFrame()

    logger.info("Concatenating batches...")
    try:
        final_data = pd.concat(all_data, axis=1)
        # Remove duplicate ticker columns
        final_data = final_data.loc[:, ~final_data.columns.duplicated()]
        logger.info("Download complete. Shape: %s", final_data.shape)
        return final_data
    except Exception as e:
        logger.error("Error concatenating batches: %s", e)
        return pd.DataFrame()


def get_ticker_data(data: pd.DataFrame, ticker: str) -> pd.DataFrame | None:
    """
    Extracts a single ticker's OHLCV DataFrame from the bulk download result.

    Handles:
    - (Ticker, Price) MultiIndex  [standard with group_by='ticker']
    - (Price, Ticker) MultiIndex  [default in some yfinance 1.x builds]
    - Single-level columns        [when only one ticker was downloaded]
    - Missing 'Close' / 'Adj Close' renaming
    """
    if data is None or data.empty:
        return None

    if not isinstance(data.columns, pd.MultiIndex):
        # Single-level: columns are directly Open/High/Low/Close/Volume
        return _normalize_ticker_df(data)

    l0_unique = set(data.columns.get_level_values(0).unique())
    price_cols = {"Open", "High", "Low", "Close", "Adj Close", "Volume"}

    if ticker in l0_unique:
        # Standard (Ticker, Price) structure
        try:
            df = data[ticker].copy()
        except KeyError:
            logger.warning("Ticker %s not found in downloaded data.", ticker)
            return None
    elif l0_unique <= price_cols:
        # (Price, Ticker) structure — extract via cross-section
        try:
            df = data.xs(ticker, axis=1, level=1).copy()
        except KeyError:
            logger.warning("Ticker %s not found in downloaded data (Price,Ticker level).", ticker)
            return None
    else:
        logger.warning("Ticker %s not found. Known tickers: %s...", ticker, list(l0_unique)[:5])
        return None

    return _normalize_ticker_df(df)


def fetch_market_caps(tickers: list[str], max_workers: int = 10) -> dict[str, float]:
    """
    Fetch market capitalization for a list of tickers using yf.Ticker().fast_info.
    Uses ThreadPoolExecutor for parallel fetching.

    Args:
        tickers (list): List of ticker symbols.
        max_workers (int): Number of parallel threads (default 10).

    Returns:
        dict: {ticker: market_cap} where market_cap is a float (or 0 if unavailable).
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    logger.info("Fetching market caps for %d tickers (%d threads)...", len(tickers), max_workers)

    def _get_mcap(ticker):
        try:
            info = yf.Ticker(ticker).fast_info
            return ticker, info.get("marketCap", 0) or 0
        except Exception:
            return ticker, 0

    result = {}
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(_get_mcap, t): t for t in tickers}
        done_count = 0
        for future in as_completed(futures):
            ticker, mcap = future.result()
            result[ticker] = mcap
            done_count += 1
            if done_count % 500 == 0:
                logger.info("  Market cap progress: %d/%d", done_count, len(tickers))

    above_500m = sum(1 for v in result.values() if v >= 500_000_000)
    logger.info("Market caps fetched. %d/%d above $500M", above_500m, len(tickers))
    return result


def fetch_industries(tickers: list[str], max_workers: int = 20) -> dict[str, str]:
    """
    Fetch industry classification for a list of tickers using yf.Ticker().info.
    Returns dict: {ticker: industry_string} (e.g. 'Biotechnology', 'Software').
    Uses ThreadPoolExecutor for parallel fetching.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    logger.info("Fetching industries for %d tickers (%d threads)...", len(tickers), max_workers)

    def _get_industry(ticker):
        try:
            info = yf.Ticker(ticker).info
            return ticker, info.get("industry", "") or ""
        except Exception:
            return ticker, ""

    result = {}
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(_get_industry, t): t for t in tickers}
        for future in as_completed(futures):
            ticker, industry = future.result()
            result[ticker] = industry

    logger.info("Industries fetched for %d tickers.", len(result))
    return result


if __name__ == "__main__":
    # simple test
    df = fetch_data(["AAPL", "SPY"], period="1mo")
    print(df.head())
    aapl = get_ticker_data(df, "AAPL")
    print(aapl.head() if aapl is not None else "AAPL not found")
