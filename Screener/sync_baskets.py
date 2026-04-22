#!/usr/bin/env python3
"""
sync_baskets.py — Sync sector_baskets.py from Basket.docx
──────────────────────────────────────────────────────────
Reads ../Basket.docx, compares it with SECTOR_BASKETS in sector_baskets.py,
and inserts any new baskets found in the docx (alphabetically).
Existing baskets are never modified or deleted.

Run automatically by START_SCREENER.command before the scan.
"""

import os
import re
import logging

logger = logging.getLogger(__name__)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _normalize_name(name: str) -> str:
    """Lowercase, remove punctuation, collapse spaces — for comparison only."""
    name = name.upper().strip()
    name = re.sub(r'[^A-Z0-9 ]', ' ', name)
    return re.sub(r'\s+', ' ', name).strip()


def _parse_docx(docx_path: str) -> dict:
    """
    Parse Basket.docx and return {basket_name: [tickers]}.
    Lines format:  BASKET NAME<\xa0 or space>TICK1, TICK2, TICK3
    """
    try:
        from docx import Document
    except ImportError:
        logger.warning("python-docx not installed — skipping basket sync.")
        return {}

    if not os.path.isfile(docx_path):
        logger.warning("Basket.docx not found at %s — skipping sync.", docx_path)
        return {}

    doc = Document(docx_path)
    baskets = {}

    for para in doc.paragraphs:
        for raw_line in para.text.split('\n'):
            line = raw_line.strip()
            if not line or ':' in line[:30]:   # skip header lines
                continue

            # Split name from tickers on non-breaking space or first comma-containing token
            if '\xa0' in line:
                name, tickers_str = line.split('\xa0', 1)
                name = name.strip()
            else:
                tokens = line.split()
                split_idx = None
                for i, tok in enumerate(tokens):
                    if ',' in tok:
                        split_idx = i
                        break
                if split_idx is None or split_idx == 0:
                    continue  # can't parse
                name = ' '.join(tokens[:split_idx]).strip()
                tickers_str = ' '.join(tokens[split_idx:]).strip()

            if not name or not tickers_str:
                continue

            # Parse tickers: split on comma/space, strip, uppercase, BRK.B → BRK-B
            raw_tickers = re.split(r'[,\s]+', tickers_str)
            tickers = []
            for t in raw_tickers:
                t = t.strip().upper().replace('.', '-')
                if re.match(r'^[A-Z][A-Z0-9\-\.]{0,8}$', t):
                    tickers.append(t)

            if tickers:
                baskets[name] = tickers

    return baskets


def _load_existing_names(baskets_path: str) -> set:
    """
    Return set of normalised basket names already in sector_baskets.py.
    We import the module directly to get the real SECTOR_BASKETS dict.
    """
    import importlib.util
    spec = importlib.util.spec_from_file_location("sector_baskets", baskets_path)
    mod  = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return {_normalize_name(k) for k in mod.SECTOR_BASKETS.keys()}


def _insert_basket_into_file(baskets_path: str, name: str, tickers: list) -> None:
    """
    Insert a new basket entry into sector_baskets.py in alphabetical order.
    Preserves all existing formatting.
    """
    with open(baskets_path, 'r', encoding='utf-8') as f:
        content = f.read()

    # Build the new entry lines
    max_line = 110
    prefix   = f'    "{name}": ['
    indent   = ' ' * len(prefix)
    joined   = ', '.join(f'"{t}"' for t in tickers)

    if len(prefix) + len(joined) + 2 <= max_line:
        new_entry = f'{prefix}{joined}],\n'
    else:
        # Wrap tickers across multiple lines (up to ~10 per line)
        chunks = [tickers[i:i+10] for i in range(0, len(tickers), 10)]
        lines  = []
        for j, chunk in enumerate(chunks):
            chunk_str = ', '.join(f'"{t}"' for t in chunk)
            if j == 0:
                lines.append(f'{prefix}{chunk_str},')
            elif j < len(chunks) - 1:
                lines.append(f'{indent}{chunk_str},')
            else:
                lines.append(f'{indent}{chunk_str}],')
        new_entry = '\n'.join(lines) + '\n'

    # Find insertion point: first key that sorts after `name` (alphabetically)
    # Pattern: lines like     "BASKET NAME": [
    key_pat = re.compile(r'^    "([^"]+)": \[', re.MULTILINE)
    insert_pos = None
    for m in key_pat.finditer(content):
        if m.group(1) > name:
            insert_pos = m.start()
            break

    if insert_pos is None:
        # Append before the closing } of SECTOR_BASKETS
        close = content.rfind('\n}')
        if close == -1:
            logger.error("Could not find closing brace in sector_baskets.py")
            return
        insert_pos = close + 1  # insert after the last newline before }

    new_content = content[:insert_pos] + new_entry + content[insert_pos:]
    with open(baskets_path, 'w', encoding='utf-8') as f:
        f.write(new_content)


# ── Main entry point ──────────────────────────────────────────────────────────

def sync_baskets() -> int:
    """
    Compare Basket.docx with sector_baskets.py and insert missing baskets.
    Returns the number of baskets added.
    """
    here         = os.path.dirname(os.path.abspath(__file__))
    docx_path    = os.path.join(here, '..', 'Basket.docx')
    baskets_path = os.path.join(here, 'sector_baskets.py')

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    logger.info("── Basket sync: reading Basket.docx ──")
    docx_baskets = _parse_docx(docx_path)
    if not docx_baskets:
        logger.info("No baskets parsed from docx — nothing to sync.")
        return 0

    existing_norm = _load_existing_names(baskets_path)
    added = 0

    for name, tickers in sorted(docx_baskets.items()):
        if _normalize_name(name) not in existing_norm:
            logger.info("  NEW basket: %r (%d tickers) → adding to sector_baskets.py", name, len(tickers))
            _insert_basket_into_file(baskets_path, name, tickers)
            existing_norm.add(_normalize_name(name))  # avoid duplicates within same run
            added += 1

    if added == 0:
        logger.info("Basket sync: no new baskets — sector_baskets.py is up to date.")
    else:
        logger.info("Basket sync: %d new basket(s) added to sector_baskets.py.", added)

    return added


if __name__ == '__main__':
    sync_baskets()
