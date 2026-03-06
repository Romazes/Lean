#!/usr/bin/env python3
"""
Update Bybit entries in Data/symbol-properties/symbol-properties-database.csv
by fetching current instrument info from the Bybit public API.

This script replicates the logic of BybitExchangeInfoDownloader.cs in
QuantConnect/Lean.Brokerages.ByBit and ExchangeInfoUpdater.cs in this repo.

Usage:
    python update_bybit_spdb.py [--data-folder PATH] [--api-url URL]
"""

import argparse
import json
import os
import sys
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Dict, List, Tuple

MARKET = "bybit"
FILTER_PREFIX = MARKET + ","


def fetch_instruments(api_url: str, category: str) -> List[dict]:
    """Fetch all instruments for the given Bybit category, handling pagination."""
    instruments = []
    cursor = ""

    while True:
        url = f"{api_url}/v5/market/instruments-info?category={category}&limit=1000"
        if cursor:
            url += "&cursor=" + urllib.parse.quote(cursor)

        try:
            with urllib.request.urlopen(url, timeout=30) as response:
                data = json.loads(response.read())
        except Exception as exc:
            print(f"Error fetching {category} instruments: {exc}", file=sys.stderr)
            raise

        if data["retCode"] != 0:
            raise RuntimeError(f"Bybit API error for {category}: {data['retMsg']}")

        result = data["result"]
        instruments.extend(result.get("list", []))

        cursor = result.get("nextPageCursor", "")
        if not cursor:
            break

    return instruments


def get_lean_symbol_name(symbol: str) -> str:
    """
    Remove the multiplier prefix from symbols like 10000LADYSUSDT.
    Symbols starting with '10' have all leading '0' and '1' characters stripped
    (e.g. 10000LADYSUSDT -> LADYSUSDT, 1000BONKUSDT -> BONKUSDT).
    Symbols like 1INCHUSDT (do not start with '10') are kept as-is.

    This replicates the C# logic in BybitExchangeInfoDownloader.cs:
        symbol.StartsWith("10") ? symbol.TrimStart('0', '1') : symbol
    Note: lstrip("01") is the Python equivalent of TrimStart('0', '1').
    """
    if symbol.startswith("10"):
        return symbol.lstrip("01")
    return symbol


def should_skip_instrument(instrument: dict, category: str) -> bool:
    """Return True if the instrument should be excluded from the SPDB."""
    # Skip non-trading instruments
    if instrument.get("status", "").lower() != "trading":
        return True

    # Skip USDC perpetual and future contracts
    settle_coin = instrument.get("settleCoin", "")
    if settle_coin.upper() == "USDC" and category in ("linear", "inverse"):
        return True

    # Skip LinearFutures (they have expiration dates / USDC Futures)
    contract_type = instrument.get("contractType", "")
    if contract_type.lower() == "linearfutures":
        return True

    # Skip InverseFutures (non-perpetual)
    if contract_type.lower() == "inversefutures":
        return True

    return False


def get_lot_size(lot_size_filter: dict, category: str) -> str:
    """Return the lot size (quantity step) for the given category."""
    if category == "spot":
        # Spot uses basePrecision
        return lot_size_filter.get("basePrecision", "0.01")
    # Linear / Inverse use qtyStep, fall back to basePrecision
    qty_step = lot_size_filter.get("qtyStep")
    if qty_step is not None:
        return qty_step
    return lot_size_filter.get("basePrecision", "0.01")


def get_security_type(category: str) -> str:
    """Map a Bybit category string to the LEAN SecurityType string."""
    return "crypto" if category == "spot" else "cryptofuture"


def instrument_to_csv_line(instrument: dict, category: str) -> Tuple[Tuple[str, str, str], str]:
    """
    Convert a Bybit instrument dict to a CSV line.

    Returns:
        (key, csv_line) where key = (market, lean_symbol, security_type).

    CSV columns:
        market, symbol, type, description, quote_currency,
        contract_multiplier, minimum_price_variation, lot_size,
        market_ticker, minimum_order_size
    """
    symbol = instrument["symbol"]
    lean_symbol = get_lean_symbol_name(symbol)
    security_type = get_security_type(category)
    quote_coin = instrument.get("quoteCoin", "")

    price_filter = instrument.get("priceFilter", {})
    tick_size = price_filter.get("tickSize", "0.01")

    lot_size_filter = instrument.get("lotSizeFilter", {})
    lot_size = get_lot_size(lot_size_filter, category)
    min_order_qty = lot_size_filter.get("minOrderQty", "")

    # market,symbol,type,description,quote_currency,contract_multiplier,
    # minimum_price_variation,lot_size,market_ticker,minimum_order_size
    csv_line = (
        f"{MARKET},{lean_symbol},{security_type},{symbol},{quote_coin},1,"
        f"{tick_size},{lot_size},{symbol},{min_order_qty}"
    )

    key = (MARKET, lean_symbol, security_type)
    return key, csv_line


def update_spdb(data_folder: str, api_url: str) -> None:
    """Fetch fresh Bybit instrument data and update the SPDB CSV in-place."""
    spdb_path = Path(data_folder) / "symbol-properties" / "symbol-properties-database.csv"

    if not spdb_path.exists():
        raise FileNotFoundError(f"Cannot find SPDB at: {spdb_path}")

    print(f"Fetching instruments from Bybit API ({api_url})...")

    # Categories to fetch: linear and inverse perpetuals + spot
    categories = ["linear", "inverse", "spot"]
    new_entries: Dict[Tuple[str, str, str], str] = {}

    for category in categories:
        print(f"  Fetching {category} instruments...")
        instruments = fetch_instruments(api_url, category)
        count_before = len(new_entries)

        for instrument in instruments:
            if should_skip_instrument(instrument, category):
                continue
            key, csv_line = instrument_to_csv_line(instrument, category)
            new_entries[key] = csv_line

        count_added = len(new_entries) - count_before
        print(f"  Added {count_added} {category} entries (total fetched: {len(instruments)})")

    print(f"Total new Bybit entries: {len(new_entries)}")

    # Read the existing CSV file
    existing_content = spdb_path.read_text(encoding="utf-8")
    existing_lines = existing_content.splitlines(keepends=True)

    # Preserve any existing Bybit entries not returned by the API (delistings/removals)
    for line in existing_lines:
        stripped = line.strip()
        if not stripped.lower().startswith(FILTER_PREFIX.lower()):
            continue
        parts = stripped.split(",")
        if len(parts) < 3:
            continue
        key = (parts[0], parts[1], parts[2])
        if key not in new_entries:
            new_entries[key] = stripped

    # Sort by security type then symbol (matching ExchangeInfoUpdater behaviour)
    sorted_entries = sorted(new_entries.items(), key=lambda x: (x[0][2], x[0][1]))

    # Rebuild the file: replace all existing bybit lines with sorted_entries
    output_lines: List[str] = []
    bybit_written = False

    for line in existing_lines:
        if line.strip().lower().startswith(FILTER_PREFIX.lower()):
            if not bybit_written:
                for _, csv_line in sorted_entries:
                    output_lines.append(csv_line + "\n")
                bybit_written = True
            # Drop old bybit line (replaced above)
        else:
            output_lines.append(line if line.endswith("\n") else line + "\n")

    if not bybit_written:
        # No existing bybit section — append at end
        output_lines.append("\n")
        for _, csv_line in sorted_entries:
            output_lines.append(csv_line + "\n")

    spdb_path.write_text("".join(output_lines), encoding="utf-8")
    print(f"Updated SPDB written to: {spdb_path}")
    print(f"Total Bybit entries written: {len(sorted_entries)}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Update Bybit entries in symbol-properties-database.csv from the Bybit API"
    )
    parser.add_argument(
        "--data-folder",
        default=os.environ.get("DATA_FOLDER", "Data"),
        help="Path to the Lean data folder (default: Data or DATA_FOLDER env var)",
    )
    parser.add_argument(
        "--api-url",
        default=os.environ.get("BYBIT_API_URL", "https://api.bybit.com"),
        help="Bybit API base URL (default: https://api.bybit.com or BYBIT_API_URL env var)",
    )

    args = parser.parse_args()
    update_spdb(args.data_folder, args.api_url)


if __name__ == "__main__":
    main()
