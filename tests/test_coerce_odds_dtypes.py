import pandas as pd
import pytest
from src.ingest.loaders import _coerce_odds_dtypes

TIMESTAMP_COLS = ["commence_time"]
FLOAT_COLS = ["price_home", "price_away"]
INTEGER_COLS = ["season"]

def test_coerce_odds_dtypes_basic():
    """Verify that _coerce_odds_dtypes coerces expected columns properly."""
    # Create dummy input data (strings, mixed types)
    df = pd.DataFrame({
        "commence_time": ["2025-10-26T12:30:00Z", "not_a_date"],
        "price_home": ["2.1", "1.8"],
        "price_away": [None, "3.4"],
        "season": ["2025", None],
        "bookmaker": ["Bet365", "Bwin"]
    })
    # Run the coercion
    out = _coerce_odds_dtypes(df.copy())
    # Check column dtypes
    assert pd.api.types.is_datetime64tz_dtype(out["commence_time"])
    assert pd.api.types.is_float_dtype(out["price_home"])
    assert pd.api.types.is_float_dtype(out["price_away"])
    assert pd.api.types.is_integer_dtype(out["season"])
    # Ensure untouched columns stay object dtype
    assert out["bookmaker"].dtype == object

def test_empty_dataframe_returns_itself():
    """Ensure that empty DataFrames or None are safely returned."""
    df_empty = pd.DataFrame()
    assert _coerce_odds_dtypes(df_empty).empty
    assert _coerce_odds_dtypes(None) is None
