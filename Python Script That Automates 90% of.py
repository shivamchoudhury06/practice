import pandas as pd
import numpy as np
from sklearn.preprocessing import LabelEncoder
import warnings
from typing import Tuple

warnings.filterwarnings("ignore")


def load_csv(file_path: str) -> pd.DataFrame:
    """Load CSV into a DataFrame."""
    return pd.read_csv(file_path)


def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """Drop duplicate rows."""
    return df.drop_duplicates()


def clean_text_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize text columns: strip, lower, collapse spaces, remove special chars."""
    obj_cols = df.select_dtypes(include=["object"]).columns
    for col in obj_cols:
        df[col] = (
            df[col]
            .astype(str)
            .str.strip()
            .str.lower()
            .str.replace(r"\s+", " ", regex=True)
            .str.replace(r"[^\w\s@.]", "", regex=True)
        )
    return df


def coerce_numeric_like_columns(df: pd.DataFrame, min_fraction: float = 0.1) -> pd.DataFrame:
    """Coerce object columns to numeric when a reasonable fraction parse as numbers."""
    for col in df.columns:
        if col not in df.select_dtypes(include=[np.number]).columns:
            coerced = pd.to_numeric(df[col], errors="coerce")
            if coerced.notna().sum() >= (len(df) * min_fraction):
                df[col] = coerced
    return df


def ensure_numeric_and_fill(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure numeric dtypes and fill missing numeric values with median."""
    num_cols = df.select_dtypes(include=[np.number]).columns
    for col in num_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")
        df[col].fillna(df[col].median(), inplace=True)
    return df


def parse_datetime_columns(df: pd.DataFrame, min_fraction: float = 0.3) -> pd.DataFrame:
    """Parse object columns to datetimes when many values parse successfully."""
    obj_cols = df.select_dtypes(include=["object"]).columns
    for col in obj_cols:
        parsed = pd.to_datetime(df[col], errors="coerce")
        if parsed.notna().sum() >= (len(df) * min_fraction):
            df[col] = parsed.dt.normalize()
    return df


def round_numeric_columns(df: pd.DataFrame, ndigits: int = 2) -> pd.DataFrame:
    """Round numeric columns to a fixed number of decimals."""
    num_cols = df.select_dtypes(include=[np.number]).columns
    if len(num_cols) > 0:
        df[num_cols] = df[num_cols].astype(float).round(ndigits)
    return df


def remove_outliers_iqr(df: pd.DataFrame) -> pd.DataFrame:
    """Remove outliers using the IQR method across numeric columns."""
    num_cols = df.select_dtypes(include=[np.number]).columns
    for col in num_cols:
        Q1 = df[col].quantile(0.25)
        Q3 = df[col].quantile(0.75)
        IQR = Q3 - Q1
        lower = Q1 - 1.5 * IQR
        upper = Q3 + 1.5 * IQR
        df = df[(df[col] >= lower) & (df[col] <= upper)]
    return df


def encode_low_cardinality(df: pd.DataFrame, max_unique: int = 10) -> pd.DataFrame:
    """Encode categorical object columns with low cardinality using LabelEncoder."""
    cat_cols = df.select_dtypes(include=["object"]).columns
    for col in cat_cols:
        if df[col].nunique(dropna=False) < max_unique:
            le = LabelEncoder()
            df[col] = le.fit_transform(df[col].astype(str))
    return df


def save_and_report(df: pd.DataFrame, output_path: str, original_shape: Tuple[int, int], verbose: bool = True) -> None:
    """Save DataFrame to CSV and print a short report when verbose."""
    df.to_csv(output_path, index=False)
    if verbose:
        print(f"Cleaning complete! Saved to {output_path}")
        print(f"Final shape: {df.shape} (reduced from {original_shape})")
        print("\nData info:\n")
        df.info()
        print("\nFirst rows:\n", df.head())


def auto_clean_data(
    file_path: str,
    output_path: str = "cleaned_data.csv",
    verbose: bool = True,
    encode_low_cardinality_flag: bool = True,
    drop_outliers_flag: bool = True,
) -> pd.DataFrame:
    """Orchestrate the cleaning pipeline using smaller helper functions."""
    df = load_csv(file_path)
    original_shape = df.shape
    if verbose:
        print(f"Loaded data: {original_shape[0]} rows, {original_shape[1]} columns")

    df = remove_duplicates(df)
    if verbose:
        print(f"After duplicates removal: {df.shape[0]} rows")

    df = clean_text_columns(df)
    df = coerce_numeric_like_columns(df)
    df = ensure_numeric_and_fill(df)
    df = parse_datetime_columns(df)
    df = round_numeric_columns(df)

    if drop_outliers_flag:
        df = remove_outliers_iqr(df)

    if encode_low_cardinality_flag:
        df = encode_low_cardinality(df)

    save_and_report(df, output_path, original_shape, verbose=verbose)

    return df