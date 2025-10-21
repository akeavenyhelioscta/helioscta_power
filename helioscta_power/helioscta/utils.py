import os

import pandas as pd
from pathlib import Path

"""
"""

def get_current_directory() -> str:
    cwd = os.getcwd()
    return cwd

def get_file_path(filename: str) -> str:
    cwd = get_current_directory()
    filepath = os.path.join(cwd, filename)
    if not Path(filepath).exists():
        raise FileNotFoundError(f"File not found: {filepath}")
    return filepath

"""
"""

def write_pandas_parquet(df: pd.DataFrame, filename: str, index: bool = False, cache_dir: str = 'cache') -> None:
    """
    """
    # cache dir
    cwd = get_current_directory()
    cache_dir = os.path.join(cwd, cache_dir)
    os.makedirs(cache_dir, exist_ok=True)

    # write
    filepath = os.path.join(cache_dir, filename)
    df.to_parquet(filepath, index=index)


def read_pandas_parquet(filename: str, cache_dir: str = 'cache') -> pd.DataFrame:
    """
    """
    # cache dir
    cwd = get_current_directory()
    cache_dir = os.path.join(cwd, cache_dir)
    os.makedirs(cache_dir, exist_ok=True)

    # read
    filepath = os.path.join(cache_dir, filename)
    if os.path.exists(filepath):
        df = pd.read_parquet(filepath)
        return df
    else:
        return None