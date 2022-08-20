import logging
from pathlib import Path
import polars as pl
import numpy as np

# TO PUT IN CONFIG FILE
col_types={
    "post_id": pl.Utf8, 
    "date": pl.Utf8,
    "year": pl.Int64,
    "price": pl.Float32,
    "sqft": pl.Int64,
    "nhood": pl.Utf8,
    "county": pl.Utf8,
    "city": pl.Utf8,
    "lat": pl.Float64,
    "lon": pl.Float64,
    "title": pl.Utf8,
    "descr": pl.Utf8
}

def load_from_path(path: Path) -> pl.DataFrame:
    #
    return pl.read_csv(
        path, 
        columns=list(col_types.keys()), 
        dtypes=col_types,
        null_values=["NA"]
    )

def load_from_url(url: str) -> pl.DataFrame:
    #
    return pl.read_csv(url)

def clean(df: pl.DataFrame) -> pl.DataFrame:
    #
    return (
        df
        .with_columns(
            [
                (
                    pl.col("date")
                    .cast(pl.Utf8, strict=False)
                    .str
                    .strptime(pl.Date, "%Y%m%d")
                    .cast(str)
                )
            ]
        )
        #.drop_nulls()
        #.fill_null(value=)
    )
    