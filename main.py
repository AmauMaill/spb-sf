from pathlib import Path
from typing import Dict
import polars as pl
from supabase import create_client, Client
from dotenv import load_dotenv
import os
import logging
from tqdm import tqdm

from src.etl import load_from_path, clean

logging.basicConfig()

# PATH OR URL CONFIG FILE
df_raw = load_from_path(Path("./rent.csv"))
df_clean = clean(df=df_raw)

def add_entry(supabase: Client, data: Dict) -> None:
    data = supabase.table('rent').insert(data).execute()

def update_entry(supabase: Client, data: Dict) -> None:
    data = supabase.table('rent').update(data).execute()

def main():

    load_dotenv()
    url: str = os.environ.get("SUPABASE_URL")
    key: str = os.environ.get("SUPABASE_KEY")
    secret_key: str = os.environ.get("SUPABASE_SECRET_KEY")
    supabase: Client = create_client(url, secret_key)
    
    #logging.error(f"{df_clean.columns}")
    #logging.error(f"{df_clean.dtypes}")
    #logging.error(f"{df_clean.head()}")
    
    print("Starting...")
    for year in tqdm(df_clean.select(pl.col("year")).unique().to_series().to_list()):
        dict_clean = (
            df_clean
            .filter(
                (pl.col("year") == year) & \
                    (pl.col("post_id").is_not_null())
            )
            .head(10_000)
            .to_dicts()
        )

        add_entry(supabase=supabase, data=dict_clean)

        
if __name__ == "__main__":
    main()
