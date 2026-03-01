import pandas as pd
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
final_parquet = os.path.join(BASE_DIR, "..", "data", "rfsd_2020_2024.parquet")
YEARS = [2020, 2021, 2022, 2023, 2024]
CHUNK_SIZE = 25000
MAX_ROWS = 200000


def process_chunk(chunk, year):
    chunk["creation_date"] = pd.to_datetime(chunk["creation_date"], errors="coerce")
    
    if "dissolution_date" in chunk.columns:
        chunk["dissolution_date"] = pd.to_datetime(chunk["dissolution_date"], errors="coerce")
    
    numeric_cols = ["age", "lat", "lon"]
    for col in numeric_cols:
        if col in chunk.columns:
            chunk[col] = pd.to_numeric(chunk[col], errors="coerce")
    
    chunk_filtered = chunk[chunk["creation_date"].dt.year == year]
    
    return chunk_filtered if len(chunk_filtered) > 0 else None


def load_and_process_year(year, total_rows):
    filename = os.path.join(BASE_DIR, "..", "data", "rfsd", f"{year}.csv")
    year_chunks = []
    
    print(f"[RFSD] Processing year {year}...")
    
    try:
        for chunk in pd.read_csv(
            filename, 
            sep=";", 
            chunksize=CHUNK_SIZE, 
            low_memory=False, 
            on_bad_lines="skip"
        ):
            if total_rows >= MAX_ROWS:
                break
                
            processed = process_chunk(chunk, year)
            if processed is not None:
                year_chunks.append(processed)
                total_rows += len(processed)
                
    except FileNotFoundError:
        print(f"[WARNING] File not found: {filename}")
    except Exception as e:
        print(f"[ERROR] Processing {year}: {e}")
    
    return year_chunks, total_rows


def clean_dataframe(df):
    print(f"[CLEAN] Raw data shape: {df.shape}")
    
    df = df.drop_duplicates(subset=["inn", "creation_date"])
    df["region"] = df["region"].fillna("unknown")
    
    print(f"[CLEAN] Cleaned shape: {df.shape}")
    
    return df


def main():
    print("=" * 50)
    print("RFSD Data Preprocessing Pipeline")
    print("=" * 50)
    
    all_chunks = []
    total_rows = 0
    
    for year in YEARS:
        year_chunks, total_rows = load_and_process_year(year, total_rows)
        all_chunks.extend(year_chunks)
        print(f"[RFSD] Year {year}: collected {len(year_chunks)} chunks")
    
    if all_chunks:
        df_rfsd = pd.concat(all_chunks, ignore_index=True)
        df_rfsd = clean_dataframe(df_rfsd)
        
        df_rfsd.to_parquet(final_parquet, index=False, compression="snappy")
        print(f"[SAVE] Saved to: {final_parquet}")
        print(f"[SAVE] Total records: {len(df_rfsd):,}")
    else:
        print("[ERROR] No data processed!")
    
    print("=" * 50)
    print("Preprocessing completed")
    print("=" * 50)


if __name__ == "__main__":
    main()
