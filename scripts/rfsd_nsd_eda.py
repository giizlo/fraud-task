import pandas as pd
import json
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "..", "data")
RFSD_PATH = os.path.join(DATA_DIR, "rfsd_2020_2024.parquet")
NSD_PATH = os.path.join(DATA_DIR, "runsd", "nsd_isin.json")

CLUSTER_PRECISION = 3
CLUSTER_THRESHOLD = 30
TOP_PEAK_MONTHS = 5


def load_rfsd_data(path):
    df = pd.read_parquet(path)
    print(f"[RFSD] Loaded shape: {df.shape}")
    return df


def load_nsd_data(path):
    with open(path, "r", encoding="utf-8") as f:
        records = [json.loads(line.strip()) for line in f if line.strip()]
    
    df = pd.DataFrame(records)
    print(f"[NSD] Loaded shape: {df.shape}")
    return df


def extract_property(props, key):
    if isinstance(props, dict) and key in props:
        val = props[key]
        return val[0] if isinstance(val, list) and val else val
    return None


def identify_sanction_records(df):
    df["topics"] = df["properties"].apply(lambda p: extract_property(p, "topics"))
    df["first_seen"] = pd.to_datetime(df["first_seen"], errors="coerce")
    df["target"] = df["target"].astype(bool)
    
    def is_sanction(topics) -> bool:
        if isinstance(topics, list):
            return any("sanction" in str(t).lower() for t in topics)
        return "sanction" in str(topics).lower()
    
    df["is_sanction"] = df["topics"].apply(is_sanction)
    
    sanctions_total = df["is_sanction"].sum()
    print(f"[NSD] Sanction records: {sanctions_total:,}")
    
    return df


def get_sanction_peak_months(df, n=TOP_PEAK_MONTHS):
    month_counts = df["first_seen"].dropna().dt.to_period("M").value_counts()
    peak_months = month_counts.nlargest(n).index
    
    print(f"[NSD] Top {n} sanction peak months:")
    for month in peak_months:
        count = month_counts[month]
        print(f"  - {month}: {count:,} records")
    
    return peak_months


def create_sanction_window_flag(df, peak_months):
    def in_sanction_window(date: pd.Timestamp) -> bool:
        if pd.isna(date):
            return False
        return date.to_period("M") in peak_months
    
    df["sanctions_window"] = df["creation_date"].apply(in_sanction_window)
    count = df["sanctions_window"].sum()
    
    print(f"[FEATURE] Companies in sanctions window: {count:,}")
    return df


def create_geographic_clusters(df):
    df["lat_round"] = df["lat"].round(CLUSTER_PRECISION)
    df["lon_round"] = df["lon"].round(CLUSTER_PRECISION)
    
    cluster_cols = ["region", "oktmo", "lat_round", "lon_round"]
    
    sanctions_mask = df["sanctions_window"]
    df["cluster_size"] = (
        df[sanctions_mask]
        .groupby(cluster_cols)["inn"]
        .transform("count")
        .reindex(df.index, fill_value=0)
    )
    
    df["cluster_flag"] = df["cluster_size"] > CLUSTER_THRESHOLD
    df["high_risk"] = df["sanctions_window"] & df["cluster_flag"]
    
    high_risk_total = df["high_risk"].sum()
    print(f"[FEATURE] High-risk companies identified: {high_risk_total:,}")
    
    return df


def export_for_sql(df):
    cols_for_sql = [
        "inn", "ogrn", "region", "creation_date", "okved", "oktmo",
        "lat", "lon", "sanctions_window", "cluster_size",
        "cluster_flag", "high_risk"
    ]
    
    companies_path = os.path.join(DATA_DIR, "companies_for_sql.csv")
    df[cols_for_sql].to_csv(companies_path, index=False)
    print(f"[EXPORT] Companies saved: {companies_path} ({len(df):,} rows)")
    
    cluster_cols = ["region", "oktmo", "lat_round", "lon_round"]
    df_clusters = (
        df.groupby(cluster_cols)
        .agg({"inn": "nunique", "high_risk": "sum"})
        .reset_index()
        .rename(columns={"inn": "companies_count", "high_risk": "high_risk_companies"})
    )
    
    clusters_path = os.path.join(DATA_DIR, "clusters_for_sql.csv")
    df_clusters.to_csv(clusters_path, index=False)
    print(f"[EXPORT] Clusters saved: {clusters_path} ({len(df_clusters):,} rows)")


def main():
    print("=" * 60)
    print("RFSD + RuNSD EDA & Feature Engineering")
    print("=" * 60)
    
    print("\n[1/4] Loading data...")
    df_rfsd = load_rfsd_data(RFSD_PATH)
    df_nsd = load_nsd_data(NSD_PATH)
    
    print("\n[2/4] Processing NSD sanctions data...")
    df_nsd = identify_sanction_records(df_nsd)
    peak_months = get_sanction_peak_months(df_nsd)
    
    print("\n[3/4] Feature engineering...")
    df_rfsd = create_sanction_window_flag(df_rfsd, peak_months)
    df_rfsd = create_geographic_clusters(df_rfsd)
    
    print("\n[4/4] Exporting for SQL...")
    export_for_sql(df_rfsd)
    
    print("\n" + "=" * 60)
    print("EDA completed successfully")
    print("=" * 60)


if __name__ == "__main__":
    main()
