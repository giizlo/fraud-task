import pandas as pd
import matplotlib.pyplot as plt
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RESULTS_DIR = os.path.join(BASE_DIR, "..", "results")
DATA_PATH = os.path.join(BASE_DIR, "..", "data", "clusters_for_sql.csv")

plt.style.use('default')


def load_cluster_data(path):
    df = pd.read_csv(path)
    print(f"[DATA] Total clusters loaded: {len(df):,}")
    return df


def calculate_iqr_threshold(sizes):
    q1, q3 = sizes.quantile([0.25, 0.75])
    iqr = q3 - q1
    threshold = q3 + 1.5 * iqr
    
    return q1, q3, iqr, threshold


def print_statistics(df, sizes, threshold):
    outliers = (df["companies_count"] > threshold).sum()
    
    print("\n" + "=" * 50)
    print("CLUSTER ANALYSIS STATISTICS")
    print("=" * 50)
    print(f"Total clusters: {len(df):,}")
    print(f"Clusters (≥2 companies): {len(sizes):,}")
    print(f"  Mean size: {sizes.mean():.1f}")
    print(f"  Median size: {sizes.median():.1f}")
    print(f"  Max size: {sizes.max():,}")
    print(f"  Q3: {sizes.quantile(0.75):.0f}")
    print(f"\nIQR Threshold: {threshold:.0f}")
    print(f"Anomalies (> {threshold:.0f}): {outliers:,} ({outliers/len(df)*100:.1f}%)")
    
    if "high_risk_companies" in df.columns:
        high_risk_total = df["high_risk_companies"].sum()
        print(f"\nHigh-risk companies: {high_risk_total:,}")
    print("=" * 50)


def get_top_clusters(df, n=10):
    top = df.nlargest(n, "companies_count")[["region", "oktmo", "companies_count"]]
    
    print(f"\nTOP-{n} MASS ADDRESSES:")
    print(top.to_string(index=False))
    
    return top


def visualize_distribution(sizes, threshold, output_path):
    fig, ax = plt.subplots(figsize=(12, 8))
    
    ax.hist(sizes, bins=50, alpha=0.7, color='steelblue', edgecolor='black', label='Clusters')
    
    ax.axvline(
        threshold, 
        color="darkred", 
        linestyle="--", 
        linewidth=3, 
        label=f'IQR threshold ({threshold:.0f})'
    )
    
    ax.set_xlim(0, min(150, sizes.max()))
    ax.set_xlabel("Companies per cluster", fontsize=12)
    ax.set_ylabel("Frequency", fontsize=12)
    ax.set_title("Распределение размеров кластеров\n(Географическая концентрация регистраций компаний)", 
                 fontsize=14, fontweight='bold')
    ax.legend(fontsize=11)
    ax.grid(True, alpha=0.3)
    
    ax.annotate(
        f"Аномалии: {(sizes > threshold).sum()}",
        xy=(threshold, ax.get_ylim()[1] * 0.9),
        xytext=(threshold + 20, ax.get_ylim()[1] * 0.8),
        arrowprops=dict(arrowstyle='->', color='darkred'),
        fontsize=11,
        color='darkred'
    )
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    
    print(f"\n[VIZ] Saved: {output_path}")


def export_results(df, top_clusters, threshold):
    os.makedirs(RESULTS_DIR, exist_ok=True)
    
    top_path = os.path.join(RESULTS_DIR, "top_mass_addresses.csv")
    top_clusters.to_csv(top_path, index=False)
    print(f"[EXPORT] TOP-10 saved: {top_path}")
    
    anomalous = df[df["companies_count"] > threshold].head(50)
    anomalous_path = os.path.join(RESULTS_DIR, "anomalous_clusters.csv")
    anomalous.to_csv(anomalous_path, index=False)
    print(f"[EXPORT] Anomalies saved: {anomalous_path} ({len(anomalous)} rows)")


def print_focus_regions(top_clusters):
    top_regions = top_clusters["region"].value_counts().head(8).index.tolist()
    print(f"\n[INSIGHT] Focus regions: {', '.join(top_regions)}")


def main():
    print("=" * 60)
    print("CLUSTER ANALYSIS — ANOMALY DETECTION MODULE")
    print("=" * 60)
    
    print("\n[1/4] Loading cluster data...")
    df_clusters = load_cluster_data(DATA_PATH)
    
    print("\n[2/4] Calculating statistics...")
    sizes = df_clusters["companies_count"][df_clusters["companies_count"] >= 2]
    q1, q3, iqr, threshold = calculate_iqr_threshold(sizes)
    print_statistics(df_clusters, sizes, threshold)
    
    print("\n[3/4] Identifying top clusters...")
    top_clusters = get_top_clusters(df_clusters, n=10)
    
    print("\n[4/4] Creating visualizations...")
    plot_path = os.path.join(RESULTS_DIR, "cluster_distribution.png")
    visualize_distribution(sizes, threshold, plot_path)
    
    print("\n[5/5] Exporting results...")
    export_results(df_clusters, top_clusters, threshold)
    
    print_focus_regions(top_clusters)
    
    print("\n" + "=" * 60)
    print("Analysis completed successfully")
    print("=" * 60)
    print("\nOUTPUT FILES:")
    print(f"  Visualization: results/cluster_distribution.png")
    print(f"  TOP-10 addresses: results/top_mass_addresses.csv")
    print(f"  Anomalies: results/anomalous_clusters.csv")


if __name__ == "__main__":
    main()
