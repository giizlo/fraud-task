import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle, FancyBboxPatch
from matplotlib.gridspec import GridSpec
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RESULTS_DIR = os.path.join(BASE_DIR, "results")
DATA_DIR = os.path.join(BASE_DIR, "data")

COLORS = {
    'primary': '#1f4e79',
    'secondary': '#c55a11',
    'accent': '#70ad47',
    'danger': '#c00000',
    'warning': '#ffc000',
    'neutral': '#7f7f7f',
    'light': '#d9e2f3',
    'background': '#f8f9fa'
}

plt.rcParams.update({
    'figure.facecolor': 'white',
    'axes.facecolor': 'white',
    'axes.edgecolor': COLORS['neutral'],
    'axes.labelcolor': COLORS['primary'],
    'text.color': '#333333',
    'xtick.color': '#333333',
    'ytick.color': '#333333',
    'grid.color': '#e0e0e0',
    'grid.alpha': 0.5,
    'axes.grid': True,
    'axes.axisbelow': True,
    'font.size': 10,
    'axes.titlesize': 12,
    'axes.labelsize': 10,
})


def load_data():
    df_clusters = pd.read_csv(os.path.join(DATA_DIR, "clusters_for_sql.csv"))
    df_companies = pd.read_csv(os.path.join(DATA_DIR, "companies_for_sql.csv"))
    return df_clusters, df_companies


def create_distribution_plot(df_clusters, output_path):
    sizes = df_clusters["companies_count"][df_clusters["companies_count"] >= 2]
    q1, q3 = sizes.quantile([0.25, 0.75])
    iqr = q3 - q1
    threshold = q3 + 1.5 * iqr
    
    fig, axes = plt.subplots(2, 1, figsize=(12, 10), 
                             gridspec_kw={'height_ratios': [3, 1]})
    
    ax1 = axes[0]
    
    n, bins, patches = ax1.hist(sizes, bins=60, alpha=0.6, 
                                  color=COLORS['primary'], 
                                  edgecolor='white', linewidth=0.5,
                                  label='Кластеры')
    
    from scipy import stats
    kde_x = np.linspace(sizes.min(), min(sizes.max(), 200), 200)
    kde = stats.gaussian_kde(sizes[sizes <= 200])
    ax1_twin = ax1.twinx()
    ax1_twin.plot(kde_x, kde(kde_x), color=COLORS['secondary'], 
                  linewidth=2, label='KDE')
    ax1_twin.set_ylabel('Плотность распределения', color=COLORS['secondary'])
    ax1_twin.tick_params(axis='y', labelcolor=COLORS['secondary'])
    ax1_twin.set_ylim(0, None)
    
    ax1.axvline(threshold, color=COLORS['danger'], linestyle='--', 
                linewidth=2.5, label=f'Порог аномалии ({threshold:.0f})')
    
    ax1.axvspan(threshold, ax1.get_xlim()[1], alpha=0.1, color=COLORS['danger'])
    
    ax1.set_xlabel('Количество компаний в кластере')
    ax1.set_ylabel('Частота')
    ax1.set_title('Распределение размеров географических кластеров\n'
                  '(IQR-метод выявления аномалий)', 
                  fontsize=14, fontweight='bold', pad=15)
    ax1.set_xlim(0, min(200, sizes.max()))
    ax1.legend(loc='upper right')
    
    ax2 = axes[1]
    bp = ax2.boxplot(sizes[sizes <= 100], vert=False, patch_artist=True,
                     widths=0.6,
                     boxprops=dict(facecolor=COLORS['light'], 
                                   edgecolor=COLORS['primary'], linewidth=1.5),
                     medianprops=dict(color=COLORS['danger'], linewidth=2),
                     whiskerprops=dict(color=COLORS['neutral']),
                     capprops=dict(color=COLORS['neutral']))
    ax2.set_xlabel('Компаний в кластере')
    ax2.set_title('Box plot (до 100 компаний)', fontsize=10)
    ax2.set_yticks([])
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight', 
                facecolor='white', edgecolor='none')
    plt.close()
    print(f"[VIZ] Distribution plot: {output_path}")


def create_regional_analysis(df_clusters, output_path):
    region_stats = df_clusters['region'].value_counts().head(15)
    
    q1, q3 = df_clusters["companies_count"][df_clusters["companies_count"] >= 2].quantile([0.25, 0.75])
    threshold = q3 + 1.5 * (q3 - q1)
    
    anomalous_by_region = df_clusters[df_clusters["companies_count"] > threshold].groupby('region').size()
    
    fig, ax = plt.subplots(figsize=(12, 8))
    
    y_pos = np.arange(len(region_stats))
    bars = ax.barh(y_pos, region_stats.values, 
                   color=COLORS['primary'], alpha=0.8, 
                   edgecolor='white', linewidth=0.5)
    
    for i, (region, val) in enumerate(region_stats.items()):
        if region in anomalous_by_region.index:
            bars[i].set_color(COLORS['danger'])
            bars[i].set_alpha(0.7)
    
    ax.set_yticks(y_pos)
    ax.set_yticklabels([r[:25] for r in region_stats.index], fontsize=9)
    ax.invert_yaxis()
    ax.set_xlabel('Количество кластеров', fontsize=11)
    ax.set_title('Распределение кластеров по регионам\n'
                 '(красным выделены регионы с аномальными кластерами)', 
                 fontsize=13, fontweight='bold', pad=15)
    
    for i, (bar, val) in enumerate(zip(bars, region_stats.values)):
        ax.text(val + max(region_stats.values) * 0.01, 
                bar.get_y() + bar.get_height()/2, 
                f'{val:,}', va='center', fontsize=9)
    
    from matplotlib.patches import Patch
    legend_elements = [
        Patch(facecolor=COLORS['primary'], alpha=0.8, label='Обычные кластеры'),
        Patch(facecolor=COLORS['danger'], alpha=0.7, label='Есть аномалии')
    ]
    ax.legend(handles=legend_elements, loc='lower right')
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"[VIZ] Regional analysis: {output_path}")


def create_top_clusters_chart(df_clusters, output_path):
    top15 = df_clusters.nlargest(15, 'companies_count')
    
    fig, ax = plt.subplots(figsize=(12, 8))
    
    colors = plt.cm.Reds(np.linspace(0.3, 0.9, len(top15)))
    
    y_pos = np.arange(len(top15))
    bars = ax.barh(y_pos, top15['companies_count'].values, color=colors, 
                   edgecolor='white', linewidth=0.5)
    
    labels = []
    for _, row in top15.iterrows():
        region_short = row['region'][:20]
        labels.append(f"{region_short}")
    
    ax.set_yticks(y_pos)
    ax.set_yticklabels(labels, fontsize=9)
    ax.invert_yaxis()
    ax.set_xlabel('Количество компаний', fontsize=11)
    ax.set_title('ТОП-15 массовых адресов регистрации\n'
                 '(координаты с наибольшей концентрацией компаний)', 
                 fontsize=13, fontweight='bold', pad=15)
    
    total_companies = df_clusters['companies_count'].sum()
    for i, (bar, val) in enumerate(zip(bars, top15['companies_count'].values)):
        pct = val / total_companies * 100
        ax.text(val + max(top15['companies_count']) * 0.01, 
                bar.get_y() + bar.get_height()/2, 
                f'{val:,} ({pct:.1f}%)', va='center', fontsize=9)
    
    sm = plt.cm.ScalarMappable(cmap='Reds', 
                                norm=plt.Normalize(vmin=top15['companies_count'].min(), 
                                                   vmax=top15['companies_count'].max()))
    sm.set_array([])
    cbar = plt.colorbar(sm, ax=ax, orientation='vertical', pad=0.02)
    cbar.set_label('Размер кластера', rotation=270, labelpad=15)
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"[VIZ] Top clusters: {output_path}")


def create_category_treemap(df_clusters, output_path):
    def categorize(count):
        if count == 1:
            return 'Single\n(1 компания)'
        elif count <= 5:
            return 'Small\n(2-5)'
        elif count <= 20:
            return 'Medium\n(6-20)'
        elif count <= 100:
            return 'Large\n(21-100)'
        else:
            return 'Mass\n(>100)'
    
    df_clusters['category'] = df_clusters['companies_count'].apply(categorize)
    cat_stats = df_clusters['category'].value_counts()
    
    cat_order = ['Single\n(1 компания)', 'Small\n(2-5)', 'Medium\n(6-20)', 
                 'Large\n(21-100)', 'Mass\n(>100)']
    cat_stats = cat_stats.reindex([c for c in cat_order if c in cat_stats.index])
    
    fig, ax = plt.subplots(figsize=(12, 8))
    
    cat_colors = [COLORS['neutral'], COLORS['accent'], COLORS['primary'], 
                  COLORS['warning'], COLORS['danger']]
    
    y_positions = [0.8, 0.55, 0.3, 0.55, 0.2]
    x_positions = [0.2, 0.5, 0.5, 0.8, 0.2]
    
    for i, (cat, count) in enumerate(cat_stats.items()):
        size = np.sqrt(count / cat_stats.max()) * 0.25
        circle = plt.Circle((x_positions[i % len(x_positions)], 
                             y_positions[i % len(y_positions)]), 
                            size, color=cat_colors[i % len(cat_colors)], 
                            alpha=0.7, ec='white', linewidth=2)
        ax.add_patch(circle)
        
        ax.text(x_positions[i % len(x_positions)], 
                y_positions[i % len(y_positions)], 
                f'{cat.replace(chr(10), " ")}\n{count:,}', 
                ha='center', va='center', fontsize=10, fontweight='bold',
                color='white' if i > 1 else '#333333')
    
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.set_aspect('equal')
    ax.axis('off')
    ax.set_title('Распределение кластеров по категориям размера', 
                 fontsize=14, fontweight='bold', pad=20)
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"[VIZ] Category treemap: {output_path}")


def create_risk_heatmap(df_clusters, output_path):
    def categorize(count):
        if count == 1:
            return 'Single'
        elif count <= 5:
            return 'Small'
        elif count <= 20:
            return 'Medium'
        elif count <= 100:
            return 'Large'
        else:
            return 'Mass'
    
    df_clusters['size_cat'] = df_clusters['companies_count'].apply(categorize)
    
    top_regions = df_clusters['region'].value_counts().head(12).index
    df_top = df_clusters[df_clusters['region'].isin(top_regions)]
    
    pivot = df_top.groupby(['region', 'size_cat']).size().unstack(fill_value=0)
    
    cat_order = ['Single', 'Small', 'Medium', 'Large', 'Mass']
    pivot = pivot[[c for c in cat_order if c in pivot.columns]]
    
    fig, ax = plt.subplots(figsize=(12, 10))

    im = ax.imshow(pivot.values, cmap='YlOrRd', aspect='auto')

    ax.set_xticks(range(len(pivot.columns)))
    ax.set_yticks(range(len(pivot.index)))
    ax.set_xticklabels(pivot.columns, fontsize=10)
    ax.set_yticklabels(pivot.index, fontsize=9)

    for i in range(len(pivot.index)):
        for j in range(len(pivot.columns)):
            val = pivot.iloc[i, j]
            if val > 0:
                ax.text(j, i, str(int(val)), ha='center', va='center',
                        color='white' if val > pivot.values.max() * 0.5 else 'black',
                        fontsize=9)

    cbar = plt.colorbar(im, ax=ax)
    cbar.set_label('Количество кластеров', rotation=270, labelpad=15)

    ax.set_title('Тепловая карта: Распределение кластеров по регионам и категориям',
                 fontsize=13, fontweight='bold', pad=15)
    ax.set_xlabel('Категория размера кластера', fontsize=11)
    ax.set_ylabel('Регион', fontsize=11)

    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"[VIZ] Risk heatmap: {output_path}")


def create_dashboard_summary(df_clusters, df_companies, output_path):
    pass


def main():
    print("=" * 60)
    print("ADVANCED VISUALIZATION MODULE")
    print("=" * 60)
    
    os.makedirs(RESULTS_DIR, exist_ok=True)
    
    print("\n[1/2] Loading data...")
    df_clusters, df_companies = load_data()
    
    print("\n[2/2] Creating visualizations...")
    
    create_distribution_plot(df_clusters, os.path.join(RESULTS_DIR, "viz_distribution.png"))
    create_regional_analysis(df_clusters, os.path.join(RESULTS_DIR, "viz_regional.png"))
    create_top_clusters_chart(df_clusters, os.path.join(RESULTS_DIR, "viz_top_clusters.png"))
    create_category_treemap(df_clusters, os.path.join(RESULTS_DIR, "viz_categories.png"))
    create_risk_heatmap(df_clusters, os.path.join(RESULTS_DIR, "viz_heatmap.png"))
    
    print("\n" + "=" * 60)
    print("All visualizations created successfully!")
    print("=" * 60)
    print("\nGenerated files:")
    for f in os.listdir(RESULTS_DIR):
        if f.startswith("viz_") and f.endswith(".png"):
            print(f"  {f}")


if __name__ == "__main__":
    main()
