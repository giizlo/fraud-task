#!/bin/bash
set -e

echo "=========================================="
echo "FRAUD-AUDIT ANALYSIS PIPELINE"
echo "=========================================="

echo "[1/5] Downloading RFSD data..."
python scripts/download_rfsd_data.py

echo ""
echo "[2/5] Preprocessing RFSD data..."
python scripts/rfsd_setting.py

echo ""
echo "[3/5] EDA and clustering..."
python scripts/rfsd_nsd_eda.py

echo ""
echo "[4/5] Anomaly analysis..."
python scripts/cluster_analysing.py

echo ""
echo "[5/5] Additional visualizations..."
python scripts/advanced_visualization.py

echo ""
echo "=========================================="
echo "PIPELINE COMPLETED SUCCESSFULLY"
echo "=========================================="
echo ""
echo "Results available in:"
echo "  - data/ (processed data)"
echo "  - results/ (visualizations and CSV)"
