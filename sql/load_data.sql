TRUNCATE TABLE audit_log CASCADE;
TRUNCATE TABLE companies CASCADE;
TRUNCATE TABLE clusters CASCADE;

ALTER SEQUENCE audit_log_id_seq RESTART WITH 1;

\echo 'Loading companies data...'

COPY companies (
    inn,
    ogrn,
    region,
    creation_date,
    okved,
    oktmo,
    lat,
    lon,
    sanctions_window,
    cluster_size,
    cluster_flag,
    high_risk
)
FROM '../data/companies_for_sql.csv'
WITH (
    FORMAT csv,
    HEADER true,
    DELIMITER ',',
    NULL '',
    ENCODING 'UTF8'
);

\echo 'Companies loaded successfully'

\echo 'Loading clusters data...'

COPY clusters (
    region,
    oktmo,
    lat_round,
    lon_round,
    companies_count,
    high_risk_companies
)
FROM '../data/clusters_for_sql.csv'
WITH (
    FORMAT csv,
    HEADER true,
    DELIMITER ',',
    NULL '',
    ENCODING 'UTF8'
);

\echo 'Clusters loaded successfully'

\echo ''
\echo '=== DATA LOAD SUMMARY ==='
\echo ''

SELECT 
    'companies' AS table_name,
    COUNT(*) AS row_count,
    COUNT(DISTINCT region) AS unique_regions
FROM companies
UNION ALL
SELECT 
    'clusters' AS table_name,
    COUNT(*) AS row_count,
    COUNT(DISTINCT region) AS unique_regions
FROM clusters;

\echo ''
\echo 'High-risk statistics:'
SELECT 
    COUNT(*) FILTER (WHERE high_risk = TRUE) AS high_risk_companies,
    COUNT(*) FILTER (WHERE cluster_flag = TRUE) AS in_mass_clusters,
    COUNT(*) FILTER (WHERE sanctions_window = TRUE) AS in_sanctions_window
FROM companies;

\echo ''
\echo 'Top 5 regions by company count:'
SELECT 
    region,
    COUNT(*) AS company_count,
    SUM(CASE WHEN high_risk THEN 1 ELSE 0 END) AS high_risk_count
FROM companies
GROUP BY region
ORDER BY company_count DESC
LIMIT 5;

\echo ''
\echo 'Top 5 clusters by size:'
SELECT 
    region,
    oktmo,
    companies_count,
    high_risk_companies
FROM clusters
ORDER BY companies_count DESC
LIMIT 5;

\echo ''
\echo '=== DATA LOAD COMPLETED ==='
