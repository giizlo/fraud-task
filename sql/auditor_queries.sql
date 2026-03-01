\echo ''
\echo '============================================================'
\echo 'AUDITOR QUERIES EXECUTION'
\echo '============================================================'
\echo ''

\echo 'Query 1: Clusters with highest risk percentage'
\echo '----------------------------------------------'

SELECT 
    region,
    oktmo,
    companies_count,
    high_risk_companies,
    ROUND(
        high_risk_companies::numeric / 
        NULLIF(companies_count, 0) * 100, 
        1
    ) AS risk_pct
FROM clusters 
WHERE high_risk_companies > 5
ORDER BY risk_pct DESC 
LIMIT 20;

\echo ''
\echo 'Query 2: High-risk companies for detailed review'
\echo '------------------------------------------------'

SELECT 
    inn,
    ogrn,
    region,
    okved,
    creation_date,
    cluster_size,
    lat,
    lon
FROM companies 
WHERE high_risk = TRUE
ORDER BY cluster_size DESC 
LIMIT 100;

\echo ''
\echo 'Query 3: Risk distribution by region'
\echo '------------------------------------'

SELECT 
    region, 
    COUNT(*) AS suspicious_count,
    ROUND(
        COUNT(*)::numeric / 
        SUM(COUNT(*)) OVER () * 100, 
        2
    ) AS pct_of_total
FROM companies 
WHERE high_risk = TRUE
GROUP BY region
HAVING COUNT(*) > 10
ORDER BY suspicious_count DESC;

\echo ''
\echo 'Query 4: Companies with cluster details (JOIN)'
\echo '-----------------------------------------------'

SELECT 
    c.inn,
    c.region,
    c.okved,
    c.creation_date,
    c.high_risk,
    cl.companies_count AS cluster_total,
    cl.high_risk_companies AS cluster_high_risk,
    ROUND(
        cl.high_risk_companies::numeric / 
        NULLIF(cl.companies_count, 0) * 100, 
        1
    ) AS cluster_risk_pct
FROM companies c
JOIN clusters cl 
    ON c.region = cl.region 
    AND c.oktmo = cl.oktmo
    AND ROUND(c.lat::numeric, 3) = cl.lat_round
    AND ROUND(c.lon::numeric, 3) = cl.lon_round
WHERE c.high_risk = TRUE
ORDER BY cl.companies_count DESC
LIMIT 50;

\echo ''
\echo 'Query 5: Risk analysis by OKVED (industry)'
\echo '------------------------------------------'

SELECT 
    SUBSTRING(okved FROM 1 FOR 2) AS okved_section,
    COUNT(*) AS total_companies,
    SUM(CASE WHEN high_risk THEN 1 ELSE 0 END) AS high_risk_count,
    ROUND(
        SUM(CASE WHEN high_risk THEN 1 ELSE 0 END)::numeric / 
        NULLIF(COUNT(*), 0) * 100, 
        2
    ) AS risk_rate_pct
FROM companies
WHERE okved IS NOT NULL
GROUP BY SUBSTRING(okved FROM 1 FOR 2)
HAVING COUNT(*) > 50
ORDER BY risk_rate_pct DESC
LIMIT 15;

\echo ''
\echo 'Query 6: Monthly registration patterns'
\echo '--------------------------------------'

SELECT 
    DATE_TRUNC('month', creation_date) AS month,
    COUNT(*) AS new_companies,
    SUM(CASE WHEN high_risk THEN 1 ELSE 0 END) AS high_risk_new,
    SUM(CASE WHEN sanctions_window THEN 1 ELSE 0 END) AS in_sanctions_window
FROM companies
WHERE creation_date >= '2020-01-01'
GROUP BY DATE_TRUNC('month', creation_date)
HAVING COUNT(*) > 100
ORDER BY month;

\echo ''
\echo 'Query 7: Mass registration addresses (>100 companies)'
\echo '-----------------------------------------------------'

SELECT 
    region,
    oktmo,
    lat_round,
    lon_round,
    companies_count,
    high_risk_companies,
    ROUND(
        high_risk_companies::numeric / 
        NULLIF(companies_count, 0) * 100, 
        1
    ) AS risk_pct,
    CASE 
        WHEN companies_count > 500 THEN 'CRITICAL'
        WHEN companies_count > 100 THEN 'HIGH'
        ELSE 'MEDIUM'
    END AS risk_level
FROM clusters
WHERE companies_count > 100
ORDER BY companies_count DESC
LIMIT 20;

\echo ''
\echo 'Query 8: Cluster size distribution statistics'
\echo '---------------------------------------------'

SELECT 
    CASE 
        WHEN companies_count = 1 THEN 'Single (1)'
        WHEN companies_count BETWEEN 2 AND 5 THEN 'Small (2-5)'
        WHEN companies_count BETWEEN 6 AND 20 THEN 'Medium (6-20)'
        WHEN companies_count BETWEEN 21 AND 100 THEN 'Large (21-100)'
        ELSE 'Mass (>100)'
    END AS size_category,
    COUNT(*) AS cluster_count,
    SUM(companies_count) AS total_companies,
    SUM(high_risk_companies) AS total_high_risk,
    ROUND(AVG(companies_count), 1) AS avg_size
FROM clusters
GROUP BY 
    CASE 
        WHEN companies_count = 1 THEN 'Single (1)'
        WHEN companies_count BETWEEN 2 AND 5 THEN 'Small (2-5)'
        WHEN companies_count BETWEEN 6 AND 20 THEN 'Medium (6-20)'
        WHEN companies_count BETWEEN 21 AND 100 THEN 'Large (21-100)'
        ELSE 'Mass (>100)'
    END
ORDER BY MIN(companies_count);

\echo ''
\echo 'Query 9: Region summary (using VIEW)'
\echo '------------------------------------'

SELECT * FROM v_region_summary LIMIT 10;

\echo ''
\echo 'Query 10: Sample company lookup (template)'
\echo '------------------------------------------'
\echo 'To check a specific INN, run:'
\echo ''
\echo 'SELECT * FROM companies WHERE inn = ''7707083893'';'
\echo ''

\echo ''
\echo '============================================================'
\echo 'SUMMARY STATISTICS'
\echo '============================================================'
\echo ''

SELECT 
    'Total companies' AS metric,
    COUNT(*)::text AS value
FROM companies
UNION ALL
SELECT 
    'High-risk companies',
    COUNT(*)::text
FROM companies WHERE high_risk = TRUE
UNION ALL
SELECT 
    'Total clusters',
    COUNT(*)::text
FROM clusters
UNION ALL
SELECT 
    'Mass clusters (>30)',
    COUNT(*)::text
FROM clusters WHERE companies_count > 30
UNION ALL
SELECT 
    'Critical clusters (>100)',
    COUNT(*)::text
FROM clusters WHERE companies_count > 100;

\echo ''
\echo '============================================================'
\echo 'ALL QUERIES EXECUTED SUCCESSFULLY'
\echo '============================================================'
