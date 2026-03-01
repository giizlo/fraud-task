DROP TABLE IF EXISTS companies CASCADE;

CREATE TABLE companies (
    inn VARCHAR(12) PRIMARY KEY,
    ogrn VARCHAR(15),
    region VARCHAR(100) NOT NULL,
    oktmo VARCHAR(11),
    lat NUMERIC(10, 6),
    lon NUMERIC(10, 6),
    creation_date DATE,
    okved VARCHAR(20),
    sanctions_window BOOLEAN DEFAULT FALSE,
    cluster_size INTEGER DEFAULT 0,
    cluster_flag BOOLEAN DEFAULT FALSE,
    high_risk BOOLEAN DEFAULT FALSE
);

COMMENT ON TABLE companies IS 'Реестр компаний с признаками риска для fraud/audit анализа';
COMMENT ON COLUMN companies.inn IS 'ИНН компании (первичный ключ)';
COMMENT ON COLUMN companies.ogrn IS 'ОГРН компании';
COMMENT ON COLUMN companies.region IS 'Регион регистрации';
COMMENT ON COLUMN companies.oktmo IS 'Код ОКТМО муниципального образования';
COMMENT ON COLUMN companies.lat IS 'Широта (координаты)';
COMMENT ON COLUMN companies.lon IS 'Долгота (координаты)';
COMMENT ON COLUMN companies.creation_date IS 'Дата создания компании';
COMMENT ON COLUMN companies.okved IS 'Основной ОКВЭД';
COMMENT ON COLUMN companies.sanctions_window IS 'Флаг: компания создана в период пика санкций';
COMMENT ON COLUMN companies.cluster_size IS 'Количество компаний в том же кластере';
COMMENT ON COLUMN companies.cluster_flag IS 'Флаг массового кластера (>30 компаний)';
COMMENT ON COLUMN companies.high_risk IS 'Комбинированный флаг высокого риска (sanctions_window + cluster_flag)';

CREATE INDEX idx_companies_high_risk ON companies(high_risk) WHERE high_risk = TRUE;
CREATE INDEX idx_companies_region ON companies(region);
CREATE INDEX idx_companies_creation_date ON companies(creation_date);
CREATE INDEX idx_companies_cluster_flag ON companies(cluster_flag) WHERE cluster_flag = TRUE;
CREATE INDEX idx_companies_okved ON companies(okved);

DROP TABLE IF EXISTS clusters CASCADE;

CREATE TABLE clusters (
    region VARCHAR(100) NOT NULL,
    oktmo VARCHAR(11) NOT NULL,
    lat_round NUMERIC(10, 3) NOT NULL,
    lon_round NUMERIC(10, 3) NOT NULL,
    companies_count INTEGER DEFAULT 0,
    high_risk_companies INTEGER DEFAULT 0,
    PRIMARY KEY (region, oktmo, lat_round, lon_round)
);

COMMENT ON TABLE clusters IS 'Географические кластеры массовой регистрации компаний';
COMMENT ON COLUMN clusters.region IS 'Регион кластера';
COMMENT ON COLUMN clusters.oktmo IS 'Код ОКТМО';
COMMENT ON COLUMN clusters.lat_round IS 'Широта (округленная до 3 знаков ~100м)';
COMMENT ON COLUMN clusters.lon_round IS 'Долгота (округленная до 3 знаков ~100м)';
COMMENT ON COLUMN clusters.companies_count IS 'Количество компаний в кластере';
COMMENT ON COLUMN clusters.high_risk_companies IS 'Количество high-risk компаний в кластере';

CREATE INDEX idx_clusters_companies_count ON clusters(companies_count DESC);
CREATE INDEX idx_clusters_high_risk ON clusters(high_risk_companies DESC) WHERE high_risk_companies > 0;

DROP TABLE IF EXISTS audit_log CASCADE;

CREATE TABLE audit_log (
    id SERIAL PRIMARY KEY,
    inn VARCHAR(12) REFERENCES companies(inn),
    auditor_name VARCHAR(100),
    check_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    risk_level VARCHAR(20),
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE audit_log IS 'Лог аудиторских проверок компаний';
COMMENT ON COLUMN audit_log.risk_level IS 'Результат проверки: confirmed/false_positive/pending';

CREATE INDEX idx_audit_log_inn ON audit_log(inn);
CREATE INDEX idx_audit_log_date ON audit_log(check_date);

CREATE OR REPLACE VIEW v_region_summary AS
SELECT 
    region,
    COUNT(*) AS total_companies,
    SUM(CASE WHEN high_risk THEN 1 ELSE 0 END) AS high_risk_count,
    ROUND(
        SUM(CASE WHEN high_risk THEN 1 ELSE 0 END)::numeric / 
        NULLIF(COUNT(*), 0) * 100, 
        2
    ) AS high_risk_pct,
    COUNT(DISTINCT CASE WHEN cluster_flag THEN oktmo END) AS mass_clusters
FROM companies
GROUP BY region
ORDER BY high_risk_count DESC;

COMMENT ON VIEW v_region_summary IS 'Сводная статистика по регионам';

CREATE OR REPLACE VIEW v_high_risk_clusters AS
SELECT 
    c.*,
    ROUND(
        high_risk_companies::numeric / 
        NULLIF(companies_count, 0) * 100, 
        1
    ) AS risk_pct
FROM clusters c
WHERE high_risk_companies > 0
ORDER BY risk_pct DESC;

COMMENT ON VIEW v_high_risk_clusters IS 'Кластеры с высокой долей high-risk компаний';

CREATE OR REPLACE FUNCTION calculate_iqr_threshold(table_name text, column_name text)
RETURNS numeric AS $$
DECLARE
    q1 numeric;
    q3 numeric;
    iqr numeric;
    threshold numeric;
BEGIN
    EXECUTE format(
        'SELECT percentile_cont(0.25) WITHIN GROUP (ORDER BY %I),
                percentile_cont(0.75) WITHIN GROUP (ORDER BY %I)
         FROM %I 
         WHERE %I >= 2',
        column_name, column_name, table_name, column_name
    ) INTO q1, q3;
    
    iqr := q3 - q1;
    threshold := q3 + 1.5 * iqr;
    
    RETURN threshold;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION calculate_iqr_threshold IS 'Расчет IQR-порога для выявления аномалий';

SELECT 
    table_name,
    column_name,
    data_type,
    is_nullable
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name IN ('companies', 'clusters', 'audit_log')
ORDER BY table_name, ordinal_position;
