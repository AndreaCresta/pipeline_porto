-- ==========================================
-- FASE 1: STAGING (Atterraggio Dati Grezzi)
-- ==========================================

-- Tabella madre logica per il partizionamento mensile
CREATE TABLE IF NOT EXISTS staging_ais_data (
    mmsi VARCHAR(20),
    ship_name TEXT,
    terminal_zona TEXT,
    lat DECIMAL(9,6),
    lon DECIMAL(9,6),
    timestamp_utc TIMESTAMP
) PARTITION BY RANGE (timestamp_utc);

-- Partizione di default per i dati anomali (date future o troppo passate)
CREATE TABLE IF NOT EXISTS staging_ais_data_default 
    PARTITION OF staging_ais_data DEFAULT;

-- (Le partizioni mensili fisiche verranno create in automatico da Airflow)

-- Indice composito per velocizzare le query di Airflow (ricerca per nave e tempo)
CREATE INDEX IF NOT EXISTS idx_staging_mmsi_time 
ON staging_ais_data USING btree (mmsi, timestamp_utc);


-- ==========================================
-- FASE 2: DATA WAREHOUSE (Star Schema)
-- ==========================================

-- 1. Dimensioni (Anagrafiche)
CREATE TABLE IF NOT EXISTS dim_navi (
    mmsi VARCHAR(20) PRIMARY KEY,
    ship_name TEXT
);

CREATE TABLE IF NOT EXISTS dim_terminal (
    codice_zona VARCHAR(50) PRIMARY KEY,
    descrizione TEXT
);

-- Popolamento manuale della dimensione Terminal
INSERT INTO dim_terminal (codice_zona, descrizione) 
VALUES 
    ('GENOVA_VOLTRI', 'PSA Genova Pra'),
    ('VADO_GATEWAY', 'Vado Gateway APM'),
    ('GENOVA_SAMPIERDARENA', 'Terminal San Giorgio / SECH'),
    ('ALTRO_LIGURIA', 'In navigazione o in rada')
ON CONFLICT (codice_zona) DO NOTHING;

-- 2. Tabella dei Fatti (Eventi Logistici)
CREATE TABLE IF NOT EXISTS fact_movimenti (
    id_movimento SERIAL PRIMARY KEY,
    mmsi VARCHAR(20) REFERENCES dim_navi(mmsi),
    codice_zona VARCHAR(50) REFERENCES dim_terminal(codice_zona),
    orario_arrivo TIMESTAMP NOT NULL,
    orario_partenza TIMESTAMP,
    -- Impedisce di registrare due arrivi identici per la stessa nave
    CONSTRAINT uk_movimento_nave UNIQUE (mmsi, orario_arrivo)
);

-- Indice per velocizzare le JOIN tra fatti e dimensioni in Power BI
CREATE INDEX IF NOT EXISTS idx_fact_mmsi_zona 
ON fact_movimenti USING btree (mmsi, codice_zona);


-- ==========================================
-- FASE 3: BUSINESS INTELLIGENCE (KPI e Viste)
-- ==========================================

-- 1. Vista Monitoraggio Real-Time (Navi Presenti ORA)
CREATE OR REPLACE VIEW vw_navi_presenti_ora AS
SELECT 
    m.mmsi,
    n.ship_name,
    t.descrizione AS terminal,
    m.orario_arrivo,
    NOW() - m.orario_arrivo AS tempo_trascorso,
    EXTRACT(EPOCH FROM (NOW() - m.orario_arrivo))/3600 AS ore_trascorse
FROM fact_movimenti m
JOIN dim_navi n ON m.mmsi = n.mmsi
JOIN dim_terminal t ON m.codice_zona = t.codice_zona
WHERE m.orario_partenza IS NULL
  AND m.codice_zona != 'ALTRO_LIGURIA';

-- 2. Vista Materializzata: Tempi di Ciclo (Turnaround Time) e Overstay
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_kpi_tempi_porto AS
SELECT 
    m.mmsi,
    n.ship_name,
    t.descrizione AS terminal,
    m.orario_arrivo,
    m.orario_partenza,
    COALESCE(m.orario_partenza, NOW()) - m.orario_arrivo AS tempo_in_porto,
    EXTRACT(EPOCH FROM (COALESCE(m.orario_partenza, NOW()) - m.orario_arrivo))/3600 AS ore_in_porto,
    -- LOGICA OVERSTAY: Identifica se una nave sosta per più di 48 ore nel terminal
    CASE 
        WHEN EXTRACT(EPOCH FROM (COALESCE(m.orario_partenza, NOW()) - m.orario_arrivo))/3600 > 48 THEN TRUE
        ELSE FALSE
    END AS overstay_flag,
    CASE 
        WHEN m.orario_partenza IS NULL THEN 'IN PORTO'
        ELSE 'PARTITA'
    END AS stato_attuale
FROM fact_movimenti m
LEFT JOIN dim_navi n ON m.mmsi = n.mmsi
LEFT JOIN dim_terminal t ON m.codice_zona = t.codice_zona
WHERE m.codice_zona != 'ALTRO_LIGURIA';

-- 3. Vista dedicata alle Anomalie (Navi attualmente bloccate in Overstay)
CREATE OR REPLACE VIEW vw_anomalie_overstay AS
SELECT 
    mmsi, 
    ship_name, 
    terminal, 
    orario_arrivo, 
    ore_in_porto
FROM mv_kpi_tempi_porto
WHERE overstay_flag = TRUE 
  AND stato_attuale = 'IN PORTO';

-- 4. Vista Analisi Rada (Navi in attesa fuori dal porto)
CREATE OR REPLACE VIEW vw_kpi_rada AS
SELECT 
    m.mmsi,
    n.ship_name,
    m.orario_arrivo AS inizio_attesa_rada,
    COALESCE(m.orario_partenza, NOW()) - m.orario_arrivo AS tempo_in_rada
FROM fact_movimenti m
LEFT JOIN dim_navi n ON m.mmsi = n.mmsi
WHERE m.codice_zona = 'ALTRO_LIGURIA';