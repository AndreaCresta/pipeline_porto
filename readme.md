# Sviluppo di una pipeline ETL automatizzata per il monitoraggio dei dati AIS nel traffico marittimo del Porto di Genova

Questo repository contiene l'infrastruttura, il codice sorgente e la documentazione del mio progetto di tesi nel campo del Data Engineering. Il sistema è progettato per intercettare, trasformare e storicizzare in tempo reale i dati del traffico marittimo (AIS) nei principali terminal della Liguria, con l'obiettivo di calcolare KPI logistici avanzati (tempi di attesa, congestione, overstay).

## Stack Tecnologico
* **Infrastruttura:** Docker, Docker Compose
* **Database:** PostgreSQL 18
* **ETL & Scripting:** Python 3.12 (psycopg2, websockets, asyncio)
* **Amministrazione DB:** pgAdmin 4
* **Data Visualization & BI:** Microsoft Power BI

---

## Struttura del Progetto
```text
/
├── docker-compose.yml       # Definizione dell'infrastruttura containerizzata
├── schema.sql               # DDL per la creazione della tabella di staging
├── ingestion_pipeline.py    # Script Python principale per l'ETL in tempo reale
└── README.md                # Documentazione di progetto
```

## Guida all'Installazione e Avvio Rapido

Per replicare l'ambiente di sviluppo in locale, seguire questi passaggi:

1. **Avvio Infrastruttura Docker**
   Avviare il demone Docker e lanciare i container in background:
   ```bash
   docker-compose up -d
   ```

2. **Creazione Tabella**
   Accedere a pgAdmin (`http://localhost:8080`), registrare il server puntando all'host `db_tesi` ed eseguire lo script `schema.sql` all'interno del database.

3. **Setup Ambiente Python (macOS PEP 668)**
   Per rispettare le restrizioni di sicurezza su macOS, ho isolato le dipendenze in un ambiente virtuale:
   ```bash
   python3 -m venv tesi_env
   source tesi_env/bin/activate
   pip install websockets psycopg2-binary
   ```

4. **Avvio Ingestion**
   Lanciare l'ascolto in tempo reale:
   ```bash
   python3 ingestion_pipeline.py
   ```

---

## Fase 1: Data Ingestion e Setup Infrastrutturale

In questa prima fase ho progettato e implementato l'intera pipeline di acquisizione dati (Ingestion), partendo dall'infrastruttura fino alla scrittura in database.

### 1. Infrastruttura Containerizzata
Ho scelto di containerizzare l'ambiente di database tramite Docker per garantire la totale riproducibilità del sistema, isolandolo dal sistema operativo host. Ho sviluppato il seguente `docker-compose.yml`:

```yaml
version: '3.8'

services:
  db_tesi:
    image: postgres:18
    container_name: postgres_porto
    environment:
      POSTGRES_USER: admin_tesi
      POSTGRES_PASSWORD: password_sicura
      POSTGRES_DB: logistica_liguria
    ports:
      - "5432:5432"
    volumes:
      - dati_porto:/var/lib/postgresql 

  pgadmin:
    image: dpage/pgadmin4
    container_name: pgadmin_interfaccia
    environment:
      PGADMIN_DEFAULT_EMAIL: studente@tesi.it
      PGADMIN_DEFAULT_PASSWORD: admin
    ports:
      - "8080:80"
    depends_on:
      - db_tesi

volumes:
  dati_porto:
```
*Nota tecnica:* Ho adottato PostgreSQL 18 per sfruttare le recenti performance di I/O asincrono. Questo ha richiesto la mappatura del volume direttamente su `/var/lib/postgresql` per mantenere la compatibilità con il nuovo sistema di gestione dei metadati della v18.

### 2. Data Definition Language (DDL)
Ho modellato la tabella di atterraggio (Staging) per accogliere i dati grezzi in tempo reale. Ho tipizzato le coordinate con `DECIMAL(9,6)` per ottenere una tolleranza spaziale di circa 11 cm, un requisito stringente per le successive logiche di geofencing al molo.

```sql
CREATE TABLE staging_ais_data (
    mmsi VARCHAR(20),
    ship_name TEXT,
    terminal_zona TEXT,
    lat DECIMAL(9,6),
    lon DECIMAL(9,6),
    timestamp_utc TIMESTAMP
);
```

### 3. Pipeline ETL in Python
Ho sviluppato lo script `ingestion_pipeline.py` per connettersi in streaming WebSocket all'API di AISStream. Lo script esegue le seguenti operazioni in volo (In-Flight Processing):

1. **Estrazione e Filtraggio:** Richiede solo i messaggi di tipo "PositionReport" all'interno di specifiche Bounding Boxes (Genova e Vado Ligure).
2. **Trasformazione (Geofencing):** Valuta latitudine e longitudine in real-time, assegnando un'etichetta di zona (es. `GENOVA_VOLTRI`, `VADO_GATEWAY`) tramite poligoni predefiniti.
3. **Data Cleansing:** Intercetta e normalizza i timestamp. Ho implementato un blocco che tronca i nanosecondi forniti dall'API (`.split('.')[0]`) per renderli compatibili con lo standard ISO richiesto da PostgreSQL.
4. **Caricamento Sicuro:** Scrive i record nel container Postgres utilizzando query SQL parametrizzate tramite `psycopg2` per prevenire vulnerabilità di injection.

```python
import asyncio
import websockets
import json
import psycopg2

DB_CONFIG = {
    "host": "localhost",
    "port": "5432",
    "database": "logistica_liguria",
    "user": "admin_tesi",
    "password": "password_sicura"
}

def identifica_terminal(lat, lon):
    if 44.420 <= lat <= 44.435 and 8.740 <= lon <= 8.785:
        return "GENOVA_VOLTRI"
    elif 44.260 <= lat <= 44.285 and 8.430 <= lon <= 8.460:
        return "VADO_GATEWAY"
    elif 44.395 <= lat <= 44.415 and 8.870 <= lon <= 8.910:
        return "GENOVA_SAMPIERDARENA"
    return "ALTRO_LIGURIA"

async def get_port_data():
    api_key = "INSERIRE_API_KEY"
    url = "wss://stream.aisstream.io/v0/stream"

    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    async with websockets.connect(url) as websocket:
        sub_msg = {
            "APIKey": api_key,
            "BoundingBoxes": [[[44.38, 8.70], [44.45, 8.95]], [[44.24, 8.40], [44.30, 8.50]]],
            "FilterMessageTypes": ["PositionReport"]
        }
        await websocket.send(json.dumps(sub_msg))

        async for message in websocket:
            data = json.loads(message)
            meta = data['MetaData']
            
            # Normalizzazione del timestamp per compatibilità con PostgreSQL 18
            clean_time = meta['time_utc'].split('.')[0]
            zona = identifica_terminal(meta['latitude'], meta['longitude'])

            query = """INSERT INTO staging_ais_data (mmsi, ship_name, terminal_zona, lat, lon, timestamp_utc) 
                       VALUES (%s, %s, %s, %s, %s, %s)"""
            cursor.execute(query, (meta['MMSI'], meta['ShipName'], zona, meta['latitude'], meta['longitude'], clean_time))
            conn.commit()
            print(f"Salvato: [{zona}] {meta['ShipName']}")

asyncio.run(get_port_data())
```

<img width="2463" height="1311" alt="stagin_ais_data" src="https://github.com/user-attachments/assets/df8d481b-aa13-48a8-a72a-83ac67152473" />


---

## Fase 2: Processing & Data Modeling

L'obiettivo di questa fase è la trasformazione del dato "grezzo" (Raw Data) in "informazione strutturata" (Analytics-Ready Data) per rispondere ai requisiti logistici della tesi. In questa fase, il sistema evolve da una singola tabella di atterraggio a uno **Star Schema** ottimizzato per il calcolo dei KPI.

### 1. Ottimizzazione e Performance (Indexing)
Per garantire la scalabilità della pipeline e gestire migliaia di record AIS in tempo reale, ho implementato indici specializzati sulla tabella di staging:

* **`idx_ais_timestamp`**: Indice B-Tree sulla colonna `timestamp_utc` per velocizzare le query di ordinamento temporale e il partizionamento logico dei dati.
* **`idx_ais_mmsi`**: Indice per ottimizzare il raggruppamento e il filtraggio dei messaggi appartenenti alla medesima unità navale.
* **`idx_ais_mmsi_time`**: Indice composito (`mmsi`, `timestamp_utc DESC`) progettato specificamente per le query di ricostruzione della rotta, riducendo drasticamente i tempi di esecuzione per la ricerca dell'ultima posizione nota.

```sql

CREATE INDEX idx_ais_timestamp ON staging_ais_data (timestamp_utc);


CREATE INDEX idx_ais_mmsi ON staging_ais_data (mmsi);


CREATE INDEX idx_ais_mmsi_time ON staging_ais_data (mmsi, timestamp_utc DESC);
```

### 2. Architettura Star Schema (Data Warehouse Design)

Per trasformare il flusso continuo di dati AIS in metriche logistiche interrogabili, il processo di Data Engineering è stato strutturato attorno a un modello a stella (Star Schema). Questo approccio garantisce l'integrità del dato, elimina le ridondanze e ottimizza le performance del database per le analisi logistico-portuali.

#### 2.1 Le Tabelle del Modello
L'architettura separa rigorosamente i fatti (eventi dinamici) dalle dimensioni (anagrafiche e dati di contesto):

* **Tabelle Dimensione (Anagrafiche):**
  * **`dim_navi`**: Memorizza i dati statici delle navi, come il codice MMSI e il nome (Ship Name). Risolve il problema della ridondanza presente nello staging, dove il nome della nave viene inutilmente ripetuto per ogni singola coordinata inviata.

```sql

CREATE TABLE dim_navi (
    mmsi VARCHAR(20) PRIMARY KEY,
    ship_name TEXT,
    data_inserimento TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


INSERT INTO dim_navi (mmsi, ship_name)
SELECT DISTINCT mmsi, ship_name
FROM staging_ais_data
WHERE mmsi IS NOT NULL
ON CONFLICT (mmsi) DO NOTHING;
```  

  * **`dim_terminal`**: Contiene la definizione geografica (poligoni di Geofencing) dei terminal monitorati, quali Genova Voltri (PSA Pra'), Genova Sampierdarena e Vado Gateway. Rende le interrogazioni geografiche indipendenti dal codice applicativo Python.

```sql

CREATE TABLE dim_terminal (
    codice_zona VARCHAR(50) PRIMARY KEY,
    nome_esteso TEXT,
    citta TEXT
);


INSERT INTO dim_terminal (codice_zona, nome_esteso, citta) VALUES
('GENOVA_VOLTRI', 'PSA Pra''', 'Genova'),
('GENOVA_SAMPIERDARENA', 'Terminal Sampierdarena', 'Genova'),
('VADO_GATEWAY', 'Vado Gateway Terminal', 'Savona'),
('ALTRO_LIGURIA', 'Mar Ligure (In Transito)', 'N/A')
ON CONFLICT (codice_zona) DO NOTHING;
```  

  * **`dim_tempo`**: Gerarchia temporale (Ora, Giorno, Mese) pianificata per le analisi aggregate, essenziale per identificare pattern ciclici di congestione.

```sql

CREATE TABLE dim_tempo (
    data_id DATE PRIMARY KEY,
    anno INT,
    mese INT,
    giorno INT,
    nome_giorno VARCHAR(20),
    is_weekend BOOLEAN
);


INSERT INTO dim_tempo (data_id, anno, mese, giorno, nome_giorno, is_weekend)
SELECT 
    datum AS data_id,
    EXTRACT(YEAR FROM datum) AS anno,
    EXTRACT(MONTH FROM datum) AS mese,
    EXTRACT(DAY FROM datum) AS giorno,
    TO_CHAR(datum, 'Day') AS nome_giorno,
    EXTRACT(ISODOW FROM datum) IN (6, 7) AS is_weekend
FROM generate_series('2024-01-01'::DATE, '2026-12-31'::DATE, '1 day'::interval) AS datum
ON CONFLICT (data_id) DO NOTHING;
```  


* **Tabella dei Fatti (Eventi):**
  * **`fact_movimenti`**: È il cuore del sistema analitico. Registra esclusivamente gli eventi di "Ingresso" e "Uscita" dalle aree terminal, relazionando l'ID della nave (`mmsi`), l'ID del terminal e due timestamp cruciali: `orario_arrivo` e `orario_partenza`.

```sql

CREATE TABLE fact_movimenti (
    id_movimento SERIAL PRIMARY KEY,
    mmsi VARCHAR(20) REFERENCES dim_navi(mmsi),
    codice_zona VARCHAR(50) REFERENCES dim_terminal(codice_zona),
    orario_arrivo TIMESTAMP,
    orario_partenza TIMESTAMP,
    data_elaborazione TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

```sql

-- Estrazione degli eventi logistici e inserimento nella Fact Table
INSERT INTO fact_movimenti (mmsi, codice_zona, orario_arrivo, orario_partenza)
WITH cambi_stato AS (
    -- Step A: Affianchiamo a ogni record la zona in cui si trovava la nave nel record precedente
    SELECT 
        mmsi,
        terminal_zona,
        timestamp_utc,
        LAG(terminal_zona) OVER (PARTITION BY mmsi ORDER BY timestamp_utc) as zona_precedente
    FROM staging_ais_data
    WHERE mmsi IS NOT NULL
),
arrivi_partenze AS (
    -- Step B: Teniamo solo i momenti in cui la zona CAMBIA (è un Arrivo).
    -- Usiamo LEAD per cercare il timestamp del prossimo cambio di zona (che sarà la Partenza).
    SELECT 
        mmsi,
        terminal_zona,
        timestamp_utc AS orario_arrivo,
        LEAD(timestamp_utc) OVER (PARTITION BY mmsi ORDER BY timestamp_utc) AS orario_partenza
    FROM cambi_stato
    WHERE terminal_zona IS DISTINCT FROM zona_precedente
)
-- Step C: Filtriamo e salviamo solo gli scali reali nei porti (ignorando il transito in mare aperto)
SELECT 
    mmsi,
    terminal_zona AS codice_zona,
    orario_arrivo,
    orario_partenza
FROM arrivi_partenze
WHERE terminal_zona != 'ALTRO_LIGURIA' 
  AND orario_partenza IS NOT NULL;

```

### 3. Logica di Business e KPI Logistici

La modellazione e le pipeline SQL sono state ingegnerizzate per estrarre tre livelli di metriche fondamentali per l'analisi delle performance e la gestione portuale:

#### 3.1. Analisi dei Tempi di Ciclo (Turnaround Time)
Il ciclo logistico della nave viene frammentato e calcolato in due fasi distinte per isolare le inefficienze:
* **Time in Rada (Waiting Time):** Misura il tempo che la nave trascorre nell'area di ancoraggio (identificata come transito o attesa nel Mar Ligure) prima di ricevere l'autorizzazione all'ormeggio. Un valore medio alto in questo KPI è il principale indicatore di congestione del terminal.

```sql

-- Query di controllo per identificare i tempi di attesa medi e i picchi di congestione
SELECT 
    ship_name, 
    orario_inizio_rada, 
    orario_fine_rada, 
    ore_in_rada 
FROM vw_kpi_tempi_rada
WHERE ore_in_rada > 1.0  -- Filtriamo solo le attese significative superiori a un'ora
ORDER BY ore_in_rada DESC;

```

* **Vista SQL per il calcolo del Time in Rada (Attesa in mare):**
  Questa vista riutilizza la logica delle Window Functions per isolare i periodi di transito o attesa fuori dai terminal logistici, calcolando le ore di permanenza nella zona "ALTRO_LIGURIA".

```sql
-- Creazione della Vista per il calcolo del Time in Rada (Attesa)
CREATE OR REPLACE VIEW vw_kpi_tempi_rada AS
WITH cambi_stato AS (
    SELECT 
        mmsi,
        terminal_zona,
        timestamp_utc,
        LAG(terminal_zona) OVER (PARTITION BY mmsi ORDER BY timestamp_utc) as zona_precedente
    FROM staging_ais_data
    WHERE mmsi IS NOT NULL
),
arrivi_partenze_rada AS (
    SELECT 
        mmsi,
        terminal_zona,
        timestamp_utc AS orario_inizio_rada,
        LEAD(timestamp_utc) OVER (PARTITION BY mmsi ORDER BY timestamp_utc) AS orario_fine_rada
    FROM cambi_stato
    WHERE terminal_zona IS DISTINCT FROM zona_precedente
)
SELECT 
    r.mmsi,
    n.ship_name,
    r.orario_inizio_rada,
    r.orario_fine_rada,
    ROUND(EXTRACT(EPOCH FROM (r.orario_fine_rada - r.orario_inizio_rada)) / 3600, 2) AS ore_in_rada
FROM arrivi_partenze_rada r
LEFT JOIN dim_navi n ON r.mmsi = n.mmsi
WHERE r.terminal_zona = 'ALTRO_LIGURIA' 
  AND r.orario_fine_rada IS NOT NULL;
```

* **Time in Port (Dwell Time):** Calcolato come differenza matematica tra il timestamp di uscita e quello di entrata dalla zona di geofencing del terminal (`orario_partenza - orario_arrivo`). Rappresenta il tempo effettivo di operatività per il carico/scarico container.

```sql

-- Creazione della Vista per il calcolo del Time in Port e Overstay
CREATE OR REPLACE VIEW vw_kpi_tempi_porto AS
SELECT 
    m.id_movimento,
    m.mmsi,
    n.ship_name,
    t.nome_esteso AS terminal,
    m.orario_arrivo,
    m.orario_partenza,
    -- 1. Calcolo della permanenza esatta (Giorni e Ore)
    (m.orario_partenza - m.orario_arrivo) AS permanenza_totale,
    -- 2. Calcolo della permanenza in ore decimali (Perfetto per i grafici di Power BI)
    ROUND(EXTRACT(EPOCH FROM (m.orario_partenza - m.orario_arrivo)) / 3600, 2) AS ore_in_porto,
    -- 3. Identificazione Overstay (Flag Vero/Falso se la sosta supera le 72 ore)
    CASE 
        WHEN EXTRACT(EPOCH FROM (m.orario_partenza - m.orario_arrivo)) / 3600 > 72 THEN TRUE 
        ELSE FALSE 
    END AS flag_overstay
FROM fact_movimenti m
LEFT JOIN dim_navi n ON m.mmsi = n.mmsi
LEFT JOIN dim_terminal t ON m.codice_zona = t.codice_zona;

```

#### 3.2. Identificazione Anomalie e Overstay
Tramite viste SQL materializzate, il sistema filtra automaticamente i dati per far emergere i casi critici (Outliers):
* **Overstay al Molo:** Identificazione delle navi che superano le soglie standard di permanenza (es. > 72 ore al Vado Gateway), segnalando possibili guasti, ispezioni doganali o inefficienze nelle operazioni di piazzale.

```sql

-- Vista dedicata esclusivamente all'analisi delle anomalie (Overstay > 72h)
CREATE OR REPLACE VIEW vw_analisi_overstay AS
SELECT 
    ship_name,
    terminal,
    orario_arrivo,
    orario_partenza,
    permanenza_totale,
    ore_in_porto
FROM vw_kpi_tempi_porto
WHERE flag_overstay = TRUE
ORDER BY ore_in_porto DESC;

```

* **Colli di Bottiglia Infrastrutturali:** Mappatura delle zone (es. Voltri vs. Sampierdarena) con i più alti tempi di attesa medi, fornendo dati cruciali per l'ottimizzazione dei flussi.

```sql

-- Vista per il confronto delle performance tra i terminal (Bottlenecks)
CREATE OR REPLACE VIEW vw_kpi_confronto_terminal AS
SELECT 
    terminal,
    COUNT(id_movimento) as numero_scali,
    ROUND(AVG(ore_in_porto), 2) as media_ore_permanenza,
    SUM(CASE WHEN flag_overstay THEN 1 ELSE 0 END) as totale_overstay
FROM vw_kpi_tempi_porto
GROUP BY terminal;

```

#### 3.3. Data Cleansing e Integrità
Per garantire l'affidabilità dei KPI, sono state automatizzate procedure di pulizia del dato a livello di database:
* Rimozione dei "rimbalzi GPS" (Ghost Ping) e delle coordinate outlier generate da errori di trasmissione dell'antenna AIS.
* Deduplicazione tecnica degli eventi per assicurare che ogni scalo nave generi un singolo record fattuale nella tabella `fact_movimenti`.

```sql

-- 1. Rimozione record incompleti (coordinate mancanti)
DELETE FROM staging_ais_data
WHERE lat IS NULL 
   OR lon IS NULL;

-- 2. Deduplicazione tecnica
-- Rimuove i messaggi identici inviati dalla stessa nave nello stesso istante
DELETE FROM staging_ais_data a
USING staging_ais_data b
WHERE a.ctid < b.ctid 
  AND a.mmsi = b.mmsi 
  AND a.timestamp_utc = b.timestamp_utc;

```

---

## Fasi Successive del Progetto

* [ ] **Fase 3: Orchestrazione e Automazione (Apache Airflow)**
  * *Pianificato:* Schedulazione dei processi ETL batch.
* [ ] **Fase 4: Data Visualization (Power BI)**
  * *Pianificato:* Sviluppo dashboard interattiva aziendale con metriche di Congestione e allarmi di Overstay.
