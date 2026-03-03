# Sviluppo di una pipeline ETL automatizzata per il monitoraggio dei dati AIS nel traffico marittimo del Porto di Genova

Questo repository contiene l'infrastruttura, il codice sorgente e la documentazione del mio progetto di Data Engineering. Il sistema è progettato per intercettare, trasformare e storicizzare in tempo reale i dati del traffico marittimo (AIS) nei principali terminal della Liguria, con l'obiettivo di calcolare KPI logistici avanzati (tempi di attesa, congestione, overstay).

## Stack Tecnologico
* **Infrastruttura:** Docker, Docker Compose
* **Database:** PostgreSQL 18
* **ETL & Scripting:** Python 3.12 (psycopg2, websockets, asyncio)
* **Amministrazione DB:** pgAdmin 4
* **Automazione/Orchestrazione:** Apache Airflow 2.8.1
* **Data Visualization & BI:** Microsoft Power BI

---
<details>
  <summary><b>📂 Clicca per visualizzare la struttura delle cartelle</b></summary>

## Struttura del Progetto
```text
/
├── docker-compose.yml       # Definizione dell'infrastruttura containerizzata
├── schema.sql               # DDL per la creazione della tabella di staging
├── ingestion_pipeline.py    # Script Python principale per l'ETL in tempo reale
└── README.md                # Documentazione di progetto
```
</details>

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
      POSTGRES_PASSWORD: [INSERISCI LA TUA PASSWORD]
      POSTGRES_DB: logistica_liguria
    ports:
      - "5432:5432"
    volumes:
      - dati_porto:/var/lib/postgresql/data

  pgadmin:
    image: dpage/pgadmin4
    container_name: pgadmin_interfaccia
    environment:
      PGADMIN_DEFAULT_EMAIL: studente@tesi.it
      PGADMIN_DEFAULT_PASSWORD: [INSERISCI LA TUA PASSWORD]
    ports:
      - "8080:80"
    depends_on:
      - db_tesi

volumes:
  dati_porto:
```
*Nota tecnica:* Ho adottato PostgreSQL 18 per sfruttare le recenti performance di I/O asincrono. Il volume è mappato su `/var/lib/postgresql/data`, path standard per la persistenza dei dati in PostgreSQL.

### 2. Data Definition Language (DDL)
Per garantire prestazioni elevate e scalabilità su miliardi di righe, ho adottato un'architettura basata sul **Table Partitioning** nativo di PostgreSQL. Questo approccio permette al database di eseguire il Partition Pruning, leggendo esclusivamente i "cassetti" temporali rilevanti e ignorando il resto dello storico, riducendo i tempi di query da minuti a frazioni di secondo.

#### 2.1 Tabella di staging e Geofencing
La tabella di atterraggio accoglie i dati grezzi in tempo reale. Le coordinate sono tipizzate con DECIMAL(9,6) per ottenere una tolleranza spaziale di circa 11 cm, fondamentale per le logiche di precisione sui moli.

```sql

CREATE TABLE staging_ais_data (
    mmsi VARCHAR(20),
    ship_name TEXT,
    terminal_zona TEXT,
    lat DECIMAL(9,6),
    lon DECIMAL(9,6),
    timestamp_utc TIMESTAMP
) PARTITION BY RANGE (timestamp_utc);

-- Definizione delle Partizioni Mensili
CREATE TABLE staging_ais_data_2026_02 PARTITION OF staging_ais_data
    FOR VALUES FROM ('2026-02-01') TO ('2026-03-01');

CREATE TABLE staging_ais_data_2026_03 PARTITION OF staging_ais_data
    FOR VALUES FROM ('2026-03-01') TO ('2026-04-01');

-- Partizione di Default (per catturare eventuali dati fuori intervallo)
CREATE TABLE staging_ais_data_default PARTITION OF staging_ais_data DEFAULT;

```

## 2.2 Architettura Dati: Table Partitioning (PostgreSQL)
I dati vengono smistati automaticamente in partizioni mensili fisicamente separate (es. `staging_ais_data_2026_03`, `staging_ais_data_2026_04`) tramite `PARTITION BY RANGE (timestamp_utc)`. Questo permette al motore di eseguire la *Partition Pruning*, leggendo esclusivamente i "cassetti" temporali rilevanti e riducendo i tempi di query da minuti a frazioni di secondo.

Per garantire la **Business Continuity** senza interventi manuali, la pipeline include un task dedicato in Airflow che calcola dinamicamente il primo giorno del mese entrante ed esegue `CREATE TABLE IF NOT EXISTS` per creare in anticipo la partizione del mese successivo, prevenendo errori di tipo *Out of Range*.

```sql
-- Esempio di comando DDL eseguito dinamicamente da Airflow:
CREATE TABLE IF NOT EXISTS staging_ais_data_YYYY_MM PARTITION OF staging_ais_data
FOR VALUES FROM ('YYYY-MM-01') TO ('YYYY-MM-01' + INTERVAL '1 month');

```

## Architettura di Data Ingestion: Pattern Producer-Consumer
Per gestire i picchi di traffico dei messaggi AIS (es. arrivo di intere flotte) ed evitare colli di bottiglia o *lock* sul database, lo script di ingestion (`ingestion_pipeline.py`) è stato riprogettato utilizzando un'architettura **asincrona con coda in memoria (Buffering)** basata su `asyncio`.

Il flusso è diviso in due worker indipendenti:
1. **Producer (Ricevitore API):** Ascolta il websocket in tempo reale. Appena riceve un JSON, lo decodifica, applica la logica di *Geofencing* per identificare il terminal e inserisce la tupla pulita in una `asyncio.Queue()`.
2. **Consumer (Scrittore DB):** Monitora la coda. Invece di eseguire una singola `INSERT` per ogni nave (che saturerebbe la rete), estrae fino a 100 record alla volta e li scrive nel database con una singola istruzione di **Batch Insert** (`execute_values` di `psycopg2`).

**Vantaggi ottenuti:**
* **Resilienza (Buffering):** Se il database rallenta, i dati si accumulano temporaneamente nella coda in RAM senza essere persi (il websocket non deve "aspettare").
* **Performance:** La scrittura a blocchi abbatte drasticamente il carico su PostgreSQL, permettendo di ingerire migliaia di segnali al secondo.

### Elaborazione in Volo (In-Flight Processing)
Oltre al pattern Producer-Consumer, lo script esegue le seguenti operazioni in tempo reale durante l'ingestion:

1. **Estrazione e Filtraggio:** Richiede solo i messaggi di tipo "PositionReport" all'interno di specifiche Bounding Boxes (Genova e Vado Ligure).
2. **Trasformazione (Geofencing):** Valuta latitudine e longitudine in real-time, assegnando un'etichetta di zona (es. `GENOVA_VOLTRI`, `VADO_GATEWAY`) tramite poligoni predefiniti.
3. **Data Cleansing:** Intercetta e normalizza i timestamp. Il codice tronca i nanosecondi forniti dall'API (`.split('.')[0]`) per renderli compatibili con lo standard ISO richiesto da PostgreSQL.
4. **Caricamento Sicuro:** Scrive i record utilizzando query SQL parametrizzate (`execute_values`) per prevenire vulnerabilità di SQL injection e ottimizzare le risorse di rete.

### 3. Pipeline ETL in Python

```python
import asyncio
import websockets
import json
import psycopg2
from psycopg2 import Error
from psycopg2.extras import execute_values # Ottimizzato per inserimenti multipli

# --- 1. CONFIGURAZIONE DATABASE E CODA ---
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "logistica_liguria"
DB_USER = "admin_tesi"
DB_PASS = "[INSERISCI LA TUA PASSWORD]"

BATCH_SIZE = 100  # Numero di navi da salvare in un colpo solo
QUEUE = asyncio.Queue() # La "Coda" di parcheggio in memoria

# --- 2. LOGICA DI BUSINESS (Geofencing) ---
def identifica_terminal(lat, lon):
    if 44.420 <= lat <= 44.435 and 8.740 <= lon <= 8.785:
        return "GENOVA_VOLTRI"
    elif 44.260 <= lat <= 44.285 and 8.430 <= lon <= 8.460:
        return "VADO_GATEWAY"
    elif 44.395 <= lat <= 44.415 and 8.870 <= lon <= 8.910:
        return "GENOVA_SAMPIERDARENA"
    return "ALTRO_LIGURIA"

# --- 3A. Lavoratore A (RICEVITORE: API -> CODA) ---
async def websocket_listener():
    api_key = "b7f29d875e4c6ccd444fd2c0e1d22a2a0ef57f9a"
    url = "wss://stream.aisstream.io/v0/stream"

    subscribe_message = {
        "APIKey": api_key,
        "BoundingBoxes": [
            [[44.380, 8.700], [44.450, 8.950]],
            [[44.240, 8.400], [44.300, 8.500]]
        ],
        "FilterMessageTypes": ["PositionReport"]
    }

    # Aggiungiamo un loop infinito per l'auto-riconnessione
    while True:
        try:
            print("📡 Lavoratore A: Tentativo di connessione all'API AISStream...")
            async with websockets.connect(url) as websocket:
                await websocket.send(json.dumps(subscribe_message))
                print("✅ Lavoratore A: Connesso all'API! Inizio a riempire la coda...\n" + "-"*50)

                async for message in websocket:
                    data = json.loads(message)
                    
                    mmsi = data['MetaData']['MMSI']
                    ship_name = data['MetaData']['ShipName']
                    lat = data['MetaData']['latitude']
                    lon = data['MetaData']['longitude']
                    time_utc = data['MetaData']['time_utc'].split('.')[0]
                    zona = identifica_terminal(lat, lon)

                    # Crea una tupla col dato e lo sbatte SUBITO nella coda in memoria
                    record = (mmsi, ship_name, zona, lat, lon, time_utc)
                    await QUEUE.put(record)
                    
        except Exception as e:
            # Se la rete cade, va in timeout o il server li caccia, lo script non muore.
            print(f"⚠️ Lavoratore A: Errore di connessione API ({e}). Riprovo tra 5 secondi...")
            await asyncio.sleep(5) # Aspetta 5 secondi e poi il ciclo "while" riparte!

# --- 3B. Lavoratore B (SCRITTORE: CODA -> DATABASE) ---
async def db_writer():
    try:
        print("🔄 Lavoratore B: Connessione al database PostgreSQL in corso...")
        connessione = psycopg2.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASS)
        cursore = connessione.cursor()
        print("✅ Lavoratore B: Database connesso! Pronto a scaricare la coda.")

        while True:
            batch = []
            
            # 1. Si ferma ad aspettare che ci sia almeno un segnale nella coda
            record = await QUEUE.get()
            batch.append(record)
            
            # 2. Se la coda è piena, raccoglie velocemente gli altri (fino a 100)
            while len(batch) < BATCH_SIZE and not QUEUE.empty():
                batch.append(QUEUE.get_nowait())
            
            # 3. Scrive tutto il blocco nel DB con UNA SOLA operazione!
            sql_insert = """
                INSERT INTO staging_ais_data (mmsi, ship_name, terminal_zona, lat, lon, timestamp_utc)
                VALUES %s;
            """
            # execute_values fa la "Batch Insert", migliaia di volte più veloce
            execute_values(cursore, sql_insert, batch)
            connessione.commit()
            
            print(f"💾 Salvato un blocco di {len(batch)} navi in un colpo solo! (Rimaste in coda: {QUEUE.qsize()})")

    except Error as e:
        print(f"❌ Errore del Database: {e}")
    finally:
        if 'connessione' in locals() and connessione:
            cursore.close()
            connessione.close()
            print("\n🔒 Connessione al database chiusa in modo sicuro.")

# --- 4. AVVIO DEL PROGRAMMA ---
async def main():
    # Facciamo partire ENTRAMBI i lavoratori in parallelo (Coroutines)
    task_ricevitore = asyncio.create_task(websocket_listener())
    task_scrittore = asyncio.create_task(db_writer())
    
    # Il programma non si ferma finché non c'è un errore o uno stop manuale
    await asyncio.gather(task_ricevitore, task_scrittore)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Monitoraggio interrotto manualmente.")
```

<img width="852" height="531" alt="stagin_ais_data" src="https://github.com/user-attachments/assets/cf8d9bea-1053-4320-bd86-763b4aab22e6" />


---

## Processing & Data Modeling

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
    data_elaborazione TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uk_movimento_nave UNIQUE (mmsi, orario_arrivo)
);
```

```sql

INSERT INTO fact_movimenti (mmsi, codice_zona, orario_arrivo)
SELECT mmsi, terminal_zona, orario_arrivo
FROM (
    SELECT 
        mmsi,
        terminal_zona,
        timestamp_utc AS orario_arrivo,
        LAG(terminal_zona) OVER (PARTITION BY mmsi ORDER BY timestamp_utc) as zona_precedente
    FROM staging_ais_data
    WHERE mmsi IS NOT NULL
) sub
WHERE terminal_zona != 'ALTRO_LIGURIA' 
  AND terminal_zona IS DISTINCT FROM zona_precedente
ON CONFLICT (mmsi, orario_arrivo) DO NOTHING;

```

### 3. Logica di Business e KPI Logistici (Ottimizzazione BI)
Per garantire che le dashboard di Power BI si carichino istantaneamente senza sovraccaricare il database con calcoli complessi on-the-fly, la logica di business non è basata su query dirette o viste standard, ma su **Viste Materializzate (Materialized Views)**. Questa scelta architetturale permette a PostgreSQL di pre-calcolare i KPI logistici e salvarli fisicamente su disco. Power BI leggerà solo questi "fotogrammi" pre-calcolati, riducendo i tempi di interrogazione da minuti a frazioni di secondo.

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

CREATE MATERIALIZED VIEW mv_kpi_tempi_porto AS
SELECT 
    m.id_movimento,
    m.mmsi,
    n.ship_name,
    t.nome_esteso AS terminal,
    m.orario_arrivo,
    m.orario_partenza,
    -- Se la nave non è ancora partita, calcola il tempo fino ad ORA (NOW())
    ROUND(EXTRACT(EPOCH FROM (COALESCE(m.orario_partenza, NOW()) - m.orario_arrivo)) / 3600, 2) AS ore_in_porto,
    CASE 
        WHEN EXTRACT(EPOCH FROM (COALESCE(m.orario_partenza, NOW()) - m.orario_arrivo)) / 3600 > 72 THEN TRUE 
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
    ore_in_porto
FROM mv_kpi_tempi_porto
WHERE flag_overstay = TRUE
ORDER BY ore_in_porto DESC;

```

* **Colli di Bottiglia Infrastrutturali:** Mappatura delle zone (es. Voltri vs. Sampierdarena) con i più alti tempi di attesa medi, fornendo dati cruciali per l'ottimizzazione dei flussi.

```sql

CREATE MATERIALIZED VIEW mv_kpi_confronto_terminal AS
SELECT 
    terminal,
    COUNT(id_movimento) as numero_scali,
    ROUND(AVG(ore_in_porto), 2) as media_ore_permanenza,
    SUM(CASE WHEN flag_overstay THEN 1 ELSE 0 END) as totale_overstay
FROM mv_kpi_tempi_porto
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
### 3.4 Monitoraggio Real-Time: Navi Presenti in Porto
Per rispondere alle esigenze operative dei terminalisti (es. *Quali navi sono ormeggiate in questo istante?*), il sistema espone una vista dinamica che identifica le unità navali attualmente in banchina. 

#### Filtro di Recency e Gestione Segnale Intermittente
A differenza delle analisi storiche, il monitoraggio in tempo reale deve gestire il problema dei "dati orfani" (navi che escono dall'area di copertura senza inviare il segnale di partenza). Per garantire l'accuratezza del dato, è stato implementato un **Filtro di Recency a 24 ore**: se una nave non trasmette posizioni da oltre un giorno, viene automaticamente esclusa dal monitoraggio live per evitare "falsi positivi" nella dashboard causati da perdite intermittenti del segnale AIS.

```sql

-- Creazione della vista di monitoraggio live
CREATE VIEW vw_navi_presenti_ora AS
SELECT 
    f.mmsi,
    n.ship_name,
    t.nome_esteso AS terminal,
    f.orario_arrivo,
    -- Calcolo dinamico della sosta in corso rispetto al tempo reale
    ROUND(EXTRACT(EPOCH FROM (NOW() - f.orario_arrivo)) / 3600, 2) AS ore_permanenza_attuale
FROM fact_movimenti f
LEFT JOIN dim_navi n ON f.mmsi = n.mmsi
LEFT JOIN dim_terminal t ON f.codice_zona = t.codice_zona
WHERE f.orario_partenza IS NULL                       -- Solo movimenti non ancora conclusi
  AND f.orario_arrivo > NOW() - INTERVAL '24 hours'; -- Filtro di Recency (24h)

```
---

## Fase 3: Orchestrazione e Automazione (Apache Airflow)

Per trasformare il modello logico sviluppato nella Fase 2 in una vera pipeline ETL di produzione (Data Warehouse automatizzato), ho introdotto **Apache Airflow** come motore di orchestrazione. Questo strumento permette di schedulare, monitorare e gestire le dipendenze dei flussi di trasformazione dati, garantendo che le logiche di business vengano applicate in modo sequenziale, scalabile e resiliente.

Per avvicinare l'architettura a un sistema **Near Real-Time** (fondamentale per il monitoraggio logistico in porto), ho abbandonato l'elaborazione oraria standard in favore di un approccio di **Micro-batching**, schedulando l'esecuzione della pipeline ogni 5 minuti tramite espressione Cron (`*/5 * * * *`).

### 3.1 Evoluzione dell'Infrastruttura (Docker Compose)
L'ambiente containerizzato è stato espanso per includere i micro-servizi di Airflow, affiancandoli al database PostgreSQL. L'infrastruttura ora comprende:
* **`airflow-init`**: Container effimero dedicato alla creazione dei metadati iniziali e dell'utente amministratore.
* **`airflow-webserver`**: Espone l'interfaccia di monitoraggio (UI) sulla porta `8081`.
* **`airflow-scheduler`**: Il demone che valuta le tempistiche (ogni 5 minuti) e le dipendenze per innescare l'esecuzione dei task Python.

Tutti i servizi comunicano internamente tramite una rete Docker dedicata (`tesi_network`), garantendo la risoluzione sicura dei nomi host (l'operatore Airflow si connette al servizio `db_tesi` per l'esecuzione delle query tramite SQLAlchemy).

```yaml
x-airflow-common: &airflow-common
  image: apache/airflow:2.8.1
  environment: &airflow-common-env
    AIRFLOW__CORE__EXECUTOR: LocalExecutor
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://admin_tesi:[INSERISCI LA TUA PASSWORD]@db_tesi:5432/logistica_liguria
    AIRFLOW__CORE__LOAD_EXAMPLES: 'false'
  volumes:
    - ./dags:/opt/airflow/dags
    - ./logs:/opt/airflow/logs
    - ./plugins:/opt/airflow/plugins
  depends_on:
    - db_tesi
  networks:
    - tesi_network

services:
  db_tesi:
    image: postgres:18
    container_name: postgres_porto
    environment:
      POSTGRES_USER: admin_tesi
      POSTGRES_PASSWORD: [INSERISCI LA TUA PASSWORD]
      POSTGRES_DB: logistica_liguria
    ports:
      - "5432:5432"
    volumes:
      - dati_porto:/var/lib/postgresql/data
    networks:
      - tesi_network

  pgadmin:
    image: dpage/pgadmin4
    container_name: pgadmin_interfaccia
    environment:
      PGADMIN_DEFAULT_EMAIL: studente@tesi.it
      PGADMIN_DEFAULT_PASSWORD: [INSERISCI LA TUA PASSWORD]
    ports:
      - "8080:80"
    depends_on:
      - db_tesi
    networks:
      - tesi_network

  airflow-init:
    <<: *airflow-common
    container_name: airflow_init
    command: version
    environment:
      <<: *airflow-common-env
      _AIRFLOW_DB_UPGRADE: 'true'
      _AIRFLOW_WWW_USER_CREATE: 'true'
      _AIRFLOW_WWW_USER_USERNAME: admin
      _AIRFLOW_WWW_USER_PASSWORD: [INSERISCI LA TUA PASSWORD]

  airflow-webserver:
    <<: *airflow-common
    container_name: airflow_webserver
    command: webserver
    ports:
      - "8081:8080"
    healthcheck:
      test: [ "CMD", "curl", "--fail", "http://localhost:8080/health" ]
      interval: 30s
      timeout: 10s
      retries: 5
    depends_on:
      - db_tesi
      - airflow-init

  airflow-scheduler:
    <<: *airflow-common
    container_name: airflow_scheduler
    command: scheduler
    depends_on:
      - db_tesi
      - airflow-init

volumes:
  dati_porto:


networks:
  tesi_network:
    driver: bridge
```

### 3.2 Progettazione del DAG (Directed Acyclic Graph)
La logica di aggiornamento è stata codificata in Python all'interno del file `pipeline_logistica.py`. Il DAG, denominato `pipeline_mar_ligure_completa`, esegue 7 task distinti ed è stato progettato per rispondere a tre requisiti fondamentali del Data Engineering: **Integrità del dato, Idempotenza ed Esecuzione Parallela**.

```python
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'andrea',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'pipeline_mar_ligure_completa', 
    default_args=default_args,
    schedule='*/5 * * * *',
    catchup=False,
    tags=['logistica', 'tesi'],
) as dag:
```

#### 3.2.1 Data Cleansing Automatizzato
Come teorizzato nella progettazione logica del database, il primo step della pipeline garantisce l'affidabilità dei KPI pulendo la tabella di staging dalle anomalie prima di procedere a qualsiasi calcolo.
* **`pulisci_coordinate_nulle`**: Rimuove i "Ghost Ping" (record con `lat` o `lon` mancanti).
* **`deduplica_staging`**: Rimuove i messaggi identici inviati dalla stessa nave nello stesso istante, mantenendo solo il record più recente tramite il puntatore fisico `ctid`.

```python
    # TASK 1: Pulizia coordinate mancanti
    pulisci_coordinate_nulle = SQLExecuteQueryOperator(
        task_id='pulisci_coordinate_nulle',
        conn_id='connessione_db_tesi', 
        sql="DELETE FROM staging_ais_data WHERE lat IS NULL OR lon IS NULL OR mmsi IS NULL;"
    )

    # TASK 2: Deduplicazione tecnica
    deduplica_staging = SQLExecuteQueryOperator(
        task_id='deduplica_staging',
        conn_id='connessione_db_tesi', 
        sql="""
        DELETE FROM staging_ais_data a
        USING staging_ais_data b
        WHERE a.ctid < b.ctid 
          AND a.mmsi = b.mmsi 
          AND a.timestamp_utc = b.timestamp_utc;
        """
    )
```

#### 3.2.2 Gestione delle Dimensioni (Parallel Processing)
Nei flussi in tempo reale (come lo streaming AIS), capita frequentemente che un evento (Fatto) faccia riferimento a un'entità (Dimensione) non ancora registrata a sistema, generando il problema delle *"Late Arriving Dimensions"*. 
Per evitare violazioni dei vincoli di chiave esterna (Foreign Key), la pipeline estrae proattivamente le nuove anagrafiche dalla tabella di staging e le inserisce nelle dimensioni `dim_navi` e `dim_terminal`. 

Per ottimizzare i tempi di elaborazione del micro-batch, i due task vengono **eseguiti in parallelo** dallo Scheduler di Airflow. Entrambi utilizzano il costrutto `ON CONFLICT DO NOTHING` per garantire l'idempotenza, permettendo l'esecuzione continua senza generare errori di duplicazione.

```python
    # TASK 3A: Aggiorna l'anagrafica delle navi
    aggiorna_dim_navi = SQLExecuteQueryOperator(
        task_id='aggiorna_dim_navi',
        conn_id='connessione_db_tesi', 
        sql="""
        INSERT INTO dim_navi (mmsi, ship_name)
        SELECT DISTINCT ON (mmsi) mmsi, ship_name
        FROM staging_ais_data
        WHERE mmsi IS NOT NULL
        ORDER BY mmsi, timestamp_utc DESC
        ON CONFLICT (mmsi) DO UPDATE SET ship_name = EXCLUDED.ship_name;
        """
    )

    # TASK 3B: Aggiorna le zone portuali in parallelo
    aggiorna_dim_terminal = SQLExecuteQueryOperator(
        task_id='aggiorna_dim_terminal',
        conn_id='connessione_db_tesi', 
        sql="""
        INSERT INTO dim_terminal (codice_zona, nome_esteso, citta)
        SELECT DISTINCT terminal_zona, terminal_zona, 'Da definire'
        FROM staging_ais_data
        WHERE terminal_zona != 'ALTRO_LIGURIA' 
          AND terminal_zona IS NOT NULL
        ON CONFLICT (codice_zona) DO NOTHING;
        """
    )
```

#### 3.2.3 Elaborazione dei Tempi di Permanenza (Fact Table)
Una volta garantita l'integrità referenziale per le dimensioni nave e terminal, gli ultimi due task calcolano in modo incrementale gli eventi logistici. Invece di ricalcolare tutto lo storico, il sistema gestisce arrivi e partenze separatamente:
* **Arrivi (Task 4):** Identifica il primo segnale di ingresso nel terminal e crea il record. L'idempotenza è garantita dalla clausola `ON CONFLICT DO NOTHING` (basata sul vincolo di unicità), che impedisce la duplicazione se il DAG rielabora lo stesso dato.
* **Partenze (Task 5):** Aggiorna i record "aperti" (con partenza `NULL`). Intercetta l'ultimo orario utile in cui la nave era nel terminal prima di trasmettere un nuovo segnale in mare aperto (`ALTRO_LIGURIA`), chiudendo così il calcolo della sosta.

```python
    # TASK 4: Calcola gli arrivi (Idempotenza Garantita)
    aggiorna_fact_movimenti = SQLExecuteQueryOperator(
        task_id='aggiorna_fact_movimenti',
        conn_id='connessione_db_tesi', 
        sql="""
        INSERT INTO fact_movimenti (mmsi, codice_zona, orario_arrivo)
        SELECT mmsi, terminal_zona, orario_arrivo
        FROM (
            SELECT 
                mmsi,
                terminal_zona,
                timestamp_utc AS orario_arrivo,
                LAG(terminal_zona) OVER (PARTITION BY mmsi ORDER BY timestamp_utc) as zona_precedente
            FROM staging_ais_data
            WHERE mmsi IS NOT NULL
        ) sub
        WHERE terminal_zona != 'ALTRO_LIGURIA' 
          AND terminal_zona IS DISTINCT FROM zona_precedente
        ON CONFLICT (mmsi, orario_arrivo) DO NOTHING;
        """
    )

    # TASK 5: Calcola le partenze (Evitando incroci col passato)
    aggiorna_partenze = SQLExecuteQueryOperator(
        task_id='aggiorna_partenze',
        conn_id='connessione_db_tesi', 
        sql="""
        UPDATE fact_movimenti f
        SET orario_partenza = sub.ultima_visto
        FROM (
            SELECT s1.mmsi, s1.terminal_zona, MAX(s1.timestamp_utc) as ultima_visto
            FROM staging_ais_data s1
            WHERE s1.terminal_zona != 'ALTRO_LIGURIA'
            AND EXISTS (
                SELECT 1 FROM staging_ais_data s2 
                WHERE s2.mmsi = s1.mmsi 
                AND s2.timestamp_utc > s1.timestamp_utc 
                AND s2.terminal_zona = 'ALTRO_LIGURIA'
            )
            GROUP BY s1.mmsi, s1.terminal_zona
        ) sub
        WHERE f.mmsi = sub.mmsi 
          AND f.codice_zona = sub.terminal_zona 
          AND f.orario_partenza IS NULL
          AND sub.ultima_visto > f.orario_arrivo; -- <-- LA MAGIA È QUI: La partenza deve essere successiva all'arrivo!
        """
    )
```
#### 3.2.4 Ottimizzazione per Business Intelligence (Task 6)
L'ultimo step del micro-batch è dedicato all'aggiornamento dei dati per Power BI. Poiché le Viste Materializzate sono "statiche", è compito di Airflow forzarne l'aggiornamento (`REFRESH`) non appena il calcolo dei nuovi movimenti (Arrivi e Partenze) è terminato. Questo garantisce che la dashboard mostri sempre dati allineati in tempo reale, senza mai eseguire calcoli complessi lato BI.

```python
    # TASK 6: Aggiorna i KPI per Power BI (Refresh Viste Materializzate)
    aggiorna_kpi_bi = SQLExecuteQueryOperator(
        task_id='aggiorna_kpi_bi',
        conn_id='connessione_db_tesi', 
        sql="""
        REFRESH MATERIALIZED VIEW mv_kpi_tempi_porto;
        REFRESH MATERIALIZED VIEW mv_kpi_confronto_terminal;
        """
    )
```

### 3.3 Orchestrazione e Monitoraggio
Il flusso logico finale è vincolato dalla seguente istruzione Python, che impone al motore di Airflow le corrette dipendenze topologiche:

```python
# Esecuzione logica: Pulizia -> Aggiornamento Dimensioni in parallelo -> Calcolo Arrivi -> Chiusura Partenze
auto_creazione_partizione >> pulisci_coordinate_nulle >> deduplica_staging >> [aggiorna_dim_navi, aggiorna_dim_terminal] >> aggiorna_fact_movimenti >> aggiorna_partenze >> aggiorna_kpi_bi
```

Questa configurazione garantisce che il caricamento della Fact Table avvenga *esclusivamente* se la fase di Data Cleansing e il censimento parallelo delle dimensioni si sono conclusi con successo, preservando l'assoluta coerenza strutturale del Data Warehouse.

<img width="985" height="244" alt="airflow_automazioen" src="https://github.com/user-attachments/assets/9e6dde75-9f7b-4105-b3c3-7a081c0dbcf2" />

---

## Fasi Successive del Progetto

* [x] **Fase 1: Data Ingestion e Setup Infrastrutturale**
* [x] **Fase 2: Processing & Data Modeling**
* [x] **Fase 3: Orchestrazione e Automazione (Apache Airflow)**
* [ ] **Fase 4: Data Visualization & Business Intelligence (Power BI)**
  * *Pianificato:* Connessione in Import/DirectQuery tra Power BI e le viste materializzate in PostgreSQL per lo sviluppo di una dashboard direzionale, focalizzata sul monitoraggio visivo dei KPI logistici (tempi di ciclo) e degli allarmi di Overstay nei principali terminal di Genova e Vado.
