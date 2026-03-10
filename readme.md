# Sviluppo di una pipeline ETL automatizzata per il monitoraggio dei dati AIS nel traffico marittimo del Porto di Genova

Questo repository contiene l'infrastruttura, il codice sorgente e la documentazione del mio progetto di Data Engineering. Il sistema è progettato per intercettare, trasformare e storicizzare in tempo reale i dati del traffico marittimo (AIS) nei principali terminal della Liguria, con l'obiettivo di calcolare KPI logistici avanzati (tempi di attesa, congestione, overstay).

## Stack Tecnologico
* **Infrastruttura:** Docker, Docker Compose
* **Database:** PostgreSQL 18
* **ETL & Scripting:** Python 3.12 (psycopg2, websockets, asyncio)
* **Amministrazione DB:** pgAdmin 4
* **Automazione/Orchestrazione:** Apache Airflow 2.8.1
* **Data Visualization & BI:** Metabase


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
   

## Fase 1: Data Ingestion e Setup Infrastrutturale

In questa prima fase ho progettato e implementato l'intera pipeline di acquisizione dati (Ingestion), partendo dall'infrastruttura fino alla scrittura in database.

### 1.1 Infrastruttura Containerizzata
Ho scelto di containerizzare l'ambiente di database tramite Docker per garantire la totale riproducibilità del sistema, isolandolo dal sistema operativo host. Ho sviluppato il seguente `docker-compose.yml`:

<details>
  <summary><kbd>Clicca per visualizzare il codice</kbd></summary>

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
</details>

*Nota tecnica:* Ho adottato PostgreSQL 18 per sfruttare le recenti performance di I/O asincrono. Il volume è mappato su `/var/lib/postgresql/data`, path standard per la persistenza dei dati in PostgreSQL.

### 1.2 Data Definition Language (DDL)
Per garantire prestazioni elevate e scalabilità su miliardi di righe, ho adottato un'architettura basata sul **Table Partitioning** nativo di PostgreSQL. Questo approccio permette al database di eseguire il Partition Pruning, leggendo esclusivamente i "cassetti" temporali rilevanti e ignorando il resto dello storico, riducendo i tempi di query da minuti a frazioni di secondo.

#### 1.2.1 Tabella di staging e Geofencing
La tabella di atterraggio accoglie i dati grezzi in tempo reale. Le coordinate sono tipizzate con DECIMAL(9,6) per ottenere una tolleranza spaziale di circa 11 cm, fondamentale per le logiche di precisione sui moli.

<details>
  <summary><kbd>Clicca per visualizzare il codice</kbd></summary>

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
</details>

#### 1.2.2 Architettura Dati: Table Partitioning (PostgreSQL)
I dati vengono smistati automaticamente in partizioni mensili fisicamente separate (es. `staging_ais_data_2026_03`, `staging_ais_data_2026_04`) tramite `PARTITION BY RANGE (timestamp_utc)`. Questo permette al motore di eseguire la *Partition Pruning*, leggendo esclusivamente i "cassetti" temporali rilevanti e riducendo i tempi di query da minuti a frazioni di secondo.

Per garantire la **Business Continuity** senza interventi manuali, la pipeline include un task dedicato in Airflow che calcola dinamicamente il primo giorno del mese entrante ed esegue `CREATE TABLE IF NOT EXISTS` per creare in anticipo la partizione del mese successivo, prevenendo errori di tipo *Out of Range*.

<details>
  <summary><kbd>Clicca per visualizzare il codice</kbd></summary>

```sql
-- Esempio di comando DDL eseguito dinamicamente da Airflow:
CREATE TABLE IF NOT EXISTS staging_ais_data_YYYY_MM PARTITION OF staging_ais_data
FOR VALUES FROM ('YYYY-MM-01') TO ('YYYY-MM-01' + INTERVAL '1 month');

```
</details>

### 1.3 Architettura di Data Ingestion: Pattern Producer-Consumer
Per gestire i picchi di traffico dei messaggi AIS (es. arrivo di intere flotte) ed evitare colli di bottiglia o *lock* sul database, lo script di ingestion (`ingestion_pipeline.py`) è stato riprogettato utilizzando un'architettura **asincrona con coda in memoria (Buffering)** basata su `asyncio`.

Il flusso è stato progettato attorno a tre componenti logici principali:
1. **Producer (Ricevitore API):** Ascolta il websocket in tempo reale. Appena riceve un JSON, lo decodifica e applica due livelli di filtraggio prima di inserire il dato in coda:
   - **Lista Nera MMSI (Blacklist):** Verifica il codice MMSI della nave contro una lista di 22 navi di servizio portuale identificate manualmente tramite MarineTraffic (rimorchiatori, draghe, navi antinquinamento, battelli locali). Le navi in lista nera vengono scartate silenziosamente a monte, prima di raggiungere il database.
   - **Geofencing:** Applica la logica di identificazione del terminal tramite bounding box geografiche e inserisce la tupla pulita in una `asyncio.Queue()`.
2. **Consumer (Scrittore DB):** Monitora la coda. Invece di eseguire una singola `INSERT` per ogni nave, estrae fino a 100 record alla volta e li scrive nel database con una singola istruzione di **Batch Insert** (`execute_values` di `psycopg2`).
3. **Supervisor (Fault Tolerance):** Un meccanismo di controllo silente (`try-except` asincrono) che avvolge il sistema. Funge da "guardiano": se il server dell'API AIS cade o la rete del server si disconnette, il Supervisor blocca il crash dell'applicazione, attende 5 secondi e innesca una riconnessione automatica infinita, garantendo un sistema resiliente e un monitoraggio 24/7.

**Vantaggi ottenuti:**
* **Resilienza (Buffering):** Se il database rallenta, i dati si accumulano temporaneamente nella coda in RAM senza essere persi (il websocket non deve "aspettare").
* **Performance:** La scrittura a blocchi abbatte drasticamente il carico su PostgreSQL, permettendo di ingerire migliaia di segnali al secondo.
* **Fault Tolerance (Resilienza di Rete):** Il ricevitore websocket è avvolto in un ciclo di controllo (`try-except`). Se l'API AIS si disconnette o la rete cade, lo script intercetta l'eccezione, attende 5 secondi e tenta una riconnessione automatica infinita. Questo evita crash fatali e garantisce un uptime del sistema 24/7.

#### 1.3.1 Elaborazione in Volo (In-Flight Processing)
Oltre al pattern Producer-Consumer, lo script esegue le seguenti operazioni in tempo reale durante l'ingestion:

1. **Estrazione e Filtraggio:** Richiede solo i messaggi di tipo "PositionReport" all'interno di specifiche Bounding Boxes (Genova e Vado Ligure).
2. **Trasformazione (Geofencing):** Valuta latitudine e longitudine in real-time, assegnando un'etichetta di zona (es. `GENOVA_VOLTRI`, `VADO_GATEWAY`) tramite poligoni predefiniti.
3. **Data Cleansing:** Intercetta e normalizza i timestamp. Il codice tronca i nanosecondi forniti dall'API (`.split('.')[0]`) per renderli compatibili con lo standard ISO richiesto da PostgreSQL.
4. **Caricamento Sicuro:** Scrive i record utilizzando query SQL parametrizzate (`execute_values`) per prevenire vulnerabilità di SQL injection e ottimizzare le risorse di rete.

#### 1.3.2 Pipeline ETL in Python
Di seguito è riportata l'implementazione completa del processo appena descritto. Oltre a orchestrare i due worker in parallelo, il codice integra un meccanismo di **Fault Tolerance (Auto-Riconnessione)**: qualora il server dell'API esterna dovesse interrompere la trasmissione per instabilità di rete, lo script non va in crash, ma esegue tentativi di connessione ciclici ogni 5 secondi. Questo garantisce un monitoraggio ininterrotto (24/7) senza alcun intervento manuale.

<details>
  <summary><kbd>Clicca per visualizzare il codice</kbd></summary>

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
    api_key = "[INSERISCI LA TUA API KEY]"
    url = "wss://stream.aisstream.io/v0/stream"

    subscribe_message = {
        "APIKey": api_key,
        "BoundingBoxes": [
            [[44.380, 8.700], [44.450, 8.950]],
            [[44.240, 8.400], [44.300, 8.500]]
        ],
        "FilterMessageTypes": ["PositionReport"]
    }

    # Configurazione SSL per macOS
    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    # Lista nera: navi di servizio portuale escluse dalla pipeline
    MMSI_BLACKLIST = {
        247539300, 247317800, 247301200, 256004628, 247287200, 247337400,
        247453600, 247317300, 247317700, 247062300, 247245100, 247244200,
        256003619, 215548000, 247277900, 228376800, 247338600, 247382900,
        247209800, 247423700, 247377400, 247299200
    }

    while True:
        try:
            print("📡 Lavoratore A: Tentativo di connessione all'API AISStream...")
            async with websockets.connect(
                url,
                ssl=ssl_context,
                open_timeout=60,
                ping_interval=20,
                ping_timeout=20
            ) as websocket:
                await websocket.send(json.dumps(subscribe_message))
                print("✅ Lavoratore A: Connesso all'API! Inizio a ricevere dati...\n" + "-"*50)

                async for message in websocket:
                    data = json.loads(message)

                    # Estrazione sicura dei dati
                    meta = data.get('MetaData', {})
                    mmsi = meta.get('MMSI')
                    ship_name = meta.get('ShipName', 'Sconosciuta')
                    lat = meta.get('latitude')
                    lon = meta.get('longitude')
                    time_utc = meta.get('time_utc', '').split('.')[0]

                    if mmsi and lat and lon and int(mmsi) not in MMSI_BLACKLIST:
                        zona = identifica_terminal(lat, lon)
                        record = (mmsi, ship_name, zona, lat, lon, time_utc)
                        await QUEUE.put(record)

        except Exception as e:
            print(f"⚠️ Lavoratore A: Errore di connessione API ({e}). Riprovo tra 30 secondi...")
            await asyncio.sleep(30)

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

</details>

<img width="852" height="531" alt="stagin_ais_data" src="https://github.com/user-attachments/assets/cf8d9bea-1053-4320-bd86-763b4aab22e6" />

<sub> *Figura 1: Vista unificata della tabella di staging. I dati mostrati sono georeferenziati in tempo reale e già fisicamente partizionati.* </sub>


## Fase 2: Processing, Data Modeling e KPI

L'obiettivo di questa fase è la trasformazione del dato "grezzo" (Raw Data) in "informazione strutturata" (Analytics-Ready Data) per rispondere ai requisiti logistici della tesi. In questa fase, il sistema evolve da una singola tabella di atterraggio a uno **Star Schema** ottimizzato per il calcolo dei KPI.

### 2.1 Ottimizzazione e Performance (Indexing)
Per garantire la scalabilità della pipeline e gestire migliaia di record AIS in tempo reale, ho implementato indici specializzati sulla tabella di staging:

* **`idx_ais_timestamp`**: Indice B-Tree sulla colonna `timestamp_utc` per velocizzare le query di ordinamento temporale e il partizionamento logico dei dati.
* **`idx_ais_mmsi`**: Indice per ottimizzare il raggruppamento e il filtraggio dei messaggi appartenenti alla medesima unità navale.
* **`idx_ais_mmsi_time`**: Indice composito (`mmsi`, `timestamp_utc DESC`) progettato specificamente per le query di ricostruzione della rotta, riducendo drasticamente i tempi di esecuzione per la ricerca dell'ultima posizione nota.

<details>
  <summary><kbd>Clicca per visualizzare il codice</kbd></summary>

```sql

CREATE INDEX idx_ais_timestamp ON staging_ais_data (timestamp_utc);


CREATE INDEX idx_ais_mmsi ON staging_ais_data (mmsi);


CREATE INDEX idx_ais_mmsi_time ON staging_ais_data (mmsi, timestamp_utc DESC);
```

</details>

### 2.2 Architettura Star Schema (Data Warehouse Design)

Per trasformare il flusso continuo di dati AIS in metriche logistiche interrogabili, il processo di Data Engineering è stato strutturato attorno a un modello a stella (Star Schema). Questo approccio garantisce l'integrità del dato, elimina le ridondanze e ottimizza le performance del database per le analisi logistico-portuali.
L'architettura separa rigorosamente i fatti (eventi dinamici) dalle dimensioni (anagrafiche e dati di contesto):

#### 2.2.1 Tabelle Dimensione (Anagrafiche)

  * **`dim_navi`**: Memorizza i dati statici delle navi, come il codice MMSI e il nome (Ship Name). Risolve il problema della ridondanza presente nello staging, dove il nome della nave viene inutilmente ripetuto per ogni singola coordinata inviata.

<details>
  <summary><kbd>Clicca per visualizzare il codice</kbd></summary>

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

</details>

  * **`dim_terminal`**: Contiene la definizione geografica (poligoni di Geofencing) dei terminal monitorati, quali Genova Voltri (PSA Pra'), Genova Sampierdarena e Vado Gateway. Rende le interrogazioni geografiche indipendenti dal codice applicativo Python.

<details>
  <summary><kbd>Clicca per visualizzare il codice</kbd></summary>

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

</details>

  * **`dim_tempo`**: Gerarchia temporale (Ora, Giorno, Mese) pianificata per le analisi aggregate, essenziale per identificare pattern ciclici di congestione.

<details>
  <summary><kbd>Clicca per visualizzare il codice</kbd></summary>

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

</details>

#### 2.2.2 Tabella dei Fatti (Eventi)

  * **`fact_movimenti`**: È il cuore del sistema analitico. Registra esclusivamente gli eventi di "Ingresso" e "Uscita" dalle aree terminal, relazionando l'ID della nave (`mmsi`), l'ID del terminal e due timestamp cruciali: `orario_arrivo` e `orario_partenza`.

<details>
  <summary><kbd>Clicca per visualizzare il codice</kbd></summary>

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

</details>

## 2.3 Logica di Business e viste materializzate
Per garantire che le dashboard di Metabase si carichino istantaneamente senza sovraccaricare il database con calcoli complessi on-the-fly, la logica di business non è basata su query dirette o viste standard, ma su **Viste Materializzate (Materialized Views)**. Questa scelta architetturale permette a PostgreSQL di pre-calcolare i KPI logistici e salvarli fisicamente su disco. Metabase leggerà solo questi "fotogrammi" pre-calcolati, riducendo i tempi di interrogazione da minuti a frazioni di secondo.

#### 2.3.1 Analisi dei Tempi di Ciclo (Turnaround Time)
Il ciclo logistico della nave viene frammentato e calcolato in due fasi distinte per isolare le inefficienze:
* **Time in Rada (Waiting Time):** Misura il tempo che la nave trascorre nell'area di ancoraggio (identificata come transito o attesa nel Mar Ligure) prima di ricevere l'autorizzazione all'ormeggio. Un valore medio alto in questo KPI è il principale indicatore di congestione del terminal.

<details>
  <summary><kbd>Clicca per visualizzare il codice</kbd></summary>

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

</details>

* **Vista SQL per il calcolo del Time in Rada (Attesa in mare):**
  Questa vista riutilizza la logica delle Window Functions per isolare i periodi di transito o attesa fuori dai terminal logistici, calcolando le ore di permanenza nella zona "ALTRO_LIGURIA".

<details>
  <summary><kbd>Clicca per visualizzare il codice</kbd></summary>

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

</details>

* **Time in Port (Dwell Time):** Calcolato come differenza matematica tra il timestamp di uscita e quello di entrata dalla zona di geofencing del terminal (`orario_partenza - orario_arrivo`). Rappresenta il tempo effettivo di operatività per il carico/scarico container.

<details>
  <summary><kbd>Clicca per visualizzare il codice</kbd></summary>

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

</details>

#### 2.3.2. Identificazione Anomalie e Overstay
Tramite viste SQL materializzate, il sistema filtra automaticamente i dati per far emergere i casi critici (Outliers):
* **Overstay al Molo:** Identificazione delle navi che superano le soglie standard di permanenza (es. > 72 ore al Vado Gateway), segnalando possibili guasti, ispezioni doganali o inefficienze nelle operazioni di piazzale.

<details>
  <summary><kbd>Clicca per visualizzare il codice</kbd></summary>

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

</details>

* **Colli di Bottiglia Infrastrutturali:** Mappatura delle zone (es. Voltri vs. Sampierdarena) con i più alti tempi di attesa medi, fornendo dati cruciali per l'ottimizzazione dei flussi.

<details>
  <summary><kbd>Clicca per visualizzare il codice</kbd></summary>

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

</details>

#### 2.3.3 Data Cleansing e Integrità
Per garantire l'affidabilità dei KPI, sono state automatizzate procedure di pulizia del dato a livello di database:
* Rimozione dei "rimbalzi GPS" (Ghost Ping) e delle coordinate outlier generate da errori di trasmissione dell'antenna AIS.
* Deduplicazione tecnica degli eventi per assicurare che ogni scalo nave generi un singolo record fattuale nella tabella `fact_movimenti`.

<details>
  <summary><kbd>Clicca per visualizzare il codice</kbd></summary>

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

</details>

#### 2.3.4 Monitoraggio Real-Time: Navi Presenti in Porto
Per rispondere alle esigenze operative dei terminalisti (es. *Quali navi sono ormeggiate in questo istante?*), il sistema espone una vista dinamica che identifica le unità navali attualmente in banchina. 

<details>
  <summary><kbd>Clicca per visualizzare il codice</kbd></summary>

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

</details>

#### 2.3.5 Filtro di Recency e Gestione Segnale Intermittente
A differenza delle analisi storiche, il monitoraggio in tempo reale deve gestire il problema dei "dati orfani" (navi che escono dall'area di copertura senza inviare il segnale di partenza). Per garantire l'accuratezza del dato, è stato implementato un **Filtro di Recency a 24 ore**: se una nave non trasmette posizioni da oltre un giorno, viene automaticamente esclusa dal monitoraggio live per evitare "falsi positivi" nella dashboard causati da perdite intermittenti del segnale AIS.

## Fase 3: Orchestrazione e Automazione (Apache Airflow)

Per trasformare il modello logico sviluppato nella Fase 2 in una vera pipeline ETL di produzione (Data Warehouse automatizzato), ho introdotto **Apache Airflow** come motore di orchestrazione. Questo strumento permette di schedulare, monitorare e gestire le dipendenze dei flussi di trasformazione dati, garantendo che le logiche di business vengano applicate in modo sequenziale, scalabile e resiliente.

Per avvicinare l'architettura a un sistema **Near Real-Time** (fondamentale per il monitoraggio logistico in porto), ho abbandonato l'elaborazione oraria standard in favore di un approccio di **Micro-batching**, schedulando l'esecuzione della pipeline ogni 5 minuti tramite espressione Cron (`*/5 * * * *`).

### 3.1 Evoluzione dell'Infrastruttura (Docker Compose)
L'ambiente containerizzato è stato espanso per includere i micro-servizi di Airflow, affiancandoli al database PostgreSQL. L'infrastruttura ora comprende:
* **`airflow-init`**: Container effimero dedicato alla creazione dei metadati iniziali e dell'utente amministratore.
* **`airflow-webserver`**: Espone l'interfaccia di monitoraggio (UI) sulla porta `8081`.
* **`airflow-scheduler`**: Il demone che valuta le tempistiche (ogni 5 minuti) e le dipendenze per innescare l'esecuzione dei task Python.

Tutti i servizi comunicano internamente tramite una rete Docker dedicata (`tesi_network`), garantendo la risoluzione sicura dei nomi host (l'operatore Airflow si connette al servizio `db_tesi` per l'esecuzione delle query tramite SQLAlchemy).

<details>
  <summary><kbd>Clicca per visualizzare il codice</kbd></summary>

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

</details>

### 3.2 Progettazione del DAG (Directed Acyclic Graph)
La logica di aggiornamento è stata codificata in Python all'interno del file `pipeline_logistica.py`. Il DAG, denominato `pipeline_mar_ligure_completa`, esegue 7 task distinti ed è stato progettato per rispondere a tre requisiti fondamentali del Data Engineering: **Integrità del dato, Idempotenza ed Esecuzione Parallela**.

<details>
  <summary><kbd>Clicca per visualizzare il codice</kbd></summary>

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

</details>

#### 3.2.1 Automazione del Partizionamento (Auto-Partitioning)
Per garantire che il database sia sempre pronto ad accogliere i dati del mese successivo (Business Continuity), il primo vero task del DAG è una funzione Python dedicata alla gestione proattiva delle partizioni. Utilizzando le librerie `datetime` e `dateutil`, il task calcola il mese solare successivo ed esegue dinamicamente il comando DDL su PostgreSQL, creando la partizione fisica in modo completamente automatizzato.

<details>
  <summary><kbd>Clicca per visualizzare il codice</kbd></summary>

```python
    def crea_partizione_mese_prossimo():
        import psycopg2
        from datetime import date
        from dateutil.relativedelta import relativedelta
        
        prossimo_mese = date.today().replace(day=1) + relativedelta(months=1)
        mese_dopo = prossimo_mese + relativedelta(months=1)
        
        nome_tabella = f"staging_ais_data_{prossimo_mese.strftime('%Y_%m')}"
        data_inizio = prossimo_mese.strftime('%Y-%m-01')
        data_fine = mese_dopo.strftime('%Y-%m-01')
        
        conn = psycopg2.connect(
            host="db_tesi", port="5432",
            database="logistica_liguria",
            user="admin_tesi", password="password_sicura"
        )
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {nome_tabella} PARTITION OF staging_ais_data
            FOR VALUES FROM ('{data_inizio}') TO ('{data_fine}');
        """)
        cur.close()
        conn.close()
        print(f"✅ Partizione {nome_tabella} verificata/creata con successo.")

    auto_creazione_partizione = PythonOperator(
        task_id='auto_creazione_partizione',
        python_callable=crea_partizione_mese_prossimo,
    )
```    
</details>

#### 3.2.2 Data Cleansing Automatizzato
Come teorizzato nella progettazione logica del database, il primo step della pipeline garantisce l'affidabilità dei KPI pulendo la tabella di staging dalle anomalie prima di procedere a qualsiasi calcolo.
* **`pulisci_coordinate_nulle`**: Rimuove i "Ghost Ping" (record con `lat` o `lon` mancanti).
* **`deduplica_staging`**: Rimuove i messaggi identici inviati dalla stessa nave nello stesso istante, mantenendo solo il record più recente tramite il puntatore fisico `ctid`.

<details>
  <summary><kbd>Clicca per visualizzare il codice</kbd></summary>

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

</details>

#### 3.2.3 Gestione delle Dimensioni (Parallel Processing)
Nei flussi in tempo reale (come lo streaming AIS), capita frequentemente che un evento (Fatto) faccia riferimento a un'entità (Dimensione) non ancora registrata a sistema, generando il problema delle *"Late Arriving Dimensions"*. 
Per evitare violazioni dei vincoli di chiave esterna (Foreign Key), la pipeline estrae proattivamente le nuove anagrafiche dalla tabella di staging e le inserisce nelle dimensioni `dim_navi` e `dim_terminal`. 

Per ottimizzare i tempi di elaborazione del micro-batch, i due task vengono **eseguiti in parallelo** dallo Scheduler di Airflow. Entrambi utilizzano il costrutto `ON CONFLICT DO NOTHING` per garantire l'idempotenza, permettendo l'esecuzione continua senza generare errori di duplicazione.

<details>
  <summary><kbd>Clicca per visualizzare il codice</kbd></summary>

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

</details>

#### 3.2.4 Elaborazione dei Tempi di Permanenza (Arrivi e Partenze)
Una volta garantita l'integrità referenziale per le dimensioni nave e terminal, gli ultimi due task calcolano in modo incrementale gli eventi logistici. Invece di ricalcolare tutto lo storico, il sistema gestisce arrivi e partenze separatamente:
* **Arrivi (Task 4):** Identifica il primo segnale di ingresso nel terminal e crea il record. L'idempotenza è garantita dalla clausola `ON CONFLICT DO NOTHING` (basata sul vincolo di unicità), che impedisce la duplicazione se il DAG rielabora lo stesso dato.
* **Partenze (Task 5):** Aggiorna i record "aperti" (con partenza `NULL`). Intercetta l'ultimo orario utile in cui la nave era nel terminal prima di trasmettere un nuovo segnale in mare aperto (`ALTRO_LIGURIA`), chiudendo così il calcolo della sosta.

<details>
  <summary><kbd>Clicca per visualizzare il codice</kbd></summary>

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

</details>

#### 3.2.5 Ottimizzazione per Business Intelligence (Aggiornamento Viste Materializzate)
L'ultimo step del micro-batch è dedicato all'aggiornamento dei dati per Metabase. Poiché le Viste Materializzate sono "statiche", è compito di Airflow forzarne l'aggiornamento (`REFRESH`) non appena il calcolo dei nuovi movimenti (Arrivi e Partenze) è terminato. Questo garantisce che la dashboard mostri sempre dati allineati in tempo reale, senza mai eseguire calcoli complessi lato BI.

<details>
  <summary><kbd>Clicca per visualizzare il codice</kbd></summary>

```python
    # TASK 6: Aggiorna i KPI per Metabase (Refresh Viste Materializzate)
    aggiorna_kpi_bi = SQLExecuteQueryOperator(
        task_id='aggiorna_kpi_bi',
        conn_id='connessione_db_tesi', 
        sql="""
        REFRESH MATERIALIZED VIEW mv_kpi_tempi_porto;
        REFRESH MATERIALIZED VIEW mv_kpi_confronto_terminal;
        """
    )
```

</details>

### 3.3 Orchestrazione e Monitoraggio
Il flusso logico finale è vincolato dalla seguente istruzione Python, che impone al motore di Airflow le corrette dipendenze topologiche:

```python
# Esecuzione logica: Pulizia -> Aggiornamento Dimensioni in parallelo -> Calcolo Arrivi -> Chiusura Partenze
auto_creazione_partizione >> pulisci_coordinate_nulle >> deduplica_staging >> [aggiorna_dim_navi, aggiorna_dim_terminal] >> aggiorna_fact_movimenti >> aggiorna_partenze >> aggiorna_kpi_bi
```

Questa configurazione garantisce che il caricamento della Fact Table avvenga *esclusivamente* se la fase di Data Cleansing e il censimento parallelo delle dimensioni si sono conclusi con successo, preservando l'assoluta coerenza strutturale del Data Warehouse.

<img width="1597" height="247" alt="airflow_automazione" src="https://github.com/user-attachments/assets/03722422-8b1f-4fed-bf94-5eaefca4a002" />

<sub> *Figura 2: Grafico di esecuzione del DAG in Airflow: dipendenze topologiche e parallelismo dei task.* </sub>


## Fase 4: Data Visualization & Business Intelligence (Metabase)

L'ultimo strato dell'architettura trasforma i dati processati e storicizzati in insight decisionali operativi. Per questa fase è stato implementato **Metabase**, uno strumento di Business Intelligence open-source, containerizzato tramite Docker e integrato nella rete `tesi_network` esistente. La scelta di Metabase rispetto a soluzioni proprietarie (es. Power BI, Tableau) è motivata dalla sua natura open-source, dalla semplicità di deployment via Docker e dalla capacità di connettersi nativamente a PostgreSQL senza driver aggiuntivi.

### 4.1 Evoluzione dell'Infrastruttura (Docker Compose)

Il container Metabase è stato aggiunto al `docker-compose.yml` esistente come quarto servizio, affiancandosi a `db_tesi`, `pgadmin` e ai servizi Airflow. Per garantire la persistenza dei metadati interni di Metabase (domande salvate, layout della dashboard, utenti), è stato creato un database PostgreSQL dedicato (`metabase_app_db`), separato dal database operativo `logistica_liguria`. Questo approccio è più robusto rispetto al database H2 embedded di default, che perde la configurazione ad ogni riavvio del container.

La creazione automatica del database avviene tramite uno script SQL (`init-db/create_metabase_db.sql`) montato nel container PostgreSQL, che viene eseguito una sola volta al primo avvio dell'infrastruttura:

<details>
  <summary><kbd>Clicca per visualizzare il codice</kbd></summary>

```sql
CREATE DATABASE metabase_app_db;
GRANT ALL PRIVILEGES ON DATABASE metabase_app_db TO admin_tesi;
```

</details>

Il servizio Metabase nel `docker-compose.yml` punta a questo database tramite le variabili d'ambiente `MB_DB_*`:

<details>
  <summary><kbd>Clicca per visualizzare il codice</kbd></summary>

```yaml
  metabase:
    image: metabase/metabase:latest
    container_name: metabase_bi
    restart: always
    ports:
      - "3000:3000"
    environment:
      - MB_DB_TYPE=postgres
      - MB_DB_DBNAME=metabase_app_db
      - MB_DB_PORT=5432
      - MB_DB_USER=admin_tesi
      - MB_DB_PASS=[INSERISCI LA TUA PASSWORD]
      - MB_DB_HOST=db_tesi
    networks:
      - tesi_network
    depends_on:
      - db_tesi
```

</details>

*Nota tecnica:* La variabile `MB_DB_HOST=db_tesi` sfrutta la risoluzione DNS interna di Docker: Metabase raggiunge il database PostgreSQL tramite il nome del servizio, senza esporre la porta all'esterno. Una volta avviato, l'interfaccia è accessibile su `http://localhost:3000`.

### 4.2 Architettura di Visualizzazione: Disaccoppiamento tramite Viste Materializzate

Metabase non interroga direttamente le tabelle operative (`fact_movimenti`, `staging_ais_data`). La connessione avviene in **lettura esclusiva** sulle Viste Materializzate (`mv_kpi_tempi_porto` e `mv_kpi_confronto_terminal`), già calcolate e aggiornate da Airflow ogni 5 minuti.

Questa scelta architetturale garantisce due proprietà fondamentali:

* **Decoupling (Disaccoppiamento):** Il carico computazionale (aggregazioni, join, calcolo delle medie) è interamente gestito da Airflow sul database. Metabase legge solo il risultato pre-calcolato, senza mai interferire con i processi di ingestion o orchestrazione.
* **Latenza Zero lato BI:** Le dashboard si caricano in pochi millisecondi poiché i dati sono già materializzati fisicamente su disco. Non vengono mai eseguiti calcoli complessi on-the-fly lato visualizzazione.

### 4.3 Domande di Business e Progettazione della Dashboard

La dashboard direzionale **"Monitoraggio Porto di Genova"** è stata progettata attorno a quattro domande operative fondamentali, ciascuna mappata su uno o più pannelli visivi:

#### D1 — Quali navi sono in porto adesso?
**Pannello: Mappa Navale Real-Time**

Una mappa geografica interattiva mostra la posizione corrente di tutte le navi attive nelle aree monitorate. I marker sono distribuiti visivamente nei tre cluster corrispondenti ai terminal (Voltri a ovest, Sampierdarena e porto centro a est). La fonte dati è la vista `mv_kpi_tempi_porto`, filtrata sui record con `orario_partenza IS NULL`, ovvero le navi ancora ormeggiate. Il pannello numerico adiacente (**Ping ricevuti nelle ultime 24 ore**) contestualizza il volume di segnali AIS processati dalla pipeline: al momento del test, **11.866 segnali** in 24 ore, confermando la continuità operativa del sistema di ingestion.

#### D2 — Quanto tempo stanno le navi in porto?
**Pannelli: Tempo di ciclo delle navi + Efficienza dei terminal**

Due pannelli complementari rispondono alla stessa domanda da angolazioni diverse. Il grafico a barre orizzontali (**Tempo di ciclo**) mostra la media delle ore di permanenza per terminal: Vado Gateway e Genova Voltri si attestano intorno a 1.3 ore di sosta media, mentre Genova Sampierdarena registra circa 0.6 ore. Il grafico a barre verticali (**Efficienza dei terminal**) ripropone la stessa metrica in formato comparativo, rendendo immediata l'identificazione del terminal con il Turnaround Time più elevato. Entrambi i pannelli attingono dalla colonna `media_ore_permanenza` della vista `mv_kpi_confronto_terminal`.

#### D3 — In quali ore arriva più traffico?
**Pannelli: Trend degli arrivi + Fasce orarie critiche**

Il grafico a linea (**Trend degli arrivi**) mostra l'andamento temporale del numero di arrivi registrati: si osserva un picco significativo nella prima rilevazione (32 arrivi a mezzanotte del 10 marzo, corrispondente al primo ciclo di elaborazione del DAG dopo l'avvio del sistema), seguito da una stabilizzazione tra 4 e 8 arrivi per ciclo nelle ore successive. Lo scatter plot (**Fasce orarie critiche**) disaggrega gli arrivi per ora del giorno, evidenziando concentrazioni nelle fasce 10:00, 15:00–17:00. Questi dati permettono ai responsabili logistici di anticipare i picchi di occupazione e pianificare le risorse di banchina.

#### D4 — Quali terminal sono più congestionati?
**Pannello: Occupazione Live dei Terminal**

Il grafico a ciambella (**Occupazione Live dei Terminal**) mostra la distribuzione percentuale delle navi attualmente in porto tra i tre terminal monitorati. Al momento del test: Genova Voltri **53%**, Vado Gateway **33%**, Genova Sampierdarena **13%**. Il totale di **15 navi attive** rappresenta la fotografia istantanea dello stato operativo del porto. La fonte dati è la colonna `numero_scali` aggregata per `terminal` dalla vista `mv_kpi_confronto_terminal`.

<img width="1075" height="862" alt="dashboard1" src="https://github.com/user-attachments/assets/a9e74fa0-f953-4da2-be2d-f885776b4527" />
<img width="1075" height="842" alt="dashboard2" src="https://github.com/user-attachments/assets/e54eba82-d952-4cf2-b6eb-2dd323124845" />


### 4.4 Sincronizzazione Near Real-Time: Architettura a 3 Livelli

Il sistema è progettato per garantire un monitoraggio *quasi live* mantenendo la coerenza del database relazionale. Il flusso end-to-end si articola su tre livelli temporali sovrapposti:

| Livello | Componente | Frequenza | Descrizione |
|---|---|---|---|
| **1 - Ingestion** | `ingestion_pipeline.py` | Continua (real-time) | Inserimento batch su `staging_ais_data` non appena i segnali AIS arrivano via WebSocket |
| **2 - Orchestrazione** | Apache Airflow DAG | Ogni 5 minuti | Il DAG processa lo staging, consolida i movimenti e lancia `REFRESH MATERIALIZED VIEW` |
| **3 - Visualizzazione** | Metabase | Ogni 60 secondi | Auto-refresh frontend che rilegge le viste materializzate aggiornate |

Il disallineamento massimo fisiologico tra la posizione reale di una nave e la sua visualizzazione in dashboard è di **~5 minuti**, determinato dalla cadenza di esecuzione del DAG Airflow (il collo di bottiglia più lento della catena).

### 4.5 Resilienza e Troubleshooting

Durante lo sviluppo e il testing della pipeline end-to-end sono state identificate e risolte le seguenti criticità:

* **Database metadati Metabase mancante:** Nella configurazione iniziale, il container Metabase crashava in loop perché il database `metabase_app_db` specificato nelle variabili `MB_DB_*` non esisteva su PostgreSQL. La soluzione adottata è stata la creazione di uno script SQL di inizializzazione (`init-db/create_metabase_db.sql`) montato in `/docker-entrypoint-initdb.d/`, eseguito automaticamente da PostgreSQL al primo avvio. Questo approccio garantisce che il database esista prima che Metabase tenti di connettersi.

* **Allineamento Schemi (Data Mapping):** La colonna `terminal_zona` prodotta dalla logica di Geofencing Python (`ingestion_pipeline.py`) deve corrispondere esattamente al campo `codice_zona` in `dim_terminal`. Qualsiasi discrepanza nei valori stringa (es. `GENOVA_VOLTRI` vs `genova_voltri`) causerebbe violazioni di Foreign Key nella `fact_movimenti`. Il sistema è stato validato garantendo la coerenza maiuscole/minuscole in tutta la pipeline.

* **Idempotenza dei Task Airflow:** Tutti i task di aggiornamento dimensionale sfruttano il costrutto `ON CONFLICT (chiave) DO UPDATE/NOTHING`, garantendo che esecuzioni multiple dello stesso DAG (es. in caso di retry automatico) non generino duplicati o errori nel Data Warehouse.

* **Bounding Box Geofencing Sampierdarena:** Nella prima configurazione, la bounding box del terminal Sampierdarena era troppo estesa, causando la classificazione erronea di ormeggiatori, VDF e imbarcazioni da diporto come navi del terminal container. La box è stata ristretta al solo fronte banchina PSA SECH (Calata Sanità/Bettolo): `lat 44.404–44.410 / lon 8.907–8.918`, eliminando i falsi positivi.

* **Rilevamento Partenze e Gestione Segnale Intermittente:** Un movimento viene marcato come concluso (`PARTITA`) solo quando il sistema intercetta un segnale della medesima nave nella zona `ALTRO_LIGURIA` con `timestamp_utc` **strettamente successivo** all'`orario_arrivo`. Questo vincolo, implementato nella clausola `AND sub.ultima_visto > f.orario_arrivo` del Task 5, previene la chiusura erronea di movimenti ancora aperti causata da segnali GPS ritardati o fuori sequenza.

* **Filtraggio Navi di Servizio Portuale (MMSI Blacklist):** L'analisi dei dati raccolti ha evidenziato la presenza sistematica di navi di servizio portuale (rimorchiatori, draghe, navi antinquinamento, battelli passeggeri locali) all'interno delle bounding box dei terminal, specialmente a Genova Voltri e Sampierdarena. Queste navi, pur essendo fisicamente presenti nell'area, non rappresentano traffico commerciale rilevante per i KPI logistici. Il problema è stato risolto costruendo una **lista nera di 22 MMSI** identificati manualmente tramite MarineTraffic e verificati uno per uno per tipo di nave. Il filtro viene applicato nel Lavoratore A prima che il record entri in coda, garantendo che il database contenga esclusivamente navi cargo, container e bulk carrier. La lista nera è documentata direttamente nel codice sorgente con nome e tipo per ogni MMSI:

<details>
  <summary><kbd>Mostra lista nera MMSI (22 voci)</kbd></summary>

| MMSI | Nome | Tipo |
|---|---|---|
| 247539300 | VB INSIGNIA | Rimorchiatore |
| 247317800 | SAN GENNARO PRIMO | Rimorchiatore |
| 247301200 | G.LORIS | Rimorchiatore |
| 256004628 | OFFSHORE PROGRESS | High Speed Craft |
| 247287200 | BREZZAMARE | Rimorchiatore |
| 247337400 | CAVALIER SERGIO M. | Rimorchiatore |
| 247453600 | ECO NAPOLI | Nave di servizio |
| 247317300 | SANT'AGOSTINO | Rimorchiatore |
| 247317700 | SAN ANTONIO | Rimorchiatore |
| 247062300 | SAN MARCO SECONDO | Rimorchiatore |
| 247245100 | NINO I | Rimorchiatore |
| 247244200 | SAN MARCO I | Rimorchiatore |
| 256003619 | VOE JARL | Rimorchiatore |
| 215548000 | FABIO DUO' | Draga (Hopper Dredger) |
| 247277900 | REDEEMER | Nave antinquinamento |
| 228376800 | JIF HELIOS | Utility Vessel |
| 247338600 | TECNE | Tanker bunkeraggio |
| 247382900 | ITALIA | Rimorchiatore (Towing Vessel) |
| 247209800 | MAREXPRESS | Passenger (battello locale) |
| 247423700 | CAPO VADO | Salvage/Rescue Vessel |
| 247377400 | SABATIA | Passenger (battello locale) |
| 247299200 | (senza nome) | Nave di servizio non identificata |

</details>

* **Timeout e Stabilità WebSocket:** Il listener AIS è configurato con parametri espliciti di `open_timeout=60`, `ping_interval=20` e `ping_timeout=20` per gestire connessioni instabili su macOS. In caso di disconnessione, il meccanismo di auto-riconnessione con attesa di 30 secondi previene loop aggressivi che saturerebbero i tentativi di connessione verso il server AISStream.