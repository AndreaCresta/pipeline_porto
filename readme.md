# ⚓ Monitoraggio Logistico Portuale: Mar Ligure Data Pipeline

Questo repository contiene l'infrastruttura, il codice sorgente e la documentazione del mio progetto di tesi in Data Engineering. Il sistema è progettato per intercettare, trasformare e storicizzare in tempo reale i dati del traffico marittimo (AIS) nei principali terminal della Liguria, con l'obiettivo di calcolare KPI logistici avanzati (tempi di attesa, congestione, overstay).

## 🛠️ Stack Tecnologico
* **Infrastruttura:** Docker, Docker Compose
* **Database:** PostgreSQL 18
* **ETL & Scripting:** Python 3.12 (psycopg2, websockets, asyncio)
* **Amministrazione DB:** pgAdmin 4
* **Data Visualization & BI:** Microsoft Power BI (Pianificato per Fase 4)

---

## 📂 Struttura del Progetto
```text
/
├── docker-compose.yml       # Definizione dell'infrastruttura containerizzata
├── schema.sql               # DDL per la creazione della tabella di staging
├── ingestion_pipeline.py    # Script Python principale per l'ETL in tempo reale
└── README.md                # Documentazione di progetto
```

---

## 🚀 Fase 1: Data Ingestion e Setup Infrastrutturale (Completata)

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

---

## ⚙️ Guida all'Installazione e Avvio Rapido

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

## 🔜 Fasi Successive del Progetto

* [ ] **Fase 2: Processing & Business Logic (SQL)**
  * *Pianificato:* Sviluppo di viste e procedure SQL per il calcolo del Time in Port e del Time in Rada.
* [ ] **Fase 3: Orchestrazione (Apache Airflow)**
  * *Pianificato:* Schedulazione dei processi ETL batch.
* [ ] **Fase 4: Data Visualization (Power BI)**
  * *Pianificato:* Sviluppo dashboard interattiva aziendale con metriche di Congestione e allarmi di Overstay.