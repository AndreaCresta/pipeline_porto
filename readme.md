⚓ Monitoraggio Logistico Portuale: Mar Ligure Data Pipeline
Questo repository contiene l'infrastruttura, il codice sorgente e la documentazione del mio progetto di tesi in Data Engineering. Il sistema è progettato per intercettare, trasformare e storicizzare in tempo reale i dati del traffico marittimo (AIS) nei principali terminal della Liguria, con l'obiettivo di calcolare KPI logistici avanzati (tempi di attesa, congestione, overstay).

🛠️ Stack Tecnologico
Infrastruttura: Docker, Docker Compose

Database: PostgreSQL 18

ETL & Scripting: Python 3.12 (psycopg2, websockets, asyncio)

Monitoraggio DB: pgAdmin 4

📂 Struttura del Progetto
Plaintext
/
├── docker-compose.yml       # Definizione dell'infrastruttura containerizzata
├── schema.sql               # DDL per la creazione della tabella di staging
├── ingestion_pipeline.py    # Script Python principale per l'ETL in tempo reale
├── requirements.txt         # Dipendenze Python (websockets, psycopg2-binary)
└── README.md                # Documentazione di progetto
🚀 Fase 1: Data Ingestion e Setup Infrastrutturale (Completata)
In questa prima fase ho progettato e implementato l'intera pipeline di acquisizione dati (Ingestion), partendo dall'infrastruttura fino alla scrittura in database.

1. Infrastruttura Containerizzata

Ho scelto di containerizzare l'ambiente di database tramite Docker per garantire la totale riproducibilità del sistema, isolandolo dal sistema operativo host. Ho sviluppato il seguente docker-compose.yml:

YAML
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
Nota tecnica: Ho adottato PostgreSQL 18 per testare le nuove performance di I/O asincrono. Questo ha richiesto la mappatura del volume direttamente su /var/lib/postgresql per mantenere la compatibilità con il nuovo sistema di gestione dei metadati della v18.

2. Data Definition Language (DDL)

Ho modellato la tabella di atterraggio (Staging) per accogliere i dati grezzi in tempo reale. Ho tipizzato le coordinate con DECIMAL(9,6) per ottenere una tolleranza spaziale di circa 11 cm, fondamentale per le successive logiche di geofencing al molo.

SQL
CREATE TABLE staging_ais_data (
    mmsi VARCHAR(20),
    ship_name TEXT,
    terminal_zona TEXT,
    lat DECIMAL(9,6),
    lon DECIMAL(9,6),
    timestamp_utc TIMESTAMP
);
3. Pipeline ETL in Python

Ho sviluppato lo script ingestion_pipeline.py per connettersi in streaming WebSocket all'API di AISStream. Lo script esegue le seguenti operazioni in volo (In-Flight Processing):

Estrazione e Filtraggio: Richiede solo i messaggi di tipo "PositionReport" all'interno di specifiche Bounding Boxes (Genova e Vado Ligure).

Trasformazione (Geofencing): Valuta latitudine e longitudine in real-time, assegnando un'etichetta di zona (es. GENOVA_VOLTRI, VADO_GATEWAY) tramite poligoni predefiniti a livello di codice.

Data Cleansing: Intercetta e normalizza i timestamp. Ho implementato un blocco che tronca i nanosecondi forniti dall'API (.split('.')[0]) per renderli compatibili con lo standard ISO richiesto da PostgreSQL.

Caricamento Sicuro: Scrive i record nel container Postgres utilizzando query SQL parametrizzate tramite psycopg2 per prevenire vulnerabilità di injection.

⚙️ Guida all'Installazione e Avvio Rapido
Per replicare l'ambiente di sviluppo in locale, seguire questi passaggi:

Avvio Infrastruttura Docker
Avviare il demone Docker e lanciare i container in background:

Bash
docker-compose up -d
Creazione Tabella
Accedere a pgAdmin (http://localhost:8080), registrare il server puntando all'host db_tesi ed eseguire lo script schema.sql all'interno del database logistica_liguria.

Setup Ambiente Python
Per rispettare le restrizioni PEP 668 su macOS, creare e attivare un ambiente virtuale isolato:

Bash
python3 -m venv tesi_env
source tesi_env/bin/activate
pip install websockets psycopg2-binary
Avvio Ingestion
Lanciare l'ascolto in tempo reale:

Bash
python3 ingestion_pipeline.py
🔜 Fasi Successive del Progetto
[ ] Fase 2: Processing & Business Logic (SQL)

Pianificato: Sviluppo di viste e procedure SQL per il calcolo del Time in Port e del Time in Rada.

[ ] Fase 3: Orchestrazione (Apache Airflow)

Pianificato: Schedulazione dei processi ETL batch.

[ ] Fase 4: Data Visualization (PowerBI)

Pianificato: Sviluppo dashboard aziendale con metriche di Congestione e allarmi di Overstay.