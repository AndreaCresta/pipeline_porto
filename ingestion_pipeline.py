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
DB_PASS = "password_sicura"

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

    async with websockets.connect(url) as websocket:
        await websocket.send(json.dumps(subscribe_message))
        print("📡 Lavoratore A: Connesso all'API! Inizio a riempire la coda...\n" + "-"*50)

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