import asyncio
import websockets
import json
import psycopg2
import ssl
import certifi
from psycopg2 import Error
from psycopg2.extras import execute_values 

# --- 1. CONFIGURAZIONE DATABASE E CODA ---
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "logistica_liguria"
DB_USER = "admin_tesi"
DB_PASS = "password_sicura"

BATCH_SIZE = 100  
QUEUE = asyncio.Queue() 

# --- 2. LOGICA DI BUSINESS (Geofencing) ---
def identifica_terminal(lat, lon):
    if 44.410 <= lat <= 44.435 and 8.740 <= lon <= 8.800:
        return 'GENOVA_VOLTRI'
    elif 44.260 <= lat <= 44.285 and 8.430 <= lon <= 8.460:
        return "VADO_GATEWAY"
    elif 44.395 <= lat <= 44.415 and 8.870 <= lon <= 8.910:
        return "GENOVA_SAMPIERDARENA"
    return "ALTRO_LIGURIA"

# --- 3A. Lavoratore A (RICEVITORE: API -> CODA) ---
async def websocket_listener():
    # Usiamo la tua API KEY valida
    api_key = "e54f403f88919f11189fe244adbf1f2fc96993d8"
    url = "wss://stream.aisstream.io/v0/stream"

    subscribe_message = {
        "APIKey": api_key,
        "BoundingBoxes": [
            [[44.380, 8.700], [44.450, 8.950]],
            [[44.240, 8.400], [44.300, 8.500]]
        ],
        "FilterMessageTypes": ["PositionReport"]
    }

    # CONFIGURAZIONE SSL PER MAC (Risolve il problema dell'handshake)
    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    while True:
        try:
            print("📡 Lavoratore A: Tentativo di connessione all'API AISStream...")
            
            # Connessione con timeout esteso e SSL bypass
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
                    
                    # --- LISTA NERA MMSI ---
                    # Navi di servizio portuale identificate manualmente tramite MarineTraffic
                    # e verificate per tipo: rimorchiatori, draghe, navi antinquinamento,
                    # salvage/rescue vessel, utility vessel, passenger locali.
                    MMSI_BLACKLIST = {
                        247539300,  # VB INSIGNIA        — Rimorchiatore
                        247317800,  # SAN GENNARO PRIMO  — Rimorchiatore
                        247301200,  # G.LORIS            — Rimorchiatore
                        256004628,  # OFFSHORE PROGRESS  — High Speed Craft
                        247287200,  # BREZZAMARE         — Rimorchiatore
                        247337400,  # CAVALIER SERGIO M. — Rimorchiatore
                        247453600,  # ECO NAPOLI         — Nave di servizio
                        247317300,  # SANT'AGOSTINO      — Rimorchiatore
                        247317700,  # SAN ANTONIO        — Rimorchiatore
                        247062300,  # SAN MARCO SECONDO  — Rimorchiatore
                        247245100,  # NINO I             — Rimorchiatore
                        247244200,  # SAN MARCO I        — Rimorchiatore
                        256003619,  # VOE JARL           — Rimorchiatore
                        215548000,  # FABIO DUO'         — Draga (Hopper Dredger)
                        247277900,  # REDEEMER           — Nave antinquinamento
                        228376800,  # JIF HELIOS         — Utility Vessel
                        247338600,  # TECNE              — Tanker bunkeraggio
                        247382900,  # ITALIA             — Rimorchiatore (Towing Vessel)
                        247209800,  # MAREXPRESS         — Passenger (battello locale)
                        247423700,  # CAPO VADO          — Salvage/Rescue Vessel
                        247377400,  # SABATIA            — Passenger (battello locale)
                        247299200,  # (senza nome)       — Nave di servizio non identificata
                    }

                    if mmsi and lat and lon and int(mmsi) not in MMSI_BLACKLIST:
                        zona = identifica_terminal(lat, lon)
                        record = (mmsi, ship_name, zona, lat, lon, time_utc)
                        await QUEUE.put(record)
                    
        except Exception as e:
            print(f"⚠️ Lavoratore A: Errore di connessione ({e}). Riprovo tra 10 secondi...")
            await asyncio.sleep(10)

# --- 3B. Lavoratore B (SCRITTORE: CODA -> DATABASE) ---
async def db_writer():
    connessione = None
    try:
        print("🔄 Lavoratore B: Connessione al database PostgreSQL...")
        connessione = psycopg2.connect(
            host=DB_HOST, 
            port=DB_PORT, 
            database=DB_NAME, 
            user=DB_USER, 
            password=DB_PASS
        )
        cursore = connessione.cursor()
        print("✅ Lavoratore B: Database connesso!")

        while True:
            # Prende il primo record disponibile
            record = await QUEUE.get()
            batch = [record]
            
            # Riempie il batch se ci sono altri record pronti
            while len(batch) < BATCH_SIZE and not QUEUE.empty():
                batch.append(QUEUE.get_nowait())
            
            sql_insert = """
                INSERT INTO staging_ais_data (mmsi, ship_name, terminal_zona, lat, lon, timestamp_utc)
                VALUES %s;
            """
            execute_values(cursore, sql_insert, batch)
            connessione.commit()
            
            print(f"💾 Salvato blocco di {len(batch)} navi. (In coda: {QUEUE.qsize()})")

    except Error as e:
        print(f"❌ Errore del Database: {e}")
    finally:
        if connessione:
            connessione.close()
            print("🔒 Connessione chiusa.")

# --- 4. AVVIO ---
async def main():
    await asyncio.gather(
        websocket_listener(),
        db_writer()
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Monitoraggio interrotto.")