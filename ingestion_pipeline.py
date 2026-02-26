import asyncio
import websockets
import json
import psycopg2
from psycopg2 import Error

# --- 1. CONFIGURAZIONE DATABASE (Docker) ---
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "logistica_liguria"
DB_USER = "admin_tesi"
DB_PASS = "password_sicura"

# --- 2. LOGICA DI BUSINESS (Geofencing) ---
def identifica_terminal(lat, lon):
    # Geofencing Genova PSA Pra' (Voltri)
    if 44.420 <= lat <= 44.435 and 8.740 <= lon <= 8.785:
        return "GENOVA_VOLTRI"
    # Geofencing Savona - Vado Gateway
    elif 44.260 <= lat <= 44.285 and 8.430 <= lon <= 8.460:
        return "VADO_GATEWAY"
    # Altre zone di Genova (Sampierdarena/Centro)
    elif 44.395 <= lat <= 44.415 and 8.870 <= lon <= 8.910:
        return "GENOVA_SAMPIERDARENA"
    return "ALTRO_LIGURIA"

# --- 3. PIPELINE DI INGESTION (API -> SQL) ---
async def get_port_data():
    api_key = "b7f29d875e4c6ccd444fd2c0e1d22a2a0ef57f9a"
    url = "wss://stream.aisstream.io/v0/stream"

    subscribe_message = {
        "APIKey": api_key,
        "BoundingBoxes": [
            [[44.380, 8.700], [44.450, 8.950]], # Area Genova
            [[44.240, 8.400], [44.300, 8.500]]  # Area Savona/Vado
        ],
        "FilterMessageTypes": ["PositionReport"]
    }

    try:
        # Connessione al Database locale (Docker)
        print("🔄 Connessione al database PostgreSQL in corso...")
        connessione = psycopg2.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASS)
        cursore = connessione.cursor()
        print("✅ Database connesso con successo!")

        # Connessione al WebSocket (API AIS)
        async with websockets.connect(url) as websocket:
            await websocket.send(json.dumps(subscribe_message))
            print("📡 Connesso all'API! In attesa delle navi liguri...\n" + "-"*50)

            # Loop infinito che ascolta i dati in tempo reale
            async for message in websocket:
                data = json.loads(message)
                
                # Estrazione
                mmsi = data['MetaData']['MMSI']
                ship_name = data['MetaData']['ShipName']
                lat = data['MetaData']['latitude']
                lon = data['MetaData']['longitude']
                time_utc = data['MetaData']['time_utc'].split('.')[0]

                # Trasformazione (Aggiungiamo la nostra etichetta)
                zona = identifica_terminal(lat, lon)

                # Caricamento (Load in SQL)
                sql_insert = """
                    INSERT INTO staging_ais_data (mmsi, ship_name, terminal_zona, lat, lon, timestamp_utc)
                    VALUES (%s, %s, %s, %s, %s, %s);
                """
                # Usiamo le variabili parametrizzate (%s) per sicurezza e stabilità
                cursore.execute(sql_insert, (mmsi, ship_name, zona, lat, lon, time_utc))
                
                # Salviamo fisicamente il dato nel disco
                connessione.commit()

                # Print a schermo per darti un feedback visivo
                print(f"💾 Salvato nel DB: [{zona}] {ship_name} (MMSI: {mmsi})")

    except Error as e:
        print(f"❌ Errore del Database: {e}")
    except Exception as e:
        print(f"❌ Errore generico: {e}")
    finally:
        # Chiusura sicura delle porte quando spegni lo script
        if 'connessione' in locals() and connessione:
            cursore.close()
            connessione.close()
            print("\n🔒 Connessione al database chiusa in modo sicuro.")

# --- 4. AVVIO DEL PROGRAMMA ---
if __name__ == "__main__":
    try:
        # Se usi Jupyter Notebook, ricorda di usare: await get_port_data()
        asyncio.run(get_port_data())
    except KeyboardInterrupt:
        print("\n🛑 Monitoraggio interrotto manualmente.")