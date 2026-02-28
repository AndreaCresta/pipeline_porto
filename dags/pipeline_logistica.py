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

    # 1. PULIZIA: Rimuoviamo dati sporchi
    pulisci_dati = SQLExecuteQueryOperator(
        task_id='pulisci_dati',
        conn_id='connessione_db_tesi', 
        sql="DELETE FROM staging_ais_data WHERE lat IS NULL OR lon IS NULL OR mmsi IS NULL;"
    )

    # 2. DIMENSIONI: Aggiorniamo le anagrafiche (Nomi navi e Terminal)
    aggiorna_dimensioni = SQLExecuteQueryOperator(
        task_id='aggiorna_dimensioni',
        conn_id='connessione_db_tesi', 
        sql="""
        -- Aggiorna Navi
        INSERT INTO dim_navi (mmsi, ship_name)
        SELECT DISTINCT ON (mmsi) mmsi, ship_name FROM staging_ais_data 
        ORDER BY mmsi, timestamp_utc DESC
        ON CONFLICT (mmsi) DO UPDATE SET ship_name = EXCLUDED.ship_name;

        -- Aggiorna Terminal
        INSERT INTO dim_terminal (codice_zona, nome_esteso)
        SELECT DISTINCT terminal_zona, terminal_zona FROM staging_ais_data
        WHERE terminal_zona != 'ALTRO_LIGURIA'
        ON CONFLICT (codice_zona) DO NOTHING;
        """
    )

    # 3. FATTI - ARRIVI: Registra quando una nave entra nel terminal
    # Nota: Usiamo una sottostima per prendere solo il PRIMO segnale utile
    registra_arrivi = SQLExecuteQueryOperator(
        task_id='registra_arrivi',
        conn_id='connessione_db_tesi', 
        sql="""
        INSERT INTO fact_movimenti (mmsi, codice_zona, orario_arrivo)
        SELECT DISTINCT ON (mmsi, terminal_zona) mmsi, terminal_zona, timestamp_utc
        FROM staging_ais_data
        WHERE terminal_zona != 'ALTRO_LIGURIA'
        ORDER BY mmsi, terminal_zona, timestamp_utc ASC
        ON CONFLICT (mmsi, codice_zona, orario_arrivo) DO NOTHING;
        """
    )

    # 4. FATTI - PARTENZE: Questo è il pezzo mancante! 
    # Cerca l'ultimo segnale nel terminal prima che la nave torni in "ALTRO_LIGURIA"
    registra_partenze = SQLExecuteQueryOperator(
        task_id='registra_partenze',
        conn_id='connessione_db_tesi', 
        sql="""
        UPDATE fact_movimenti f
        SET orario_partenza = sub.ultima_visto
        FROM (
            SELECT s1.mmsi, s1.terminal_zona, MAX(s1.timestamp_utc) as ultima_visto
            FROM staging_ais_data s1
            WHERE s1.terminal_zona != 'ALTRO_LIGURIA'
            AND EXISTS (
                -- La nave deve avere un segnale successivo fuori dal terminal
                SELECT 1 FROM staging_ais_data s2 
                WHERE s2.mmsi = s1.mmsi 
                AND s2.timestamp_utc > s1.timestamp_utc 
                AND s2.terminal_zona = 'ALTRO_LIGURIA'
            )
            GROUP BY s1.mmsi, s1.terminal_zona
        ) sub
        WHERE f.mmsi = sub.mmsi 
          AND f.codice_zona = sub.terminal_zona 
          AND f.orario_partenza IS NULL;
        """
    )

    pulisci_dati >> aggiorna_dimensioni >> registra_arrivi >> registra_partenze