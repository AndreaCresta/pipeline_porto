from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'andrea',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'pipeline_mar_ligure_completa', 
    default_args=default_args,
    description='Pipeline ETL completa: Cleansing, Dimensioni e Fatti',
    schedule='*/5 * * * *',
    catchup=False,
    tags=['logistica', 'tesi', 'produzione'],
) as dag:

    # TASK 1: Pulizia coordinate mancanti (dal Readme)
    pulisci_coordinate_nulle = SQLExecuteQueryOperator(
        task_id='pulisci_coordinate_nulle',
        conn_id='connessione_db_tesi', 
        sql="""
        DELETE FROM staging_ais_data
        WHERE lat IS NULL OR lon IS NULL;
        """
    )

    # TASK 2: Deduplicazione tecnica (dal Readme)
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

    # TASK 3A: Aggiorna l'anagrafica delle navi
    aggiorna_dim_navi = SQLExecuteQueryOperator(
        task_id='aggiorna_dim_navi',
        conn_id='connessione_db_tesi', 
        sql="""
        INSERT INTO dim_navi (mmsi, ship_name)
        SELECT mmsi, MAX(ship_name) AS ship_name
        FROM staging_ais_data
        WHERE mmsi IS NOT NULL
        GROUP BY mmsi
        ON CONFLICT (mmsi) DO NOTHING;
        """
    )

    # TASK 3B: Aggiorna le zone portuali
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

    # TASK 4: Calcola gli arrivi e le partenze
    aggiorna_fact_movimenti = SQLExecuteQueryOperator(
        task_id='aggiorna_fact_movimenti',
        conn_id='connessione_db_tesi', 
        sql="""
        TRUNCATE TABLE fact_movimenti CASCADE;
        
        INSERT INTO fact_movimenti (mmsi, codice_zona, orario_arrivo, orario_partenza)
        WITH cambi_stato AS (
            SELECT 
                mmsi,
                terminal_zona,
                timestamp_utc,
                LAG(terminal_zona) OVER (PARTITION BY mmsi ORDER BY timestamp_utc) as zona_precedente
            FROM staging_ais_data
            WHERE mmsi IS NOT NULL
        ),
        arrivi_partenze AS (
            SELECT 
                mmsi,
                terminal_zona,
                timestamp_utc AS orario_arrivo,
                LEAD(timestamp_utc) OVER (PARTITION BY mmsi ORDER BY timestamp_utc) AS orario_partenza
            FROM cambi_stato
            WHERE terminal_zona IS DISTINCT FROM zona_precedente
        )
        SELECT 
            mmsi,
            terminal_zona AS codice_zona,
            orario_arrivo,
            orario_partenza
        FROM arrivi_partenze
        WHERE terminal_zona != 'ALTRO_LIGURIA' 
          AND orario_partenza IS NOT NULL;
        """
    )

    # Esecuzione logica: Pulizia -> Aggiornamento Dimensioni -> Calcolo Fatti
    pulisci_coordinate_nulle >> deduplica_staging >> [aggiorna_dim_navi, aggiorna_dim_terminal] >> aggiorna_fact_movimenti