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
    'pipeline_mar_ligure_v2', # L'ho chiamato v2 così lo riconosciamo subito!
    default_args=default_args,
    description='Pipeline ETL per i terminal di Genova e Vado',
    schedule='@hourly', # <-- Risolto il primo warning
    catchup=False,
    tags=['logistica', 'tesi'],
) as dag:

    # TASK 1: Svuotare e ricaricare la Fact Table
    aggiorna_fact_movimenti = SQLExecuteQueryOperator( # <-- Risolto il secondo warning
        task_id='aggiorna_fact_movimenti',
        conn_id='connessione_db_tesi', 
        sql="""
        TRUNCATE TABLE fact_movimenti;
        
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

    aggiorna_fact_movimenti