from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta

# Definiamo i parametri di base del nostro DAG
default_args = {
    'owner': 'andrea',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1), # Data fittizia di inizio
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Creiamo il DAG: girerà ogni ora ('@hourly')
with DAG(
    'pipeline_mar_ligure_v1',
    default_args=default_args,
    description='Pipeline ETL per i terminal di Genova e Vado',
    schedule_interval='@hourly',
    catchup=False,
    tags=['logistica', 'tesi'],
) as dag:

    # TASK 1: Svuotare e ricaricare la Fact Table
    # (Usiamo truncate per non duplicare i dati con le nuove run)
    aggiorna_fact_movimenti = PostgresOperator(
        task_id='aggiorna_fact_movimenti',
        postgres_conn_id='connessione_db_tesi', # Lo configureremo nell'interfaccia web
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

    # Qui in futuro potremmo aggiungere altri task (es. pulizia vecchi dati)
    # Per ora eseguiamo solo l'aggiornamento
    aggiorna_fact_movimenti