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

    # TASK 1: Pulizia (corretto per essere più robusto)
    pulisci_coordinate_nulle = SQLExecuteQueryOperator(
        task_id='pulisci_coordinate_nulle',
        conn_id='connessione_db_tesi', 
        sql="DELETE FROM staging_ais_data WHERE lat IS NULL OR lon IS NULL OR mmsi IS NULL;"
    )

    # TASK 2: Deduplicazione
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

    # TASK 3A: Aggiorna Navi (Sistemata con DISTINCT ON)
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

    # TASK 3B: Aggiorna Terminal
    aggiorna_dim_terminal = SQLExecuteQueryOperator(
        task_id='aggiorna_dim_terminal',
        conn_id='connessione_db_tesi', 
        sql="""
        INSERT INTO dim_terminal (codice_zona, nome_esteso)
        SELECT DISTINCT terminal_zona, terminal_zona
        FROM staging_ais_data
        WHERE terminal_zona != 'ALTRO_LIGURIA' AND terminal_zona IS NOT NULL
        ON CONFLICT (codice_zona) DO NOTHING;
        """
    )

    # TASK 4: Arrivi (Sistemata con ON CONFLICT corretta)
    aggiorna_fact_movimenti = SQLExecuteQueryOperator(
        task_id='aggiorna_fact_movimenti',
        conn_id='connessione_db_tesi', 
        sql="""
        INSERT INTO fact_movimenti (mmsi, codice_zona, orario_arrivo)
        SELECT DISTINCT ON (mmsi, terminal_zona, timestamp_utc)
            mmsi, terminal_zona, timestamp_utc
        FROM staging_ais_data
        WHERE terminal_zona != 'ALTRO_LIGURIA'
        ORDER BY mmsi, terminal_zona, timestamp_utc ASC
        ON CONFLICT DO NOTHING;
        """
    )

    # TASK 5: IL TASK MANCANTE - Registra le partenze
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
          AND f.orario_partenza IS NULL;
        """
    )

    # TASK 6: Aggiorna i KPI per Power BI (Refresh Viste Materializzate)
    aggiorna_kpi_bi = SQLExecuteQueryOperator(
        task_id='aggiorna_kpi_bi',
        conn_id='connessione_db_tesi', 
        sql="""
        REFRESH MATERIALIZED VIEW mv_kpi_tempi_porto;
        REFRESH MATERIALIZED VIEW mv_kpi_confronto_terminal;
        """
    )

    # Esecuzione logica
    pulisci_coordinate_nulle >> deduplica_staging >> [aggiorna_dim_navi, aggiorna_dim_terminal] >> aggiorna_fact_movimenti >> aggiorna_partenze >> aggiorna_kpi_bi