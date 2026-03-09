from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta, timezone, date
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'andrea',
    'start_date': datetime(2023, 1, 1, tzinfo=timezone.utc),
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

    def gestione_partizioni():
        import psycopg2
        from datetime import date
        from dateutil.relativedelta import relativedelta
        
        # Gestiamo due mesi: Corrente e Prossimo
        mesi_da_controllare = [
            date.today().replace(day=1), # Mese corrente (Marzo)
            date.today().replace(day=1) + relativedelta(months=1) # Mese prossimo (Aprile)
        ]
        
        # Connessione usando il nome del servizio docker 'postgres_porto'
        conn = psycopg2.connect(
            host="postgres_porto", 
            port="5432",
            database="logistica_liguria",
            user="admin_tesi", 
            password="password_sicura"
        )
        conn.autocommit = True
        cur = conn.cursor()
        
        for mese in mesi_da_controllare:
            mese_successivo = mese + relativedelta(months=1)
            nome_tabella = f"staging_ais_data_y{mese.strftime('%Y')}_m{mese.strftime('%m')}"
            data_inizio = mese.strftime('%Y-%m-01')
            data_fine = mese_successivo.strftime('%Y-%m-01')
            
            query = f"""
                CREATE TABLE IF NOT EXISTS {nome_tabella} PARTITION OF staging_ais_data
                FOR VALUES FROM ('{data_inizio}') TO ('{data_fine}');
            """
            cur.execute(query)
            print(f"✅ Verifica partizione: {nome_tabella} ({data_inizio} -> {data_fine})")
            
        cur.close()
        conn.close()

    # Task 0: Assicura che le tabelle esistano PRIMA di pulirle
    auto_creazione_partizione = PythonOperator(
        task_id='auto_creazione_partizione',
        python_callable=gestione_partizioni,
    )

    # TASK 1: Pulizia
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

    # TASK 3A: Aggiorna Navi
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

    # TASK 3B: Aggiorna Terminal (Corretto con nome_esteso)
    aggiorna_dim_terminal = SQLExecuteQueryOperator(
        task_id='aggiorna_dim_terminal',
        conn_id='connessione_db_tesi', 
        sql="""
        INSERT INTO dim_terminal (codice_zona, nome_esteso, citta)
        SELECT DISTINCT terminal_zona, terminal_zona, 'Da definire'
        FROM staging_ais_data
        WHERE terminal_zona != 'ALTRO_LIGURIA' AND terminal_zona IS NOT NULL
        ON CONFLICT (codice_zona) DO NOTHING;
        """
    )

    # TASK 4: Calcola gli arrivi
    aggiorna_fact_movimenti = SQLExecuteQueryOperator(
        task_id='aggiorna_fact_movimenti',
        conn_id='connessione_db_tesi', 
        sql="""
        INSERT INTO fact_movimenti (mmsi, codice_zona, orario_arrivo)
        SELECT mmsi, terminal_zona, orario_arrivo
        FROM (
            SELECT 
                mmsi,
                terminal_zona,
                timestamp_utc AS orario_arrivo,
                LAG(terminal_zona) OVER (PARTITION BY mmsi ORDER BY timestamp_utc) as zona_precedente
            FROM staging_ais_data
            WHERE mmsi IS NOT NULL
        ) sub
        WHERE terminal_zona != 'ALTRO_LIGURIA' 
          AND terminal_zona IS DISTINCT FROM zona_precedente
        ON CONFLICT (mmsi, orario_arrivo) DO NOTHING;
        """
    )

    # TASK 5: Calcola le partenze
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
          AND f.orario_partenza IS NULL
          AND sub.ultima_visto > f.orario_arrivo;
        """
    )

    # TASK 6: Refresh Viste
    aggiorna_kpi_bi = SQLExecuteQueryOperator(
        task_id='aggiorna_kpi_bi',
        conn_id='connessione_db_tesi', 
        sql="""
        REFRESH MATERIALIZED VIEW mv_kpi_confronto_terminal;
        """
    )

    # Workflow
    auto_creazione_partizione >> pulisci_coordinate_nulle >> deduplica_staging >> [aggiorna_dim_navi, aggiorna_dim_terminal] >> aggiorna_fact_movimenti >> aggiorna_partenze >> aggiorna_kpi_bi