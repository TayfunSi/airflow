from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta
import pandas as pd
import os
import sys

sys.path.append(os.path.dirname(__file__))
from utils import case4_join_taxi_with_zones, case4_check_data_quality

# Pfad-Konfiguration (basierend auf deiner Funktion)
base_dir = os.path.dirname(os.path.abspath(__file__))  # Verzeichnis der DAG-Datei
raw_file_path = os.path.join(base_dir, "../data/raw/taxi_data_today.parquet")  # Relativer Pfad zum Raw-File

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 15),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'email': ['tayfun.simsek@mhp.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

with DAG(
    dag_id='case4',
    default_args=default_args,
    schedule_interval=None,
    catchup=True,
    max_active_runs=1,
    description='Kompletter ETL-Workflow: Trigger, Filter, Join, Data Quality & Email',
) as dag:

    wait_for_raw_file = FileSensor(
        task_id='wait_for_raw_parquet',
        filepath=raw_file_path,
        poke_interval=30,
        timeout=60*60,
        mode='reschedule'
    )

    def filter_taxi_data_daily(execution_date, **kwargs):
        input_path = raw_file_path
        output_dir = os.path.join(base_dir, "../data/processed")
        os.makedirs(output_dir, exist_ok=True)

        year = execution_date.year
        month = execution_date.month
        day = execution_date.day

        output_path = f"{output_dir}/taxi_data_{year}-{month:02d}-{day:02d}.parquet"

        df = pd.read_parquet(input_path)
        df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])

        df_filtered = df[
            (df["tpep_pickup_datetime"].dt.date == execution_date.date())
        ]

        df_filtered.to_parquet(output_path, index=False)
        print(f"✅ Gefiltert: {len(df_filtered)} Zeilen für {year}-{month:02d}-{day:02d}")

    filter_task = PythonOperator(
        task_id='filter_taxi_data',
        python_callable=filter_taxi_data_daily,
        provide_context=True
    )

    join_task = PythonOperator(
        task_id='join_with_zone_data',
        python_callable=case4_join_taxi_with_zones
    )

    quality_check_task = PythonOperator(
        task_id='data_quality_check',
        python_callable=case4_check_data_quality
    )

    email_task = EmailOperator(
        task_id='send_success_email',
        to='tayfun.simsek@mhp.com',
        subject='Airflow ETL Job erfolgreich abgeschlossen',
        html_content="""<h3>Der ETL-Job <b>case4_full_etl_with_email</b> wurde erfolgreich ausgeführt.</h3>
                        <p>Monatliche Verarbeitung abgeschlossen.</p>"""
    )

    wait_for_raw_file >> filter_task >> join_task >> quality_check_task # >> email_task