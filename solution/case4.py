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

base_dir = os.path.dirname(os.path.abspath(__file__))
taxi_file = os.path.join(base_dir, "../data/raw/taxi_data_today.parquet")
zone_file = os.path.join(base_dir, "../data/raw/updated_zones.csv")
processed_dir = os.path.join(base_dir, "../data/processed")
os.makedirs(processed_dir, exist_ok=True)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 15),
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
    'email': ['your.email@example.com'],
    'email_on_failure': True,
    'email_on_retry': False
}

with DAG(
    dag_id='case4',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    description='Paralleler ETL-Workflow mit zwei Datenquellen und Reporting',
) as dag:

    # Sensors
    wait_for_taxi = FileSensor(
        task_id='wait_for_taxi_file',
        filepath=taxi_file,
        poke_interval=30,
        timeout=3600,
        mode='reschedule'
    )

    wait_for_zone = FileSensor(
        task_id='wait_for_zone_file',
        filepath=zone_file,
        poke_interval=30,
        timeout=3600,
        mode='reschedule'
    )

    # Ladefunktionen
    def load_taxi_data(**kwargs):
        df = pd.read_parquet(taxi_file)
        df.to_parquet(os.path.join(processed_dir, "taxi_loaded.parquet"), index=False)

    def load_zone_data(**kwargs):
        df = pd.read_csv(zone_file)
        df.to_csv(os.path.join(processed_dir, "zones_loaded.csv"), index=False)

    load_taxi = PythonOperator(
        task_id='load_taxi_data',
        python_callable=load_taxi_data
    )

    load_zone = PythonOperator(
        task_id='load_zone_data',
        python_callable=load_zone_data
    )

    # Join (ausgelagert in utils)
    join_data = PythonOperator(
        task_id='join_taxi_with_zones',
        python_callable=case4_join_taxi_with_zones
    )

    # Qualitätsprüfung
    check_quality = PythonOperator(
        task_id='data_quality_check',
        python_callable=case4_check_data_quality
    )

    # Aggregation / Reporting
    def create_report(**kwargs):
        file_path = os.path.join(processed_dir, "joined_data.parquet")
        df = pd.read_parquet(file_path)
        result = df.groupby("Zone").agg(
            fahrten_anzahl=("trip_distance", "count"),
            durchschnittspreis=("total_amount", "mean")
        ).reset_index()

        output_csv = os.path.join(processed_dir, "zone_report.csv")
        result.to_csv(output_csv, index=False)
        print("📊 Report gespeichert:", output_csv)

    report_task = PythonOperator(
        task_id='generate_report',
        python_callable=create_report
    )

    #email_task = EmailOperator(
    #    task_id='send_success_email',
    #    to='your.email@example.com',
    #    subject='🚀 Airflow Case4 Report abgeschlossen',
    #    html_content="<p>ETL-Pipeline erfolgreich durchgelaufen. Der Report ist verfügbar.</p>"
    #)

    # Ablauf definieren
    [wait_for_taxi >> load_taxi, wait_for_zone >> load_zone] >> join_data
    join_data >> check_quality >> report_task # >> email_task