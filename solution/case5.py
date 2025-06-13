from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from datetime import datetime, timedelta
import pandas as pd
import os
import sys

sys.path.append(os.path.dirname(__file__))
from utils import case5_join_all_data

base_dir = os.path.dirname(os.path.abspath(__file__))
taxi_file = os.path.join(base_dir, "../data/raw/taxi_data.parquet")
zone_file = os.path.join(base_dir, "../data/raw/zones.csv")
processed_dir = os.path.join(base_dir, "../data/processed/case5")
os.makedirs(processed_dir, exist_ok=True)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 15),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'email': ['your.email@example.com'],
    'email_on_failure': True,
    'email_on_retry': False
}

with DAG(
    dag_id='case5',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    description='Erweiterter Case5: mehrere Reports aus Taxi-Daten generieren',
) as dag:

    wait_for_taxi = FileSensor(
        task_id='wait_for_taxi_file',
        filepath=taxi_file,
        poke_interval=30,
        timeout=600,
        mode='reschedule'
    )

    wait_for_zone = FileSensor(
        task_id='wait_for_zone_file',
        filepath=zone_file,
        poke_interval=30,
        timeout=600,
        mode='reschedule'
    )

    def generate_advanced_reports(**kwargs):
        df = case5_join_all_data(taxi_file, zone_file)

        df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
        df['hour'] = df['tpep_pickup_datetime'].dt.hour
        df['trip_km'] = df['trip_distance'] * 1.60934  # in km

        night = df[df['hour'].between(0, 5)]
        long = df[df['trip_km'] > 10]
        rush = df[df['hour'].isin([7,8,9,16,17,18,19])]

        night_dir = os.path.join(processed_dir, "night_trips")
        long_dir = os.path.join(processed_dir, "long_trips")
        rush_dir = os.path.join(processed_dir, "rush_hour")

        os.makedirs(night_dir, exist_ok=True)
        os.makedirs(long_dir, exist_ok=True)
        os.makedirs(rush_dir, exist_ok=True)

        night.to_csv(os.path.join(night_dir, "night_trips.csv"), index=False)
        long.to_csv(os.path.join(long_dir, "long_trips.csv"), index=False)
        rush.to_csv(os.path.join(rush_dir, "rush_hour_trips.csv"), index=False)

        with open(os.path.join(processed_dir, "summary.log"), "w") as f:
            f.write(f"Total records: {len(df)}\n")
            f.write(f"Night trips: {len(night)}\n")
            f.write(f"Long trips (>10km): {len(long)}\n")
            f.write(f"Rush hour trips: {len(rush)}\n")

        print("✅ Reports erstellt und Logs geschrieben.")

    report_task = PythonOperator(
        task_id='generate_advanced_reports',
        python_callable=generate_advanced_reports
    )

    [wait_for_taxi, wait_for_zone] >> report_task
