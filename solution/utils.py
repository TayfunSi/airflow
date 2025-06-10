# dags/utils.py
from datetime import timedelta
import pandas as pd
import logging
import os

def case1_copy_taxi_zone_manually():
    # Basisverzeichnis: Ort der aktuellen Datei (z.B. dags/utils.py)
    base_dir = os.path.dirname(os.path.abspath(__file__))

    # Pfade relativ zum Basisverzeichnis
    raw_path = os.path.join(base_dir, "../data/raw/zones.csv")
    processed_dir = os.path.join(base_dir, "../data/processed")
    processed_file = os.path.join(processed_dir, "zones_copied.csv")

    os.makedirs(processed_dir, exist_ok=True)
    df = pd.read_csv(raw_path)
    df.to_csv(processed_file, index=False)

    logging.info(f"✅ Datei kopiert von {raw_path} nach {processed_file}")


def case2_copy_taxi_zone_on_file_entry():
    # Basisverzeichnis: Ort der aktuellen Datei (z.B. dags/utils.py)
    base_dir = os.path.dirname(os.path.abspath(__file__))

    # Pfade relativ zum Basisverzeichnis
    raw_path = os.path.join(base_dir, "../data/raw/zones_13062025.csv")
    processed_dir = os.path.join(base_dir, "../data/processed")
    processed_file = os.path.join(processed_dir, "zones_13062025.csv")
    
    os.makedirs(processed_dir, exist_ok=True)
    df = pd.read_csv(raw_path)
    df.to_csv(processed_file, index=False)


def case3_join_taxi_with_zones(execution_date=None):
    # Basisverzeichnis: Ort der aktuellen Datei (z.B. dags/utils.py)
    base_dir = os.path.dirname(os.path.abspath(__file__))


    # Dynamisch Jahr und Monat vom execution_date ermitteln
    first_of_month = execution_date.replace(day=1)
    prev_month_end = first_of_month - timedelta(days=1)
    year = prev_month_end.year
    month = prev_month_end.month

    taxi_path = os.path.join(base_dir, f"../data/processed/taxi_data_{year}-{month:02d}.parquet")
    zone_path = os.path.join(base_dir, "../data/raw/zones.csv")
    output_path = os.path.join(base_dir, f"../data/processed/taxi_data_{year}-{month:02d}_enriched.parquet")

    df_taxi = pd.read_parquet(taxi_path)
    df_zone = pd.read_csv(zone_path)

    # Join für Pickup-Location
    df_joined = df_taxi.merge(
        df_zone[["LocationID", "Borough", "Zone"]],
        how="left",
        left_on="PULocationID",
        right_on="LocationID"
    ).rename(columns={
        "Borough": "PUBorough",
        "Zone": "PUZone"
    }).drop("LocationID", axis=1)

    # Join für Dropoff-Location
    df_joined = df_joined.merge(
        df_zone[["LocationID", "Borough", "Zone"]],
        how="left",
        left_on="DOLocationID",
        right_on="LocationID"
    ).rename(columns={
        "Borough": "DOBorough",
        "Zone": "DOZone"
    }).drop("LocationID", axis=1)

    os.makedirs("data/processed", exist_ok=True)
    df_joined.to_parquet(output_path, index=False)
    print(f"✅ Enriched Dataset gespeichert unter: {output_path}")


def case4_join_taxi_with_zones(execution_date=None, **kwargs):
    """
    Joins die tagesgefilterte Taxidatei mit den Zoneninformationen.
    Nimmt execution_date aus dem Airflow-Kontext, um den Dateinamen dynamisch zu erstellen.
    """

    # 📅 Datumskomponenten extrahieren
    year = execution_date.year
    month = execution_date.month
    day = execution_date.day

    # 📁 Dateipfade
    # Basisverzeichnis: Ort der aktuellen Datei (z.B. dags/utils.py)
    base_dir = os.path.dirname(os.path.abspath(__file__))

    taxi_path = os.path.join(base_dir, f"../data/processed/taxi_data_{year}-{month:02d}-{day:02d}.parquet")
    zone_path = os.path.join(base_dir, "../data/raw/zones.csv")
    output_path = os.path.join(base_dir, f"../data/processed/taxi_data_{year}-{month:02d}-{day:02d}_enriched.parquet")

    # 📚 Daten laden
    df_taxi = pd.read_parquet(taxi_path)
    df_zone = pd.read_csv(zone_path)

    # 🚕 Join für Pickup-Zone
    df_joined = df_taxi.merge(
        df_zone[["LocationID", "Borough", "Zone"]],
        how="left",
        left_on="PULocationID",
        right_on="LocationID"
    ).rename(columns={
        "Borough": "PUBorough",
        "Zone": "PUZone"
    }).drop("LocationID", axis=1)

    # 🛬 Join für Dropoff-Zone
    df_joined = df_joined.merge(
        df_zone[["LocationID", "Borough", "Zone"]],
        how="left",
        left_on="DOLocationID",
        right_on="LocationID"
    ).rename(columns={
        "Borough": "DOBorough",
        "Zone": "DOZone"
    }).drop("LocationID", axis=1)

    # 💾 Speichern
    df_joined.to_parquet(output_path, index=False)
    print(f"✅ Enriched Dataset gespeichert unter: {output_path}")


def case4_check_data_quality(execution_date=None):

    # 📅 Datumskomponenten extrahieren
    year = execution_date.year
    month = execution_date.month
    day = execution_date.day

    # Basisverzeichnis: Ort der aktuellen Datei (z.B. dags/utils.py)
    base_dir = os.path.dirname(os.path.abspath(__file__))

    parquet_path = os.path.join(base_dir, f"../data/processed/taxi_data_{year}-{month:02d}-{day:02d}_enriched.parquet")
    df = pd.read_parquet(parquet_path)
    total_rows = len(df)
    null_counts = df.isnull().sum().to_dict()

    print(f"✅ Datenqualität Check - Gesamtzeilen: {total_rows}")
    print(f"Nullwerte je Spalte: {null_counts}")