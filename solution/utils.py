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


def case4_join_taxi_with_zones(**kwargs):
    """
    Führt den Join zwischen Taxi- und Zonendaten durch.
    Erwartet:
        - taxi_loaded.parquet
        - zones_loaded.csv
    Speichert:
        - joined_data.parquet
    """

    # Basisverzeichnis für relative Dateipfade
    base_dir = os.path.dirname(os.path.abspath(__file__))
    processed_dir = os.path.join(base_dir, "../data/processed")

    taxi_path = os.path.join(processed_dir, "taxi_loaded.parquet")
    zone_path = os.path.join(processed_dir, "zones_loaded.csv")
    output_path = os.path.join(processed_dir, "joined_data.parquet")

    df_taxi = pd.read_parquet(taxi_path)
    df_zones = pd.read_csv(zone_path)

    df_joined = df_taxi.merge(df_zones, left_on="PULocationID", right_on="LocationID", how="left")
    df_joined.to_parquet(output_path, index=False)

    print(f"✅ Join erfolgreich: {len(df_joined)} Zeilen gespeichert unter joined_data.parquet")


def case4_check_data_quality(**kwargs):
    """
    Führt einfache Datenqualitätschecks durch.
    Gibt Warnungen aus, stoppt aber die Pipeline **nicht**.
    """

    # Basisverzeichnis für relative Dateipfade
    base_dir = os.path.dirname(os.path.abspath(__file__))
    processed_dir = os.path.join(base_dir, "../data/processed")
    
    path = os.path.join(processed_dir, "joined_data.parquet")
    df = pd.read_parquet(path)

    errors = []

    if df.isnull().sum().any():
        errors.append("❌ Es gibt fehlende Werte in den Daten.")

    if (df["total_amount"] < 0).any():
        errors.append("❌ Es gibt negative Beträge in 'total_amount'.")

    if (df["trip_distance"] <= 0).any():
        errors.append("❌ Es gibt Fahrten mit 0 oder negativer 'trip_distance'.")

    if errors:
        print("⚠️ Datenqualität unzureichend – Workshop-Modus: Pipeline läuft trotzdem weiter.")
        for err in errors:
            print(f"   ↪ {err}")
    else:
        print("✅ Datenqualität in Ordnung.")