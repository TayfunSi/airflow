"""
TODO: Use-Case Beschreibung – Case 5

Ihr erstellt einen erweiterten ETL-Workflow für Taxi-Daten (Parquet-Datei) und Zonendaten (CSV).
Ziel ist es, neben dem Join und der Datenqualität auch mehrere thematische Reports zu erzeugen.

Der Workflow soll folgende Schritte ausführen:

1. Auf die Verfügbarkeit beider Dateien warten:
   - taxi_data.parquet
   - zones.csv

2. Daten verbinden (Join):
   - Taxi-Daten mit Zonendaten anhand der LocationID mergen.
   - Die Funktion dafür ist in `utils.py` definiert.

3. Mehrere Reports erzeugen:
   a) **Nachtfahrten (00:00–06:00 Uhr)**
   b) **Lange Fahrten (>10 km)**
   c) **Rush Hour Trips (07–09 Uhr, 16–19 Uhr)**

4. Die Reports sollen jeweils als CSV-Dateien in separaten Unterordnern unter `data/processed/case5/` gespeichert werden.

5. Ein Logfile `summary.log` schreiben mit Infos zur Anzahl Fahrten insgesamt, Nachtfahrten, langen Fahrten, Rush Hour Fahrten.

6. Nur Theorie: Nach erfolgreichem Durchlauf soll eine E-Mail mit den Resultaten verschickt werden.
   Hinweis: Das reine Hinzufügen des EmailOperators reicht hier – in der Praxis wäre zusätzliche Konfiguration erforderlich.

"""


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
taxi_file = os.path.join(___)
zone_file = os.path.join(___)
processed_dir = os.path.join(base_dir, "../data/processed/case5")
os.makedirs(processed_dir, exist_ok=True)

default_args = {
    # eine Wiederholung bei Failure, Wiederholung in 1 Minute
}

with DAG(
    ___
) as dag:

    # Wir warten auf taxi_data_case5.parquet und zones_case5.csv
    ___

    def generate_advanced_reports(**kwargs):
        df = case5_join_all_data(taxi_file, zone_file)

        df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
        # Wir brauchen die Stunde
        # Wir brauchen die die Distanz in km (nicht in Meilen?)

        night = ___ # Nachtfahrten zw. 0 bis 5 Uhr
        long = ___ # Lang ist alles über 10 km
        rush = ___ # rush ist 7, 8, 9, 16, 17, 18, 19 Uhr

        # Wir brauchen je ein directory im unterordner processed_dir für nacht, lang und rush
        
        ____

        # Und schreiben sie jeweils nun als csv weg

        ____

        # Und erstellen ein Report file .log mit der Anzahl der Einträge: Total, Nacht, Long und Rush

        ____

        print("✅ Reports erstellt und Logs geschrieben.")

    # und nun rufen wir den Task der oben definierten Funktion auf

    # Task-Anordnung: Die Funktion wird erst aufgebaut, wenn BEIDE Files da sind, und das unabhängig voneinander :)

    [wait_for_taxi, wait_for_zone] >> report_task
