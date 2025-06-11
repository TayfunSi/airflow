"""
🚕 Use Case: Monatlich gefilterte Taxi-Daten analysieren (Case 3)

📊 Ziel: Die monatlich gespeicherten Rohdaten der Taxis sollen jeweils am 1. eines Monats automatisiert gefiltert und 
im Anschluss mit Zoneninformationen angereichert werden. Dies entspricht einem typischen ETL-Prozess für Reporting-Zwecke:

1️⃣ Am 1. Tag jedes Monats um 10 Uhr soll automatisch eine Filterung der Rohdaten aus `taxi_data.parquet` stattfinden.
2️⃣ Gefiltert werden jeweils lediglich die Daten des Vormonats.
3️⃣ Anschließend werden die gefilterten Daten mit den Zonendaten (aus `zones.csv`) gejoined – über eine bereits vorbereitete Funktion aus `utils.py`.

🔄 Dieser DAG soll:
- monatlich getriggert werden - am 01. jedes Monats, um 10 Uhr (Cron Expression finden)
- vergangene Runs nachholen können
- das Ausführungsdatum (`execution_date`) korrekt für den Vormonat berechnen.

💡 Hinweis:
- Die Filter-Funktion ist im DAG definiert und nicht in utils.py - damit demonstrieren wir, dass Funktionen auch direkt im DAG definiert werden können.
- Die Join-Funktion steht bereit und wird importiert.
"""

# case3_template.py

from airflow import DAG
from airflow.operators.python import ________
from datetime import ________, ________
import pandas as pd
import os
import sys

# Damit du Funktionen aus utils.py importieren kannst
sys.path.append(os.path.dirname(__file__))

# TODO: Importiere die Funktion, die den Join durchführt
from utils import _______________

# TODO: Definiere sinnvolle Standard-Argumente. Startdatum: 01.06.2025
default_args = {
    ___
}

# TODO: den DAG definieren: Run am 1. jedes Monats um 10 Uhr, Vergangene Runs sollen nachgeholt werden.
with DAG(
    ______
) as dag:

    # ------------------------------
    # 🧠 Aufgabe 1: Daten filtern
    # ------------------------------

    # TODO: Erstelle hier eine Funktion, die:
    # - Die Datei taxi_data.parquet einliest
    # - Den Vormonat zur execution_date bestimmt
    # - Die Daten nach Monat filtert
    # - Die gefilterte Datei speichert als taxi_data_YYYY-MM.parquet
    
    # Wir lagern diese Funktion an dieser Stelle bewusst nicht aus.

    def filter_taxi_data_dynamic(execution_date, **kwargs):
        # Basisverzeichnis bestimmen
        base_dir = os.path.dirname(os.path.abspath(__file__))

        # Eingabe- und Ausgabe-Pfade
        input_path = os.path.join(base_dir, "___________")  # wir benötigen hier das raw taxi_data file
        output_dir = os.path.join(base_dir, "___________")  # und wollen es in den processed-Ordner schreiben
        os.makedirs(output_dir, exist_ok=True)

        # TODO: Berechne das Jahr und den Monat des Vormonats
        first_of_month = execution_date._____               # Ersetze den Tag durch den 1. des Monats. Tipp: replace
        prev_month_end = ________                           # Tipp: der 1. des jetzigen Monats abzüglich 1 Tag
        year = prev_month_end._____                         
        month = prev_month_end._____

        # TODO: Setze Pfad zur Zieldatei zu "taxi_data_2025-05.parquet"
        output_path = f"{output_dir}/__________"            # Arbeite hier dynamisch mit year und month. Month dabei bitte 2-stellig

        # Daten einlesen und filtern

        # TODO: das Raw-File (taxi_data) als parquet lesen
        df = pd.______

        # TODO: Wandle die Spalte in ein Datetime um. Tipp: to_datetime
        df["tpep_pickup_datetime"] = pd.________
        df_filtered = df[
            (df["tpep_pickup_datetime"].dt.year == _____) & # Wir wollen dynamisch nach Jahr
            (df["tpep_pickup_datetime"].dt.month == _____)  # und nach Monat filtern
        ]
        df_filtered.to_parquet(output_path, index=False)


    # TODO: Erstelle einen Operator, der die obige Funktion ausführt
    filter_task = ___________(
        ___ = ___,
        ___ =__________,
        provide_context=True  # Hinweis: notwendig für execution_date
    )

    # ------------------------------
    # 🧠 Aufgabe 2: Join durchführen
    # ------------------------------

    # TODO: Erstelle einen weiteren PythonOperator
    #       Nutze die ausgelagerte Funktion.
    join_task = ___________

    # TODO: Setze Reihenfolge der Tasks (Zuerst filtern, dann joinen)
    ________________