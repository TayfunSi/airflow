from airflow import DAG
_____                                       # Platzhalter für Sensor
_____                                       # Platzhalter für Sensor
from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta
import pandas as pd
import os
import sys

# TODO: Importiere eure Hilfsfunktionen aus utils
sys.path.append(os.path.dirname(__file__))
from utils import case4_join_taxi_with_zones, case4_check_data_quality

"""
TODO: Use-Case Beschreibung

Ihr erstellt einen ETL-Workflow für Taxi-Daten (tägliche Parquet-Datei) und Zonendaten (CSV).
Der Workflow soll folgende Schritte ausführen:

1. Auf die Verfügbarkeit beider Dateien warten (FileSensor)
2. Taxi-Daten laden und speichern (Parquet -> processed)
3. Zonendaten laden und speichern (CSV -> processed)
4. Taxi- und Zonendaten anhand LocationID verbinden (Join)
5. Datenqualität prüfen (z.B. fehlende Werte)
6. Einen einfachen Report erzeugen (Aggregation nach Zone: Anzahl Fahrten, Durchschnittspreis)
7. nur Theorie: Nach erfolgreichem Durchlauf eine Erfolgsmail senden, umfangreiche Konfigurationen erforderlich.

Die Pfade basieren auf dem base_dir (Verzeichnis der DAG-Datei).
Die Teilnehmer sollen den Großteil der einzelnen Steps eigenständig implementieren.
"""

base_dir = os.path.dirname(os.path.abspath(__file__))

# TODO: Definiere hier Pfade zu Rohdaten und Verzeichnis für Verarbeitete Dateien
taxi_file = os.path.join(base_dir, "______")             # neue Datei heißt updated_taxi_data.parquet
zone_file = os.path.join(base_dir, "______")             # neue Datei heißt updated_zones.csv
processed_dir = os.path.join(base_dir, "______")         # Daten sollen in processed
os.makedirs(processed_dir, exist_ok=True)

default_args = {
    ______

    # nur Theorie
    # 'email': ['your.email@example.com'],
    # 'email_on_failure': True,
    # 'email_on_retry': False
}

# TODO: Definiere DAG - manueller Run, vergangene Runs werden NICHT nachgeholt, maximal 1 aktiver run.
with DAG(
    ______
) as dag:

    # TODO: Warte auf Taxi-Datei.
    #       Welcher Sensor? Oben anschließend importieren.
    #       Prüfung alle 30 Sekunden mit rescheduling, maximal 60 Minuten.   
    wait_for_taxi = ______(
        ___
    )

    # TODO: Warte auf Zone-Datei.
    #       An wait_for_taxi orientieren.
    _____

    # TODO: Lade Taxi-Daten in processed Ordner (Parquet)
    def load_taxi_data(**kwargs):
        # Hier Taxi-Parquet lesen und speichern
        # Tipp: read_parquet, to_parquet
        # Zielordner: (os.path.join(processed_dir, "taxi_loaded.parquet"), index=False)
        # pass entfernen
        pass
    
    # TODO: Benötigen Operator und die zu nutzende Funktion.
    load_taxi = _______

    # TODO: Lade Zonendaten in processed Ordner (CSV)
    def load_zone_data(**kwargs):
        # analog load_taxi_data()
        pass

    # TODO: analog load_taxi
    load_zone = _______

    # TODO: Join Taxi- und Zone-Daten mit ausgelagerter join-Funktion
    join_data = _______

    # TODO: Prüfe Datenqualität mit ausgelagerter check-Funktion
    check_quality = _______

    # TODO: Funktion: Erstelle Report (Aggregation nach Zone, Ausgabe CSV)
    def create_report(**kwargs):
        # 1. Gejointen Datensatz laden. Kombi aus Pfad und read_parquet.
        ________
        df = ______
        # 2. Gruppiere nach Zone, aggregiere nach anzahl trip_distance und durchschnitt total_amount
        result = df.groupby(____).agg(
            fahrten_anzahl=(_____, _____),
            durchschnittspreis=(_____, _____)
        ).reset_index()

        # 3. Definiere Pfad + Dateiname und speichere result als csv ab
        output_csv = ______
        result._____
        print("📊 Report gespeichert:", output_csv)

    # TODO: Erstelle den Report durch die Funktion - vergiss den Operator nicht.
    report_task = ______

    # Bonus TODO: Sende Erfolgs-Email
    #email_task = EmailOperator(
    #    task_id='send_success_email',
    #    to='your.email@example.com',
    #    subject='🚀 Airflow Case4 Report abgeschlossen',
    #    html_content="<p>ETL-Pipeline erfolgreich durchgelaufen. Der Report ist verfügbar.</p>"
    #)

    # TODO: Task-Reihenfolge definieren
    #       Reihenfolge: Der Join erfolgt erst, wenn Taxi-Daten UND Zone-Daten geupdatet wurden. Das Updaten läuft parallel!
    #                    Nach dem Join erfolgt der Qualitycheck. Anschließend wird ein Report erstellt.
    #                    Darauf folgt die Mail, den Task dazu auskommentieren.
    # 
    #       Tipp: Parallel laufende Jobs in eckige Klammern setzen. :)