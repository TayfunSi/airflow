# case1_template.py

"""
🎯 Use-Case: Einfache Dateningestion (Case 1)

Ziel:
Du entwickelst einen minimalen Airflow-DAG, der eine CSV-Datei (`zones.csv`) einliest
und in ein neues Verzeichnis (`data/processed/`) als CSV speichert.

Die Funktion für das Einlesen und Speichern wurde bereits ausgelagert und steht dir in `utils.py` zur Verfügung.
Deine Aufgabe besteht darin, den DAG zu konfigurieren und die passende Funktion korrekt aufzurufen.

Schritte:
1. Setze die DAG-Metadaten (Startdatum, Schedule, Catchup-Verhalten)
2. Importiere alle benötigten Airflow-Operatoren und die Funktion aus `utils.py`
3. Verwende den PythonOperator, um deine Funktion als Task auszuführen
4. Der DAG soll manuell gestartet werden (kein schedule)
"""

from airflow import DAG
from airflow.operators.python import ______
from datetime import datetime
import sys
import os

# Damit du eigene Funktionen aus utils importieren kannst:
sys.path.append(os.path.dirname(__file__))

_____________                                                      # TODO: Importiere hier die für case1 vorgesehene Funktion aus utils.py

default_args = {
    'owner': 'airflow',
    'start_date': _______________,                                 # TODO: Setze ein Startdatum für deinen DAG. Vergangenheit!
    'retries': 0,
}

with DAG(
    dag_id='case1',
    default_args=default_args,
    schedule_interval=_____________,                               # TODO: Der DAG soll nur manuell getriggert werden.
    catchup=____,                                                  # TODO: Vergangene Runs sollen nicht abgearbeitet werden
    description='Lädt zones.csv aus und speichert in processed'
) as dag:

    ingest_task = _________(                                       # TODO: Wähle einen geeigneten Operator für Python Funktionen und importiere ihn in Zeile 3
        task_id='_______________',                                 # TODO: Wähle einen passenden Task-Namen, z. B. 'ingest_zone_data'
        python_callable=_____________                              # TODO: Setze hier deine Python-Funktion als callable ein
    )

    ingest_task                                                    # Der DAG startet (und endet) mit ingest_task