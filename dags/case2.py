"""
🗂️ Use Case: Automatisiertes Laden von eingehenden Zonendaten (Case 2)

In diesem Szenario beobachten wir den Dateiordner `../data/raw`, in dem regelmäßig neue Zonendaten eintreffen sollen – 
z.B. von einem externen System, Datenlieferanten oder Uploads durch andere Prozesse.

Sobald eine neue Datei erkannt wird (z.B. `zones_13062025.csv`), soll diese automatisch verarbeitet und in den Zielordner `../data/processed/` geschrieben werden.

Dieser Workflow simuliert ein typisches Use-Case-Muster aus der Praxis:
📥 Warte → 📄 Verarbeite → ✅ Ablegen

Ziel ist es, dass ihr:
- den Airflow ...-Sensor nutzt, um auf eine Datei zu warten,
- einen Operator erstellt, der die Datei verarbeitet (z.B. Validierung, Abspeichern),
- den Ablauf eigenständig im DAG aufbaut.

Hinweis: Die Datei `zones_13062025.csv` soll beim Start des DAGs noch NICHT vorhanden sein. 
Wir simulieren das Eintreffen der Datei während der DAG auf sie wartet. Erst wenn sie eintrifft, läuft der Workflow weiter.
"""

from airflow import DAG
from airflow.operators.python import ______                     # TODO: Importiere den richtigen Sensor
from airflow.sensors.filesystem import ______                   # TODO: Importiere den richtigen Sensor
from datetime import datetime, timedelta
import sys
import os

# Konfiguriere Pfade
base_dir = os.path.dirname(os.path.abspath(__file__))

raw_file_path = os.path.join(base_dir, "../data/raw/zones_13062025.csv")

# Damit du Funktionen aus utils.py importieren kannst
sys.path.append(os.path.dirname(__file__))

from utils import _______________                                   # TODO: Importiere die richtige Funktion für diesen Case

# TODO: Default Argumente nun eigenständig ausfüllen
default_args = {
    'owner': '______',
    'start_date': ______,                                           # muss in der Vergangenheit liegen
    'retries': 0,
    'retry_delay': _______,                                         # wir simulieren ein timedelta von 5 Minuten, auch wenn retries: 0 ist. Tipp: timedelta()
}

with DAG(
    dag_id='case2',
    default_args=default_args,
    ______ = '______',                                              # TODO: Der DAG soll täglich laufen
    _____ = _____,                                                  # TODO: Vergangene Runs sollen nicht abgearbeitet werden
    description='Wartet auf Datei und verarbeitet sie automatisch'
) as dag:

    # TODO: Sensor, der auf das Vorhandensein einer Datei wartet
    wait_for_file = _______(                                        # Bitte oben importieren
        task_id='______',                                           # TODO: Wähle einen passenden Task-Namen
        _______=raw_file_path,                                      # TODO: wir geben den Pfad zur Datei an
        poke_interval=______,                                       # alle 30 Sekunden prüfen
        timeout=______,                                             # nach 10 Minuten abbrechen, auch Minute * 60 möglich
        mode='______________'                                       # blockierend oder rescheduling möglich, bitte rescheduling
    )

    # TODO: Operator, der die Datei verarbeitet
    process_file = ________(                                        # Bitte oben importieren
        task_id='________',                                         # TODO: Wähle einen passenden Task-Namen
        python_callable=______________                              # TODO: Setze hier deine Python-Funktion als callable ein
    )

    # TODO: Setze die Tasks in richtige Reihenfolge
    _____________