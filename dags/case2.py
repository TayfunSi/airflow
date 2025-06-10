"""
🗂️ Use Case: Automatisiertes Laden von eingehenden Zonendaten (Case 2)

In diesem Szenario beobachten wir den Dateiordner `../data/raw`, in dem regelmäßig neue Zonendaten eintreffen sollen – z.B. von einem externen System, Datenlieferanten oder Uploads durch andere Prozesse.

Sobald eine neue Datei erkannt wird (z.B. `zones_13062025.csv`), soll diese automatisch verarbeitet und in den Zielordner `../data/processed/` geschrieben werden.

Dieser Workflow simuliert ein typisches Use-Case-Muster aus der Praxis:
📥 Warte → 📄 Verarbeite → ✅ Ablegen

Ziel ist es, dass ihr:
- den Airflow FileSensor nutzt, um auf eine Datei zu warten,
- einen PythonOperator erstellt, der die Datei verarbeitet (z.B. Validierung, Abspeichern),
- den Ablauf eigenständig im DAG aufbaut.

Hinweis: Die Datei `zones_13062025.csv` **muss und soll! beim Start des DAGs noch nicht vorhanden sein**. Erst wenn sie eintrifft, läuft der Workflow weiter.
"""

from airflow import DAG
from airflow.operators.python import ______                      # TODO: Importiere PythonOperator
from airflow.sensors.filesystem import ______                   # TODO: Importiere FileSensor
from datetime import datetime, timedelta
import sys
import os

# Konfiguriere Pfade
base_dir = os.path.dirname(os.path.abspath(__file__))

# TODO: Setze hier den relativen Pfad zur Datei, die getriggert werden soll
raw_file_path = os.path.join(base_dir, "../data/raw/zones_13062025.csv")  # diese Datei aber noch nicht erstellen!!!

# Damit du Funktionen aus utils.py importieren kannst
sys.path.append(os.path.dirname(__file__))

# TODO: Importiere die richtige Funktion für diesen Case
# from utils import _______________

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
    # TODO: Der DAG soll täglich laufen
    ______ = '______',
    # TODO: Vergangene Runs sollen nicht abgearbeitet werden
    _____ = _____,
    description='Wartet auf Datei und verarbeitet sie danach automatisch'
) as dag:

    # TODO: Sensor, der auf das Vorhandensein einer Datei wartet
    wait_for_file = _______(                                        # Bitte importieren
        # TODO: Wähle einen passenden Task-Namen
        task_id='______',
        # TODO: wir geben den Pfad zur Datei an
        _______=raw_file_path,
        poke_interval=______,                                       # alle 30 Sekunden prüfen - Integer
        timeout=______,                                             # nach 600 Sekunden abbrechen, auch Minute * 60 möglich - Integer
        mode='______________'                                       # blockierend oder rescheduling möglich, bitte rescheduling
    )

    # TODO: Operator, der die Datei verarbeitet
    process_file = ________(                                        # Bitte oben importieren
        # TODO: Wähle einen passenden Task-Namen
        task_id='________',
        # TODO: Setze hier deine Python-Funktion als callable ein
        python_callable=______________  
    )

    # TODO: Setze die Tasks in richtige Reihenfolge
    wait_for_file >> process_file

    # -------------------
    # 🧪 TESTANLEITUNG:
    # -------------------
    # TODO: Ihr startet den DAG in der UI und werdet sehen, dass der Job mit der Bezeichnung unter wait_for_file -> task_id
    #       alle 30 Sekunden prüft, ob die Datei ../data/raw/zones_13062025.csv vorliegt. 
    #       Bitte NACH dem DAG-Run die Datei zones.csv kopieren und mit der entsprechenden Bezeichnung in ../data/raw ablegen.
    #       Anschließend sollte der DAG mit process_file fortfahren und die Datei im Zielordner ../data/processed ablegen.