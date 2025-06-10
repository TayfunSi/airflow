# 📘 Apache Airflow Cheat Sheet

---

## 🧰 Wichtige Operatoren

| Operator         | Beschreibung                         | Import                                                  |
|------------------|--------------------------------------|---------------------------------------------------------|
| `PythonOperator` | Führt eine Python-Funktion aus        | `from airflow.operators.python import PythonOperator`   |
| `BashOperator`   | Führt Bash-Befehle aus                | `from airflow.operators.bash import BashOperator`       |
| `EmailOperator`  | Versendet Emails                      | `from airflow.operators.email import EmailOperator`     |
| `DummyOperator`  | Platzhalter (z. B. für Visualisierung)| `from airflow.operators.dummy import DummyOperator`     |
| `FileSensor`     | Wartet auf eine Datei im Dateisystem  | `from airflow.sensors.filesystem import FileSensor`     |

---

## 🔁 Wichtige `default_args` Parameter

| Schlüssel           | Beschreibung                                    |
|---------------------|--------------------------------------------------|
| `owner`             | Verantwortliche Person                          |
| `start_date`        | Zeitpunkt, ab dem der DAG laufen darf           |
| `retries`           | Anzahl der Wiederholungsversuche                |
| `retry_delay`       | Wartezeit zwischen Wiederholungen (`timedelta`) |
| `email`             | Empfänger für Benachrichtigungen                |
| `email_on_failure`  | Sende Email bei Fehlschlag                      |
| `email_on_retry`    | Sende Email bei Retry                           |

---

## ⏰ Schedule Optionen

| Wert             | Bedeutung                                |
|------------------|-------------------------------------------|
| `None`           | Nur manuell startbar                     |
| `'@once'`        | Einmalige Ausführung                     |
| `'@daily'`       | Täglich um Mitternacht                   |
| `'0 0 1 * *'`    | Jeden 1. des Monats um 00:00 Uhr         |
| `'@hourly'`      | Stündlich                                |
| `'@weekly'`      | Wöchentlich                              |

---

## 🔗 Task-Verkettung

```python
task1 >> task2         # task2 wird nach task1 ausgeführt
task1 << task2         # task1 wird vor task2 ausgeführt
task1 >> [task2, task3]  # task1 führt zu beiden Tasks
```

---

## 🧪 Nützliche Hinweise

- `catchup=True`: Frühere geplante Runs nachholen (default: True)
- `provide_context=True`: Ermöglicht Zugriff auf `execution_date`, `ds`, etc.
- `mode='reschedule'`: Sensoren blockieren keine Worker-Slots
- `poke_interval=30`: Sensor prüft alle 30 Sekunden
- `timeout=600`: Sensor gibt nach 10 Minuten auf

---

## 🔧 Beispiel: PythonOperator

```python
def meine_funktion(**kwargs):
    print("Hello Airflow!")

mein_task = PythonOperator(
    task_id='sage_hallo',
    python_callable=meine_funktion,
    provide_context=True
)
```