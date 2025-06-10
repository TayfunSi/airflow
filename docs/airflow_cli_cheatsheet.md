# 🧠 Airflow CLI Cheat-Sheet

## 🚀 Start & Stop

```bash
# Starte den Scheduler (führt DAGs aus)
airflow scheduler

# Starte den Webserver (für UI-Zugriff, standardmäßig unter http://localhost:8080)
airflow webserver -p 8080

# Stoppe Webserver/Scheduler via Ctrl+C oder per Prozessbeendigung (siehe unten)
# Anschließend
# Beende alle laufenden Airflow-Prozesse (Webserver, Scheduler, etc.)
pkill -f airflow

---

## 🔧 Initial Setup

```bash
# Initialisiere die Datenbank (nur beim ersten Start erforderlich)
airflow db init

# Erstelle einen Admin-User
airflow users create \
  --username admin \
  --firstname Max \
  --lastname Mustermann \
  --role Admin \
  --email max@example.com \
  --password admin
```

---

## 📂 Logs & DAG-Neuladen

```bash
# Logs eines Tasks anzeigen
less ~/airflow/logs/<dag_id>/<task_id>/<execution_date>/1.log

# DAG-Dateien neu parsen (z. B. nach Änderungen)
airflow dags reserialize
```

## 📜 DAG Management

```bash
# Liste aller DAGs anzeigen
airflow dags list

# Details zu einem DAG (Baumansicht)
airflow dags show <dag_id>

# Manuell einen DAG triggern
airflow dags trigger <dag_id>

# Liste aller DAG-Runs
airflow dags list-runs -d <dag_id>

# DAG pausieren / aktivieren
airflow dags pause <dag_id>
airflow dags unpause <dag_id>
```

---

## 🧩 Task Management

```bash
# Alle Tasks eines DAGs anzeigen
airflow tasks list <dag_id>

# Task manuell ausführen (z. B. zum Testen)
airflow tasks run <dag_id> <task_id> <execution_date>

# Task-Status zurücksetzen (z. B. bei fehlgeschlagenen Runs)
airflow tasks clear <dag_id> --only-running
```

---

## 📆 Datum & Zeitformate

```bash
# Beispiel: Manueller Run für 9. Juni 2025
airflow dags trigger <dag_id> --exec-date 2025-06-09T00:00:00
```

---


## ❌ Prozesse sicher beenden

```bash
# Beende alle laufenden Airflow-Prozesse (Webserver, Scheduler, etc.)
pkill -f airflow

# Oder gezielt Prozesse suchen:
ps aux | grep airflow
kill <PID>
```

💡 `pkill -f airflow` ist besonders nützlich, wenn sich Prozesse aufgehängt haben.

---

## ⚙️ Entwickler-Tipp: Auto-Reload für DAGs

```bash
# Starte Webserver mit automatischem Reload bei Dateiänderungen
airflow webserver --reload
```

---

## 📁 Standardverzeichnisse (AIRFLOW_HOME)

```text
~/airflow/
├── dags/              # DAG-Skripte
├── logs/              # Task-Ausführungslogs
├── airflow.db         # SQLite-Datenbank (lokal)
```

---