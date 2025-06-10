# 1. Repository clonen

(machen wir gemeinsam)

---

## 2. Python installieren

```bash
python3 -m venv airflow_env
source airflow_env/bin/activate  # macOS/Linux
# ODER
.\airflow_env\Scripts\activate  # Windows
```

---

## 3. Airflow installieren

```bash
pip install "apache-airflow==2.9.2"

# (Optional) Alte lokale Datenbank entfernen – Pfad ggf. anpassen
rm /Users/tsimsek/airflow/airflow.db

# Datenbank initialisieren
airflow db init
```

---

## 4. User für Web UI anlegen

```bash
airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com
```

---

## 5. Anpassungen in airflow.cfg

```ini
# Öffne airflow.cfg und passe folgenden Eintrag an:
dags_folder = /Users/tsimsek/airflow/dags  # Pfad zwingend anpassen
```

---

## 6. Airflow Webserver und Scheduler starten

**Terminal 1:**

```bash
airflow webserver --port 8080
```

**Terminal 2:**

```bash
airflow scheduler
```

---

## 7. Airflow UI öffnen

```text
http://localhost:8080
```

---

## 8. Airflow stoppen

```bash
pkill -f airflow
```