# 🚀 Pipeline Big Data - Guide de Démarrage

Ce projet met en œuvre une pipeline Big Data complète avec **Spark**, **MongoDB**, **Kafka** et **MinIO**.

## Prérequis

- Docker & Docker Compose
- Python 3.10+

## 1. Démarrage de l'Infrastructure

```bash
docker-compose up -d
```

Vérifiez que tout tourne : `docker ps`

## 2. Configuration du Conteneur Spark (une seule fois)

```bash
docker exec -u 0 spark-master sh -c "mkdir -p /home/spark/.ivy2/cache && chown -R 185:185 /home/spark"
docker exec -u 0 spark-master python3 -m pip install -r /opt/spark-apps/requirements.txt
```

## 3. Installation des Dépendances Locales

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## 4. Exécuter la Pipeline ELT

### Ingestion Bronze (local)

```bash
python flows/bronze_ingestion.py
```

### Transformation Silver (conteneur Spark)

```bash
docker exec spark-master python3 /opt/spark-apps/flows/silver_spark.py
```

### Transformation Gold (conteneur Spark)

```bash
docker exec spark-master python3 /opt/spark-apps/flows/gold_spark.py
```

## 5. Couche Opérationnelle & Dashboard

### Chargement MongoDB

```bash
python flows/load_mongo.py
```

### Lancer l'API (FastAPI)

```bash
uvicorn api.main:app --host 0.0.0.0 --port 8000
```

_Accès : http://localhost:8000/docs_

### Lancer le Dashboard (Streamlit)

```bash
streamlit run dashboard/streamlit_app.py
```

_Accès : http://localhost:8501_

## 6. Machine Learning (Churn Prediction)

```bash
docker exec spark-master python3 /opt/spark-apps/flows/ml_churn_spark.py
```

## 7. Streaming Temps Réel (Kafka + Spark)

**Terminal 1 : Producteur**

```bash
python streaming/producer.py
```

**Terminal 2 : Consommateur Spark**

```bash
docker exec spark-master python3 /opt/spark-apps/streaming/consumer.py
```

## 8. Configuration Metabase (MongoDB)

1. Accédez à http://localhost:3000
2. Créez un compte admin
3. Ajoutez une base de données :
   - **Type** : MongoDB
   - **Host** : `mongo`
   - **Port** : `27017`
   - **Database** : `analytics_db`
   - **Username** : `root`
   - **Password** : `rootpassword`
   - **Authentication Database** : `admin`

## URLs d'Accès

| Service       | URL                   |
| ------------- | --------------------- |
| MinIO Console | http://localhost:9001 |
| Prefect UI    | http://localhost:4200 |
| Spark Master  | http://localhost:8080 |
| Mongo Express | http://localhost:8081 |
| FastAPI       | http://localhost:8000 |
| Streamlit     | http://localhost:8501 |
| Metabase      | http://localhost:3000 |
