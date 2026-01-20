from fastapi import FastAPI, HTTPException
from pymongo import MongoClient
import os
from typing import List, Dict, Any

app = FastAPI(title="Big Data API", description="API exposing Gold layer data from MongoDB")

MONGO_URI = os.getenv("MONGO_URI", "mongodb://root:rootpassword@localhost:27017/")
DB_NAME = "analytics_db"

def get_db():
    client = MongoClient(MONGO_URI)
    return client[DB_NAME]

@app.get("/")
def read_root():
    return {"status": "online", "message": "Welcome to the Big Data Analytics API"}

@app.get("/stats/ca_pays")
def get_ca_pays():
    db = get_db()
    data = list(db["kpi_ca_pays"].find({}, {"_id": 0}))
    return data

@app.get("/stats/vol_mensuel")
def get_vol_mensuel():
    db = get_db()
    data = list(db["kpi_vol_mensuel"].find({}, {"_id": 0}))
    return data

@app.get("/stats/ca_mensuel")
def get_ca_mensuel():
    db = get_db()
    data = list(db["kpi_ca_mensuel"].find({}, {"_id": 0}))
    return data

@app.get("/stats/top_produits")
def get_top_produits():
    db = get_db()
    data = list(db["kpi_top_produits"].find({}, {"_id": 0}).limit(10))
    return data

@app.get("/stats/clients")
def get_client_stats():
    db = get_db()
    data = list(db["kpi_stats_clients"].find({}, {"_id": 0}).limit(100))
    return data
