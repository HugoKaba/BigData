import pandas as pd
from io import BytesIO
from prefect import flow, task
from pymongo import MongoClient
from config import BUCKET_GOLD, get_minio_client
import os

MONGO_URI = "mongodb://root:rootpassword@localhost:27017/"
DB_NAME = "analytics_db"

def get_mongo_db():
    client = MongoClient(MONGO_URI)
    return client[DB_NAME]

@task(name="load_collection_to_mongo")
def load_collection(collection_name: str, parquet_file: str):
    client_minio = get_minio_client()
    try:
        objects = client_minio.list_objects(BUCKET_GOLD, prefix=parquet_file, recursive=True)
        parquet_files = [obj.object_name for obj in objects if obj.object_name.endswith(".parquet")]
        
        if not parquet_files:
             print(f"No parquet files found for {parquet_file}")
             return

        dfs = []
        for obj_name in parquet_files:
            response = client_minio.get_object(BUCKET_GOLD, obj_name)
            data = response.read()
            response.close()
            response.release_conn()
            dfs.append(pd.read_parquet(BytesIO(data)))
            
        full_df = pd.concat(dfs, ignore_index=True)
        
        db = get_mongo_db()
        collection = db[collection_name]
        collection.drop() 
        
        records = full_df.to_dict("records")
        if records:
            collection.insert_many(records)
            print(f"Inserted {len(records)} records into {collection_name}")
        else:
            print(f"No records to insert for {collection_name}")

    except Exception as e:
        print(f"Error loading {parquet_file} to Mongo: {e}")

@flow(name="Load to MongoDB")
def load_mongo_flow():
    kpis = [
        ("dim_temps", "dim_temps.parquet"),
        ("kpi_ca_pays", "kpi_ca_pays.parquet"),
        ("kpi_vol_mensuel", "kpi_vol_mensuel.parquet"),
        ("kpi_ca_mensuel", "kpi_ca_mensuel_croissance.parquet"),
        ("kpi_stats_clients", "kpi_stats_clients.parquet"),
        ("kpi_top_produits", "kpi_top_produits.parquet")
    ]
    
    for coll, file in kpis:
        load_collection(coll, file)

if __name__ == "__main__":
    load_mongo_flow()
