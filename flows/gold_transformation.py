import pandas as pd
from io import BytesIO
from prefect import flow, task
from config import BUCKET_SILVER, BUCKET_GOLD, get_minio_client

@task(name="extract_from_silver")
def extract_from_silver(object_name: str) -> pd.DataFrame:
    """Read Parquet from silver bucket."""
    client = get_minio_client()
    response = client.get_object(BUCKET_SILVER, object_name)
    try:
        data = response.read()
        df = pd.read_parquet(BytesIO(data))
        return df
    finally:
        response.close()
        response.release_conn()

@task(name="create_kpi_ventes_pays")
def create_kpi_ventes_pays(df_clients: pd.DataFrame, df_achats: pd.DataFrame) -> pd.DataFrame:
    """Calculate total sales per country."""
    merged = df_achats.merge(df_clients, on="id_client")
    kpi = merged.groupby("pays")["montant"].sum().reset_index()
    kpi.columns = ["pays", "total_ventes"]
    return kpi

@task(name="load_to_gold")
def load_to_gold(df: pd.DataFrame, object_name: str):
    """Save KPI DataFrame as Parquet in gold bucket."""
    client = get_minio_client()
    if not client.bucket_exists(BUCKET_GOLD):
        client.make_bucket(BUCKET_GOLD)
        
    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer, index=False)
    parquet_data = parquet_buffer.getvalue()
    
    client.put_object(
        BUCKET_GOLD,
        object_name,
        BytesIO(parquet_data),
        length=len(parquet_data)
    )
    print(f"Loaded {object_name} to {BUCKET_GOLD}")

@flow(name="Gold Transformation Flow")
def gold_transformation_flow():
    """Main flow for Gold layer: Silver Parquet -> Gold KPIs."""
    df_clients = extract_from_silver("clients.parquet")
    df_achats = extract_from_silver("achats.parquet")
    
    # KPI 1: Ventes par Pays
    kpi_pays = create_kpi_ventes_pays(df_clients, df_achats)
    load_to_gold(kpi_pays, "kpi_ventes_par_pays.parquet")
    
    # KPI 2: Top Produits
    kpi_produits = df_achats.groupby("produit")["montant"].sum().sort_values(ascending=False).reset_index()
    load_to_gold(kpi_produits, "kpi_top_produits.parquet")

if __name__ == "__main__":
    gold_transformation_flow()
