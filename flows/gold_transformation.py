import pandas as pd
from io import BytesIO
from prefect import flow, task
from config import BUCKET_SILVER, BUCKET_GOLD, get_minio_client

@task(name="extract_from_silver")
def extract_from_silver(object_name: str) -> pd.DataFrame:
    client = get_minio_client()
    response = client.get_object(BUCKET_SILVER, object_name)
    try:
        data = response.read()
        return pd.read_parquet(BytesIO(data))
    finally:
        response.close()
        response.release_conn()

@task(name="load_to_gold")
def load_to_gold(df: pd.DataFrame, object_name: str):
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

@flow(name="Gold Transformation Flow")
def gold_transformation_flow():
    df_clients = extract_from_silver("clients.parquet")
    df_achats = extract_from_silver("achats.parquet")
    
    dates = pd.to_datetime(df_achats["date_achat"])
    dim_temps = pd.DataFrame({
        "date": dates.dt.date,
        "jour": dates.dt.day,
        "semaine": dates.dt.isocalendar().week,
        "mois": dates.dt.month,
        "annee": dates.dt.year,
        "jour_semaine": dates.dt.dayofweek
    }).drop_duplicates()
    load_to_gold(dim_temps, "dim_temps.parquet")
    
    merged = df_achats.merge(df_clients, on="id_client")
    
    ca_pays = merged.groupby("pays")["montant"].sum().reset_index().sort_values("montant", ascending=False)
    load_to_gold(ca_pays, "kpi_ca_pays.parquet")
    
    vol_periode = merged.set_index("date_achat").resample("M")["id_achat"].count().reset_index()
    vol_periode.columns = ["mois", "volume_ventes"]
    load_to_gold(vol_periode, "kpi_vol_mensuel.parquet")
    
    ca_mensuel = merged.set_index("date_achat").resample("M")["montant"].sum().reset_index()
    ca_mensuel["croissance_mom"] = ca_mensuel["montant"].pct_change() * 100
    load_to_gold(ca_mensuel, "kpi_ca_mensuel_croissance.parquet")
    
    dist_stats = merged.groupby("id_client")["montant"].agg(["sum", "count", "mean"]).reset_index()
    dist_stats.columns = ["id_client", "total_depense", "nb_achats", "panier_moyen"]
    load_to_gold(dist_stats, "kpi_stats_clients.parquet")
    
    top_produits = merged.groupby("produit").agg({"montant": "sum", "id_achat": "count"}).reset_index()
    top_produits.columns = ["produit", "ca_total", "nb_ventes"]
    load_to_gold(top_produits, "kpi_top_produits.parquet")

if __name__ == "__main__":
    gold_transformation_flow()
