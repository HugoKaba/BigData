import pandas as pd
from io import BytesIO
from prefect import flow, task
from config import BUCKET_BRONZE, BUCKET_SILVER, get_minio_client

@task(name="extract_from_bronze")
def extract_from_bronze(object_name: str) -> pd.DataFrame:
    client = get_minio_client()
    response = client.get_object(BUCKET_BRONZE, object_name)
    try:
        data = response.read()
        return pd.read_csv(BytesIO(data))
    finally:
        response.close()
        response.release_conn()

@task(name="transform_clients")
def transform_clients(df: pd.DataFrame) -> pd.DataFrame:
    df = df.dropna(subset=["id_client", "email"])
    df["date_inscription"] = pd.to_datetime(df["date_inscription"])
    df["id_client"] = df["id_client"].astype(int)
    df["nom"] = df["nom"].str.strip().str.title()
    df["email"] = df["email"].str.strip().str.lower()
    df = df.drop_duplicates(subset=["id_client"])
    return df

@task(name="transform_achats")
def transform_achats(df: pd.DataFrame) -> pd.DataFrame:
    df = df.dropna(subset=["id_achat", "id_client", "montant"])
    df = df[df["montant"] > 0]
    df["date_achat"] = pd.to_datetime(df["date_achat"])
    df["id_achat"] = df["id_achat"].astype(int)
    df["id_client"] = df["id_client"].astype(int)
    df["montant"] = df["montant"].astype(float)
    df["produit"] = df["produit"].str.strip()
    df = df.drop_duplicates(subset=["id_achat"])
    return df

@task(name="load_to_silver")
def load_to_silver(df: pd.DataFrame, object_name: str):
    client = get_minio_client()
    if not client.bucket_exists(BUCKET_SILVER):
        client.make_bucket(BUCKET_SILVER)
    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer, index=False)
    parquet_data = parquet_buffer.getvalue()
    client.put_object(
        BUCKET_SILVER,
        object_name.replace(".csv", ".parquet"),
        BytesIO(parquet_data),
        length=len(parquet_data)
    )

@flow(name="Silver Transformation Flow")
def silver_transformation_flow():
    df_clients = extract_from_bronze("clients.csv")
    df_clients_clean = transform_clients(df_clients)
    load_to_silver(df_clients_clean, "clients.csv")
    df_achats = extract_from_bronze("achats.csv")
    df_achats_clean = transform_achats(df_achats)
    load_to_silver(df_achats_clean, "achats.csv")

if __name__ == "__main__":
    silver_transformation_flow()
