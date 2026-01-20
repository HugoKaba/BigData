from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType, DateType
from prefect import flow, task
from config import BUCKET_BRONZE, BUCKET_SILVER
from spark_utils import get_spark_session

@task(name="spark_transform_clients")
def spark_transform_clients() -> None:
    spark = get_spark_session("Silver-Clients")
    
    input_path = f"s3a://{BUCKET_BRONZE}/clients.csv"
    df = spark.read.option("header", "true").csv(input_path)
    
    df = df.dropna(subset=["id_client", "email"]) \
           .withColumn("date_inscription", F.to_date(F.col("date_inscription"))) \
           .withColumn("id_client", F.col("id_client").cast(IntegerType())) \
           .withColumn("nom", F.initcap(F.trim(F.col("nom")))) \
           .withColumn("email", F.lower(F.trim(F.col("email")))) \
           .dropDuplicates(["id_client"])
           
    output_path = f"s3a://{BUCKET_SILVER}/clients.parquet"
    df.write.mode("overwrite").parquet(output_path)
    print(f"Wrote clients to {output_path}")

@task(name="spark_transform_achats")
def spark_transform_achats() -> None:
    spark = get_spark_session("Silver-Achats")
    
    input_path = f"s3a://{BUCKET_BRONZE}/achats.csv"
    df = spark.read.option("header", "true").csv(input_path)
    
    df = df.dropna(subset=["id_achat", "id_client", "montant"]) \
           .filter(F.col("montant") > 0) \
           .withColumn("date_achat", F.to_date(F.col("date_achat"))) \
           .withColumn("id_achat", F.col("id_achat").cast(IntegerType())) \
           .withColumn("id_client", F.col("id_client").cast(IntegerType())) \
           .withColumn("montant", F.col("montant").cast(DoubleType())) \
           .withColumn("produit", F.trim(F.col("produit"))) \
           .dropDuplicates(["id_achat"])
           
    output_path = f"s3a://{BUCKET_SILVER}/achats.parquet"
    df.write.mode("overwrite").parquet(output_path)
    print(f"Wrote achats to {output_path}")

@task(name="spark_join_denormalize")
def spark_join_denormalize() -> None:
    spark = get_spark_session("Silver-Join")
    
    df_clients = spark.read.parquet(f"s3a://{BUCKET_SILVER}/clients.parquet")
    df_achats = spark.read.parquet(f"s3a://{BUCKET_SILVER}/achats.parquet")
    
    df_denorm = df_achats.join(df_clients, on="id_client", how="inner")
    
    output_path = f"s3a://{BUCKET_SILVER}/denormalized_achats.parquet"
    df_denorm.write.mode("overwrite").parquet(output_path)
    print(f"Wrote denormalized data to {output_path}")

@flow(name="Silver Spark Flow")
def silver_spark_flow():
    spark_transform_clients()
    spark_transform_achats()
    spark_join_denormalize()

if __name__ == "__main__":
    silver_spark_flow()
