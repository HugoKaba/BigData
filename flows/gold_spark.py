from pyspark.sql import functions as F
from pyspark.sql.window import Window
from prefect import flow, task
from config import BUCKET_SILVER, BUCKET_GOLD
from spark_utils import get_spark_session

@task(name="spark_gold_kpis")
def spark_gold_kpis() -> None:
    spark = get_spark_session("Gold-KPIs")
    
    df_achats = spark.read.parquet(f"s3a://{BUCKET_SILVER}/achats.parquet")
    df_clients = spark.read.parquet(f"s3a://{BUCKET_SILVER}/clients.parquet")
    
    dim_temps = df_achats.select("date_achat").distinct() \
        .withColumn("date", F.col("date_achat")) \
        .withColumn("jour", F.dayofmonth("date")) \
        .withColumn("semaine", F.weekofyear("date")) \
        .withColumn("mois", F.month("date")) \
        .withColumn("annee", F.year("date")) \
        .withColumn("jour_semaine", F.dayofweek("date")) \
        .drop("date_achat")
        
    dim_temps.write.mode("overwrite").parquet(f"s3a://{BUCKET_GOLD}/dim_temps.parquet")
    
    merged = df_achats.join(df_clients, on="id_client")
    
    ca_pays = merged.groupBy("pays") \
        .agg(F.sum("montant").alias("montant")) \
        .orderBy(F.col("montant").desc())
        
    ca_pays.write.mode("overwrite").parquet(f"s3a://{BUCKET_GOLD}/kpi_ca_pays.parquet")
    
    vol_periode = merged.withColumn("mois_date", F.trunc("date_achat", "Month")) \
        .groupBy("mois_date") \
        .agg(F.count("id_achat").alias("volume_ventes")) \
        .withColumnRenamed("mois_date", "mois")
        
    vol_periode.write.mode("overwrite").parquet(f"s3a://{BUCKET_GOLD}/kpi_vol_mensuel.parquet")
    
    ca_mensuel = merged.withColumn("mois_date", F.trunc("date_achat", "Month")) \
        .groupBy("mois_date") \
        .agg(F.sum("montant").alias("montant")) \
        .orderBy("mois_date")
    
    window_spec = Window.orderBy("mois_date")
    ca_mensuel = ca_mensuel.withColumn("prev_montant", F.lag("montant").over(window_spec)) \
        .withColumn("croissance_mom", (F.col("montant") - F.col("prev_montant")) / F.col("prev_montant") * 100) \
        .drop("prev_montant") \
        .withColumnRenamed("mois_date", "mois")
        
    ca_mensuel.write.mode("overwrite").parquet(f"s3a://{BUCKET_GOLD}/kpi_ca_mensuel_croissance.parquet")
    
    dist_stats = merged.groupBy("id_client") \
        .agg(
            F.sum("montant").alias("total_depense"),
            F.count("id_achat").alias("nb_achats"),
            F.mean("montant").alias("panier_moyen")
        )
        
    dist_stats.write.mode("overwrite").parquet(f"s3a://{BUCKET_GOLD}/kpi_stats_clients.parquet")
    
    top_produits = merged.groupBy("produit") \
        .agg(
            F.sum("montant").alias("ca_total"),
            F.count("id_achat").alias("nb_ventes")
        )
        
    top_produits.write.mode("overwrite").parquet(f"s3a://{BUCKET_GOLD}/kpi_top_produits.parquet")
    
    print("Gold transformation complete.")

@flow(name="Gold Spark Flow")
def gold_spark_flow():
    spark_gold_kpis()

if __name__ == "__main__":
    gold_spark_flow()
