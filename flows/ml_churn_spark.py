from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml import Pipeline
from pyspark.ml.functions import vector_to_array
from prefect import flow, task
from config import BUCKET_SILVER, BUCKET_GOLD
from spark_utils import get_spark_session

@task(name="spark_ml_churn")
def spark_ml_churn() -> None:
    spark = get_spark_session("Spark-ML-Churn")
    
    path = f"s3a://{BUCKET_SILVER}/denormalized_achats.parquet"
    df = spark.read.parquet(path)
    
    max_date = df.select(F.max("date_achat")).collect()[0][0]
    
    features = df.groupBy("id_client").agg(
        F.datediff(F.lit(max_date), F.max("date_achat")).alias("recency"),
        F.count("id_achat").alias("frequency"),
        F.sum("montant").alias("monetary_sum"),
        F.mean("montant").alias("monetary_avg"),
        F.first("pays").alias("pays")
    )
    
    features = features.withColumn("is_churn", F.when(F.col("recency") > 60, 1).otherwise(0))
    
    indexer = StringIndexer(inputCol="pays", outputCol="pays_index", handleInvalid="keep")
    
    assembler = VectorAssembler(
        inputCols=["frequency", "monetary_sum", "monetary_avg", "pays_index"],
        outputCol="features"
    )
    
    rf = RandomForestClassifier(labelCol="is_churn", featuresCol="features", numTrees=20)
    
    pipeline = Pipeline(stages=[indexer, assembler, rf])
    
    model = pipeline.fit(features)
    predictions = model.transform(features)
    
    predictions = predictions.withColumn("churn_prob", vector_to_array("probability")[1])
    
    predictions = predictions.withColumn("risk_segment", 
        F.when(F.col("churn_prob") > 0.7, "High Risk")
         .when(F.col("churn_prob") > 0.3, "Medium Risk")
         .otherwise("Low Risk")
    )
    
    final_df = predictions.select(
        "id_client", "recency", "frequency", "monetary_sum", "monetary_avg", "pays", 
        "is_churn", "churn_prob", "risk_segment"
    )
    
    output_path = f"s3a://{BUCKET_GOLD}/ml_churn_spark_predictions.parquet"
    final_df.write.mode("overwrite").parquet(output_path)
    print(f"Predictions saved to {output_path}")

@flow(name="Spark ML Churn Flow")
def ml_churn_spark_flow():
    spark_ml_churn()

if __name__ == "__main__":
    ml_churn_spark_flow()
