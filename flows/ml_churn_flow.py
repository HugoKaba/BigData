from io import BytesIO
import pandas as pd
from prefect import flow, task
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import LabelEncoder
from config import BUCKET_SILVER, BUCKET_GOLD, get_minio_client
import datetime

@task(name="extract_training_data")
def extract_training_data() -> pd.DataFrame:
    """Load denormalized data from Silver layer."""
    client = get_minio_client()
    response = client.get_object(BUCKET_SILVER, "denormalized_achats.parquet")
    data = response.read()
    response.close()
    response.release_conn()
    return pd.read_parquet(BytesIO(data))

@task(name="feature_engineering")
def feature_engineering(df: pd.DataFrame) -> pd.DataFrame:
    """Create features for Churn Prediction."""
    df["date_achat"] = pd.to_datetime(df["date_achat"])
    
    snapshot_date = df["date_achat"].max() + datetime.timedelta(days=1)
    
    features = df.groupby("id_client").agg({
        "date_achat": lambda x: (snapshot_date - x.max()).days,
        "id_achat": "count",
        "montant": ["sum", "mean"],
        "pays": "first"
    })
    
    features.columns = ["recency", "frequency", "monetary", "avg_basket", "pays"]
    features = features.reset_index()
    
    features["is_churn"] = (features["recency"] > 60).astype(int)
    
    return features

@task(name="train_model_and_predict")
def train_model_and_predict(df: pd.DataFrame) -> pd.DataFrame:
    """Train a simple Random Forest and generate probabilities."""
    
    le = LabelEncoder()
    df["pays_encoded"] = le.fit_transform(df["pays"])
    
    X = df[["frequency", "monetary", "avg_basket", "pays_encoded"]]
    y = df["is_churn"]
    
    model = RandomForestClassifier(n_estimators=100, random_state=42)
    model.fit(X, y)
    
    df["churn_probability"] = model.predict_proba(X)[:, 1]
    
    def labeling(row):
        if row["churn_probability"] > 0.7:
            return "High Risk"
        elif row["churn_probability"] > 0.3:
            return "Medium Risk"
        else:
            return "Low Risk"
            
    df["risk_segment"] = df.apply(labeling, axis=1)
    
    return df

@task(name="save_predictions_to_gold")
def save_predictions_to_gold(df: pd.DataFrame):
    """Save the results to Gold bucket."""
    client = get_minio_client()
    if not client.bucket_exists(BUCKET_GOLD):
        client.make_bucket(BUCKET_GOLD)
        
    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer, index=False)
    parquet_data = parquet_buffer.getvalue()
    
    client.put_object(
        BUCKET_GOLD,
        "ml_churn_predictions.parquet",
        BytesIO(parquet_data),
        length=len(parquet_data)
    )

@flow(name="ML Churn Prediction Flow")
def ml_churn_flow():
    df_raw = extract_training_data()
    df_features = feature_engineering(df_raw)
    df_scored = train_model_and_predict(df_features)
    save_predictions_to_gold(df_scored)

if __name__ == "__main__":
    ml_churn_flow()
