import sys
from pathlib import Path

# Add flows directory to path to import config
sys.path.append(str(Path(__file__).parent.parent / "flows"))

from config import get_minio_client, BUCKET_SOURCES, BUCKET_BRONZE, BUCKET_SILVER, BUCKET_GOLD

def verify_pipeline():
    client = get_minio_client()
    buckets = [BUCKET_SOURCES, BUCKET_BRONZE, BUCKET_SILVER, BUCKET_GOLD]
    
    print("=== Pipeline Verification ===")
    for bucket in buckets:
        if client.bucket_exists(bucket):
            print(f"\nBucket: {bucket}")
            objects = client.list_objects(bucket)
            for obj in objects:
                print(f" - {obj.object_name} ({obj.size} bytes)")
        else:
            print(f"\n[!] Bucket {bucket} does not exist yet.")

if __name__ == "__main__":
    verify_pipeline()
