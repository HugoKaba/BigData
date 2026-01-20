import time
from silver_transformation import silver_transformation_flow as silver_pandas
from gold_transformation import gold_transformation_flow as gold_pandas
from silver_spark import silver_spark_flow as silver_spark
from gold_spark import gold_spark_flow as gold_spark

def benchmark():
    print("--- BENCHMARK STARTED ---")
    
    start_pandas = time.time()
    try:
        print("Running Pandas Pipeline...")
        silver_pandas()
        gold_pandas()
    except Exception as e:
        print(f"Pandas Pipeline Failed: {e}")
    end_pandas = time.time()
    
    start_spark = time.time()
    try:
        print("Running Spark Pipeline...")
        silver_spark()
        gold_spark()
    except Exception as e:
        print(f"Spark Pipeline Failed: {e}")
    end_spark = time.time()
    
    duration_pandas = end_pandas - start_pandas
    duration_spark = end_spark - start_spark
    
    print("\n--- RESULTS ---")
    print(f"Pandas Duration: {duration_pandas:.2f} seconds")
    print(f"Spark Duration:  {duration_spark:.2f} seconds")
    print(f"Difference:      {duration_pandas - duration_spark:.2f} seconds")

if __name__ == "__main__":
    benchmark()
