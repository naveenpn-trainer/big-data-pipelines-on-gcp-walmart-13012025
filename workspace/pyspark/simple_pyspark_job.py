import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import count

# Main entry point
if __name__ == "__main__":
    # Create Spark Session
    spark = SparkSession.builder \
        .appName("Employee Aggregation") \
        .getOrCreate()

    # Read command-line arguments
    if len(sys.argv) != 3:
        print("Usage: spark-submit employee_aggregation.py <input_path> <output_path>")
        sys.exit(1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]

    # Read the input data
    employee_df = spark.read.csv(path=input_path, header=True, inferSchema=True)

    # Perform aggregation
    result_df = employee_df.groupBy("DepartmentID").agg(count("*").alias("total_count"))

    # Save the result
    result_df.write.mode("overwrite").format("csv").save(output_path)

    # Stop the SparkSession
    spark.stop()
