from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

if __name__ == '__main__':
    spark = SparkSession.builder.appName("Demo App").master("local").getOrCreate()
    print(type(spark))

    user_df = spark.read.parquet("../../resources/dataset/users/parquet_format/*")
    print(type(user_df))

    # user_df.show(truncate=False, n= 30)

    # df = user_df.select(col("id").alias("user_id"),col("designation"))
    # df.show()

    # user_df.filter(col("designation")=="other").select("id").show()

    user_df.groupBy(col("designation")).agg(count("*").alias("total_count")).show()
