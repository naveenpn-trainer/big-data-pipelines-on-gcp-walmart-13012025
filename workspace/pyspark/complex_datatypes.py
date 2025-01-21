from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
if __name__ == '__main__':
    spark = SparkSession.builder.appName("Spark Streaming Demo").master("local").getOrCreate()
    user_df = spark.read.csv("../resources/dataset/employee.csv", sep="|",header=True, inferSchema=True,quote="'")
    user_df.show()

    user_df.na.drop(subset=["col_id","col_name"]).show()