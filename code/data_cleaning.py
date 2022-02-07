import pyspark
from pyspark.sql import SparkSession


if __name__ == '__main__':
    spark = SparkSession.builder.master("local").getOrCreate()

    df = spark.sql("select 'spark' as hello ")

    print(df.show())
