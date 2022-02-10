import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract, regexp_replace, explode

PATH = ""

spark = SparkSession.builder.master("local").getOrCreate()

df = spark.read.option("multiline","true").json("../data/2020/*.json")

df.printSchema()

df1 = df.select(regexp_extract('original air date', r'\([^)]*\)', 0).alias('original_air_date_country'))

df2 = df.select(regexp_replace('original air date', r'\([^)]*\)', "").alias('original_air_date'))

def remove_duplicates(dataframe):
    unique_dataframe = dataframe.dropDuplicates()
    return unique_dataframe

def save_to_parquet(dataframe):
    dataframe.write.parquet(PATH)
    
def explode_array_columns(dataframe, col_name):
    exploded_dataframe = dataframe.select(df['imdbID'],explode(df[col_name]))
    return exploded_dataframe
