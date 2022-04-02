from pyspark.sql.functions import col, udf
from pyspark.sql.types import IntegerType
from textblob import TextBlob


def process(spark, config):
    db_name = config['athena']['db_name']

    senti_tbl_name = config['athena']['senti_tbl_name']
    senti_s3_loc = config['athena']['senti_s3_loc']

    movie_s3_loc = config['athena']['movie_s3_loc']

    dataframe = filter_movie_data(spark, movie_s3_loc)

    sentiment_score_udf = udf(lambda x: get_sentiment(x), IntegerType())

    sentiment_dataframe = dataframe.select(col("imdbID"), 
                            sentiment_score_udf(col("plot")).alias("plot_sentiment"))

    sentiment_dataframe.write.mode('append').parquet(senti_s3_loc)

    create_sentiment_table(spark, db_name, senti_tbl_name, senti_s3_loc)


def filter_movie_data(spark, movie_s3_loc):
    dataframe = spark.read.parquet(movie_s3_loc)
    filtered_dataframe = dataframe.select(col('imdbID'), col('plot'))

    # remove all movie plots having value as unknown 
    cond = col('plot') != 'unknown'

    final_dataframe = filtered_dataframe.filter(cond)
    return final_dataframe


def get_sentiment(text):
    sentiment = TextBlob(text).sentiment.polarity
    if sentiment > 0.5:
        return 1
    elif sentiment < 0.5:
        return -1
    else:
        return 0


def create_sentiment_table(spark, db_name, tbl_name, s3_loc):
    SQL = """
        CREATE EXTERNAL TABLE IF NOT EXISTS {db_name}.{tbl_name}
        (
            `imdbID` DOUBLE,
            `plot_sentiment` INT
        )
        ROW FORMAT SERDE 
        'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
        STORED AS INPUTFORMAT 
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
        OUTPUTFORMAT 
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
        LOCATION '{s3_loc}'
        """.format(db_name=db_name, tbl_name=tbl_name, s3_loc=s3_loc)

    spark.sql(SQL)