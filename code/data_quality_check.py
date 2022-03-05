from utils import create_spark_session, load_config
from pyspark.sql.functions import col 

APP_NAME = "data_quality_check"

def row_count_check(spark, db_name, tbl_name):
    SQL = """ SELECT COUNT(*) FROM {db_name}.{tbl_name} """.format(db_name=db_name, tbl_name=tbl_name)
    count_df = spark.sql(SQL)

    if count_df.collect[0][0] > 0:
        print("Row count check successful")
    else:
        raise Exception("Row count check has failed for {db_name}.{tbl_name}".format(db_name=db_name, tbl_name=tbl_name))


def non_null_check(spark, db_name, tbl_name):
    SQL = """ SELECT imdbID FROM {db_name}.{tbl_name} """.format(db_name=db_name, tbl_name=tbl_name)

    data_df = spark.sql(SQL)

    filter_cond = col('imdbID').isNull() 

    filtered_df = data_df.filter(filter_cond)

    if filtered_df.count == 0:
        print("Non-NULL check successful")
    else:
        raise Exception("Non-NULL check has failed for {db_name}.{tbl_name}".format(db_name=db_name, tbl_name=tbl_name))

    

if __name__=="__main__":
        
    spark = create_spark_session(APP_NAME)

    config_data = load_config() 

    db_name = config_data['athena']['db_name']

    movie_tbl_name = config_data['athena']['movie_tbl_name']

    genre_tbl_name = config_data['athena']['genre_tbl_name']

    artist_tbl_name = config_data['athena']['artist_tbl_name']

    music_tbl_name = config_data['athena']['music_tbl_name']

    director_tbl_name = config_data['athena']['director_tbl_name']

    producer_tbl_name = config_data['athena']['producer_tbl_name']

    writer_tbl_name = config_data['athena']['writer_tbl_name']

    row_count_check(spark, db_name, movie_tbl_name)
    row_count_check(spark, db_name, genre_tbl_name)
    row_count_check(spark, db_name, artist_tbl_name)
    row_count_check(spark, db_name, music_tbl_name)
    row_count_check(spark, db_name, director_tbl_name)
    row_count_check(spark, db_name, producer_tbl_name)
    row_count_check(spark, db_name, writer_tbl_name)

    non_null_check(spark, db_name, movie_tbl_name)
    non_null_check(spark, db_name, genre_tbl_name)
    non_null_check(spark, db_name, artist_tbl_name)
    non_null_check(spark, db_name, music_tbl_name)
    non_null_check(spark, db_name, director_tbl_name)
    non_null_check(spark, db_name, producer_tbl_name)
    non_null_check(spark, db_name, writer_tbl_name)

