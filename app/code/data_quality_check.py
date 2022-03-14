from pyspark.sql.functions import col 

def process(spark, config):
    db_name = config['athena']['db_name']

    movie_tbl_name = config['athena']['movie_tbl_name']

    genre_tbl_name = config['athena']['genre_tbl_name']

    artist_tbl_name = config['athena']['artist_tbl_name']

    music_tbl_name = config['athena']['music_tbl_name']

    director_tbl_name = config['athena']['director_tbl_name']

    producer_tbl_name = config['athena']['producer_tbl_name']

    writer_tbl_name = config['athena']['writer_tbl_name']

    table_exists_check(spark, db_name, movie_tbl_name)
    table_exists_check(spark, db_name, genre_tbl_name)
    table_exists_check(spark, db_name, artist_tbl_name)
    table_exists_check(spark, db_name, music_tbl_name)
    table_exists_check(spark, db_name, director_tbl_name)
    table_exists_check(spark, db_name, producer_tbl_name)
    table_exists_check(spark, db_name, writer_tbl_name)

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


def table_exists_check(spark, db_name, tbl_name):
    if spark._jsparkSession.catalog().tableExists(db_name, tbl_name):
        print("Table exists")
    else:
        raise Exception("Table {db_name}.{tbl_name} does not exist".format(db_name=db_name, tbl_name=tbl_name))


def row_count_check(spark, db_name, tbl_name):
    SQL = """ SELECT COUNT(*) FROM {db_name}.{tbl_name} """.format(db_name=db_name, tbl_name=tbl_name)
    count_df = spark.sql(SQL)

    if count_df.collect()[0][0] > 0:
        print("Row count check successful")
    else:
        raise Exception("Row count check has failed for {db_name}.{tbl_name}".format(db_name=db_name, tbl_name=tbl_name))


def non_null_check(spark, db_name, tbl_name):
    SQL = """ SELECT imdbID FROM {db_name}.{tbl_name} """.format(db_name=db_name, tbl_name=tbl_name)

    data_df = spark.sql(SQL)

    filter_cond = col('imdbID').isNull() 

    filtered_df = data_df.filter(filter_cond)

    if filtered_df.count() == 0:
        print("Non-NULL check successful")
    else:
        raise Exception("Non-NULL check has failed for {db_name}.{tbl_name}".format(db_name=db_name, tbl_name=tbl_name))


