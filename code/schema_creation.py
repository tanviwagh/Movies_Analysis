from utils import create_spark_session, load_config

APP_NAME = "schema_creation"

def create_movie_table(spark, db_name, tbl_name, s3_loc):
    SQL = """
        CREATE EXTERNAL TABLE {db_name}.{tbl_name}
        (
            `imdbID` DOUBLE,
            `localized_title` STRING,
            `languages` STRING, 
            `runtimes` INT, 
            `original_air_date` STRING, 
            `original_air_date_country` STRING, 
            `plot` STRING
        )
        ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
        STORED AS PARQUET
        LOCATION '{s3_loc}'
        """.format(db_name=db_name, tbl_name=tbl_name, s3_loc=s3_loc)

    spark.sql(SQL)


def create_genre_table(spark, db_name, tbl_name, s3_loc):
    SQL = """
        CREATE EXTERNAL TABLE {db_name}.{tbl_name}
        (
            `imdbID` DOUBLE,
            `genres` STRING
        )
        ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
        STORED AS PARQUET
        LOCATION '{s3_loc}'
        """.format(db_name=db_name, tbl_name=tbl_name, s3_loc=s3_loc)

    spark.sql(SQL)


def create_artist_table(spark, db_name, tbl_name, s3_loc):
    SQL = """
        CREATE EXTERNAL TABLE {db_name}.{tbl_name}
        (
            `imdbID` DOUBLE,
            `movie_cast` STRING
        )
        ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
        STORED AS PARQUET
        LOCATION '{s3_loc}'
        """.format(db_name=db_name, tbl_name=tbl_name, s3_loc=s3_loc)

    spark.sql(SQL)


def create_music_department_table(spark, db_name, tbl_name, s3_loc):
    SQL = """
        CREATE EXTERNAL TABLE {db_name}.{tbl_name}
        (
            `imdbID` DOUBLE,
            `music_department` STRING
        )
        ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
        STORED AS PARQUET
        LOCATION '{s3_loc}'
        """.format(db_name=db_name, tbl_name=tbl_name, s3_loc=s3_loc)

    spark.sql(SQL)


def create_director_table(spark, db_name, tbl_name, s3_loc):
    SQL = """
    CREATE EXTERNAL TABLE {db_name}.{tbl_name}
    (
        `imdbID` DOUBLE,
        `directors` STRING
    )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS PARQUET
    LOCATION '{s3_loc}'
    """.format(db_name=db_name, tbl_name=tbl_name, s3_loc=s3_loc)

    spark.sql(SQL)


def create_producer_table(spark, db_name, tbl_name, s3_loc):
    SQL = """
   CREATE EXTERNAL TABLE {db_name}.{tbl_name}
    (
        `imdbID` DOUBLE,
        `producers` STRING
    )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS PARQUET
    LOCATION '{s3_loc}'
    """.format(db_name=db_name, tbl_name=tbl_name, s3_loc=s3_loc)

    spark.sql(SQL)

def create_writer_table(spark, db_name, tbl_name, s3_loc):
    SQL = """
   CREATE EXTERNAL TABLE {db_name}.{tbl_name}
    (
        `imdbID` DOUBLE,
        `writers` STRING
    )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS PARQUET
    LOCATION '{s3_loc}'
    """.format(db_name=db_name, tbl_name=tbl_name, s3_loc=s3_loc)

    spark.sql(SQL)


if __name__=="__main__":
    spark = create_spark_session(APP_NAME)

    config_data = load_config() 
    db_name = config_data['athena']['db_name']

    movie_tbl_name = config_data['athena']['movie_tbl_name']
    movie_s3_loc = config_data['athena']['movie_s3_loc']

    genre_tbl_name = config_data['athena']['genre_tbl_name']
    genre_s3_loc = config_data['athena']['genre_s3_loc']

    artist_tbl_name = config_data['athena']['artist_tbl_name']
    artist_s3_loc = config_data['athena']['artist_s3_loc']

    music_tbl_name = config_data['athena']['music_tbl_name']
    music_s3_loc = config_data['athena']['music_s3_loc']

    director_tbl_name = config_data['athena']['director_tbl_name']
    director_s3_loc = config_data['athena']['director_s3_loc']

    producer_tbl_name = config_data['athena']['producer_tbl_name']
    producer_s3_loc = config_data['athena']['producer_s3_loc']

    writer_tbl_name = config_data['athena']['writer_tbl_name']
    writer_s3_loc = config_data['athena']['writer_s3_loc']

    create_movie_table(spark, db_name, movie_tbl_name, movie_s3_loc)
    create_genre_table(spark, db_name, genre_tbl_name, genre_s3_loc)
    create_artist_table(spark, db_name, artist_tbl_name, artist_s3_loc)
    create_music_department_table(spark, db_name, music_tbl_name, music_s3_loc)
    create_director_table(spark, db_name, director_tbl_name, director_s3_loc)
    create_producer_table(spark, db_name, producer_tbl_name, producer_s3_loc)
    create_writer_table(spark, db_name, writer_tbl_name, writer_s3_loc)