

def process(spark, config):
    db_name = config['athena']['db_name']

    movie_tbl_name = config['athena']['movie_tbl_name']
    movie_s3_loc = config['athena']['movie_s3_loc']

    genre_tbl_name = config['athena']['genre_tbl_name']
    genre_s3_loc = config['athena']['genre_s3_loc']

    artist_tbl_name = config['athena']['artist_tbl_name']
    artist_s3_loc = config['athena']['artist_s3_loc']

    music_tbl_name = config['athena']['music_tbl_name']
    music_s3_loc = config['athena']['music_s3_loc']

    director_tbl_name = config['athena']['director_tbl_name']
    director_s3_loc = config['athena']['director_s3_loc']

    producer_tbl_name = config['athena']['producer_tbl_name']
    producer_s3_loc = config['athena']['producer_s3_loc']

    writer_tbl_name = config['athena']['writer_tbl_name']
    writer_s3_loc = config['athena']['writer_s3_loc']

    create_db(spark, db_name)
    create_movie_table(spark, db_name, movie_tbl_name, movie_s3_loc)
    create_genre_table(spark, db_name, genre_tbl_name, genre_s3_loc)
    create_artist_table(spark, db_name, artist_tbl_name, artist_s3_loc)
    create_music_department_table(spark, db_name, music_tbl_name, music_s3_loc)
    create_director_table(spark, db_name, director_tbl_name, director_s3_loc)
    create_producer_table(spark, db_name, producer_tbl_name, producer_s3_loc)
    create_writer_table(spark, db_name, writer_tbl_name, writer_s3_loc)

def create_db(spark, db_name):
    SQL = """
        CREATE DATABASE IF NOT EXISTS {db_name}
        """.format(db_name=db_name)

    spark.sql(SQL)

def create_movie_table(spark, db_name, tbl_name, s3_loc):
    SQL = """
        CREATE EXTERNAL TABLE IF NOT EXISTS {db_name}.{tbl_name}
        (
            `imdbID` DOUBLE,
            `movie_title` STRING,
            `language` STRING, 
            `runtime` INT, 
            `original_air_date` DATE, 
            `original_air_date_country` STRING, 
            `plot` STRING
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


def create_genre_table(spark, db_name, tbl_name, s3_loc):
    SQL = """
        CREATE EXTERNAL TABLE IF NOT EXISTS {db_name}.{tbl_name}
        (
            `imdbID` DOUBLE,
            `genre` STRING
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


def create_artist_table(spark, db_name, tbl_name, s3_loc):
    SQL = """
        CREATE EXTERNAL TABLE IF NOT EXISTS {db_name}.{tbl_name}
        (
            `imdbID` DOUBLE,
            `movie_cast` STRING
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


def create_music_department_table(spark, db_name, tbl_name, s3_loc):
    SQL = """
        CREATE EXTERNAL TABLE IF NOT EXISTS {db_name}.{tbl_name}
        (
            `imdbID` DOUBLE,
            `music_department` STRING
        )
        ROW FORMAT SERDE 
        'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
        STORED AS INPUTFORMAT 
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
        OUTPUTFORMAT 
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
        LOCATION'{s3_loc}'
        """.format(db_name=db_name, tbl_name=tbl_name, s3_loc=s3_loc)

    spark.sql(SQL)


def create_director_table(spark, db_name, tbl_name, s3_loc):
    SQL = """
    CREATE EXTERNAL TABLE IF NOT EXISTS {db_name}.{tbl_name}
    (
        `imdbID` DOUBLE,
        `director_name` STRING
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


def create_producer_table(spark, db_name, tbl_name, s3_loc):
    SQL = """
   CREATE EXTERNAL TABLE IF NOT EXISTS {db_name}.{tbl_name}
    (
        `imdbID` DOUBLE,
        `producer_name` STRING
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

def create_writer_table(spark, db_name, tbl_name, s3_loc):
    SQL = """
   CREATE EXTERNAL TABLE IF NOT EXISTS {db_name}.{tbl_name}
    (
        `imdbID` DOUBLE,
        `writer_name` STRING
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

