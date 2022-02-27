from utils import create_spark_session, load_config

CREATE EXTERNAL TABLE test.movie
(
    `imdbID` DOUBLE,
    `localized_title` STRING,
	`languages` STRING, 
	`runtimes` INT, 
	`original_air_date` DATE, 
	`original_air_date_country` STRING, 
	`plot` STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS PARQUET
LOCATION 's3://movie-analysis-bucket/parquet/movie/'



CREATE EXTERNAL TABLE test.genre
(
    `imdbID` DOUBLE,
    `genres` STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS PARQUET
LOCATION 's3://movie-analysis-bucket/parquet/genre/'



CREATE EXTERNAL TABLE test.movie_cast
(
    `imdbID` DOUBLE,
    `cast` STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS PARQUET
LOCATION 's3://movie-analysis-bucket/parquet/cast/'



CREATE EXTERNAL TABLE test.director
(
    `imdbID` DOUBLE,
    `directors` STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS PARQUET
LOCATION 's3://movie-analysis-bucket/parquet/directors/'




CREATE EXTERNAL TABLE test.writer
(
    `imdbID` DOUBLE,
    `writers` STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS PARQUET
LOCATION 's3://movie-analysis-bucket/parquet/writers/'


CREATE EXTERNAL TABLE test.producer
(
    `imdbID` DOUBLE,
    `producers` STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS PARQUET
LOCATION 's3://movie-analysis-bucket/parquet/producers/'


CREATE EXTERNAL TABLE test.music
(
    `imdbID` DOUBLE,
    `music_department` STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS PARQUET
LOCATION 's3://movie-analysis-bucket/parquet/music_department/'


