import boto3
from pyspark.sql.functions import col, regexp_extract, regexp_replace, explode, split, udf, date_format, to_date
from pyspark.sql.types import DoubleType, DateType, IntegerType

def process(spark, config):

    data_folder_name = config['data']['data_folder_name']

    parquet_folder_name = config['data']['parquet_folder_name']

    emr_path = config['paths']['emr_path']

    movie_tbl_name = config['athena']['movie_tbl_name']

    genre_tbl_name = config['athena']['genre_tbl_name']

    artist_tbl_name = config['athena']['artist_tbl_name']

    music_tbl_name = config['athena']['music_tbl_name']

    director_tbl_name = config['athena']['director_tbl_name']

    producer_tbl_name = config['athena']['producer_tbl_name']

    writer_tbl_name = config['athena']['writer_tbl_name']

    s3_bucket_path = config['s3_bucket_details']['s3_bucket_path']

    bucket_name = config['s3_bucket_details']['s3_bucket_data_path']
    bucket= bucket_name.split('/')[2:]
    bucket_name= '/'.join(bucket)

    # s3_client = connect_to_aws_service_client('s3')
    s3_client = boto3.client('s3')

    all_dirs = []

    for obj_list in s3_client.list_objects(Bucket=bucket_name)['Contents']:
        key = obj_list['Key']

        key = key.split('/')[:2]
        key = '/'.join(key)
        all_dirs.append(key)
    
    all_dirs = set(all_dirs)
    all_dirs = list(all_dirs)

    for key in all_dirs:
        input_df = read_json(spark, data_folder_name, key, bucket_name)

        non_null_df = fill_null_values(input_df)

        formatted_df = clean_date_column(non_null_df)

        formatted_df = convert_list_column(formatted_df)
     
        base_df = input_df

        cols_list = ['movie_cast','music_department', 'genre','director_name', 'writer_name', 'producer_name']

        for col_name in cols_list:
            d_type = dict(base_df.dtypes)[col_name]
            if d_type == 'string':
                base_df = convert_to_array_type(base_df, col_name)

        converted_df = base_df

        for col_name in cols_list:
            d_type = dict(converted_df.dtypes)[col_name]
            if d_type == 'array<string>':
                converted_df = explode_array_columns(converted_df, col_name)   

        unique_movie_df = formatted_df.dropDuplicates()

        unique_movie_df = remove_quotes(unique_movie_df, True)
        converted_df = remove_quotes(converted_df, False)

        idx = 0

        movie_cast_df = converted_df.select(col('imdbID'), col(cols_list[idx])).dropDuplicates()
        music_department_df = converted_df.select(col('imdbID'), col(cols_list[idx+1])).dropDuplicates()
        genres_df = converted_df.select(col('imdbID'), col(cols_list[idx+2])).dropDuplicates()
        directors_df = converted_df.select(col('imdbID'), col(cols_list[idx+3])).dropDuplicates()
        writers_df = converted_df.select(col('imdbID'), col(cols_list[idx+4])).dropDuplicates()
        producers_df = converted_df.select(col('imdbID'), col(cols_list[idx+5])).dropDuplicates()

        
        output_path = 's3://' + bucket_name + '/' + parquet_folder_name + '/' + movie_tbl_name
        save_to_parquet(unique_movie_df, output_path)

        output_path = 's3://' + bucket_name + '/' + parquet_folder_name + '/' + artist_tbl_name
        save_to_parquet(movie_cast_df, output_path)
        
        output_path = 's3://' + bucket_name + '/' + parquet_folder_name + '/' + music_tbl_name
        save_to_parquet(music_department_df, output_path)
        
        output_path = 's3://' + bucket_name + '/' + parquet_folder_name + '/' + genre_tbl_name
        save_to_parquet(genres_df, output_path)
        
        output_path = 's3://' + bucket_name + '/' + parquet_folder_name + '/' + director_tbl_name
        save_to_parquet(directors_df, output_path)
        
        output_path = 's3://' + bucket_name + '/' + parquet_folder_name + '/' + producer_tbl_name
        save_to_parquet(producers_df, output_path)
        
        output_path = 's3://' + bucket_name + '/' + parquet_folder_name + '/' + writer_tbl_name
        save_to_parquet(writers_df, output_path)

              
def read_json(spark, folder_name, key, bucket_name):

    dataframe = spark.read.option("multiline","true").json('s3://'+ bucket_name + '/' + key + "/*.json")
    
    cols = [ col('imdbID'), col('localized title'), col('languages'), col('runtimes'), col('original air date'),
            col('plot'), col('cast'), col('music department'), col('genres'),
            col('directors'), col('writers'), col('producers') ]

    dataframe = dataframe.select(*cols)

    dataframe = dataframe.withColumnRenamed("localized title", "movie_title")
    dataframe = dataframe.withColumnRenamed("music department", "music_department")
    dataframe = dataframe.withColumnRenamed("cast", "movie_cast")
    dataframe = dataframe.withColumnRenamed("original air date", "original_air_date")
    dataframe = dataframe.withColumnRenamed("runtimes", "runtime")
    dataframe = dataframe.withColumnRenamed("languages", "language")
    dataframe = dataframe.withColumnRenamed("genres", "genre")
    dataframe = dataframe.withColumnRenamed("directors", "director_name")
    dataframe = dataframe.withColumnRenamed("writers", "writer_name")
    dataframe = dataframe.withColumnRenamed("producers", "producer_name")

   
    dataframe = dataframe.withColumn("imdbID",dataframe.imdbID.cast(DoubleType()))
    dataframe = dataframe.withColumn("runtime",dataframe.runtime.cast(IntegerType()))
    

    return dataframe 

def fill_null_values(dataframe):
    null_dict = {'imdbID': 0, 'runtime': 0, 'original_air_date': '31 Dec 9999 (Unknown)', 'movie_title': 'unknown'}
    
    dataframe = dataframe.na.fill(null_dict)
    
    other_cols_list = [ 'language' , 'plot', 'movie_cast', 'music_department',
                        'genre', 'director_name', 'writer_name', 'producer_name']
    
    
    for col_name in other_cols_list:
        d_type = dict(dataframe.dtypes)[col_name]
        if d_type == 'string':
            dataframe = dataframe.na.fill(value="unknown", subset=[col_name])
        
    return dataframe

def clean_date_column(dataframe):
    cols = [col('imdbID'), col('movie_title'), col('language'), col('runtime'), col('original_air_date'),
            col('original_air_date_country'), col('plot')]

    temp_dataframe = dataframe.withColumn("original_air_date_country", regexp_extract('original_air_date', r'\([^)]*\)', 0))

    result_dataframe = temp_dataframe.withColumn("original_air_date", regexp_replace('original_air_date', r'\([^)]*\)', "")).select(*cols)

    final_dataframe = result_dataframe.withColumn("original_air_date_country", regexp_replace('original_air_date_country', r'\)|\(', "")).select(*cols)
    
    
    final_dataframe = final_dataframe.withColumn("original_air_date", regexp_replace('original_air_date', ' ', "")).select(*cols)

    final_dataframe = final_dataframe.withColumn('original_air_date', date_format(to_date('original_air_date', 'ddMMMyyyy'), 'yyyy-MM-dd')).select(*cols)

    final_dataframe = final_dataframe.withColumn("original_air_date", final_dataframe.original_air_date.cast(DateType()))

    return final_dataframe


def convert_list_column(dataframe):
    split_udf = udf(lambda x: x.split(',')[0])    

    split_dataframe = dataframe.withColumn("language", split_udf(col("language")))

    temp_dataframe = split_dataframe.withColumn("language", regexp_replace('language', r'\]|\[', ""))
        
    if dict(temp_dataframe.dtypes)["plot"] != "string":
        join_udf = udf(lambda x: ",".join(x))
        final_dataframe = temp_dataframe.withColumn("plot", join_udf(col("plot")))
        
    else:
        final_dataframe = temp_dataframe
        
    final_dataframe = final_dataframe.withColumn("plot", regexp_replace('plot', r'\]|\[', ""))

    return final_dataframe



def save_to_parquet(dataframe, parquet_path):

    dataframe.write.mode('append').parquet(parquet_path)
    

def convert_to_array_type(dataframe, col_name):
    array_dataframe = dataframe.withColumn(col_name, split(dataframe[col_name],",")).\
                      select(col('imdbID'), col('movie_cast'), col('music_department'), 
                                col('genre'), col('director_name'), col('writer_name'), 
                                col('producer_name')).withColumnRenamed('col', col_name)
    
    return array_dataframe


def explode_array_columns(dataframe, col_name):
    exploded_dataframe = dataframe.withColumn(col_name, explode(dataframe[col_name])).\
                         select(col('imdbID'), col('movie_cast'), col('music_department'), 
                                col('genre'), col('director_name'), col('writer_name'), 
                                col('producer_name')).withColumnRenamed('col', col_name)

    exploded_dataframe = exploded_dataframe.withColumn(col_name, regexp_replace(col_name, r'\]|\[', ""))
    
    return exploded_dataframe

def remove_quotes(dataframe, movie_flag):
    if movie_flag == True:
        cols = [ col('imdbID'), col('movie_title'), col('language'), col('runtime'), col('original_air_date'),
            col('original_air_date_country'), col('plot') ]

        dataframe = dataframe.select(*cols)
    
        final_dataframe = dataframe.withColumn('language', regexp_replace('language', '"', ''))
        final_dataframe = final_dataframe.withColumn('plot', regexp_replace('plot', '"', ''))
        
    else:
        cols = [ col('imdbID'), col('movie_cast'), col('music_department'), col('genre'),
            col('director_name'), col('writer_name'), col('producer_name') ]

        dataframe = dataframe.select(*cols)
        
        final_dataframe = dataframe.withColumn('movie_cast', regexp_replace('movie_cast', '"', ''))
        final_dataframe = final_dataframe.withColumn('music_department', regexp_replace('music_department', '"', ''))
        final_dataframe = final_dataframe.withColumn('genre', regexp_replace('genre', '"', ''))

        final_dataframe = final_dataframe.withColumn('director_name', regexp_replace('director_name', '"', ''))  
        final_dataframe = final_dataframe.withColumn('writer_name', regexp_replace('writer_name', '"', ''))  
        final_dataframe = final_dataframe.withColumn('producer_name', regexp_replace('producer_name', '"', ''))  
    
    return final_dataframe
