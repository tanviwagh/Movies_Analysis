from utils import create_spark_session, load_config, arg_parser
from pyspark.sql.functions import col, regexp_extract, regexp_replace, explode, split, udf, date_format, to_date
from pyspark.sql.types import DoubleType, DateType, IntegerType
import os 
import shutil

APP_NAME = "data_cleaning"


def read_json(folder_name, dir, emr_path ):
    #dataframe = spark.read.option("multiline","true").json("../" + folder_name + "/" + dir + "/*.json")
    dataframe = spark.read.option("multiline","true").json("file://" + emr_path  + folder_name + "/" + dir + "/*.json")
    
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

    #final_dataframe = final_dataframe.withColumn("original_air_date", final_dataframe.original_air_date.cast(DateType()))
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
    if not os.path.exists(parquet_path):
        os.makedirs(parquet_path)

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

if __name__=="__main__":
        
    spark = create_spark_session(APP_NAME)

    config_data = load_config() 

    data_folder_name = config_data['data']['data_folder_name']

    parquet_folder_name = config_data['data']['parquet_folder_name']

    emr_path = config_data['paths']['emr_path']

    movie_tbl_name = config_data['athena']['movie_tbl_name']

    genre_tbl_name = config_data['athena']['genre_tbl_name']

    artist_tbl_name = config_data['athena']['artist_tbl_name']

    music_tbl_name = config_data['athena']['music_tbl_name']

    director_tbl_name = config_data['athena']['director_tbl_name']

    producer_tbl_name = config_data['athena']['producer_tbl_name']

    writer_tbl_name = config_data['athena']['writer_tbl_name']

    s3_bucket_path = config_data['s3_bucket_details']['s3_bucket_path']

    try:
        shutil.rmtree('../' + parquet_folder_name)
    except:
        pass

    all_dirs = os.listdir('../' + data_folder_name)

    ngrams = arg_parser('Please specify parquet location')

    local_parquet_path = "file://" + emr_path + parquet_folder_name + '/'

    s3_parquet_path = s3_bucket_path + parquet_folder_name + '/'

    if ngrams == 'local':
        output_path = local_parquet_path 
    elif ngrams == 's3':
        output_path = s3_parquet_path

    for dir in all_dirs:
        input_df = read_json(data_folder_name, dir, emr_path)

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


        # output_path = "file://" + emr_path + parquet_folder_name + '/'  + movie_tbl_name
        output_path = output_path + movie_tbl_name
        save_to_parquet(unique_movie_df, output_path)

        # output_path = "file://" + emr_path + parquet_folder_name + '/' + artist_tbl_name
        output_path = output_path + artist_tbl_name
        save_to_parquet(movie_cast_df, output_path)
        
        # output_path = "file://" + emr_path + parquet_folder_name + '/' + music_tbl_name
        output_path = output_path + music_tbl_name
        save_to_parquet(music_department_df, output_path)
        
        # output_path = "file://" + emr_path + parquet_folder_name + '/' + genre_tbl_name
        output_path = output_path + genre_tbl_name
        save_to_parquet(genres_df, output_path)
        
        # output_path = "file://" + emr_path + parquet_folder_name + '/' + director_tbl_name
        output_path = output_path + director_tbl_name
        save_to_parquet(directors_df, output_path)
        
        # output_path = "file://" + emr_path + parquet_folder_name + '/' + producer_tbl_name
        output_path = output_path + producer_tbl_name
        save_to_parquet(writers_df, output_path)
        
        # output_path = "file://" + emr_path + parquet_folder_name + '/' + writer_tbl_name
        output_path = output_path + writer_tbl_name
        save_to_parquet(producers_df, output_path)

            

