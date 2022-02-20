from utils import create_spark_session, load_config
from pyspark.sql.functions import col, regexp_extract, regexp_replace, explode, split, udf
import os 

APP_NAME = "data_cleaning"


def read_json(folder_name, dir, emr_path ):
    #dataframe = spark.read.option("multiline","true").json("../" + folder_name + "/" + dir + "/*.json")
    dataframe = spark.read.option("multiline","true").json("file://" + emr_path  + folder_name + "/" + dir + "/*.json")
    
    cols = [ col('imdbID'), col('localized title'), col('languages'), col('runtimes'), col('original air date'),
            col('plot'), col('cast'), col('music department'), col('genres'),
            col('directors'), col('writers'), col('producers') ]

    dataframe = dataframe.select(*cols)

    dataframe = dataframe.withColumnRenamed("localized title", "localized_title")
    dataframe = dataframe.withColumnRenamed("music department", "music_department")

    return dataframe 


def clean_date_column(dataframe):
    cols = [col('imdbID'), col('localized_title'), col('languages'), col('runtimes'), col('original_air_date'),
            col('original_air_date_country'), col('plot')]

    temp_dataframe = dataframe.withColumn("original_air_date_country", regexp_extract('original air date', r'\([^)]*\)', 0))

    result_dataframe = temp_dataframe.withColumn("original_air_date", regexp_replace('original air date', r'\([^)]*\)', "")).select(*cols)

    final_dataframe = result_dataframe.withColumn("original_air_date_country", regexp_replace('original_air_date_country', r'\)|\(', "")).select(*cols)

    return final_dataframe


def convert_list_column(dataframe):
    if dict(dataframe.dtypes)["plot"] != "string":
        join_udf = udf(lambda x: ",".join(x))
        join_dataframe = dataframe.withColumn("plot", join_udf(col("plot")))
        
        final_dataframe = join_dataframe.withColumn("plot", regexp_replace('plot', r'\]|\[', ""))

    else:
        join_dataframe = dataframe
        split_udf = udf(lambda x: x.split(',')[0])

        split_dataframe = join_dataframe.withColumn("languages", split_udf(col("languages")))

        final_dataframe = split_dataframe.withColumn("languages", regexp_replace('languages', r'\]|\[', ""))

    return final_dataframe



def remove_duplicates(dataframe):
    unique_dataframe = dataframe.dropDuplicates()

    return unique_dataframe


def save_to_parquet(dataframe, parquet_path):
    dataframe.write.parquet(parquet_path)
    

def convert_to_array_type(dataframe, col_name):
    array_dataframe = dataframe.withColumn(col_name, split(dataframe[col_name],",")).\
                      select(col('imdbID'), col('cast'), col('music_department'), 
                                col('genres'), col('directors'), col('writers'), 
                                col('producers')).withColumnRenamed('col', col_name)
    
    return array_dataframe


def explode_array_columns(dataframe, col_name):
    exploded_dataframe = dataframe.withColumn(col_name, explode(dataframe[col_name])).\
                         select(col('imdbID'), col('cast'), col('music_department'), 
                                col('genres'), col('directors'), col('writers'), 
                                col('producers')).withColumnRenamed('col', col_name)

    exploded_dataframe = exploded_dataframe.withColumn(col_name, regexp_replace(col_name, r'\]|\[', ""))
    
    return exploded_dataframe



if __name__=="__main__":

    spark = create_spark_session(APP_NAME)

    config_data = load_config() 

    data_folder_name = config_data['data']['data_folder_name']

    parquet_folder_name = config_data['data']['parquet_folder_name']

    emr_path = config_data['paths']['emr_path']

    all_dirs = os.listdir('../' + data_folder_name)

    for dir in all_dirs:
        input_df = read_json(data_folder_name, dir)

        formatted_df = clean_date_column(input_df)

        formatted_df = convert_list_column(formatted_df)

        output_path = "file://" + emr_path + parquet_folder_name + '/' + dir + '/' + "movie"

        if not os.path.exists(output_path):
            os.makedirs(output_path)
        else:
            print("Path exits")
        
        save_to_parquet(formatted_df, output_path)
            
        base_df = input_df

        cols_list = ['cast','music_department', 'genres','directors', 'writers', 'producers']

        for col_name in cols_list:
            d_type = dict(base_df.dtypes)[col_name]
            if d_type == 'string':
                base_df = convert_to_array_type(base_df, col_name)

        converted_df = base_df

        for col_name in cols_list:
            d_type = dict(converted_df.dtypes)[col_name]
            if d_type == 'array<string>':
                converted_df = explode_array_columns(converted_df, col_name)   
            
        idx = 0

        cast_df = converted_df.select(col('imdbID'), col(cols_list[idx]))
        music_df = converted_df.select(col('imdbID'), col(cols_list[idx+1]))
        genre_df = converted_df.select(col('imdbID'), col(cols_list[idx+2]))
        director_df = converted_df.select(col('imdbID'), col(cols_list[idx+3]))
        writer_df = converted_df.select(col('imdbID'), col(cols_list[idx+4]))
        producer_df = converted_df.select(col('imdbID'), col(cols_list[idx+5]))

        unique_cast_df = remove_duplicates(cast_df)
        unique_music_df = remove_duplicates(music_df)
        unique_genre_df = remove_duplicates(genre_df)
        unique_director_df = remove_duplicates(director_df)
        unique_writer_df = remove_duplicates(writer_df)
        unique_producer_df = remove_duplicates(producer_df)

        for col_name in cols_list:
            #output_path = '../' + parquet_folder_name + '/' + dir + '/' + col_name
            output_path = "file://" + emr_path + parquet_folder_name + '/' + dir + '/' + col_name

            if not os.path.exists(output_path):
                os.makedirs(output_path)

            save_to_parquet(converted_df, output_path)
            

