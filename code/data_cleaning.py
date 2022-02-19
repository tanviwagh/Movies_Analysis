from utils import create_spark_session, load_config
from pyspark.sql.functions import col, regexp_extract, regexp_replace, explode, split
import os 

APP_NAME = "data_cleaning"


def read_json(folder_name, dir):
    dataframe = spark.read.option("multiline","true").json("../" + folder_name + "/" + dir + "/*.json")
    
    return dataframe 


def clear_bracket_cols(dataframe):
    cols = [col('imdbID'), col('localized title'), col('languages'), col('runtimes'), col('original_air_date'),
        col('original_air_date_country'), col('plot')]

    temp_dataframe = dataframe.withColumn("original_air_date_country", regexp_extract('original air date', r'\([^)]*\)', 0))

    result_dataframe = temp_dataframe.withColumn("original_air_date", regexp_replace('original air date', r'\([^)]*\)', "")).select(*cols)

    final_dataframe = result_dataframe.withColumn("original_air_date_country", regexp_replace('original_air_date_country', r'\)|\(', "")).select(*cols)

    return final_dataframe


def remove_duplicates(dataframe):
    unique_dataframe = dataframe.dropDuplicates()

    return unique_dataframe


def save_to_parquet(dataframe, parquet_path):
    dataframe.write.parquet(parquet_path)
    

def convert_to_array_type(dataframe, col_name):
    array_dataframe = dataframe.withColumn(col_name, split(dataframe[col_name],",")).\
                      select(col('imdbID'), col('cast'), col('music department'), 
                                col('genres'), col('directors'), col('writers'), 
                                col('producers')).withColumnRenamed('col', col_name)
    
    return array_dataframe


def explode_array_columns(dataframe, col_name):
    exploded_dataframe = dataframe.withColumn(col_name, explode(dataframe[col_name])).\
                         select(col('imdbID'), col('cast'), col('music department'), 
                                col('genres'), col('directors'), col('writers'), 
                                col('producers')).withColumnRenamed('col', col_name)
    
    return exploded_dataframe


if __name__=="__main__":

    spark = create_spark_session(APP_NAME)

    config_data = load_config() 

    data_folder_name = config_data['data']['data_folder_name']

    parquet_folder_name = config_data['data']['parquet_folder_name']

    all_dirs = os.listdir('../' + data_folder_name)

    for dir in all_dirs:
        input_df = read_json(data_folder_name, dir)

        formatted_df = clear_bracket_cols(input_df)

        base_df = formatted_df

        cols_list = ['cast','music department', 'genres','directors', 'writers', 'producers']

        for col_name in cols_list:
            d_type = dict(base_df.dtypes)[col_name]
            if d_type == 'string':
                base_df = convert_to_array_type(base_df, col_name)

        converted_df = base_df

        for col_name in cols_list:
            d_type = dict(converted_df.dtypes)[col_name]
            if d_type == 'array<string>':
                converted_df = explode_array_columns(converted_df, col_name)   

        # need to filter data according to tables 
        
        output_path = '../' + parquet_folder_name + '/' + dir 

        save_to_parquet(converted_df, output_path)
            

