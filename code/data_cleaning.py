from utils import create_spark_session, load_config
from pyspark.sql.functions import col, regexp_extract, regexp_replace, explode, split
import os 

APP_NAME = "data_cleaning"

PARQUET_PATH = "" 

cols_list = ['cast','music department','directors', 'writers', 'producers']

def read_json(folder_name, dir):
    df = spark.read.option("multiline","true").json("../" + folder_name + "/" + dir + "/*.json")
    return df 


def clear_bracket_cols(df):
    df1 = df.select(regexp_extract('original air date', r'\([^)]*\)', 0).alias('original_air_date_country'))

    df2 = df1.select(regexp_replace('original air date', r'\([^)]*\)', "").alias('original_air_date'))

    return df2


def remove_duplicates(dataframe):
    unique_dataframe = dataframe.dropDuplicates()
    return unique_dataframe


def save_to_parquet(dataframe):
    dataframe.write.parquet(PARQUET_PATH)
    

def explode_array_columns(dataframe, col_name):
    exploded_dataframe = dataframe.select(df['imdbID'],explode(df[col_name]))
    return exploded_dataframe


def convert_to_array_type(df, col):
    alias_name = col + '_alias'
    df = df.select(df['*'], split(col(col),",").alias(alias_name))
    return df


def get_dataframes(df):
    for col in cols_list:
        d_type = dict(df.dtypes)[col]
    if d_type == 'array<string>':
        df1 = explode_array_columns(df, col)   
    else:
        df1 = df.select(col)
 

if __name__=="__main__":

    spark = create_spark_session(APP_NAME)

    config_data = load_config() 

    folder_name = config_data['data']['folder_name']

    all_dirs = os.listdir(folder_name)

    for dir in all_dirs:
        df = read_json(folder_name, dir)

        df1 = clear_bracket_cols(df)