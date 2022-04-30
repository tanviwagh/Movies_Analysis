from app.code import get_movie_data, data_quality_check, schema_creation, data_cleaning, sentiment_analysis
from app.utils.helper import create_spark_session, load_config

jobs = {
    'get_movie_data' : get_movie_data.process, 
    'data_quality_check' : data_quality_check.process,
    'data_cleaning': data_cleaning.process,
    'schema_creation': schema_creation.process, 
    'sentiment_analysis': sentiment_analysis.process
}

def run(params):
    print("PARAMS: ", params)
    app_name = params['job_name']
    config_path_name = params['path']

    spark = create_spark_session(app_name)
    config = load_config(config_path_name)

    process_function = jobs[app_name]
    process_function(spark=spark, config=config)
