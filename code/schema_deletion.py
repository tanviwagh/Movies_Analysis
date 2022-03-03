from utils import create_spark_session, load_config

APP_NAME = "schema_deletion"

def delete_db(spark, db_name):
    SQL = """
        DROP DATABASE {db_name} CASCADE
        """.format(db_name=db_name)

    spark.sql(SQL)

if __name__=="__main__":
    spark = create_spark_session(APP_NAME)

    config_data = load_config() 
    db_name = config_data['athena']['db_name']

    delete_db(spark, db_name)