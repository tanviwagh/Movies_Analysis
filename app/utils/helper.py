import boto3
import yaml
from yaml.loader import SafeLoader
from pyspark.sql import SparkSession
from app.utils.s3_helper import read_s3_file

def load_config(path):
    file = read_s3_file(path)
    config_data = yaml.load(file, Loader=SafeLoader)

    return config_data


def connect_to_aws_service_resource(service_name, access_key, secret_key, region_name):
    aws_resource = boto3.resource(service_name, 
                 region_name=region_name,
                 aws_access_key_id=access_key, 
                 aws_secret_access_key=secret_key)

    return aws_resource


def create_spark_session(app_name):
    spark = SparkSession.builder \
            .master("local") \
            .appName(app_name) \
            .enableHiveSupport() \
            .getOrCreate()

    return spark 