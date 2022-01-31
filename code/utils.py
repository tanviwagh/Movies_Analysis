import boto3
import yaml
from yaml.loader import SafeLoader

def load_config():
    with open('../config/config.yml') as file:
        config_data = yaml.load(file, Loader=SafeLoader)

    return config_data

def connect_to_aws_service_client(service_name, access_key, secret_key, region_name):
    aws_client = boto3.client(service_name, 
                 region_name=region_name,
                 aws_access_key_id=access_key, 
                 aws_secret_access_key=secret_key)

    return aws_client

def connect_to_aws_service_resource(service_name, access_key, secret_key, region_name):
    aws_resource = boto3.resource(service_name, 
                 region_name=region_name,
                 aws_access_key_id=access_key, 
                 aws_secret_access_key=secret_key)

    return aws_resource
