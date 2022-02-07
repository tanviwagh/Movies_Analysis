from utils import connect_to_aws_service_client, load_config
import glob
import os 

def delete_dynamodb_table(table_name):
    response = dynamodb_client.delete_table(TableName='movie-analysis-table')

if __name__ == '__main__':

    config_data = load_config() 

    access_key = config_data['aws_credentials']['access_key']
    secret_key = config_data['aws_credentials']['secret_key']

    region_name = config_data['s3_bucket_details']['region_name']
    
    table_name = config_data['dynamodb_table_details']['table_name']
    
    dynamodb_client = connect_to_aws_service_client('dynamodb', access_key, secret_key, region_name)

    try:
        delete_dynamodb_table(table_name)
    except: 
        print('Table does not exist.')