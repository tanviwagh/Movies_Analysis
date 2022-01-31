from utils import connect_to_aws_service_resource, connect_to_aws_service_client, load_config
import glob
import os 

def create_dynamodb_table(table_name, partition_key, sort_key):
            
    table = dynamodb_resource.create_table(
        TableName=table_name,
        KeySchema=[
            {
                'AttributeName': partition_key,
                'KeyType': 'HASH'  # Partition key
            },
            {
                'AttributeName': sort_key,
                'KeyType': 'RANGE'  # Sort key
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': partition_key,
                'AttributeType': 'S'
            },
            {
                'AttributeName': sort_key,
                'AttributeType': 'N'
            },

        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        }
    )
    return table


   
# def push_data_to_dynamodb(file, buck_name, key):
#     dynamodb_resource.upload_file(Filename=file, Bucket=buck_name, Key=key)


if __name__ == '__main__':

    config_data = load_config() 

    access_key = config_data['aws_credentials']['access_key']
    secret_key = config_data['aws_credentials']['secret_key']

    region_name = config_data['s3_bucket_details']['region_name']
    # buck_name = config_data['s3_bucket_details']['bucket_name']
    table_name = config_data['dynamodb_table_details']['table_name']
    partition_key = config_data['dynamodb_table_details']['partition_key']
    sort_key = config_data['dynamodb_table_details']['sort_key']

      
    # s3_client = connect_to_aws_service_client('s3', access_key, secret_key, region_name)
    dynamodb_resource = connect_to_aws_service_resource('dynamodb', access_key, secret_key, region_name)

    movie_table = create_dynamodb_table(table_name, partition_key, sort_key)




