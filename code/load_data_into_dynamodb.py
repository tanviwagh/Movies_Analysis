from utils import connect_to_aws_service_resource, connect_to_aws_service_client, load_config
import glob
import os 
import json

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
                'AttributeType': 'S'
            },

        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        }
    )
    return table


   
def push_data_to_dynamodb(table_name, item):
    dynamoTable = dynamodb_resource.Table(table_name)
    dynamoTable.put_item(Item = item)


if __name__ == '__main__':

    config_data = load_config() 

    access_key = config_data['aws_credentials']['access_key']
    secret_key = config_data['aws_credentials']['secret_key']

    region_name = config_data['s3_bucket_details']['region_name']
    buck_name = config_data['s3_bucket_details']['bucket_name']
    table_name = config_data['dynamodb_table_details']['table_name']
    partition_key = config_data['dynamodb_table_details']['partition_key']
    sort_key = config_data['dynamodb_table_details']['sort_key']

      
    s3_client = connect_to_aws_service_client('s3', access_key, secret_key, region_name)
    dynamodb_resource = connect_to_aws_service_resource('dynamodb', access_key, secret_key, region_name)

    try:
        movie_table = create_dynamodb_table(table_name, partition_key, sort_key)
    except: 
        print('Table already exists.')

    try:
        for obj_list in s3_client.list_objects(Bucket=buck_name)['Contents']:
            key = obj_list['Key']
            #print(key)
            content_object = s3_client.get_object(Bucket=buck_name, Key=key)
            text = content_object["Body"].read().decode()
            item = json.loads(text)
            print(type(item))
            
            push_data_to_dynamodb(table_name, item)
    except:
        print('Bucket is empty. Nothing to load.')  



