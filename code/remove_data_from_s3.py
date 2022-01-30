from utils import connect_to_aws_service, load_config
import glob
import os 

def delete_bucket(bucket_name):
    s3_client.delete_bucket(Bucket=bucket_name)
   
def delete_data_from_bucket(bucket_name, key):
    s3_client.delete_object(Bucket=bucket_name, Key=key)

if __name__ == '__main__':

    config_data = load_config() 

    access_key = config_data['aws_credentials']['access_key']
    secret_key = config_data['aws_credentials']['secret_key']

    region_name = config_data['s3_bucket_details']['region_name']
    bucket_name = config_data['s3_bucket_details']['bucket_name']

    folder_name = config_data['data']['folder_name']
    
    s3_client = connect_to_aws_service('s3', access_key, secret_key, region_name)

    json_files = glob.glob('../data/*.json')

    for filename in json_files:
        key = "%s/%s" % (folder_name, os.path.basename(filename))
        delete_data_from_bucket(bucket_name, key)
 
    delete_bucket(bucket_name)