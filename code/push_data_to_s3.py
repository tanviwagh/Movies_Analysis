from utils import connect_to_aws_service, load_config
import glob
import os 

def create_bucket(buck_name):
    s3_client.delete_bucket(Bucket=buck_name)
   
def push_to_s3_bucket(buck_name, key):
    s3_client.create_object(Bucket=buck_name, Key=key)

if __name__ == '__main__':

    config_data = load_config() 

    access_key = config_data['aws_credentials']['access_key']
    secret_key = config_data['aws_credentials']['secret_key']

    region_name = config_data['s3_bucket_details']['region_name']
    buck_name = config_data['s3_bucket_details']['bucket_name']

    folder_name = config_data['data']['folder_name']
    
    s3_client = connect_to_aws_service('s3', access_key, secret_key, region_name)

    create_bucket(buck_name)
    
    # need to add code for 'year' folder

    json_files = glob.glob('../' + folder_name + '/*.json')

    for filename in json_files:
        key = "%s/%s" % (folder_name, os.path.basename(filename))
        push_to_s3_bucket('../' + key ,buck_name, key)
 
    