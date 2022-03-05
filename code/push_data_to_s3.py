from utils import connect_to_aws_service_client, load_config, create_spark_session, arg_parser
import glob
import os 

APP_NAME = "push_data_to_s3"

def create_bucket(buck_name):
    s3_client.create_bucket(Bucket=buck_name)
    response_public = s3_client.put_public_access_block(
        Bucket=buck_name,
        PublicAccessBlockConfiguration={
            'BlockPublicAcls': True,
            'IgnorePublicAcls': True,
            'BlockPublicPolicy': True,
            'RestrictPublicBuckets': True
        },)
   
def push_to_s3_bucket(file, buck_name, key):
    s3_client.upload_file(Filename=file, Bucket=buck_name, Key=key)

if __name__ == '__main__':
    spark = create_spark_session(APP_NAME)
    
    config_data = load_config() 

    access_key = config_data['aws_credentials']['access_key']
    secret_key = config_data['aws_credentials']['secret_key']

    region_name = config_data['s3_bucket_details']['region_name']
    buck_name = config_data['s3_bucket_details']['bucket_name']

    folder_name = config_data['data']['parquet_folder_name']
    
    s3_client = connect_to_aws_service_client('s3', access_key, secret_key, region_name)

    create_bucket(buck_name)

    ngrams = arg_parser('Please state json or parquet')
    if ngrams != '':
        try:
            for subdir in os.listdir('../' + folder_name):
                sub_folder_name = '/' + subdir 
            
                all_files = glob.glob('../' + folder_name + sub_folder_name + '/*.' + ngrams)
                #print(json_files)

                for filename in all_files:
                    key = "%s/%s" % (folder_name + sub_folder_name, os.path.basename(filename))
                    print(key)
                    push_to_s3_bucket('../' + key ,buck_name, key)
        except:
            print('Bucket does not exist.')