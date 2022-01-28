from utils import connect_to_aws_service, load_config

   
def push_to_s3(local_file):
    s3_client.upload_file(Filename=local_file, Bucket=buck_name, Key=key)

if __name__ == '__main__':
    config_data = load_config() 

    ACCESS_KEY = config_data['aws_credentials']['ACCESS_KEY']
    SECRET_KEY = config_data['aws_credentials']['SECRET_KEY']
    
    s3_client = connect_to_aws_service('s3',ACCESS_KEY,SECRET_KEY)

    key = config_data['s3_bucket_details']['key']
    buck_name = config_data['s3_bucket_details']['bucket_name']
    # key = "dump/file.txt"
    # buck_name = 'myybuckk'
    push_to_s3('C:/Users/Tanvi Wagh/Documents/odmp/dp.txt')
 

