import boto3
import yaml
from yaml.loader import SafeLoader

# Open the file and load the file
def load_config():
    with open('C:/Users/Tanvi Wagh/Documents/odmp/Movies_Analysis/config/config.yml') as f:
        config_data = yaml.load(f, Loader=SafeLoader)
    return config_data

    
# ACCESS_KEY = data['aws_credentials']['ACCESS_KEY']
# SECRET_KEY = data['aws_credentials']['SECRET_KEY']

def connect_to_aws_service(service_name,ACCESS_KEY,SECRET_KEY):
    aws_client = boto3.client(service_name, 
                  region_name='us-east-1', 
                  aws_access_key_id=ACCESS_KEY, 
                  aws_secret_access_key=SECRET_KEY)
    return aws_client

'''
def connect_to_aws_service(service_name)


'''


