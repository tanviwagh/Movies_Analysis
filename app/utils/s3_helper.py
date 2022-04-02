import boto3 

def path_to_bucket_key(path):
    path = path.split('/')
    bucket = path[2]
    key = '/'.join(path[3:])

    return bucket, key


def read_s3_file(path, encoding='utf8'):
    s3_client = boto3.client('s3')

    bucket, key = path_to_bucket_key(path)

    obj = s3_client.get_object(Bucket=bucket, Key=key)

    return obj['Body'].read().decode(encoding)

