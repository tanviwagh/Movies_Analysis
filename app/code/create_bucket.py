from app.utils.helper import connect_to_aws_service_client


def process(spark, config):

    buck_name = config['s3_bucket_details']['bucket_name']

    s3_client = connect_to_aws_service_client('s3')

    create_bucket(s3_client, buck_name)


def create_bucket(s3_client, buck_name):
    s3_client.create_bucket(Bucket=buck_name)
    response_public = s3_client.put_public_access_block(
        Bucket=buck_name,
        PublicAccessBlockConfiguration={
            'BlockPublicAcls': True,
            'IgnorePublicAcls': True,
            'BlockPublicPolicy': True,
            'RestrictPublicBuckets': True
        },)



