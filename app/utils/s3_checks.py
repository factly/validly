import boto3

from app.core.config import Settings

settings = Settings()


def get_s3_resource(
    s3_access_key: str, s3_secret_key: str, s3_endpoint_url: str, resource: str
):
    try:
        session = boto3.Session(
            aws_access_key_id=s3_access_key,
            aws_secret_access_key=s3_secret_key,
        )

        s3_resource = session.resource(resource, endpoint_url=s3_endpoint_url)
    except Exception as e:
        raise ValueError(f"Error connecting to S3: {e}")
    else:
        return s3_resource
