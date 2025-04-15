import os
import boto3
from utils import http_io as ufh

SECRET_SIDECAR_URL = os.environ.get("SECRET_SIDECAR_URL", "http://localhost:8080")


def get_secret(secret_name):
    secret = ufh.get_http_response(url=f"{SECRET_SIDECAR_URL}/{secret_name}")
    return secret

def get_aws_session(aws_iam_user: str, aws_region: str):
    aws_iam_user_access_key = get_secret(f"IAM_USER_{aws_iam_user.upper()}_ACCESS_KEY")
    aws_iam_user_secret = get_secret(f"IAM_USER_{aws_iam_user.upper()}_SECRET")
    if aws_iam_user_access_key and aws_iam_user_secret and aws_region:
        aws_session = boto3.Session(
            aws_access_key_id=aws_iam_user_access_key,
            aws_secret_access_key=aws_iam_user_secret,
            region_name=aws_region,
        )
        return aws_session
    else:
        raise RuntimeError("Unable to create AWS session.")
