import json
import boto3

_cache = {}

def get_secret(secret_name: str, region: str = "us-east-1") -> dict:
    if secret_name in _cache:
        return _cache[secret_name]
    client = boto3.client("secretsmanager", region_name=region)
    response = client.get_secret_value(SecretId=secret_name)
    value = json.loads(response["SecretString"])
    _cache[secret_name] = value
    return value
