import boto3

_tables = {}

def get_table(table_name: str, region: str = "us-east-1"):
    if table_name not in _tables:
        _tables[table_name] = boto3.resource("dynamodb", region_name=region).Table(table_name)
    return _tables[table_name]
