import datetime
from dateutil.tz import tzlocal

import boto3
from botocore.stub import Stubber


class GlueStubber(object):
    def __init__(self):
        self.client = boto3.client("glue")
        self.stubber = Stubber(self.client)

    def add_response_for_method(self, method_name, response_body, request_params):
        self.stubber.add_response(method_name, response_body, request_params)

    def add_error_for_method(self, method_name, error_code, error_message, http_code, request_params):
        self.stubber.add_client_error(method_name, error_code, error_message, http_code, None, request_params)


def build_db_response(dbname):
    return {
        'Name': dbname,
        'CreateTime': datetime.datetime(2018, 7, 5, 14, 18, 50, tzinfo=tzlocal()),
        'CreateTableDefaultPermissions': [{
            'Principal': {'DataLakePrincipalIdentifier': 'EVERYONE'},
            'Permissions': ['ALL']
        }]
    }


def build_table_response(dbname, tablename, partition_keys, columns):
    return {
        "DatabaseName": dbname,
        "Name": tablename,
        "CreatedBy": "username1",
        "Owner": "username2",
        "CreateTime": datetime.datetime.utcnow(),
        "Description": "table description",
        "IsRegisteredWithLakeFormation": False,
        "LastAccessTime": datetime.datetime(1969, 12, 31, 16, 0, tzinfo=tzlocal()), # This is often "0"
        "LastAnalyzedTime": datetime.datetime.utcnow(),
        "Parameters": {},
        "PartitionKeys": [{'Name': x, 'Type': 'string'} for x in partition_keys],
        "Retention": 0,
        "StorageDescriptor": {
            "BucketColumns": [],
            "Columns": [{'Name': x, 'Type': 'string'} for x in partition_keys],
            "Compressed": False,
            "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
            "Location": "s3://<bucket>/<prefix>",
            "NumberOfBuckets": 0,
            "Parameters": {},
            "SerdeInfo": {
                "Name": "SerdeName",
                "Parameters": {},
                "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
            },
            "SkewedInfo": {
                "SkewedColumnNames":  [],
                "SkewedColumnValueLocationMaps": {},
                "SkewedColumnValues": []
            },
            "SortColumns": [],
            "StoredAsSubDirectories": False
        },
        "TableType": "EXTERNAL_TABLE",
        "UpdateTime": datetime.datetime.utcnow()
    }
