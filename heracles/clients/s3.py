# Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import boto3

class S3Client(object):
    _instance = None
    _spill_bucket = None
    _spill_prefix = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = object.__new__(cls)
            S3Client._instance.client = boto3.client('s3')
        return cls._instance

    def __init__(self):
        self.client = self._instance.client
    
    def call_s3(self, method_name, **kwargs):
        """Provide a generic method to call S3 with the pre-defined s3 path"""
        if S3Client._spill_bucket and 'Bucket' not in kwargs:
            kwargs['Bucket'] = S3Client._spill_bucket
        if S3Client._spill_prefix and kwargs['Key']:
            kwargs['Key'] = S3Client._spill_prefix  + kwargs['Key']

        method = getattr(self.client, method_name)
        response = method(**kwargs)
        return response
    
    def spill_response_to_s3(self, **kwargs):
        spilled = None
        try:
            spilled = self.call_s3('put_object', **kwargs)
        except Exception as e: print(e)
        if spilled:
            spilled = 's3://{}/{}{}'.format(self._spill_bucket, self._spill_prefix, kwargs['Key']) 
            print("{} is spilled to S3.".format(kwargs['Key']))
        return spilled
        
    def check_spill_exists(self, **kwargs):
        spill_path = None
        try:
            spill_path = self.call_s3('head_object', **kwargs)
        except Exception as e: print("Object doesn't exist or inaccessible: {}".format(e))
        if spill_path:
            print("Spill {} exists! Responding".format(kwargs['Key']))
            spill_path = 's3://{}/{}{}'.format(self._spill_bucket, self._spill_prefix, kwargs['Key']) 
        return spill_path
        
    def download_all_partitions(self, **kwargs):
        response = None
        try:
            response = self.call_s3('get_object', **kwargs)
        except Exception as e: print(e)
        if response:
            response = response['Body'].read().decode('utf-8')
        return response