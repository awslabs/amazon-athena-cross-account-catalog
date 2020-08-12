# Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os

from heracles.handlers.handlerbase import HandlerBase
from heracles.handlers.getters import GetAllDatabases, GetDatabase, GetAllTables, GetTable, GetPartitions
from heracles.clients.glue import GlueClient


# Instantiate the GlueClient with a default Catalog ID if provided
if 'TARGET_ACCOUNT_ID' in os.environ:
    GlueClient._catalog_id = os.environ['TARGET_ACCOUNT_ID']
    
# Connect to Glue Catalog in the user-defined region if provided and valid. Else use same region as this function.
available_regions = ["af-south-1", "eu-north-1", "ap-south-1", "eu-west-3", "eu-west-2", "eu-south-1", "eu-west-1", "ap-northeast-3", "ap-northeast-2", "me-south-1", "ap-northeast-1", "sa-east-1", "ca-central-1", "ap-east-1", "ap-southeast-1", "ap-southeast-2", "eu-central-1", "us-east-1", "us-east-2", "us-west-1", "us-west-2"]
if 'CATALOG_REGION' in os.environ and os.environ['CATALOG_REGION'] in available_regions:
    GlueClient._catalog_region = os.environ['CATALOG_REGION']
else:
    print("No valid region passed in CATALOG_REGION. Switching to default region {}". format(os.environ['AWS_DEFAULT_REGION']))
    GlueClient._catalog_region = os.environ['AWS_DEFAULT_REGION']

def handler(event, context):
    api_name = event.get('apiName')
    api_request = event.get('apiRequest', None)
    print("Calling {}({})".format(api_name, api_request))

    klas = HandlerBase.get_class_for_api_name(api_name)
    result = klas().execute(api_request)

    api_response = {
        'apiName': api_name,
        'spilled': False,
        'spillPath': None,
        'apiResponse': result
    }
    return api_response
