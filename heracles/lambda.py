# Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os

from heracles.handlers.handlerbase import HandlerBase
from heracles.handlers.getters import GetAllDatabases, GetDatabase, GetAllTables, GetTable, GetPartitions
from heracles.clients.glue import GlueClient


# Instantiate the GlueClient with a default Catalog ID if provided
if 'TARGET_ACCOUNT_ID' in os.environ:
    GlueClient._catalog_id = os.environ['TARGET_ACCOUNT_ID']


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
