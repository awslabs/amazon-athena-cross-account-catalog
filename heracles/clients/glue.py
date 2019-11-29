# Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import boto3


class GlueClient(object):
    _instance = None
    _catalog_id = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = object.__new__(cls)
            GlueClient._instance.client = boto3.client('glue')
        return cls._instance

    def __init__(self):
        self.client = self._instance.client

    def call_remote_catalog(self, method_name, **kwargs):
        """Provide a generic method to call a Glue Data Catalog with the pre-defined catalog ID"""
        if GlueClient._catalog_id and 'CatalogId' not in kwargs:
            kwargs['CatalogId'] = GlueClient._catalog_id

        method = getattr(self.client, method_name)
        response = method(**kwargs)
        return response

    def get_database(self, **kwargs):
        """Retrieve a single database from the Glue Data Catalog."""
        return self.call_remote_catalog('get_database', **kwargs)

    def get_all_databases(self):
        """Retrieve all databases from the Glue Data Catalog and return as a dictionary of Database objects."""
        return self.fetch_all("get_databases", 'DatabaseList')

    def get_all_database_names(self):
        """Retrieve all databases from the Glue Data Catalog and return as a simple list."""
        resp = self.fetch_all("get_databases", 'DatabaseList')
        database_names = [r.get('Name') for r in resp]
        return database_names

    def get_table(self, **kwargs):
        """Retrieve a single table from the Glue Data Catalog."""
        return self.call_remote_catalog('get_table', **kwargs)

    def get_all_tables(self, **kwargs):
        """Rtreive all tables from teh Glue Data Catalog and return as a dictionary of Table objects."""
        return self.fetch_all('get_tables', 'TableList', **kwargs)

    def get_all_table_names(self, **kwargs):
        """Retrieve all tables from the Glue Data Catalog and return as a simple list."""
        resp = self.fetch_all('get_tables', 'TableList', **kwargs)
        table_names = [r.get('Name') for r in resp]
        return table_names

    def get_partition(self, **kwargs):
        """Retrieve a single table from the Glue Data Catalog."""
        return self.call_remote_catalog('get_partition', **kwargs)

    def get_all_partitions(self, **kwargs):
        """Retrieve all partitions from the a Glue Data Catalog table and return as a list of partition objects."""
        resp = self.fetch_all('get_partitions', 'Partitions', **kwargs)
        return {'Partitions': resp}

    def fetch_all(self, method_name, response_key, **kwargs):
        """Fetches all responses from the Glue API and concatenates the values of `response_key` into a list."""
        responses = []
        params = kwargs.copy()

        while True:
            response = self.call_remote_catalog(method_name, **params)
            responses.extend(response.get(response_key, []))
            if 'NextToken' in response:
                params['NextToken'] = response.get('NextToken')
            else:
                break

        return responses
