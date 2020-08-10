# Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from heracles.handlers.handlerbase import HandlerBase
from heracles.handlers.hive_mappers import HiveMappers
from heracles.hive.hive_metastore import ttypes
from heracles.clients.glue import GlueClient


class GetAllDatabases(HandlerBase):
    def execute(self, request_dict):
        return {'databases': GlueClient().get_all_database_names()}


class GetDatabases(HandlerBase):
    def execute(self, request_dict):
        return {'databases': GlueClient().get_all_database_names()}


class GetAllDatabaseObjects(HandlerBase):
    def execute(self, request_dict):
        databases = GlueClient().get_all_databases()
        database_list = []
        for database in databases:
            hive_database = HiveMappers.map_glue_database(database)
            database_list.append(self.encode_response(hive_database).decode('utf-8'))

        return {'databaseObjects': database_list}


class GetDatabase(HandlerBase):
    def execute(self, request_dict):
        databaseName = request_dict.get('dbName')
        response = GlueClient().get_database(Name=databaseName)

        db = ttypes.Database(
            name=response['Database']['Name'],
        )
        return {"database": self.encode_response(db).decode('utf-8')}


class GetDatabaseNames(HandlerBase):
    def execute(self, request_dict):
        return {'databases': GlueClient().get_all_database_names()}

class ListDatabases(HandlerBase):
    def execute(self, request_dict):
        databases = GlueClient().get_all_databases()
        database_list = []
        for database in databases:
            hive_database = HiveMappers.map_glue_database(database)
            database_list.append(self.encode_response(hive_database).decode('utf-8'))
        return {'databases': database_list}

class GetAllTables(HandlerBase):
    def execute(self, request_dict):
        databaseName = request_dict.get('dbName')
        filterString = request_dict.get('filter', '.*') or '.*'  # The actual dictionary may have a "None" value, but we need to pass a wildcard

        return {"tables": GlueClient().get_all_table_names(
            DatabaseName=databaseName,
            Expression=filterString,
        )}


class GetTableObjects(HandlerBase):
    def execute(self, request_dict):
        # Hive allows the client to provide a list of table names, however Glue does not.
        # So what we'll do is just get all tables and then filter them by the provided names.
        databaseName = request_dict.get('dbName')
        tableNames = request_dict.get('tableNames')
        tables = GlueClient().get_all_tables(DatabaseName=databaseName)

        table_list = []
        for table in tables:
            if table['Name'] not in tableNames:
                continue
            hive_table = HiveMappers.map_glue_table(databaseName, table['Name'], table)
            table_list.append(self.encode_response(hive_table).decode('utf-8'))

        return {'tables': table_list}


class GetTable(HandlerBase):
    def execute(self, request_dict):
        databaseName = request_dict.get('dbName')
        tableName = request_dict.get('tableName')
        response = GlueClient().get_table(
            DatabaseName=databaseName,
            Name=tableName
        )
        table = HiveMappers.map_glue_table(databaseName, tableName, response['Table'])
        return {"tableDesc": self.encode_response(table).decode('utf-8')}

class ListTables(HandlerBase):
    def execute(self, request_dict):
        databaseName = request_dict.get('dbName')
        filterString = request_dict.get('filter', '.*') or '.*'  # The actual dictionary may have a "None" value, but we need to pass a wildcard
        tables = GlueClient().get_all_tables(
            DatabaseName=databaseName,
            Expression=filterString,
        )
        table_list = []

        for table in tables:
            hive_table = HiveMappers.map_glue_table(databaseName, table['Name'], table)
            table_list.append(self.encode_response(hive_table).decode('utf-8'))
        return {'tables': table_list}

class GetTableNames(HandlerBase):
    def execute(self, request_dict):
        databaseName = request_dict.get('dbName')
        filterString = request_dict.get('filter', '.*') or '.*'  # The actual dictionary may have a "None" value, but we need to pass a wildcard

        return {"tables": GlueClient().get_all_table_names(
            DatabaseName=databaseName,
            Expression=filterString,
        )}


class GetPartitions(HandlerBase):
    def execute(self, request_dict):
        databaseName = request_dict.get('dbName')
        tableName = request_dict.get('tableName')

        response = GlueClient().get_all_partitions(
            DatabaseName=databaseName,
            TableName=tableName,
        )

        partition_list = []
        for partition in response['Partitions']:
            hive_partition = HiveMappers.map_glue_partition_for_table(databaseName, tableName, partition)
            partition_list.append(self.encode_response(hive_partition).decode('utf-8'))

        return {"partitions": partition_list}


class GetPartitionsByNames(HandlerBase):
    def execute(self, request_dict):
        databaseName = request_dict.get('dbName')
        tableName = request_dict.get('tableName')

        # `names` is a slash-delimited list of partition key/value pairs that is at least size 1
        partition_names = request_dict.get('names')

        partition_list = []
        for partition in partition_names:
            part_value = self._get_partition_values(partition)
            response = GlueClient().get_partition(
                DatabaseName=databaseName,
                TableName=tableName,
                PartitionValues=[part_value]
            )
            hive_partition = HiveMappers.map_glue_partition_for_table(databaseName, tableName, response['Partition'])
            partition_list.append(self.encode_response(hive_partition).decode('utf-8'))

        return {'partitionDescs': partition_list}

    def _get_partition_values(self, request_name):
        return ','.join([x.split('=')[1] for x in request_name.split('/')])
