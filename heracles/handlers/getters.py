# Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from heracles.handlers.handlerbase import HandlerBase
from heracles.handlers.hive_mappers import HiveMappers
from heracles.hive.hive_metastore import ttypes
from heracles.clients.glue import GlueClient
from heracles.clients.s3 import S3Client
import json, sys

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
        
        # For table with spilled content, fetch response from S3 to optimize query execution time
        key = "GetPartitions-{}-{}.json".format(databaseName, tableName)
        spillPath = S3Client().check_spill_exists(Key=key)
        if spillPath:
            return {'spillPath': spillPath}
        else:
            partition_list, response = self.get_partitions(databaseName, tableName)
            
            # set spill threshold of 4MB for the response size
            output = json.dumps(partition_list)
            response_size = sys.getsizeof(output)
            if response_size > 4194304:
                print("Response size ({}) exceeds 4MB threshold. Spilling to S3.".format(response_size))
                spillPath = self.spill_partitions_to_s3('GetPartitions', databaseName, tableName, output)
                
                # Since there are two many partitions, let's proactively map and spill this response for subsequent calls getPartitionNames and getPartitionsByNames
                pv = [r.get('Values') for r in response['Partitions']]
                partition_values = GetPartitionNames().map_partition_keys_values(databaseName, tableName, pv)
                partition_values = json.dumps(partition_values)

                self.spill_partitions_to_s3('GetPartitionNames', databaseName, tableName, partition_values)
                
                # Perform similar proactive spill for GetPartitionsByNames API as well
                partitions_by_names = []
                for partition in response['Partitions']:
                    partition['CreationTime'] = HiveMappers.unix_epoch_as_int(partition.get('CreationTime', None))
                    partition['LastAccessTime'] = HiveMappers.unix_epoch_as_int(partition.get('LastAccessTime', None))
                    partitions_by_names.append(partition)
                partitions_by_names = json.dumps({"Partitions": partitions_by_names})
                self.spill_partitions_to_s3('GetPartitionByNames', databaseName, tableName, partitions_by_names)
                
                # Response for GetPartitions API
                return {'spillPath': spillPath}
            else:
                return partition_list
    
    def get_partitions(self, databaseName, tableName):
        response = GlueClient().get_all_partitions(
                DatabaseName=databaseName,
                TableName=tableName,
        )

        partition_list = []
        for partition in response['Partitions']:
            hive_partition = HiveMappers.map_glue_partition_for_table(databaseName, tableName, partition)
            partition_list.append(self.encode_response(hive_partition).decode('utf-8'))
        return {"partitions": partition_list}, response
        
    def spill_partitions_to_s3(self, api, databaseName, tableName, body):
        key = "{}-{}-{}.json".format(api, databaseName, tableName)
        body = body.encode('utf-8')
        return S3Client().spill_response_to_s3(
            Key=key,
            Body=body
        )

class GetPartitionsByNames(HandlerBase):
    def execute(self, request_dict):
        databaseName = request_dict.get('dbName')
        tableName = request_dict.get('tableName')
        
        # `names` is a slash-delimited list of partition key/value pairs that is at least size 1
        partition_names = request_dict.get('names')
        print("Partitions requested: {}". format(len(partition_names)))
        
        # Check for existing spill based on first partition value requested in the API
        range_key = "GetPartitionByNames-{}-{}-spill-{}.json".format(databaseName, tableName, self._get_partition_values(partition_names[0]))
        spillPath = S3Client().check_spill_exists(Key=range_key)
        if spillPath:
            return {'spillPath': spillPath}
        
        # For table with spilled content, fetch response from S3 to optimize query execution time
        key = "GetPartitionByNames-{}-{}.json".format(databaseName, tableName)
        if S3Client().check_spill_exists(Key=key) and len(partition_names) > 100:
            print("GetPartitionByNames Spill exists in S3. Querying from here.")
            s3_object = S3Client().download_all_partitions(Key=key)
            all_partitions = json.loads(s3_object)
            all_partitions = all_partitions['Partitions']
            partition_list = []
            for partition in partition_names:
                part_value = self._get_partition_values(partition)
                response = next((x for x in all_partitions if ','.join(x['Values']) == part_value), None)
                if response:
                    hive_partition = HiveMappers.map_glue_partition_for_table(databaseName, tableName, response)
                    partition_list.append(self.encode_response(hive_partition).decode('utf-8'))
            key = "GetPartitionByNames-{}-{}-spill-{}.json".format(databaseName, tableName, self._get_partition_values(partition_names[0]))
            body = json.dumps({'partitionDescs': partition_list}).encode('utf-8')
            spillPath = S3Client().spill_response_to_s3(
                    Key=key,
                    Body=body
                )
            return {'spillPath': spillPath}
        else:
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

class GetPartitionNames(HandlerBase):
    def execute(self, request_dict):
        databaseName = request_dict.get('dbName')
        tableName = request_dict.get('tableName')
        
        # For table with spilled content, fetch response from S3 to optimize query execution time
        key = "GetPartitionNames-{}-{}.json".format(databaseName, tableName)
        spillPath = S3Client().check_spill_exists(Key=key)
        if spillPath:
            return {'spillPath': spillPath}
        else:
            # Fetch partition values
            partition_values = GlueClient().get_all_partition_values(
                DatabaseName=databaseName,
                TableName=tableName
            )
        
            return self.map_partition_keys_values(databaseName, tableName, partition_values)
        
    def map_partition_keys_values(self, databaseName, tableName, partition_values):
        # Fetch partition keys from table
        partition_keys = GlueClient().get_table(
            DatabaseName=databaseName,
            Name=tableName
        )
        partition_keys = partition_keys['Table']['PartitionKeys']
        
        # Map partition keys with values to create final list
        partitionNames = []
        for part_value in partition_values:
            parts = []
            for i in range(len(partition_keys)):
                parts.append("{}={}".format(partition_keys[i]['Name'], part_value[i]))
            partitionNames.append('/'.join(parts))
            
        return {'partitionNames': partitionNames}