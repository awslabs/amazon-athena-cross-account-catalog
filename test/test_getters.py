import pytest

from botocore.exceptions import ClientError
from heracles.handlers.getters import GetAllDatabases, GetDatabase, GetAllTables, GetTable
from heracles.clients.glue import GlueClient
from .utils import GlueStubber, build_db_response, build_table_response


@pytest.fixture(autouse=True)
def reset_singletons():
    GlueClient._instance = None


def test_get_all_databases(mocker):
    glue_stub = GlueStubber()
    responses = [build_db_response("test_{}".format(i)) for i in range(10)]
    glue_stub.add_response_for_method('get_databases', {'DatabaseList': responses}, {})

    with mocker.patch('boto3.client', return_value=glue_stub.client):
        with glue_stub.stubber:
            response = GetAllDatabases().execute({})
            assert len(response['databases']) == 10


def test_get_database(mocker):
    glue_stub = GlueStubber()
    single_db = build_db_response("test_single")
    glue_stub.add_response_for_method('get_database', {'Database': single_db}, {'Name': 'test_single'})
    with mocker.patch('boto3.client', return_value=glue_stub.client):
        with glue_stub.stubber:
            response = GetDatabase().execute({'dbName': 'test_single'})
            # The result is a JSONThrift-encoded string, so we just make sure the database name is there
            assert '"test_single"' in response['database']

            # Getting a non-existing database raises an error
            glue_stub.add_error_for_method('get_database', 'EntityNotFoundException', ' Database notadb not found.', 404, {'Name': 'notadb'})
            with pytest.raises(ClientError) as excinfo:
                print("WHAT")
                response = GetDatabase().execute({'dbName': 'notadb'})
            assert str(excinfo.value) == 'An error occurred (EntityNotFoundException) when calling the GetDatabase operation:  Database notadb not found.'


def test_get_all_tables(mocker):
    database_name = 'defaultdb'
    glue_stub = GlueStubber()
    responses = [build_table_response(database_name, "test_{}".format(i), [], ["a", "b"]) for i in range(10)]
    glue_stub.add_response_for_method('get_tables', {'TableList': responses}, {'DatabaseName': database_name, 'Expression': '.*'})

    with mocker.patch('boto3.client', return_value=glue_stub.client):
        with glue_stub.stubber:
            response = GetAllTables().execute({'dbName': database_name})
            assert len(response['tables']) == 10
            assert response['tables'][0] == 'test_0'


def test_get_table(mocker):
    database_name = 'defaultdb'
    table_name = 'singletable'
    glue_stub = GlueStubber()
    responses = build_table_response(database_name, table_name, [], ["a", "b"])
    glue_stub.add_response_for_method('get_table', {'Table': responses}, {'DatabaseName': database_name, 'Name': table_name})

    with mocker.patch('boto3.client', return_value=glue_stub.client):
        with glue_stub.stubber:
            response = GetTable().execute({'dbName': database_name, 'tableName': table_name})
            assert 'tableDesc' in response
            print(response)
            assert '"{}"'.format(table_name) in response['tableDesc']
            assert '"{}"'.format(database_name) in response['tableDesc']
