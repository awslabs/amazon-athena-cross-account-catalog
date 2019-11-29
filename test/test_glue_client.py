import pytest

from heracles.clients.glue import GlueClient
from .utils import GlueStubber, build_db_response


@pytest.fixture(autouse=True)
def reset_singletons():
    GlueClient._instance = None


def test_get_all_databases_onecall(mocker):
    glue_stub = GlueStubber()
    responses = [build_db_response("test_{}".format(i)) for i in range(10)]
    glue_stub.add_response_for_method('get_databases', {'DatabaseList': responses}, {})

    with mocker.patch('boto3.client', return_value=glue_stub.client):
        with glue_stub.stubber:
            test_resp = GlueClient().get_all_database_names()
            assert len(test_resp) == len(responses)
            assert all(['test_' in name for name in test_resp])


def test_get_all_databases_pagination(mocker):
    glue_stub = GlueStubber()
    next_token = 'deadbeef=='
    responses = [build_db_response("test_{}".format(i)) for i in range(10)]
    glue_stub.add_response_for_method('get_databases', {'DatabaseList': responses, 'NextToken': next_token}, {})
    glue_stub.add_response_for_method('get_databases', {'DatabaseList': responses}, {'NextToken': next_token})

    with mocker.patch('boto3.client', return_value=glue_stub.client):
        with glue_stub.stubber:
            test_resp = GlueClient().get_all_database_names()
            assert len(test_resp) == len(responses)*2
            assert all(['test_' in name for name in test_resp])
