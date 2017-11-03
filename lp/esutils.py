import time
import re
import json

from requests_aws4auth import AWS4Auth
from botocore.credentials import create_credential_resolver
from botocore.session import get_session

from elasticsearch import Elasticsearch
from elasticsearch import RequestsHttpConnection, serializer
from elasticsearch import compat, exceptions
# from elasticsearch.helpers import streaming_bulk, bulk


###############################################################################
# variables to be changed


class JSONSerializerPython2(serializer.JSONSerializer):
    """Override elasticsearch library serializer to ensure it encodes utf characters during json dump.
    See original at: https://github.com/elastic/elasticsearch-py/blob/master/elasticsearch/serializer.py#L42
    A description of how ensure_ascii encodes unicode characters to ensure they can be sent across the wire
    as ascii can be found here: https://docs.python.org/2/library/json.html#basic-usage
    """
    def dumps(self, data):
        # don't serialize strings
        if isinstance(data, compat.string_types):
            return data
        try:
            return json.dumps(data, default=self.default, ensure_ascii=True)
        except (ValueError, TypeError) as e:
            raise exceptions.SerializationError(data, e)


def ConnectToElasticSearch(
        host, port=443, region="eu-central-1", MAX_RETRIES=5, **kwargs):
    # send to ES with exponential backoff
    retries = 0
    while retries < MAX_RETRIES:
        if retries > 0:
            seconds = (2**retries) * .1
            print('Could not connect to ES, retrying after %.1f seconds', seconds)
            time.sleep(seconds)
        retries = retries + 1

        s = get_session()
        credential_resolver = create_credential_resolver(s)
        credentials = credential_resolver.load_credentials()

        awsauth = AWS4Auth(
            credentials.access_key, credentials.secret_key, region, 'es',
            session_token=credentials.token)

        es = Elasticsearch(
            hosts=[{'host': host, 'port': port}],
            http_auth=awsauth,
            use_ssl=True,
            verify_certs=True,
            connection_class=RequestsHttpConnection,
            serializer=JSONSerializerPython2()
        )
        return es
    raise RuntimeError("Could not connect to elasticsearch cluster.")


def ClearIndices(es, pattern):
    ind_client = es.indices
    stats = ind_client.stats()
    for key, data in stats['indices'].items():
        if re.match(pattern, key):
            print("Key {0} matches pattern.".format(key))
            ind_client.delete(index=key)


def ListIndices(es, pattern=None):
    ind_client = es.indices
    stats = ind_client.stats()
    if (pattern):
        rval = []
        for key, data in stats['indices'].items():
            if re.match(pattern, key):
                rval.append(key)
    else:
        rval = stats['indices'].keys()
    return rval


def BasicIndexGenerator(data, options):
    while (data):
        item = data.next()
        action = {
            '_op_type': 'index',
            '_index': options['es_index'],
            '_type': options['es_type'],
            '_id': item[options['data_id_field']],
            # '_parent': 5,
            # 'pipeline': 'my-ingest-pipeline',
            '_source': item
        }
        yield action


def BulkUpload(es, data, options, IndexGenerator=BasicIndexGenerator):
    # This inline will convert the data dictlist into a list of operations
    bulk_iter = streaming_bulk(
        es, IndexGenerator(iter(data), options),
        chunk_size=(options.get("es_chunk_size", 1024))
    )
    try:
        while(bulk_iter):
            bulk_iter.next()
    except StopIteration:
        pass

# Get the elasticsearch client (you can do all kinds of stuff with it!)

# host = "search-elasticsearch-log-lrvfu5lzwj2a3b3bxbtg7tcv5i.eu-central-1.es.amazonaws.com"
# port = 443
# region = 'eu-central-1'
#
# es_client = ConnectToElasticSearch(host)

# Example: Put the account list into ES

# ingest_options = {
#     "es_index": "aws-accountlist",
#     "es_type": "aws-account",
#     "data_id_field": "ID"
# }
#BulkUpload(es_client, data, ingest_options)
