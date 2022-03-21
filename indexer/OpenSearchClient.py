import os
from opensearchpy import OpenSearch


class OpenSearchClient(object):
    def __init__(self, host, port, user, pw):
        self._index_name = os.getenv('OPENSEARCH_LICITATION_INDX_NAME')
        self._connect_open_search(host, port, user, pw)

    def _connect_open_search(self, host, port, user, pw):
        host = host
        port = port
        auth = (user, pw)
        # pem_open_search = 'file.pem' download. but it work without it

        self._client = OpenSearch(
            hosts=[{'host': host, 'port': port}],
            http_compress=True,
            http_auth=auth,
            use_ssl=False,
            verify_certs=False,
            ssl_assert_hostname=False,
            ssl_show_warn=False
        )

        index_body = {
            'settings': {
                'index': {
                    'number_of_shards': 4
                }
            },
            "mappings": {
                "properties": {
                    "lugar": {
                        "type": "text"
                    },
                    "org_contratacion": {
                        "type": "text"
                    },
                    "valor_estimado": {
                        "type": "double"
                    },
                    "tipo_contrato": {
                        "type": "text"
                    },
                    "estado": {
                        "type": "text"
                    },
                    "procedimiento": {
                        "type": "text"
                    }
                }
            }
        }

        if self._client.indices.exists(self._index_name):
            response = self._client.indices.create(self._index_name, body=index_body)
            print('\nCreating index:')
            print(response)

    def ingest(self, json_to_ingest):
        response = self._client.index(
            index=self._index_name,
            body=json_to_ingest,
            refresh=True
        )
        if '_id' in response:
            print(f'[INFO] Added document with ID={response["_id"]}')
        else:
            print(f'[ERROR] Not inserted in OpenSearch. Data={json_to_ingest}\nResponse={response}')
