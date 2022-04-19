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

    def create_index_structure(self):
        index_body = {
            'settings': {
                'index': {
                    'number_of_shards': 4
                }
            },
            "mappings": {
                "properties": {
                    "mongo_id": {
                        "type": "text"
                    },
                    "objeto": {
                        "type": "text"
                    },
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

        if not self._client.indices.exists(self._index_name):
            response = self._client.indices.create(self._index_name, body=index_body)
            print('\nCreating index:')
            print(response)
        else:
            print('[WARN] El índice ya existe. Se omite la creación')

    def clean_index(self):
        if self._client.indices.exists(self._index_name):
            print(self._client.indices.delete(self._index_name))
        else:
            print('[WARN] El índice no existe. Se omite la eliminación')

    def ingest(self, json_to_ingest):
        response = self._client.index(
            index=self._index_name,
            body=json_to_ingest,
            refresh=True
        )
        if '_id' not in response:
            print(f'[ERROR] Not inserted in OpenSearch. Data={json_to_ingest}\nResponse={response}')

        return '_id' in response
