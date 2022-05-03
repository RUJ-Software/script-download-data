import os
from elasticsearch import Elasticsearch, helpers
import configparser


class ElasticClient(object):
    def __init__(self):
        config = configparser.ConfigParser()
        config.read('config/transparencia-elastic.ini')
        self._client = Elasticsearch(
            cloud_id=config['ELASTIC']['cloud_id'],
            basic_auth=(config['ELASTIC']['user'], config['ELASTIC']['password'])
        )
        self._index_name = os.getenv('LICITATION_INDX_NAME')

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

        if not self._client.indices.exists(index=self._index_name):
            response = self._client.indices.create(index=self._index_name, body=index_body)
            print('\nCreating index:')
            print(response)
        else:
            print('[WARN] El índice ya existe. Se omite la creación')

    def clean_index(self):
        if self._client.indices.exists(index=self._index_name):
            print(self._client.indices.delete(index=self._index_name))
        else:
            print('[WARN] El índice no existe. Se omite la eliminación')

    def ingest(self, json_to_ingest):
        response = self._client.index(
            index=self._index_name,
            body=json_to_ingest,
        )
        if '_id' not in response:
            print(f'[ERROR] Not inserted in OpenSearch. Data={json_to_ingest}\nResponse={response}')

        return '_id' in response

    def refresh(self):
        self._client.indices.refresh(index=self._index_name)