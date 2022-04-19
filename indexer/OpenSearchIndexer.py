import os

from indexer import OpenSearchClient as client
from db import MongoDb as mongo


def extract_index_data(licitation):
    data = {key: licitation[key] for key in licitation.keys() & {'_id', 'objeto', 'lugar', 'org_contratacion',
                                                                 'valor_estimado', 'tipo_contrato', 'estado',
                                                                 'procedimiento'}}
    data['mongo_id'] = str(data['_id'])
    del data['_id']
    return data


class OpenSearchIndexer(object):

    def __init__(self):
        self._client = client.OpenSearchClient(os.getenv('OPENSEARCH_IP'), os.getenv('OPENSEARCH_PORT'),
                                               os.getenv('OPENSEARCH_USER'), os.getenv('OPENSEARCH_PASS'))

    def full_index(self):
        print('[INFO] Ejecutando indexación completa')
        self.clean_index()
        print('[INFO] Creando el índice en OpenSearch...')
        self._client.create_index_structure()
        self.__index_all_licitations__()
        print('[INFO] Indexación finalizada')

    def clean_index(self):
        print('[INFO] Limpiando indexación actual...')
        self._client.clean_index()
        print('[INFO] Limpieza finalizada')

    def __index_all_licitations__(self):
        mongo_client = mongo.MongoDb()
        index_num = 0
        for licitation in mongo_client.find_all_licitations():
            data_to_index = extract_index_data(licitation)
            if self._client.ingest(data_to_index):
                index_num += 1
            else:
                print(f'[ERROR] No se ha podido indexar la siguiente licitacion={licitation}')

            if (index_num % 100) == 0:
                print(f'[INFO] Indexadas {index_num} licitaciones')

        print(f'[INFO] Total de licitaciones indexadas {index_num}')
