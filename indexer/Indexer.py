import os

from indexer import ElasticClient as client
from db import MongoDb as mongo


def extract_index_data(licitation):
    data = {key: licitation[key] for key in licitation.keys() & {'_id', 'objeto', 'lugar', 'org_contratacion',
                                                                 'valor_estimado', 'tipo_contrato', 'estado',
                                                                 'procedimiento'}}
    data['mongo_id'] = str(data['_id'])
    del data['_id']
    return data


class Indexer(object):

    def __init__(self):
        self._client = client.ElasticClient()

    def full_index(self):
        print('[INFO] Ejecutando indexación completa')
        self.clean_index()
        print('[INFO] Creando el índice en OpenSearch...')
        self._client.create_index_structure()
        self.__index_licitations__(True)
        print('[INFO] Indexación finalizada')

    def update_index(self):
        print('[INFO] Ejecutando indexación de nuevos registros')
        self.__index_licitations__(False)
        print('[INFO] Indexación finalizada')

    def clean_index(self):
        print('[INFO] Limpiando indexación actual...')
        self._client.clean_index()
        print('[INFO] Limpieza finalizada')

    def __index_licitations__(self, full_index):
        mongo_client = mongo.MongoDb()
        index_num = 0
        if full_index:
            cursor = mongo_client.find_all_licitations()
        else:
            cursor = mongo_client.find_all_new_licitations()

        for licitation in cursor:
            data_to_index = extract_index_data(licitation)
            if self._client.ingest(data_to_index):
                index_num += 1
            else:
                print(f'[ERROR] No se ha podido indexar la siguiente licitacion={licitation}')

            if (index_num % 100) == 0:
                print(f'[INFO] Indexadas {index_num} licitaciones')

        print('[INFO] Refrescando indices...')
        self._client.refresh()
        print(f'[INFO] Total de licitaciones indexadas {index_num}')
