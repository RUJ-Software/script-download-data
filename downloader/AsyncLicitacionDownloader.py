import asyncio
from urllib.request import urlopen
import aiohttp
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET
from datetime import datetime

from db import MongoDb as mongo


class AsyncLicitacionDownloader(object):
    def __init__(self, observable, schedule, url):
        self._url = url
        self._observable = observable
        self._schedule = schedule
        self._execution_time = datetime.now()
        self._mongo = mongo.MongoDb()
        self._last_url = self._mongo.find_last_url()

    async def async_download(self):
        counter = 0
        prev_url = self._url
        while self._url is not None:
            tasks = []
            try:
                content = urlopen(self._url)
                print('Retriving information from', self._url)

                if prev_url not in self._url:
                    self._mongo.insert_url(self._execution_time, self._url)

                prev_url = self._url
                tree = ET.parse(content)
                root = tree.getroot()

                entry_tag = '{http://www.w3.org/2005/Atom}entry'

                for element in root:
                    if element.tag == entry_tag:
                        new_link = element.find('{http://www.w3.org/2005/Atom}link').attrib['href']
                        # Insertamos la task de los datos a descargar
                        tasks.append(asyncio.create_task(self.__download_data__(new_link)))
                        counter += 1
                    if 'rel' in element.attrib and element.attrib['rel'] == 'next':
                        self._url = element.attrib['href']
                # Lanzar la descarga de todos los datos de esta pagina
                await asyncio.gather(*tasks)
                if self._url == 'https://contrataciondelestado.es/sindicacion/sindicacion_643/licitacionesPerfilesContratanteCompleto3_20220405_150023.atom':
                    print('AQUI')
                if prev_url is self._url or (self._last_url is not None and self._last_url is self._url):
                    self._url = None
                    self._mongo.close()
                    print(f'Search Finished. Total licitations: {counter}')
            except Exception as e:
                print(e)
                print(e.__traceback__)
    
    async def __download_data__(self, url):
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url) as response:
                    soup = BeautifulSoup(await response.text('utf-8'), "html.parser")
                    raw_data = str(soup.find_all('form', class_='form')[1])
                    self._observable.on_next({
                        'raw_data': raw_data
                    })
            except Exception as e:
                print(e)
