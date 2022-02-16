import asyncio
import requests
import aiohttp
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET


class AsyncLicitacionDownloader(object):
    def __init__(self, observable, schedule, url):
        self._url = url
        self._observable = observable
        self._schedule = schedule

    async def async_download(self):
        print(self._url)
        while self._url is not None:
            tasks = []
            try:
                content = requests.get(self._url)

                tree = ET.parse(content)
                root = tree.getroot()

                entry_tag = '{http://www.w3.org/2005/Atom}entry'

                for element in root:
                    if element.tag == entry_tag:
                        new_link = element.find('{http://www.w3.org/2005/Atom}link').attrib['href']
                        print(new_link)
                        # Insertamos la task de los datos a descargar
                        tasks.append(asyncio.create_task(self.__download_data__(new_link)))
                    if 'rel' in element.attrib and element.attrib['rel'] == 'next':
                        prev_url = self._url
                        url = element.attrib['href']
                # Lanzar la descarga de todos los datos de esta pagina
                await asyncio.gather(*tasks)
                if prev_url is not url and counter < 1:
                    counter = counter + 1
                    print(counter)
                else:
                    url = None
                    print('Search Finished')

            except Exception as e:
                print(e)
                print(e.__traceback__)
    
    async def __download_data__(self, url):
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url) as response:
                    soup = BeautifulSoup(await response.text, 'lxml')
                    raw_data = soup.select('form.form')
                    self._observable.on_next({
                        'raw_data': raw_data
                    })
            except Exception as e:
                print(e)

        print("Descargando...")
        r = requests.get(url)
        print(self._raw_data)