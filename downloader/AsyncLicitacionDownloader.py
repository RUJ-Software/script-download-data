import asyncio
from urllib.request import urlopen
import aiohttp
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET


class AsyncLicitacionDownloader(object):
    def __init__(self, observable, schedule, url):
        self._url = url
        self._observable = observable
        self._schedule = schedule

    async def async_download(self):
        counter = 0
        while self._url is not None:
            tasks = []
            try:
                prev_url = self._url
                content = urlopen(self._url)
                print('Retriving information from', self._url)
                with open('url_log.txt', 'a') as f:
                    f.write(f'{self._url}\n')
                    f.close()

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
                if prev_url is self._url:
                    self._url = None
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
