from dotenv import load_dotenv
from rx import create
import sys
import os
from downloader import AsyncLicitacionDownloader as ald
import asyncio
import threading
from spark import SparkStreaming as ss
from spark import SparkStreamingContext as ssc
from indexer import OpenSearchIndexer as osi

URL = "https://contrataciondelsectorpublico.gob.es/sindicacion/sindicacion_643/" \
      "licitacionesPerfilesContratanteCompleto3.atom"

async_loop = asyncio.get_event_loop()


def licitacion_downloader(observer, schedule):
    async_downloader = ald.AsyncLicitacionDownloader(observer, schedule, URL)
    async_downloader.async_download()


def img_downloader_observable(observer, schedule):
    licitation_downloader = ald.AsyncLicitacionDownloader(observer, schedule, URL)
    threading.Thread(target=asyncio_thread, args=(async_loop, licitation_downloader,)).start()


def asyncio_thread(async_loop, licitation_downloader):
    async_loop.run_until_complete(licitation_downloader.async_download())


async def main():
    s = ss.SparkStreaming()
    observable = create(img_downloader_observable)
    #observable.subscribe(lambda x: print(x['raw_data']))
    observable.subscribe(lambda x: s.send_raw_data(x['raw_data']))


async def test():
    import aiohttp
    from bs4 import BeautifulSoup
    async with aiohttp.ClientSession() as session:
        async with session.get(os.getenv('LICITATION_ATOM_URL')) as response:
            soup = BeautifulSoup(await response.text('utf-8'), "html.parser")
            raw_data = str(soup.find_all('form')[1])
            print(raw_data)

if __name__ == "__main__":
    args = sys.argv

    if len(args) == 2:
        load_dotenv()
        if args[1] == 'server':
            spark_streaming = ssc.SparkStreamingContext()
        elif args[1] == 'client':
            asyncio.run(main())
        elif args[1] == 'full-index':
            index = osi.OpenSearchIndexer()
            index.full_index()
        elif args[1] == 'test':
            asyncio.run(test())
    else:
        print('Es necesario ejecutar el scrpipt con el parámetro server|client|test')


