from rx import create
from downloader import AsyncLicitacionDownloader as ald
import asyncio
import threading
# from pyspark import SparkStreaming as ss

URL = "https://contrataciondelsectorpublico.gob.es/sindicacion/sindicacion_643/" \
      "licitacionesPerfilesContratanteCompleto3.atom"

def licitacion_downloader(observer, schedule):
    async_downloader = ald.AsyncLicitacionDownloader(observer, schedule, URL)
    async_downloader.async_download()


def img_downloader_observable(observer, schedule):
    licitation_downloader = ald.AsyncLicitacionDownloader(observer, schedule, URL)
    async_loop = asyncio.get_event_loop()
    threading.Thread(target=_asyncio_thread, args=(async_loop, licitation_downloader,)).start()


def _asyncio_thread(async_loop, licitation_downloader):
    async_loop.run_until_complete(licitation_downloader.async_download())

if __name__ == "__main__":

    # spark_streaming = ss.SparkStreaming()

    observable = create(licitacion_downloader)
    observable.subscribe(lambda x: print(x['raw_data']))
