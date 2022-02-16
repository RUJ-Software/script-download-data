import sched
from AsyncLicitacionDownloader import AsyncLicitacionDownloader as ald
from sparkStreaming import SparkStreaming as ss

URL = "https://contrataciondelsectorpublico.gob.es/sindicacion/sindicacion_643/licitacionesPerfilesContratanteCompleto3.atom"

def licitacion_downloader(observer, schedule):
    ald.AsyncLicitacionDownloader(observable, schedule, URL)

if __name__ == "__main__":

    spark_streaming = ss.SparkStreaming()

    observable = create(licitacion_downloader)
    observable.subscribe(lambda x: sc.)