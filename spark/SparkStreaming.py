import findspark
import pyspark
import socket
import traceback
import sys
import os
from pyspark.sql.session import SparkSession

WINDOWS_LINE_ENDING = '\r\n'
UNIX_LINE_ENDING = '\n'


class SparkStreaming(object):
    def __init__(self):
        findspark.init()
        conf = pyspark.SparkConf().set("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1")
        sc = pyspark.SparkContext(appName="spark", conf=conf)
        self._spark = SparkSession(sc)

        self._s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._s.bind((os.getenv('SPARK_STREAMING_CLIENT_IP'), int(os.getenv('SPARK_STREAMING_CLIENT_PORT'))))
        self._s.listen(1)

        print("Waiting for TCP connection...")
        self._conn, self._addr = self._s.accept()

    def send_raw_data(self, raw_data):
        try:
            print("------------------------------------------")
            formatted_data = raw_data.replace(WINDOWS_LINE_ENDING, ' ').replace(UNIX_LINE_ENDING, ' ')
            formatted_data += '\n'
            print(self._conn.send(formatted_data.encode("utf-8")))
            print('Licitacion enviada!')
        except:
            e = sys.exc_info()[0]
            print("Error: %s" % e)
            print(traceback.format_exc())
