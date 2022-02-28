import findspark
import pyspark
import socket
import sys
from pyspark.sql.session import SparkSession

TCP_IP = "localhost"
TCP_PORT = 10002


class SparkStreaming(object):
    def __init__(self):
        findspark.init()
        sc = pyspark.SparkContext(appName="CALLER_02")
        self._spark = SparkSession(sc)

        self._s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._s.bind((TCP_IP, TCP_PORT))
        self._s.listen(1)

        print("Waiting for TCP connection...")
        self._conn, self._addr = self._s.accept()

    def send_raw_data(self, raw_data):
        try:
            print("------------------------------------------")
            print("Data: " + raw_data)
            print(self._conn.send(raw_data))
        except:
            e = sys.exc_info()[0]
            print("Error: %s" % e)
