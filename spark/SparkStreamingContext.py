import findspark
import pyspark
from pyspark.sql.session import SparkSession
from pyspark.streaming import StreamingContext
from model import Formatter as formatter
from db import MongoDB


LOG_LEVEL = "ERROR"
BATCH_DURATION = 3
IP = "localhost"
PORT = 10001


def transform_form(raw_licitation):
    licitation_dict = formatter.HtmlToDict.transform(raw_licitation)
    print(licitation_dict)
    return licitation_dict


def test(rdd):
    if not rdd.isEmpty():
        a = rdd.take(1)
        print(a)

class SparkStreamingContext:
    def __init__(self):
        findspark.init()
        spark = SparkSession.builder.appName('SERVER_01')\
            .config('spark.mongodb.input.uri', 'mongodb://admin:InsoData2022-@localhost:27017/licitations.licitation')\
            .config('spark.mongodb.output.uri', 'mongodb://admin:InsoData2022-@localhost:27017/licitations.licitation')\
            .getOrCreate()
        sc = spark.sparkContext


        # Create a local StreamingContext with two working thread and batch interval of 1 second
        sc.setLogLevel(LOG_LEVEL)
        # ssc = StreamingContext(sc, 1)
        ssc = StreamingContext(sc, batchDuration=BATCH_DURATION)

        # We need to create the checkpoint
        ssc.checkpoint("checkpoint")

        # Create a DStream that will connect to hostname:port, like localhost:9999
        lines = ssc.socketTextStream(IP, PORT)

        #lines.filter(lambda licitation: licitation != "").map(lambda raw_licitation: transform_form(raw_licitation))\
        #    .foreachRDD(lambda rdd: self.__save_to_mongo__(rdd))
        lines.foreachRDD(lambda rdd: test(rdd))

        ssc.start()  # Start the computation
        ssc.awaitTermination()  # Wait for the computation to terminate

    def __save_to_mongo__(self, rdd):
        # licitations = rdd.map(lambda licitation: licitation)
        if not rdd.isEmpty():
            df = rdd.toDF()
            print(df)
            df.write.format("mongo")

