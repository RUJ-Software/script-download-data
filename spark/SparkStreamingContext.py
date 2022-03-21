import findspark
from pyspark.sql.session import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql import Row
from model import Formatter as formatter


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
        with open('server_data.txt', 'w') as f:
            f.write(a[0])
            f.close()
        print(a)


def save_to_mongo(rdd):
    if not rdd.isEmpty():
        df = rdd.map(lambda x: Row(**transform_form(x))).toDF()
        df.write.format("mongo").mode('append').option("database", "licitations").option("collection", "licitation").save()


class SparkStreamingContext:
    def __init__(self):
        findspark.init()
        spark = SparkSession.builder.appName('spark')\
            .config('spark.mongodb.input.uri', 'mongodb://admin:InsoData2022-@127.0.0.1:27017')\
            .config('spark.mongodb.output.uri', 'mongodb://admin:InsoData2022-@127.0.0.1:27017')\
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

        lines.filter(lambda licitation: licitation != "").foreachRDD(lambda rdd: save_to_mongo(rdd))

        ssc.start()  # Start the computation
        ssc.awaitTermination()  # Wait for the computation to terminate
