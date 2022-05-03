import findspark
import os
from pyspark.sql.session import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql import Row
from model import Formatter as formatter


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


class SparkStreamingContext:
    def __init__(self):
        findspark.init()
        spark = SparkSession.builder.appName('spark')\
            .config('spark.mongodb.input.uri', f'mongodb://{os.getenv("MONGODB_USER")}:{os.getenv("MONGODB_PASS")}@'
                                               f'{os.getenv("MONGODB_IP")}:{os.getenv("MONGODB_PORT")}') \
            .config('spark.mongodb.output.uri', f'mongodb://{os.getenv("MONGODB_USER")}:{os.getenv("MONGODB_PASS")}@'
                                                f'{os.getenv("MONGODB_IP")}:{os.getenv("MONGODB_PORT")}')\
            .getOrCreate()
        sc = spark.sparkContext

        # Create a local StreamingContext with two working thread and batch interval of 1 second
        sc.setLogLevel(os.getenv('SPARK_STREAMING_SERVER_LOG_LEVEL'))
        # ssc = StreamingContext(sc, 1)
        ssc = StreamingContext(sc, batchDuration=int(os.getenv('SPARK_STREAMING_SERVER_BATCH_DURATION')))

        # We need to create the checkpoint
        ssc.checkpoint("checkpoint")

        # Create a DStream that will connect to hostname:port, like localhost:9999
        lines = ssc.socketTextStream(os.getenv('SPARK_STREAMING_SERVER_IP'), int(os.getenv('SPARK_STREAMING_SERVER_PORT')))

        # index_client = sc.broadcast(opc.OpenSearchClient(os.getenv('OPENSEARCH_IP'), os.getenv('OPENSEARCH_PORT'),
        #                                    os.getenv('OPENSEARCH_USER'), os.getenv('OPENSEARCH_PASS')))

        lines.filter(lambda licitation: licitation != "").foreachRDD(lambda rdd: save_to_mongo(rdd))

        ssc.start()  # Start the computation
        ssc.awaitTermination()  # Wait for the computation to terminate


def save_to_mongo(rdd):
    if not rdd.isEmpty():
        try:
            print('DONE OK')
            rdd = rdd.map(lambda x: Row(**transform_form(x))).filter(lambda licitation: licitation['estado'] == 'Resuelta')
            rdd.toDF().write.format("mongo").mode('append').option("database", "licitations").option("collection", "licitation").save()
        except Exception as ex:
            print(ex)
            print(ex.__traceback__)
