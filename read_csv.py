import sys

from pyspark import SparkConf, SparkContext
import plotly
import time


def create_pair(file_name: str):
    # constructor that loads settings with Spark properties
    spark_context = SparkContext(conf=SparkConf())

    logger = spark_context._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.WARN)


    enfermedades = spark_context \
        .textFile(file_name) \
        .map(lambda line: line.split(",")) \
        .map(lambda list: (list[5], 1)) \
        .reduceByKey(lambda x, y: x + y) \
        .sortBy(lambda pair: pair[1])

    result = enfermedades \
        .collect()

    topic = list()
    count =list()
    for pair in result:
        print(pair)
        topic.append(pair[0])
        count.append(pair[1])


    spark_context.stop()
    return [topic, count]


def make_plot(result, filepath_out):

    labels = result[0]
    values = result[1]
    trace = plotly.graph_objs.Pie(labels=labels, values=values)
    plotly.offline.plot([trace], filename=filepath_out, image='png')


class Diseases:
    def __init__(self, file_name):
        make_plot(create_pair(file_name),file_name)

    def create_pair(self,file_name):
        return create_pair(file_name)

    def make_plot(self, pair, salida):
        return make_plot(pair, salida)


cd = Diseases(sys.argv[1])
print(cd)