import sys
from collections import OrderedDict
from pyspark.sql import SparkSession
import plotly


def main(file_path):
    # Execute main function
    process_disease_indicators(file_path)


def spark_dataframe(csv_file_path):
    # Initialize spark session
    spark_session = SparkSession \
        .builder \
        .getOrCreate()

    logger = spark_session._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.WARN)

    # Create Spark Data frame
    data_frame = spark_session \
        .read \
        .format("csv") \
        .options(header='true', inferschema='true') \
        .load(csv_file_path)
    return data_frame


def process_disease_indicators(file_path):
    # Create data frame
    data_frame = spark_dataframe(file_path)
    result_set = OrderedDict()
    # Collect results and add them to an OrderedDict
    for pair in list(data_frame.groupBy("Topic").count().collect()):
        result_set[pair[0]] = pair[1]
    plot_pie(result_set, 'data_frame_results.html')


def plot_pie(data_dict, filepath_out):
    """
    Creates an html plot with Plotly, given value in a dictionary
    :param data_dict:
    :param filepath_out:
    :return:
    """
    # Add labels
    labels = list(data_dict.keys())
    # Add values
    values = list(data_dict.values())
    trace = plotly.graph_objs.Pie(labels=labels, values=values)
    # Plot graph
    plotly.offline.plot([trace], filename=filepath_out, image='png')


if __name__ == '__main__':
    """
    Python program that uses Apache Spark to find 
    """

    if len(sys.argv) != 2:
        print("Usage: python3 read_csvDataframe.py <file>", file=sys.stderr)
        exit(-1)

    main(sys.argv[1])
