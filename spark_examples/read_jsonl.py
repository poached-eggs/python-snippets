from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    FloatType
)

# import os
# import sys
#
# os.environ['PYSPARK_PYTHON'] = sys.executable
# os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

def read_json_log_file(file_path: str):
    import json
    data = []
    with open(file_path) as f:
        for line in f:
            data.append(json.loads(line))
    return data

FILE_NAME = 'logs.jsonl'
logs = read_json_log_file(FILE_NAME)

spark = SparkSession \
    .builder \
    .appName('test') \
    .getOrCreate()


# createDataFrame fails on Windows machine because of missing Hadoop binaries

# Option 1 - fails on Windows :(
# df = spark.createDataFrame(
#     logs,
#     schema=StructType(
#         [
#             StructField("id", IntegerType()),
#             StructField("metricId", IntegerType()),
#             StructField("result", FloatType())
#         ]
#     )
# )
# df.show()

## Option 2
df = spark.read.json(FILE_NAME)
df.show()

# Create df to join later
metrics = [
    {'metricId': 0, 'metricName': 'min'},
    {'metricId': 1, 'metricName': 'max'},
    {'metricId': 2, 'metricName': 'avg'}
]

## fails on Windows :(
# df_metrics = spark.createDataFrame(
#     metrics,
#     schema=StructType(
#         [
#             StructField('metricId', IntegerType()),
#             StructField('metricName', StringType())
#         ]
#     )
# )
#
# df_metrics.show()