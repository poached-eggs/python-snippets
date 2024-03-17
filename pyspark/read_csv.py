from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType

def read_csv_file(file_path: str) -> DataFrame:
    spark = SparkSession.builder.appName('foo').getOrCreate()
    df = spark.read.csv(file_path, header=True)
    return df

# Read contents
FILE_NAME = 'soccer-spi/soccer-spi/spi_global_rankings_intl.csv'
df = read_csv_file(FILE_NAME)

# Cast types where needed
df = df.select('name', 'confed', 'spi')
df = df.withColumn('spi', df['spi'].cast(DoubleType()))

# Some transformation/SQL operation
# Example: Top 2 countries per confederation using window function.
df2 = df.withColumn(
    'rank',
    rank().over(Window.partitionBy('confed').orderBy(desc('spi')))
)
df2 = df2.where(df2['rank'] <= 2)
df2.show(20)

# Example: Sum the spi per confederation, round to 2 decimal places
df3 = df.groupBy('confed') \
    .agg(sum('spi').alias('sum_spi'))
df3 = df3.withColumn('sum_spi', round(df3['sum_spi'], 2)) \
    .orderBy(desc('sum_spi'))
df3.show(5)