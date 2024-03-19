## Examples - pyspark

## Quick tips

```python
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import * 

## I/O - csv/json
spark = SparkSession.builder.appName('foo').getOrCreate()
df = spark.read.csv('file_path.csv', header=True)
df = spark.read.json('file_path.jsonl')

# casting
df = df.withColumn('spi', df['spi'].cast(DoubleType()))

# datetime - time difference
# Source: https://www.statology.org/pyspark-time-difference/
df = df.withColumn('start_time', to_timestamp('start_time', 'yyyy-MM-dd HH:mm:ss'))\
    .withColumn('end_time', to_timestamp('end_time', 'yyyy-MM-dd HH:mm:ss'))

df_new = df.withColumn(
    'hours_diff', (col('end_time').cast('long') - col('start_time').cast('long'))/3600)

# where
df2 = df.where(df["Age"] == 23)
df2 = df.where(df.Age <= 2)

# groupBy
df3 = df.groupBy('confed')
    .agg(sum('spi').alias('sum_spi'))
df3 = df3.withColumn('sum_spi', round(df3['sum_spi'], 2))
    .orderBy(desc('sum_spi'))
    
# window - rank
df2 = df.withColumn(
    'rank',
    rank().over(Window.partitionBy('confed').orderBy(desc('spi')))
)
```

## Setup pipenv
```bash
pipenv --python 3.12
pipenv install -r requirements.txt
pipenv shell
```

## Scripts
```bash
python read_csv.py      # csv files
python read_jsonl.py    # json logs (dictionary entries, 1 per line)
```


