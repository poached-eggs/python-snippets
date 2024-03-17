## Examples - pyspark

## Quick tips

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType


## I/O - csv
spark = SparkSession.builder.appName('foo').getOrCreate()
df = spark.read.csv('file_path', header=True)

# casting
df = df.withColumn('spi', df['spi'].cast(DoubleType()))

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

## Reading csv files

```bash
pipenv --python 3.12
pipenv install -r requirements.txt
pipenv shell
python read_csv.py
```

On Windows you might get
```bash
    > SUCCESS: The process with PID 22616 (child process of PID 4280) has been terminated.
SUCCESS: The process with PID 4280 (child process of PID 4380) has been terminated.
SUCCESS: The process with PID 4380 (child process of PID 21016) has been terminated.
```
This might be due to a java incompatibility.
Just type anything & press `enter` to exit.
