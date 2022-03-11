from pyspark import Row
from pyspark.sql import Row
from pyspark.sql.session import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.master('local').getOrCreate()

myManualSchema = StructType([
    StructField("some",StringType(),True),
    StructField("col",StringType(),True),
    StructField("names",LongType(),False)
])

myRow = Row("Hello", None, 1)
myDf = spark.createDataFrame([myRow], myManualSchema)
myDf.show()