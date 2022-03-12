from pyspark.sql import SparkSession

spark = SparkSession.builder.master('local').getOrCreate()
df = spark.read.format("json").load("D:/Santhu_practice/practice/Spark-The-Definitive-Guide/data/flight-data/json/2013-summary.json")
seed = 5
withReplacement = False
fraction = 0.5
print(df.sample(withReplacement, fraction, seed).count())