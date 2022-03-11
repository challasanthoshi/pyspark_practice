from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, expr

spark = SparkSession.builder.master('local').getOrCreate()
df = spark.read.format("json").load("D:/Santhu_practice/practice/Spark-The-Definitive-Guide/data/flight-data/json/2012-summary.json")
df1 = df.select(expr("*"), lit(1).alias("One")).show(2)

df1 .withColumn("One",lit(5)).show(2)

df1.withColumn("Withincountry",expr("DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME")).show(10)