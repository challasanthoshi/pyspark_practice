from pyspark.sql import SparkSession
from pyspark.sql.functions import desc

spark = SparkSession.builder.master('local').getOrCreate()

flightData = spark\
    .read\
    .option("inferschema","true")\
    .option("delimiter",",")\
    .option("header","true")\
    .csv("D:/Santhu_practice/practice/Spark-The-Definitive-Guide/data/flight-data/csv/2010-summary.csv")

flightData.createOrReplaceTempView("flight_data_2010")
df = spark.sql("""SELECT DEST_COUNTRY_NAME, sum(count) as destination_total
FROM flight_data_2010 
GROUP BY DEST_COUNTRY_NAME
ORDER BY sum(count) DESC
LIMIT 5""")
df.show(truncate=False)

df = flightData\
    .groupBy("DEST_COUNTRY_NAME")\
    .sum("count")\
    .withColumnRenamed("sum(count)","destination_total")\
    .sort(desc("destination_total"))\
    .limit(5)\
    .show(truncate=False)








