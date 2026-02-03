from spark import sql
from spark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("lazyEvalution").getOrCreate()


flight_data = spark.read.format("csv")\
    .option("header", "true")\
    .option("inferschema", "true")\
    .load("/Volumes/workspace/default/raw/flight_data.csv")

flight_data_repartition = flight_data.repartition(5)

us_flight_data = flight_data.filter("DEST_COUNTRY_NAME == 'United States'")

us_india_sing_data = us_flight_data.filter((col( "ORIGIN_COUNTRY_NAME") == "India") | (col("ORIGIN_COUNTRY_NAME") == "Singapore" ))

total_flights_ind_sing = us_india_sing_data.groupBy("ORIGIN_COUNTRY_NAME").sum("count")
total_flights_ind_sing.show()