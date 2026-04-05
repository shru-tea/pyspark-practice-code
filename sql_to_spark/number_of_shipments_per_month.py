from pyspark.sql import SparkSession
import pyspark.sql.functions as F 

spark=SparkSession.builder.appName("no_of_shipments_per_month").getOrCreate()

#amazon_shipment is already a df

#map from sql to spark
#create columns -> group by -> apply aggregate function

amazon_shipment=(
    amazon_shipment
    .withColumn("year_month",F.date_format(F.col("shipment_date"),"yyyy-MM"))
    .groupBy("year_month")
    .agg(F.countDistinct("shipment_id","sub_id").alias("count"))
    .orderBy("year_month")
)