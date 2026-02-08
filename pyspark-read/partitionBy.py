from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Spark DataFrames").getOrCreate()

from pyspark.sql.types import StructType,StructField,IntegerType,StringType

path="/Volumes/population_metrics/landing/datasets/countries_dataset/csv_data/countries_population/countries_population.csv"

schema = StructType(
    [
        StructField("country_id",IntegerType(),False),
        StructField("name",StringType(),True),
        StructField("nationality",StringType(),True),
        StructField("country_code",StringType(),True),
        StructField("iso_alpha2",StringType(),True),
        StructField("capital",StringType(),True),
        StructField("population",IntegerType(),True),
        StructField("area_km2",IntegerType(),True),
        StructField("region_id",IntegerType(),True),
        StructField("sub_region_id",IntegerType(),True)
    ]
)

df = spark.read.format("csv").schema(schema).option("header","true").load(path)
df.display

df.write.\
    format("csv").\
    partitionBy("region_id","sub_region_id").\
    mode("overwrite").\
    save("/Volumes/population_metrics/landing/datasets/output_dataset/csv/countries_population_partitioned")