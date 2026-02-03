 # JSON READ - Flattened JSON file read with corrupt records handling


from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("JsonRead").getOrCreate()
json_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("salary", FloatType(), True),
    StructField("address", StringType(), True),
    StructField("nominee", StringType(), True),
    StructField("_corrupt_record", StringType(), True)
])
json_df = (
    spark.read
            .format("json")
            .option("mode", "PERMISSIVE")
            .schema(json_schema)
            .load("/Volumes/workspace/default/raw/emp_data.json")
)
json_df.show(truncate=False)
json_df.printSchema() # to see the nested schema
# to store bad records
json_df = (
    spark.read
            .format("json")
            .option("badRecordsPath", "/Volumes/workspace/default/raw/")
            .schema(json_schema)
            .load("/Volumes/workspace/default/raw/emp_data.json")
)
json_df.show(truncate=False)

# %fs
# ls /Volumes/workspace/default/raw/20260201T104423/bad_records/
bad_records_df = spark.read.format("json").load("/Volumes/workspace/default/raw/20260201T104423/bad_records/")
bad_records_df.show(truncate=False)

