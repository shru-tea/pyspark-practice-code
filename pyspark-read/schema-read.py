from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SchemaRead").getOrCreate()

emp_schema = StructType([
    StructField("id",IntegerType(),True),
    StructField("name",StringType(),True),
    StructField("age",IntegerType(),True),
    StructField("salary",IntegerType(),True),
    StructField("address",StringType(),True),
    StructField("nominee",StringType(),True),
    StructField("_corrupt_record",StringType(),True)
])

emp_df = (
       spark.read
         .format("csv")
         .option("header","true")
         .option("inferschema","true")
         .option("mode","PERMISSIVE")
         .schema(emp_schema)
         .load("/Volumes/workspace/default/raw/emp_data.csv")
)

emp_df.show(truncate = False)

# to store bad records 

emp_df = (
    spark.read
         .format("csv")
         .option("header","true")
         .option("inferschema","true")
         .option("badRecordsPath","/Volumes/workspace/default/raw/")
         .schema(emp_schema)
         .load("/Volumes/workspace/default/raw/emp_data.csv")
)

emp_df.show(truncate = False)


# %fs
# ls /Volumes/workspace/default/raw/20260201T104423/bad_records/


bad_records_df = spark.read.format("json").load("/Volumes/workspace/default/raw/20260201T104423/bad_records/")
bad_records_df.show(truncate=False)


from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DoubleType

countries_schema = StructType([
    StructField("COUNTRY_ID",IntegerType(),True),
    StructField("NAME",StringType(),True),
    StructField("NATIONALITY",StringType(),True),
    StructField("COUNTRY_CODE",StringType(),True),
    StructField("ISO_ALPHA2",StringType(),True),
    StructField("CAPITAL",StringType(),True),
    StructField("POPULATION",IntegerType(),True),
    StructField("AREA_KM2",DoubleType(),True),
    StructField("REGION_ID",IntegerType(),True),
    StructField("SUB_REGION_ID",IntegerType(),True)
])

df=spark.read.csv(path=path,header=True,schema=countries_schema,sep=",")
df = spark.read.option("header",True).option("sep",",").csv(path)
df=spark.read.options(header=True,sep=",").csv(path)
df.display()


# more common method

#df = spark.read.format("csv").option("header","true").option("mode","PERMISSIVE").schema(countries_schema).load(path)

df = spark.read.load(path,format="csv",header="true",mode="PERMISSIVE",schema=countries_schema)
"""
df = (
    spark.read
    .format("csv")
    .option("header","true")
    .option("mode","PERMISSIVE")
    .schema(countries_schema)
    .load(path)
)"""
df.display()