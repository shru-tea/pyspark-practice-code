from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark=SparkSession.builder.appName("Basic Aggregations").getOrCreate()

'''
SELECT department, COUNT(*) AS emp_count
FROM employees
GROUP BY department;
'''

# Load the employees data into a DataFrame
employees_df=spark.read.csv("employees.csv",header=True,inferSchema=True)

#agg at the end
result_df=(
    employees_df
    .groupBy("department")
    .agg(F.count("*").alias("emp_count"))
    .select("department","emp_count")
)