from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark=SparkSession.builder.appName("Basic Aggregations Type 2").getOrCreate()

'''
SELECT department, AVG(salary) AS avg_salary
FROM employees
WHERE salary > 50000
GROUP BY department;
'''

# Sample DataFrame
data = [
    ("Alice", "HR", 60000),
    ("Bob", "HR", 45000),
    ("Charlie", "IT", 70000),
    ("David", "IT", 55000),
    ("Eve", "Finance", 80000)
]

columns=["name","department","salary"]

employees_df=spark.createDataFrame(data,columns)

#.filter->.groupBy->.agg
result_df =(
    employees_df
    .filter(F.col('salary')>50000)
    .groupBy('department')
    .agg(F.avg('salary').alias('avg_salary'))
    .select('department','avg_salary')
)