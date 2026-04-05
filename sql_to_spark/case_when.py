from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark=SparkSession.builder.appName("Case when statements").getOrCreate()

'''
SELECT 
  employee_id,
  salary,
  CASE 
    WHEN salary > 100000 THEN 'High'
    WHEN salary > 50000 THEN 'Medium'
    ELSE 'Low'
  END AS salary_band
FROM employees;
'''

data = [
    (1, 120000),
    (2, 80000), 
    (3, 40000)
]

columns=["employee_id","salary"]

employees_df=spark.createDataFrame(data,columns)

result_df = (
    employees_df
    .withColumn(
        'salary_band',
        F.when(F.col('salary')>100000,'High')
        .when(F.col('salary')>50000,'Medium')
        .otherwise('Low')
    )
    .select('employee_id','salary','salary_band')
)
