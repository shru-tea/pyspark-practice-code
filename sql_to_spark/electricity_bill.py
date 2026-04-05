'''
You have access to data from an electricity billing system detailing the electricity usage and 
cost for specific households over billing periods in the years 2023 and 2024. Your objective is 
to present the total electricity consumption, total cost, and average monthly consumption for each 
household per year, and display the output in ascending order of household ID and billing year.

Tables: electricity_bill
+-----------------+---------------+
| COLUMN_NAME | DATA_TYPE |
+-----------------+---------------+
| bill_id | int |
| household_id | int |
| billing_period | varchar(7) |
| consumption_kwh | decimal(10,2) |
| total_cost | decimal(10,2) |
+-----------------+---------------+
'''

import pyspark.sql.functions as F

results_df=electricity_df\
            .withColumn('billing_year',F.year(F.to_date('billing_period','yyyy-MM')))\
            .groupBy(['household_id','billing_year'])\
            .agg(
                F.sum('consumption_kwh').alias('total_consumption_kwh'),
                F.sum('total_cost').alias('total_cost'),
                F.avg('consumption_kwh').alias('average_monthly_consumption_kwh')
            )

results_df=results_df.orderBy(['household_id','billing_year'])


