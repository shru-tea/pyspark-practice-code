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

SELECT
  household_id,
  EXTRACT(YEAR FROM TO_DATE(billing_period,'YYYY-MM')) AS billing_year,
  SUM(consumption_kwh) AS total_consumption,
  SUM(total_cost) AS total_cost,
  AVG(consumption_kwh) AS average_monthly_consumption
  FROM electricity_bill
WHERE EXTRACT(YEAR FROM TO_DATE(billing_period,'YYYY-MM')) IN (2023, 2024)
GROUP BY 1,2
ORDER BY 1,2;