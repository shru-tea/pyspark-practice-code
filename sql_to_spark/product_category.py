'''
You are provided with a table named Products containing information about various products, including their names and prices. Write a SQL query to count number of products in each category based on its price into three categories below. Display the output in descending order of no of products.

 

1- "Low Price" for products with a price less than 100
2- "Medium Price" for products with a price between 100 and 500 (inclusive)
3- "High Price" for products with a price greater than 500.'''

from pyspark.sql import functions as F

products_df=products_df\
			.withColumn("category",
					   F.when(F.col("price")<100,'Low Price')
						.when((F.col("price")>=100) & (F.col("price")<500),'Medium Price')
						.otherwise('High Price')
					   )\
			.groupBy("category")\
			.agg(F.count('product_id').alias('no_of_products'))\
			.orderBy(F.desc("no_of_products"))

products_df.show()