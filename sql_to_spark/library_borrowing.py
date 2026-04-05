'''
Imagine you're working for a library and you're tasked with generating a report on the borrowing habits of patrons. You have two tables in your database: Books and Borrowers.

 

Write an SQL to display the name of each borrower along with a comma-separated list of the books they have borrowed in alphabetical order, display the output in ascending order of Borrower Name.
'''

from pyspark.sql import functions as F

result_df = borrowers_df\
			.join(
			books_df,
			on="bookId",
			how="inner")\
			.groupBy("borrowerName")\
			.agg(
				F.concat_ws(", ",
				F.sort_array(
				F.collect_list("bookName"))).alias("borrowedBooks"))\
			.orderBy("borrowerName")
result_df.show(truncate=False)
