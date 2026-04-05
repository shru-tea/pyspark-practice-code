from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark=SparkSession.builder.appName("AllPostsReactedWithHeart").getOrCreate()

#get the heart reaction
#we just need the post id from this table, so we select only the post id column
#using distinct early to reduce the data size for the join operation later
heart_reactions=facebook_reactions.filter(F.col("reaction")=="heart").select("post_id").distinct()

#join the heart reactions with the posts table to get the post details
result=heart_reactions.join(facebook_posts, on="post_id",how="inner")


# Import your libraries
import pyspark.sql.functions as F

# Start writing code
heart_reactions=facebook_reactions.filter(F.col('reaction')=='heart').select('post_id').distinct()

result=heart_reactions.join(facebook_posts,on='post_id',how='inner')

# To validate your solution, convert your final pySpark df to a pandas df
result.toPandas()