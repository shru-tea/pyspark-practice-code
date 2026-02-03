output_path = "/Volumes/population_metrics/landing/datasets/output_dataset/csv/countries_population"

type(df.write)
#dataframewriter object

df.write.mode("append").option("header","true").format("csv").save(output_path)