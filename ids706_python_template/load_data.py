"""
transform and load function
"""

from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Read CSV").master("local").getOrCreate()

def load(dataset="dbfs:/FileStore/HugoHu_Project_3/shopping_trends.csv"):
    # Remove the SparkSession creation
    # spark = SparkSession.builder.appName("Read CSV").getOrCreate()
    
    # Access the SparkSession from the Databricks environment
    

    shopping_trends_df = spark.read.csv(dataset, header=True, inferSchema=True)

    # Clean up column names by replacing spaces with underscores
    for col in shopping_trends_df.columns:
        new_col = col.replace(" ", "_").replace("-", "_").replace(".", "_").replace("\n", "_").replace("\t", "_").replace("(", "_").replace(")", "_").replace("{", "_").replace("}", "_").replace("=", "_")
        shopping_trends_df = shopping_trends_df.withColumnRenamed(col, new_col)

    shopping_trends_df = shopping_trends_df.withColumn("id", monotonically_increasing_id())

    # transform into a delta lakes table and store it 
    shopping_trends_df.write.format("delta").mode("overwrite").saveAsTable("shopping_trends")
    
    num_rows = shopping_trends_df.count()
    print(num_rows)
    
    return "finished transform and load"

if __name__ == "__main__":
    load()
