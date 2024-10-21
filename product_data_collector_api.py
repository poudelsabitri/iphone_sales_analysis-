from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("Product data collector").enableHiveSupport().getOrCreate()


def product_data_collector_api(spark, parquet_file_path):
    # Load the product data from the parquet file
    product_df = spark.read.parquet(parquet_file_path)

    # Save the data to a non-partitioned Hive table
    product_df.write.mode("overwrite").saveAsTable("iphone_sales_analysis.product_hive_table")

    return "iphone_sales_analysis.product_hive_table"

if __name__ == '__main__':
    filepath = 'file:///home/takeo/iphone_sales_analysis/product_Table'
    sales_data_collector_api(spark, filepath)
