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
       csv_file_path = 'file:///home/takeo/iphone_sales_analysis/product_Table/product_Table_csv'
    parquet_file_path = 'file:///home/takeo/iphone_sales_analysis/product_Table/product_Table_parquet'
    #Loading data into  dataFrame
    df = spark.read.csv(csv_file_path, header = True, inferSchema = True)
    #Writinh dataFrame to parquet file format
    df.write.parquet(parquet_file_path, mode = 'overwrite')
    #Call function product_data_collector_api after the csv data has been converted into parquet data
    product_data_collector_api(spark, parquet_file_path)
