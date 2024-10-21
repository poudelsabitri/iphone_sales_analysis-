from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("Data Preparation").enableHiveSupport().getOrCreate()


def data_preparation_api(spark, product_hive_table, sales_hive_table, target_hive_table):
    # Join sales and product data to get the necessary information
    sales_df = spark.table(sales_hive_table)
    product_df = spark.table(product_hive_table)
    
    # Create a DataFrame that identifies buyers of S8 and iPhone
    conditions = """
        sales.product_id = product.product_id AND (
        product.product_name = 'S8' OR
        product.product_name = 'iPhone')
    """
    sales_product_df = sales_df.join(product_df, on="product_id").filter(conditions).select("buyer_id", "product_name")

    # Identify buyers who bought S8 but not iPhone
    s8_buyers = sales_product_df.filter(sales_product_df.product_name == "S8").select("buyer_id").distinct()
    iphone_buyers = sales_product_df.filter(sales_product_df.product_name == "iPhone").select("buyer_id").distinct()
    
    result_df = s8_buyers.join(iphone_buyers, s8_buyers.buyer_id == iphone_buyers.buyer_id, "left_anti")

    # Save the result to the target Hive table
    result_df.write.mode("overwrite").saveAsTable(target_hive_table)

    return target_hive_table

if __name__ == '__main__':
    text_file_path = 'file:///home/takeo/iphone_sales_analysis/sales_Table'
    parquet_file_path = 'file:///home/takeo/iphone_sales_analysis/product_Table/product_Table_parquet'
    target_hive_table = "iphone_sales_analysis.buyers_report"
    
    sales_data_collector_api(spark, text_file_path)
    product_data_collector_api(spark, parquet_file_path)
    data_preparation_api(spark, "iphone_sales_analysis.product_hive_table", "iphone_sales_analysis.sales_hive_table", target_hive_table)
