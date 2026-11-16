"""
Assignment 3: Apache Spark Batch Analytics - Products Sold by Category
Author: Jit Sarkar
Repository: DWBDA-Assignment-3-PySpark-Sales-Category
Description: Uses PySpark to aggregate sales data, computing total products sold,
             total revenue, and average price per category.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, avg as spark_avg, round as spark_round, count as spark_count

def main():
    print("=" * 70)
    print("DWBDA Assignment 3: Apache Spark Sales Analytics by Category")
    print("=" * 70)

    # 1. Initialize SparkSession
    spark = SparkSession.builder \
        .appName("SalesCategoryAnalytics") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    print(f"\nApache Spark Session Initialized (Version: {spark.version})")

    # 2. Load Dataset
    csv_file = "retail_store_sales.csv"
    df = spark.read.csv(csv_file, header=True, inferSchema=True)
    print(f"\nLoaded Sales Dataset from '{csv_file}':")
    df.show(5, truncate=False)
    print("Schema Definition:")
    df.printSchema()

    # 3. Feature Transformation: Calculate Total Sales Amount
    df_with_amount = df.withColumn(
        "Sales_Amount",
        spark_round(col("Quantity") * col("Unit_Price"), 2)
    )

    # 4. Core Objective: Products Sold in Each Category
    print("\n" + "=" * 70)
    print("OBJECTIVE 1: Total Products Sold in Each Category")
    print("=" * 70)
    category_products = df_with_amount.groupBy("Category") \
        .agg(spark_sum("Quantity").alias("Total_Products_Sold")) \
        .orderBy(col("Total_Products_Sold").desc())

    category_products.show(truncate=False)

    # 5. Advanced Analysis: Category Performance Summary (Revenue, Avg Price, Orders)
    print("\n" + "=" * 70)
    print("OBJECTIVE 2: Comprehensive Category Performance Breakdown")
    print("=" * 70)
    category_summary = df_with_amount.groupBy("Category") \
        .agg(
            spark_sum("Quantity").alias("Total_Products_Sold"),
            spark_round(spark_sum("Sales_Amount"), 2).alias("Total_Revenue_USD"),
            spark_round(spark_avg("Unit_Price"), 2).alias("Avg_Unit_Price_USD"),
            spark_count("Transaction_ID").alias("Transaction_Count")
        ) \
        .orderBy(col("Total_Products_Sold").desc())

    category_summary.show(truncate=False)

    # Export to CSV via Pandas
    pdf = category_summary.toPandas()
    pdf.to_csv("spark_category_sales_summary.csv", index=False)
    print("[SUCCESS] Output exported to 'spark_category_sales_summary.csv'.")

    # 6. Spark SQL Query Demonstration
    print("\n" + "=" * 70)
    print("OBJECTIVE 3: Spark SQL Query Execution")
    print("=" * 70)
    df_with_amount.createOrReplaceTempView("sales_records")
    sql_query = """
        SELECT Category, 
               SUM(Quantity) AS Total_Sold, 
               ROUND(SUM(Sales_Amount), 2) AS Revenue
        FROM sales_records
        GROUP BY Category
        HAVING SUM(Quantity) >= 20
        ORDER BY Revenue DESC
    """
    spark.sql(sql_query).show(truncate=False)

    spark.stop()
    print("\nSpark Session Stopped.")

if __name__ == "__main__":
    main()
