from pyspark.sql.functions import col, current_timestamp

# Ler Bronze
df_bronze = spark.read \
    .option("multiline", "true") \
    .json(f"abfss://bronze@{storage_account}.dfs.core.windows.net/sales/raw/sales_raw.json")

# Camada Silver utilizando o datafram bronze
df_silver = df_bronze \
    .withColumn("rating_rate", col("rating.rate")) \
    .withColumn("rating_count", col("rating.count")) \
    .withColumn("Total_Value", col("price") * col("rating_count")) \
    .withColumn("Ingestion_Date", current_timestamp()) \
    .drop("rating", "quantity") \

df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(f"{pathSilver}sales")

# Visualizar
display(spark.read.load(f"{pathSilver}sales"))

print("✅ Camada Silver criada!")