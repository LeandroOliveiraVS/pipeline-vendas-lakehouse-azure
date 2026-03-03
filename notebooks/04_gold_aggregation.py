# ============================================
# PIPELINE DE VENDAS — LAKEHOUSE AZURE
# Notebook 04 — Agregação Gold
# ============================================

from pyspark.sql.functions import sum, count, round, avg

# Ler Silver
df_silver = spark.read.format("delta").load(
    f"abfss://silver@{storage_account}.dfs.core.windows.net/sales/"
)

# Agregação por categoria
df_gold = df_silver \
    .groupBy("category") \
    .agg(
        count("id").alias("total_products"),
        round(avg("price"), 2).alias("avg_price"),
        round(avg("rating_rate"), 2).alias("avg_rating"),
        round(sum("rating_count"), 0).alias("total_reviews"),
        round(sum("Total_Value"), 2).alias("revenue")
    ) \
    .orderBy("avg_rating", ascending=False)

# Salvar como Delta Lake
df_gold.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"abfss://gold@{storage_account}.dfs.core.windows.net/sales/")

df_gold.show()
print(f"✅ Gold criado com {df_gold.count()} categorias!")