# ============================================
# PIPELINE DE VENDAS — LAKEHOUSE AZURE
# Notebook 02 — Ingestão Bronze
# Fonte: Fake Store API via Azure Data Factory
# ============================================

# Criar estrutura de pastas
dbutils.fs.mkdirs(f"abfss://bronze@{storage_account}.dfs.core.windows.net/sales/raw/")
dbutils.fs.mkdirs(f"abfss://silver@{storage_account}.dfs.core.windows.net/sales/")
dbutils.fs.mkdirs(f"abfss://gold@{storage_account}.dfs.core.windows.net/sales/")

# Validar arquivo ingerido pelo ADF
files = dbutils.fs.ls(
    f"abfss://bronze@{storage_account}.dfs.core.windows.net/sales/raw/"
)
print(f"✅ Arquivos no Bronze: {[f.name for f in files]}")

# Ler e validar dados brutos
df_bronze = spark.read \
    .option("multiline", "true") \
    .json(f"abfss://bronze@{storage_account}.dfs.core.windows.net/sales/raw/sales_raw.json")

df_bronze.printSchema()
df_bronze.show(5)
print(f"✅ Total de registros no Bronze: {df_bronze.count()}")