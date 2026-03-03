# ============================================
# PIPELINE DE VENDAS — LAKEHOUSE AZURE
# Notebook 01 — Configuração de Acesso
# ============================================

storage_account = "stdatalakelab<nome>"
storage_key = "<key1 do portal Azure>"

# Configurar acesso ao Data Lake
spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    storage_key
)

# Validar conexão
dbutils.fs.ls(f"abfss://bronze@{storage_account}.dfs.core.windows.net/")
print("✅ Conexão com Data Lake estabelecida!")