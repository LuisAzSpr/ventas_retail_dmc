# Databricks notebook source
# MAGIC %md
# MAGIC # Setup — Proyecto Final Ventas Retail
# MAGIC Crea el catálogo, los esquemas de las 4 capas y el volume de landing.
# MAGIC Se ejecuta como primera tarea del Job, antes del pipeline.

# COMMAND ----------

CATALOG = "proyecto_final"
PROYECTO = "ventas_retail_luisazana"

# Catálogo + esquemas de las 4 capas
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
for sch in ["landing", "bronze", "silver", "gold"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{sch}")

# Volume de aterrizaje (landing) para los datos crudos
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.landing.raw_data")

# Subcarpetas por entidad dentro del volume
base = f"/Volumes/{CATALOG}/landing/raw_data/{PROYECTO}"
for ent in ["clientes", "productos", "pedidos", "detalle_pedidos"]:
    dbutils.fs.mkdirs(f"{base}/{ent}")

print("✅ Setup completado. Ruta base del volume:")
print(base)
