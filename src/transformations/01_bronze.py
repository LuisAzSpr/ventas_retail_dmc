import dlt
from pyspark.sql.functions import current_timestamp, col

CATALOG  = "proyecto_final"
PROYECTO = "ventas_retail_luisazana"
BASE = f"/Volumes/{CATALOG}/landing/raw_data/{PROYECTO}"


# --- clientes (CSV) ---
@dlt.table(name=f"{CATALOG}.bronze.clientes_raw",
           comment="Bronze - ingesta cruda de clientes vía STREAM")
def clientes_raw():
    return (spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .load(f"{BASE}/clientes")
            .withColumn("_fecha_ingesta", current_timestamp())
            .withColumn("_archivo_origen", col("_metadata.file_name")))


# --- productos (CSV) ---
@dlt.table(name=f"{CATALOG}.bronze.productos_raw",
           comment="Bronze - ingesta cruda de productos vía STREAM")
def productos_raw():
    return (spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .load(f"{BASE}/productos")
            .withColumn("_fecha_ingesta", current_timestamp())
            .withColumn("_archivo_origen", col("_metadata.file_name")))


# --- pedidos (JSON array multilínea) ---
@dlt.table(name=f"{CATALOG}.bronze.pedidos_raw",
           comment="Bronze - ingesta cruda de pedidos vía STREAM")
def pedidos_raw():
    return (spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("multiLine", "true")
            .load(f"{BASE}/pedidos")
            .withColumn("_fecha_ingesta", current_timestamp())
            .withColumn("_archivo_origen", col("_metadata.file_name")))


# --- detalle_pedidos (JSON array multilínea) ---
@dlt.table(name=f"{CATALOG}.bronze.detalle_pedidos_raw",
           comment="Bronze - ingesta cruda de detalle_pedidos vía STREAM")
def detalle_pedidos_raw():
    return (spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("multiLine", "true")
            .load(f"{BASE}/detalle_pedidos")
            .withColumn("_fecha_ingesta", current_timestamp())
            .withColumn("_archivo_origen", col("_metadata.file_name")))
