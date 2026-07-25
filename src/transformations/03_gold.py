import dlt
from pyspark.sql.functions import (col, explode, sequence, date_format,
                                   year, month, dayofmonth,
                                   min as spark_min, max as spark_max)

CATALOG = "proyecto_final"


# ---------- dim_cliente (1 fila por cliente) ----------
@dlt.table(name=f"{CATALOG}.gold.dim_cliente", comment="Gold - dimensión cliente")
def dim_cliente():
    return (spark.read.table(f"{CATALOG}.silver.clientes")
            .select(col("customer_id").alias("customer_key"),
                    "nombre", "apellido", "email", "ciudad", "pais", "segmento"))


# ---------- dim_producto (1 fila por producto) ----------
@dlt.table(name=f"{CATALOG}.gold.dim_producto", comment="Gold - dimensión producto")
def dim_producto():
    return (spark.read.table(f"{CATALOG}.silver.productos")
            .select(col("product_id").alias("product_key"),
                    "nombre_producto", "categoria", "subcategoria", "proveedor",
                    col("precio_unitario").alias("precio_lista")))


# ---------- dim_fecha (1 fila por día — GENERADA, no viene en los datos) ----------
@dlt.table(name=f"{CATALOG}.gold.dim_fecha", comment="Gold - dimensión fecha generada")
def dim_fecha():
    rango = (spark.read.table(f"{CATALOG}.silver.pedidos")
             .select(spark_min("fecha_pedido").alias("min_f"),
                     spark_max("fecha_pedido").alias("max_f")))
    fechas = rango.select(explode(sequence(col("min_f"), col("max_f"))).alias("fecha"))
    return fechas.select(
        date_format("fecha", "yyyyMMdd").cast("int").alias("date_key"),
        col("fecha"),
        year("fecha").alias("anio"),
        month("fecha").alias("mes"),
        dayofmonth("fecha").alias("dia"))


# ---------- fact_ventas (grano: línea de detalle) ----------
@dlt.table(name=f"{CATALOG}.gold.fact_ventas", comment="Gold - hechos de ventas")
@dlt.expect_or_fail("fk_cliente_no_nula", "customer_key IS NOT NULL")
@dlt.expect_or_drop("fk_producto_no_nula", "product_key IS NOT NULL")
@dlt.expect_or_drop("fk_fecha_no_nula", "date_key IS NOT NULL")
@dlt.expect("monto_no_negativo", "monto_total >= 0")
def fact_ventas():
    d = spark.read.table(f"{CATALOG}.silver.detalle_pedidos")
    p = spark.read.table(f"{CATALOG}.silver.pedidos")
    return (d.join(p, "order_id")
             .select(
                col("customer_id").alias("customer_key"),
                col("product_id").alias("product_key"),
                date_format("fecha_pedido", "yyyyMMdd").cast("int").alias("date_key"),
                col("cantidad"),
                col("precio_unitario"),
                col("descuento"),
                (col("cantidad") * col("precio_unitario") * (1 - col("descuento")))
                    .cast("decimal(14,2)").alias("monto_total")))
