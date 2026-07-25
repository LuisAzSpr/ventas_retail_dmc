import dlt
from pyspark.sql.functions import col, to_date

CATALOG = "proyecto_final"


# ---------- clientes ----------
@dlt.table(name=f"{CATALOG}.silver.clientes", comment="Silver - clientes limpios")
@dlt.expect_or_fail("customer_id_no_nulo", "customer_id IS NOT NULL")
@dlt.expect("email_valido", "email LIKE '%@%.%'")
@dlt.expect_or_drop("segmento_valido", "segmento IN ('Retail','Premium')")
def silver_clientes():
    return (spark.readStream.table(f"{CATALOG}.bronze.clientes_raw")
            .select(
                col("customer_id").cast("int").alias("customer_id"),
                col("nombre").cast("string"), col("apellido").cast("string"),
                col("email").cast("string"), col("ciudad").cast("string"),
                col("pais").cast("string"),
                to_date("fecha_registro").alias("fecha_registro"),
                col("segmento").cast("string"))
            .dropDuplicates(["customer_id"]))


# ---------- productos ----------
@dlt.table(name=f"{CATALOG}.silver.productos", comment="Silver - productos limpios")
@dlt.expect_or_fail("product_id_no_nulo", "product_id IS NOT NULL")
@dlt.expect_or_drop("precio_positivo", "precio_unitario > 0")
@dlt.expect("stock_no_negativo", "stock_actual >= 0")
def silver_productos():
    return (spark.readStream.table(f"{CATALOG}.bronze.productos_raw")
            .select(
                col("product_id").cast("int").alias("product_id"),
                col("nombre_producto").cast("string"), col("categoria").cast("string"),
                col("subcategoria").cast("string"),
                col("precio_unitario").cast("decimal(10,2)").alias("precio_unitario"),
                col("proveedor").cast("string"),
                col("stock_actual").cast("int").alias("stock_actual"))
            .dropDuplicates(["product_id"]))


# ---------- pedidos ----------
@dlt.table(name=f"{CATALOG}.silver.pedidos", comment="Silver - pedidos limpios")
@dlt.expect_or_fail("order_id_no_nulo", "order_id IS NOT NULL")
@dlt.expect_or_drop("estado_valido", "estado_pedido IN ('completado','en_proceso','cancelado')")
@dlt.expect("total_no_negativo", "total_pedido >= 0")
def silver_pedidos():
    return (spark.readStream.table(f"{CATALOG}.bronze.pedidos_raw")
            .select(
                col("order_id").cast("int").alias("order_id"),
                col("customer_id").cast("int").alias("customer_id"),
                to_date("fecha_pedido").alias("fecha_pedido"),
                col("canal_venta").cast("string"), col("estado_pedido").cast("string"),
                col("total_pedido").cast("decimal(12,2)").alias("total_pedido"))
            .dropDuplicates(["order_id"]))


# ---------- detalle_pedidos ----------
@dlt.table(name=f"{CATALOG}.silver.detalle_pedidos", comment="Silver - detalle limpio")
@dlt.expect_or_fail("order_item_id_no_nulo", "order_item_id IS NOT NULL")
@dlt.expect_or_drop("cantidad_positiva", "cantidad > 0")
@dlt.expect_or_drop("fks_no_nulas", "order_id IS NOT NULL AND product_id IS NOT NULL")
def silver_detalle_pedidos():
    return (spark.readStream.table(f"{CATALOG}.bronze.detalle_pedidos_raw")
            .select(
                col("order_item_id").cast("int").alias("order_item_id"),
                col("order_id").cast("int").alias("order_id"),
                col("product_id").cast("int").alias("product_id"),
                col("cantidad").cast("int").alias("cantidad"),
                col("precio_unitario").cast("decimal(10,2)").alias("precio_unitario"),
                col("descuento").cast("decimal(5,2)").alias("descuento"))
            .dropDuplicates(["order_item_id"]))
