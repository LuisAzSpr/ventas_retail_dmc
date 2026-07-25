# Proyecto final - Pipeline de ventas retail

Pipeline de datos en Databricks que toma 4 entidades de un negocio de ventas retail, las procesa por capas (Bronze, Silver y Gold) y termina en un modelo estrella que alimenta un dashboard.

Está hecho con Spark Declarative Pipelines (Lakeflow / Delta Live Tables) en Python. Usa STREAM para leer los datos de forma incremental, aplica reglas de calidad (expectations) en Silver y Gold, se orquesta con un Job y se despliega como Databricks Asset Bundle.

## Cómo funciona

Los archivos CSV y JSON se dejan en un Volume. Desde ahí el pipeline los procesa así:

- Bronze: los lee tal cual con STREAM, sin transformar. Son streaming tables.
- Silver: limpia, convierte tipos, quita duplicados y valida con expectations. También son streaming tables.
- Gold: arma el modelo estrella (dimensiones y tabla de hechos) como materialized views.

El dashboard se construye sobre las tablas de Gold. Un Job dispara todo el proceso.

## Estructura

```
databricks.yml            Configuracion del bundle
resources/
  pipeline.yml            El pipeline
  job.yml                 El job que lo ejecuta
src/
  00_setup.py             Crea el catalogo, los esquemas y el volume
  transformations/
    01_bronze.py
    02_silver.py
    03_gold.py
dashboard/
  dashboard_gold.lvdash.json
data/                     Los 12 archivos fuente (4 entidades, 3 lotes cada una)
```

## Catalogo y tablas

Todo vive en el catalogo `proyecto_final`, con un esquema por capa.

| Capa | Esquema | Tablas |
|------|---------|--------|
| Landing | landing | volume raw_data |
| Bronze | bronze | clientes_raw, productos_raw, pedidos_raw, detalle_pedidos_raw |
| Silver | silver | clientes, productos, pedidos, detalle_pedidos |
| Gold | gold | dim_cliente, dim_producto, dim_fecha, fact_ventas |

Los archivos se suben a: `/Volumes/proyecto_final/landing/raw_data/ventas_retail_luisazana/{entidad}/`

## Modelo estrella

`fact_ventas` es la tabla de hechos, con una fila por cada linea de detalle de un pedido. Se une a tres dimensiones: `dim_cliente`, `dim_producto` y `dim_fecha`.

Guarda las llaves `customer_key`, `product_key` y `date_key`, y las metricas `cantidad`, `precio_unitario`, `descuento` y `monto_total` (cantidad por precio unitario, restando el descuento).

## Diccionario de datos

Ademas de los campos de negocio, cada archivo trae una columna `audit_timestamp` con la fecha y hora del lote.

### clientes (CSV)

| Campo | Tipo | Descripcion |
|-------|------|-------------|
| customer_id | Integer | Identificador del cliente (clave primaria) |
| nombre | String | Nombre |
| apellido | String | Apellido |
| email | String | Correo |
| ciudad | String | Ciudad |
| pais | String | Pais |
| fecha_registro | Date | Fecha de alta |
| segmento | String | Retail o Premium |

### productos (CSV)

| Campo | Tipo | Descripcion |
|-------|------|-------------|
| product_id | Integer | Identificador del producto (clave primaria) |
| nombre_producto | String | Nombre |
| categoria | String | Categoria |
| subcategoria | String | Subcategoria |
| precio_unitario | Decimal | Precio de lista |
| proveedor | String | Proveedor |
| stock_actual | Integer | Unidades en stock |

### pedidos (JSON)

| Campo | Tipo | Descripcion |
|-------|------|-------------|
| order_id | Integer | Identificador del pedido (clave primaria) |
| customer_id | Integer | Cliente que hizo el pedido |
| fecha_pedido | Date | Fecha del pedido |
| canal_venta | String | web, app_movil o tienda_fisica |
| estado_pedido | String | completado, en_proceso o cancelado |
| total_pedido | Decimal | Monto total |

### detalle_pedidos (JSON)

| Campo | Tipo | Descripcion |
|-------|------|-------------|
| order_item_id | Integer | Identificador de la linea (clave primaria) |
| order_id | Integer | Pedido al que pertenece |
| product_id | Integer | Producto comprado |
| cantidad | Integer | Unidades |
| precio_unitario | Decimal | Precio aplicado |
| descuento | Decimal | Descuento aplicado (de 0 a 1) |

## Calidad de datos

Hay reglas (expectations) en Silver y en Gold, con las tres severidades: warn (solo avisa), drop (descarta la fila) y fail (detiene la ejecucion). En Silver revisan formato y valores validos; en Gold revisan que las llaves y los montos del modelo sean correctos.

## Como desplegarlo

1. Iniciar sesion: `databricks auth login --host <workspace>`
2. Subir los archivos de `data/` al volume, cada entidad en su carpeta.
3. Desplegar: `databricks bundle deploy -t dev`
4. Ejecutar: `databricks bundle run job_ventas_retail -t dev`
