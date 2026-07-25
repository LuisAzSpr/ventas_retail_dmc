# Proyecto Final Integrador — Pipeline de Ventas Retail

Pipeline de datos **end-to-end** sobre Databricks: ingesta incremental de 4 entidades de ventas retail, arquitectura **medallion** (Bronze → Silver → Gold), modelo **estrella** en Gold, calidad de datos con **expectations**, orquestación con **Job** y empaquetado como **Databricks Asset Bundle (DAB)**.

Todo el pipeline está construido con **Spark Declarative Pipelines** (Lakeflow / ex Delta Live Tables) en **Python**, usando `STREAM` para la ingesta incremental.

## Arquitectura

```
Archivos CSV/JSON (Volume)
        │  STREAM (Auto Loader, incremental)
        ▼
🥉 BRONZE  (STREAMING TABLE)   ingesta cruda + metadata de ingesta
        │  STREAM
        ▼
🥈 SILVER  (STREAMING TABLE)   limpieza, tipado, dedup + expectations
        │  batch
        ▼
🥇 GOLD    (MATERIALIZED VIEW) modelo estrella + expectations
        │
        ▼
📊 Dashboard de Databricks (≥4 visualizaciones)
```

Orquestado por un **Job** (`setup` → `pipeline`) y desplegado con `databricks bundle deploy`.

## Estructura del repositorio

```
.
├── databricks.yml              # Definición del Asset Bundle
├── resources/
│   ├── pipeline.yml            # Declarative Pipeline
│   └── job.yml                 # Job orquestador (setup + pipeline)
├── src/
│   ├── 00_setup.py             # Crea catálogo, esquemas y volume
│   └── transformations/
│       ├── 01_bronze.py        # Ingesta cruda (STREAMING TABLE)
│       ├── 02_silver.py        # Limpieza + expectations (STREAMING TABLE)
│       └── 03_gold.py          # Modelo estrella + expectations (MATERIALIZED VIEW)
├── dashboard/
│   └── dashboard_gold.lvdash.json   # Dashboard exportado (capa Gold)
└── data/                       # 12 archivos fuente (4 entidades x 3 batches)
```

## Catálogo, esquema y tabla por capa

| Capa | Catálogo | Esquema | Tablas | Tipo |
|------|----------|---------|--------|------|
| Landing | proyecto_final | landing | volume `raw_data` | Volume |
| Bronze | proyecto_final | bronze | `clientes_raw`, `productos_raw`, `pedidos_raw`, `detalle_pedidos_raw` | Streaming table |
| Silver | proyecto_final | silver | `clientes`, `productos`, `pedidos`, `detalle_pedidos` | Streaming table |
| Gold | proyecto_final | gold | `dim_cliente`, `dim_producto`, `dim_fecha`, `fact_ventas` | Materialized view |

**Ruta del Volume:** `/Volumes/proyecto_final/landing/raw_data/ventas_retail_luisazana/{entidad}/`

## Modelo estrella (Gold)

```
              dim_cliente
                   │
dim_producto ─ fact_ventas ─ dim_fecha
```

`fact_ventas` (grano: 1 fila por línea de detalle de pedido) — FKs `customer_key`, `product_key`, `date_key` + métricas `cantidad`, `precio_unitario`, `descuento`, `monto_total` (= cantidad × precio_unitario × (1 − descuento)).

## Diccionario de datos

> Nota: además de los campos de negocio, cada archivo fuente incluye una columna `audit_timestamp` (marca de auditoría/ingesta). En Bronze se agregan también `_fecha_ingesta` y `_archivo_origen`.

### clientes (CSV)
| Campo | Tipo | Descripción |
|-------|------|-------------|
| customer_id | Integer | Identificador único del cliente (PK) |
| nombre | String | Nombre del cliente |
| apellido | String | Apellido del cliente |
| email | String | Correo electrónico de contacto |
| ciudad | String | Ciudad de residencia |
| pais | String | País de residencia |
| fecha_registro | Date | Fecha de alta (yyyy-MM-dd) |
| segmento | String | Segmento comercial: Retail / Premium |
| audit_timestamp | Timestamp | Marca de auditoría del batch |

### productos (CSV)
| Campo | Tipo | Descripción |
|-------|------|-------------|
| product_id | Integer | Identificador único del producto (PK) |
| nombre_producto | String | Nombre comercial |
| categoria | String | Categoría |
| subcategoria | String | Subcategoría |
| precio_unitario | Decimal | Precio unitario de lista |
| proveedor | String | Proveedor |
| stock_actual | Integer | Unidades en inventario |
| audit_timestamp | Timestamp | Marca de auditoría del batch |

### pedidos (JSON)
| Campo | Tipo | Descripción |
|-------|------|-------------|
| order_id | Integer | Identificador único del pedido (PK) |
| customer_id | Integer | FK → clientes.customer_id |
| fecha_pedido | Date | Fecha del pedido |
| canal_venta | String | Canal: web / app_movil / tienda_fisica |
| estado_pedido | String | completado / en_proceso / cancelado |
| total_pedido | Decimal | Monto total del pedido |
| audit_timestamp | Timestamp | Marca de auditoría del batch |

### detalle_pedidos (JSON)
| Campo | Tipo | Descripción |
|-------|------|-------------|
| order_item_id | Integer | Identificador único de la línea (PK) |
| order_id | Integer | FK → pedidos.order_id |
| product_id | Integer | FK → productos.product_id |
| cantidad | Integer | Unidades compradas |
| precio_unitario | Decimal | Precio unitario aplicado |
| descuento | Decimal | Descuento aplicado (0 a 1) |
| audit_timestamp | Timestamp | Marca de auditoría del batch |

## Calidad de datos (expectations)

Se aplican en Silver (validez de formato) y en Gold (integridad del modelo), usando las 3 severidades: `warn` (`@dlt.expect`), `drop` (`@dlt.expect_or_drop`) y `fail` (`@dlt.expect_or_fail`).

## Despliegue

```bash
# 1. Autenticarse contra el workspace
databricks auth login --host https://<tu-workspace>

# 2. Validar y desplegar el bundle
databricks bundle validate -t dev
databricks bundle deploy -t dev

# 3. Ejecutar el job (setup + pipeline)
databricks bundle run job_ventas_retail -t dev
```

Antes de correr el pipeline, subir los 12 archivos de `data/` al Volume, respetando la estructura `/{entidad}/`.
