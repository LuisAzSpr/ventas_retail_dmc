# 🗺️ Plan de Implementación — Proyecto Final Integrador (ed13)

> Guía por fases para construir el pipeline end-to-end en una **cuenta Databricks nueva**, navegando la interfaz paso a paso, escribiendo el código en **Python**, y entregando en **GitHub** + presentación.
> Fuente única de verdad: `Instrucciones.html`. Este plan traduce cada requisito del HTML a acciones concretas.

---

## 0. Decisiones fijadas

| Decisión | Valor |
|----------|-------|
| Lenguaje del pipeline | **Python** (`@dlt` / Declarative Pipelines) |
| Cuenta / entorno | Databricks **nueva** (Free Edition o trial, serverless + Unity Catalog). No se reutiliza nada del curso |
| Catálogo | `proyecto_final` |
| Esquemas | `landing` · `bronze` · `silver` · `gold` |
| Volume | `proyecto_final.landing.raw_data` |
| Nombre de proyecto (carpeta en Volume) | `ventas_retail_luisazana` |
| Ruta raíz de datos | `/Volumes/proyecto_final/landing/raw_data/ventas_retail_luisazana/{entidad}/` |
| GitHub | Cuenta ya existente; repo nuevo (entregable obligatorio) |

---

## 1. Arquitectura objetivo

```
Archivos (12)                DECLARATIVE PIPELINE (Lakeflow / DLT, Python)              BI
CSV/JSON en Volume     ┌──────────────┐   ┌──────────────┐   ┌──────────────────┐
                       │   BRONZE     │   │   SILVER     │   │      GOLD        │   ┌───────────┐
clientes.csv    ──┐    │ STREAMING    │   │ STREAMING    │   │ MATERIALIZED VIEW│   │ DASHBOARD │
productos.csv   ──┼──▶ │ TABLE        │──▶│ TABLE        │──▶│ modelo estrella  │──▶│  ≥4 viz   │
pedidos.json    ──┤    │ (STREAM,     │   │ (STREAM,     │   │ dim_* + fact_*   │   └───────────┘
detalle.json    ──┘    │  raw+audit)  │   │  clean+exp)  │   │ (expectations)   │
                       └──────────────┘   └──────────────┘   └──────────────────┘
                              ▲ leído incrementalmente (STREAM) desde el Volume
        Todo orquestado por un JOB · empaquetado como ASSET BUNDLE · versionado en GITHUB
```

**Tipo de tabla por capa (obligatorio, sección 2 y 7 del HTML):**

| Capa | Esquema | Tipo | Rol |
|------|---------|------|-----|
| 🥉 Bronze | `bronze` | `STREAMING TABLE` (STREAM) | ingesta cruda + metadata, sin transformar |
| 🥈 Silver | `silver` | `STREAMING TABLE` (STREAM) | limpieza, tipado, dedup, **expectations** |
| 🥇 Gold | `gold` | `MATERIALIZED VIEW` | modelo estrella + **expectations** |

**Tablas (nombres exactos del HTML, sección 5):**

- Bronze: `clientes_raw`, `productos_raw`, `pedidos_raw`, `detalle_pedidos_raw`
- Silver: `clientes`, `productos`, `pedidos`, `detalle_pedidos`
- Gold: `dim_cliente`, `dim_producto`, `dim_fecha`, `fact_ventas`

**Modelo estrella (Gold):**
```
              dim_cliente
                   │
dim_producto ─ fact_ventas ─ dim_fecha
```
`fact_ventas` (grano = 1 línea de detalle): FKs `customer_key`, `product_key`, `date_key` + métricas `cantidad`, `precio_unitario`, `descuento`, `monto_total`.

---

## 2. Mapa de requisitos → fase  (para no perder ningún punto)

| # | Requisito del HTML | Fase |
|---|--------------------|------|
| 1 | Pipeline end-to-end Bronze→Gold | 4–6 |
| 2 | Dashboard sobre Gold (≥4 viz) | 8 |
| 3 | Modelo estrella (hechos + dimensiones) | 6 |
| 4 | 4 entidades (2 CSV + 2 JSON) | ✅ provistas |
| 5 | 3 batches por entidad (incremental) | ✅ provistos · 3 |
| 6 | Diccionario de datos por entidad | 10 (README) |
| 7 | Bronze→Gold 100% Declarative Pipelines | 4–6 |
| 8 | `STREAM` sobre las fuentes | 4 (Bronze), 5 (Silver) |
| 9 | Expectations en Silver **y** Gold (warn/drop/fail) | 5, 6 |
| 10 | Orquestación con un Job | 7 |
| 11 | Empaquetado como DAB | 9 |
| 12 | Volume con ruta estándar | 1–2 |
| 13 | Catálogo/esquema/tabla documentados por capa | 10 (README) |
| 14 | Archivo de **setup** (crea Volume + catálogo + esquemas) | 1 |
| 15a | Repo en GitHub | 10 |
| 15b | Presentación (imagen pipeline corriendo + imagen dashboard) | 11 |

---

## FASE 0 — Prerrequisitos (local + cuentas)

**Objetivo:** dejar listo el entorno local y las credenciales para trabajar contra la cuenta nueva.

1. **Identificar tu workspace Databricks:** inicia sesión y copia la URL del workspace (algo como `https://<xxxx>.cloud.databricks.com` o `.databricks.com`). La necesitaremos para el CLI y el bundle.
2. **Confirmar que es serverless + Unity Catalog:** en la barra lateral verás **Catalog**; si existe, Unity Catalog está activo. En Free Edition el cómputo es serverless (no hay que crear clústers).
3. **Instalar herramientas locales:**
   - **Databricks CLI** (nueva, v0.2xx+): necesaria para Asset Bundles. `winget install Databricks.DatabricksCLI` o descarga del release. Verifica: `databricks --version`.
   - **Git** (para GitHub). Verifica: `git --version`.
   - **Python 3.10+** local (solo para el CLI/bundles; el pipeline corre en Databricks).
4. **Autenticar el CLI con la cuenta nueva:**
   ```
   databricks auth login --host https://<tu-workspace>
   ```
   Abre el navegador (OAuth). Al terminar, `databricks auth profiles` debe listar tu perfil.
5. **Preparar la carpeta del proyecto local:** trabajaremos dentro de `proyecto_final_ed13/`. La estructura final del bundle se arma en la Fase 9 (ver el árbol al final).

> ✅ **Validación fase 0:** `databricks current-user me` responde con tu correo → CLI autenticado contra la cuenta correcta.

---

## FASE 1 — Setup de Unity Catalog (catálogo + esquemas + volume)

**Cubre requisito #14 (archivo de setup) y #12/#13 (estructura y documentación).**

**Objetivo:** crear el catálogo, los 4 esquemas y el volume — de forma **reproducible** en un notebook (este notebook ES el entregable "setup").

### 1.1 Navegación UI (para entender lo que el script automatiza)
- Barra lateral → **Catalog** → botón **Create catalog** → nombre `proyecto_final` (tipo: *Standard*, ubicación de almacenamiento por defecto).
- Dentro del catálogo → **Create schema** → crea `landing`, `bronze`, `silver`, `gold`.
- Dentro de `landing` → pestaña **Volumes** → **Create volume** → `raw_data` (tipo *Managed*).

### 1.2 El notebook de setup (recomendado — reemplaza los clics de arriba)
- Barra lateral → **+ New** → **Notebook**. Nómbralo `00_setup`. Lenguaje: Python (usaremos `spark.sql`).
- Contenido (esqueleto):
  ```python
  CATALOG = "proyecto_final"
  spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
  for sch in ["landing", "bronze", "silver", "gold"]:
      spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{sch}")
  spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.landing.raw_data")
  # (opcional) crear subcarpetas por entidad dentro del volume
  base = f"/Volumes/{CATALOG}/landing/raw_data/ventas_retail_luisazana"
  for ent in ["clientes", "productos", "pedidos", "detalle_pedidos"]:
      dbutils.fs.mkdirs(f"{base}/{ent}")
  ```
- Ejecútalo (Run all). Refresca **Catalog** y verifica que aparezcan catálogo, esquemas y volume.

> ⚠️ Si en tu cuenta no tienes permiso `CREATE CATALOG` (poco común en cuenta propia), avísame y adaptamos para trabajar dentro del catálogo por defecto (`workspace`) con los 4 esquemas.

> ✅ **Validación fase 1:** `proyecto_final` con esquemas landing/bronze/silver/gold y volume `raw_data` visibles en Catalog Explorer.

---

## FASE 2 — Cargar los 12 archivos al Volume

**Cubre requisito #12 (datos crudos en el Volume con la estructura exacta).**

**Objetivo:** subir cada entidad a su subcarpeta, respetando la ruta obligatoria.

Estructura destino:
```
/Volumes/proyecto_final/landing/raw_data/ventas_retail_luisazana/
├── clientes/         clientes_batch_1.csv · _2.csv · _3.csv
├── productos/        productos_batch_1.csv · _2.csv · _3.csv
├── pedidos/          pedidos_batch_1.json · _2.json · _3.json
└── detalle_pedidos/  detalle_pedidos_batch_1.json · _2.json · _3.json
```

**Opción A — UI (recomendada para aprender):**
- **Catalog** → `proyecto_final` → `landing` → Volumes → `raw_data`.
- Navega/crea la carpeta `ventas_retail_luisazana/clientes` → botón **Upload** → sube los 3 CSV de `data/clientes/`. Repite por entidad.

**Opción B — CLI (más rápida, sube todo de una):**
```
databricks fs cp -r "data/clientes"         "dbfs:/Volumes/proyecto_final/landing/raw_data/ventas_retail_luisazana/clientes"
databricks fs cp -r "data/productos"        "dbfs:/Volumes/proyecto_final/landing/raw_data/ventas_retail_luisazana/productos"
databricks fs cp -r "data/pedidos"          "dbfs:/Volumes/proyecto_final/landing/raw_data/ventas_retail_luisazana/pedidos"
databricks fs cp -r "data/detalle_pedidos"  "dbfs:/Volumes/proyecto_final/landing/raw_data/ventas_retail_luisazana/detalle_pedidos"
```

> ✅ **Validación fase 2:** `databricks fs ls dbfs:/Volumes/.../clientes` muestra los 3 archivos; ídem para las otras 3 entidades (12 en total).

---

## FASE 3 — Escribir el código del pipeline (Python, 3 archivos)

**Cubre #1, #7, #8, #9.** Escribimos 3 notebooks/archivos Python. Recomendación: crearlos como **archivos `.py`** dentro de una carpeta `src/transformations/` (así encajan directo en el bundle de la Fase 9).

> 🔑 **3 puntos técnicos críticos (de mi análisis de los datos):**
> 1. **JSON multilínea:** `pedidos` y `detalle_pedidos` son *arrays* JSON, no NDJSON → hay que leerlos con `multiLine=true`, si no Auto Loader falla.
> 2. **Columna extra `audit_timestamp`** presente en las 4 entidades (no está en el diccionario del HTML) → la conservamos como metadata.
> 3. **`dim_fecha` no existe en los datos** → hay que fabricarla.

### 3.1 `01_bronze.py` — STREAMING TABLE (ingesta cruda con STREAM)
Una tabla por entidad, sin transformar, agregando metadata de ingesta:
```python
import dlt
from pyspark.sql.functions import current_timestamp, col

BASE = "/Volumes/proyecto_final/landing/raw_data/ventas_retail_luisazana"

def bronze_stream(entidad, fmt, extra_opts):
    @dlt.table(name=f"bronze.{entidad}_raw", comment=f"Bronze raw {entidad}")
    def _t():
        r = spark.readStream.format("cloudFiles").option("cloudFiles.format", fmt)
        for k, v in extra_opts.items():
            r = r.option(k, v)
        return (r.load(f"{BASE}/{entidad}")
                 .withColumn("_ingest_ts", current_timestamp())
                 .withColumn("_source_file", col("_metadata.file_name")))
    return _t

bronze_stream("clientes", "csv",  {"header": "true"})
bronze_stream("productos", "csv", {"header": "true"})
bronze_stream("pedidos", "json",  {"multiLine": "true"})          # ← array multilínea
bronze_stream("detalle_pedidos", "json", {"multiLine": "true"})   # ← array multilínea
```

### 3.2 `02_silver.py` — STREAMING TABLE (limpieza + expectations de formato)
Lee de Bronze con `STREAM`, castea tipos, deduplica por PK, aplica **expectations** (validez estructural/formato). Las 3 severidades se reparten aquí y en Gold.
```python
import dlt
from pyspark.sql.functions import col, to_date

# clientes: PK no nula (fail), email válido (warn), segmento en lista (drop)
@dlt.table(name="silver.clientes")
@dlt.expect_or_fail("pk_no_nula", "customer_id IS NOT NULL")
@dlt.expect("email_valido", "email RLIKE '^[^@]+@[^@]+\\.[^@]+$'")
@dlt.expect_or_drop("segmento_valido", "segmento IN ('Retail','Premium')")
def s_clientes():
    return (dlt.read_stream("bronze.clientes_raw")
              .withColumn("customer_id", col("customer_id").cast("int"))
              .withColumn("fecha_registro", to_date("fecha_registro"))
              .dropDuplicates(["customer_id"]))
# … análogo para productos (precio>0 drop, stock>=0 warn),
#     pedidos (order_id fail, estado en lista drop, total>=0 warn),
#     detalle (order_item_id fail, cantidad>0 drop, FKs no nulas drop)
```
Reglas por entidad (del HTML, sección 6):

| Entidad | Regla | Severidad sugerida |
|---------|-------|--------------------|
| clientes | `customer_id` no nulo | **fail** |
| clientes | `email` formato válido | warn |
| clientes | `segmento ∈ {Retail,Premium}` | drop |
| productos | `product_id` no nulo | fail |
| productos | `precio_unitario > 0` | drop |
| productos | `stock_actual >= 0` | warn |
| pedidos | `order_id` no nulo | fail |
| pedidos | `estado_pedido ∈ {completado,en_proceso,cancelado}` | drop |
| pedidos | `total_pedido >= 0` | warn |
| detalle | `order_item_id` no nulo | fail |
| detalle | `cantidad > 0` | drop |
| detalle | FKs `order_id`/`product_id` no nulas | drop |

### 3.3 `03_gold.py` — MATERIALIZED VIEW (modelo estrella + expectations de integridad)
Dimensiones + hechos como `MATERIALIZED VIEW` (en Python DLT, una MV es un `@dlt.table` que hace `dlt.read(...)` batch, sin `read_stream`).
```python
import dlt
from pyspark.sql.functions import col, explode, sequence, to_date, min as fmin, max as fmax, date_format, year, month, dayofmonth

# --- Dimensiones ---
@dlt.table(name="gold.dim_cliente")           # 1 fila por cliente
def dim_cliente():
    return dlt.read("silver.clientes").select(
        col("customer_id").alias("customer_key"), "nombre","apellido","email","ciudad","pais","segmento")

@dlt.table(name="gold.dim_producto")          # 1 fila por producto
def dim_producto():
    return dlt.read("silver.productos").select(
        col("product_id").alias("product_key"), "nombre_producto","categoria","subcategoria","proveedor")

@dlt.table(name="gold.dim_fecha")             # 1 fila por día — GENERADA
def dim_fecha():
    rango = dlt.read("silver.pedidos").select(fmin("fecha_pedido").alias("min"), fmax("fecha_pedido").alias("max"))
    fechas = rango.select(explode(sequence(col("min"), col("max"))).alias("fecha"))
    return fechas.select(
        date_format("fecha","yyyyMMdd").cast("int").alias("date_key"),
        col("fecha"), year("fecha").alias("anio"), month("fecha").alias("mes"), dayofmonth("fecha").alias("dia"))

# --- Hechos: grano = línea de detalle; join detalle→pedidos para traer cliente y fecha ---
@dlt.table(name="gold.fact_ventas")
@dlt.expect_or_fail("fk_cliente", "customer_key IS NOT NULL")
@dlt.expect_or_drop("fk_producto", "product_key IS NOT NULL")
@dlt.expect_or_drop("monto_no_negativo", "monto_total >= 0")
def fact_ventas():
    d = dlt.read("silver.detalle_pedidos")
    p = dlt.read("silver.pedidos")
    return (d.join(p, "order_id")
             .select(
                col("customer_id").alias("customer_key"),
                col("product_id").alias("product_key"),
                date_format("fecha_pedido","yyyyMMdd").cast("int").alias("date_key"),
                "cantidad", col("precio_unitario"), "descuento",
                (col("cantidad")*col("precio_unitario")*(1-col("descuento"))).alias("monto_total")))
```

> ⚠️ **Detalle a confirmar al implementar:** el pipeline publica en `proyecto_final` con tablas en 3 esquemas (`bronze.*`, `silver.*`, `gold.*`). Lakeflow soporta *multi-schema publishing* fijando el catálogo por defecto del pipeline y calificando los nombres con el esquema, como arriba. Lo verificamos en la primera corrida.

> ✅ **Validación fase 3:** los 3 archivos escritos, con STREAM en Bronze/Silver, MV en Gold, y ≥1 expectation de cada severidad (warn/drop/fail) repartidas entre Silver y Gold.

---

## FASE 4 — Crear, configurar y ejecutar el Declarative Pipeline (UI)

**Cubre #1, #7, #8.**

1. Barra lateral → **Jobs & Pipelines** (o **Pipelines**) → **Create** → **ETL / Declarative Pipeline**.
2. **Source code:** apunta a la carpeta `src/transformations/` (súbela como carpeta en **Workspace**, o conéctala vía Git folder — ver Fase 10).
3. **Destination:** catálogo `proyecto_final`, esquema por defecto (ej. `bronze`); las tablas de silver/gold usan nombres calificados por esquema.
4. **Compute:** serverless. **Pipeline mode:** *Triggered* (no continuous, para el proyecto). **Photon:** on.
5. **Start** → observa el **grafo del pipeline**: 4 nodos Bronze → 4 Silver → 4 Gold (dims + fact). Corrige errores de esquema/tipos iterando.
6. Repite la corrida cargando batch por batch (o todos) para **demostrar el `STREAM` incremental**: cada corrida ingiere solo lo nuevo.

> 💡 **Flujo recomendado:** desarrolla y depura aquí (feedback rápido). En la Fase 9 este mismo pipeline se define como código en el bundle; podrás borrar el pipeline hecho a mano y quedarte con el del bundle.

> ✅ **Validación fase 4:** grafo en verde, tablas pobladas en bronze/silver/gold. Cuenta las filas: Gold `fact_ventas` ≈ 72; `dim_cliente` 66, `dim_producto` 60, `dim_fecha` = nº de días del rango.

---

## FASE 5 — Orquestar con un Job

**Cubre requisito #10.**

1. **Jobs & Pipelines** → **Create Job**. Nombre: `job_ventas_retail`.
2. **Task 1 (opcional):** `setup` → tipo *Notebook* → `00_setup` (crea catálogo/esquemas/volume si no existen).
3. **Task 2:** `pipeline` → tipo *Pipeline* → selecciona el Declarative Pipeline de la Fase 4. `depends_on: setup`.
4. Ejecuta el Job (**Run now**) y confirma que ambas tareas terminan OK.

> ✅ **Validación fase 5:** run del Job en verde con la tarea de pipeline disparando la actualización.

---

## FASE 6 — Dashboard sobre Gold (≥4 visualizaciones)

**Cubre requisito #2 y el entregable #2.**

1. Barra lateral → **Dashboards** → **Create dashboard**.
2. **Data:** añade las tablas `proyecto_final.gold.*`. Crea datasets (SQL) uniendo `fact_ventas` con las dimensiones.
3. Crea **al menos 4 visualizaciones** distintas, p. ej.:
   - Ventas (`monto_total`) por **categoría** (`dim_producto`).
   - Ventas por **mes** (`dim_fecha`) — línea temporal.
   - Ventas por **segmento** de cliente (`dim_cliente`) o por país.
   - Ventas por **canal_venta** / estado, o Top 10 productos.
4. **Publica** el dashboard.
5. **Exporta** el dashboard: menú del dashboard → **Export** → guarda el archivo `dashboard_gold.lvdash.json` (irá en `dashboard/` del bundle).

> ✅ **Validación fase 6:** dashboard publicado con ≥4 gráficos leyendo de Gold + archivo `.lvdash.json` exportado.

---

## FASE 7 — Empaquetar como Databricks Asset Bundle (DAB)

**Cubre requisito #11 y el entregable #3.**

Estructura objetivo (sección 8 del HTML):
```
proyecto_final_ed13/            ← raíz del bundle y del repo
├── databricks.yml              ← definición del bundle (targets dev/prod, variables)
├── resources/
│   ├── pipeline.yml            ← define el Declarative Pipeline
│   └── job.yml                 ← define el Job que orquesta el pipeline
├── src/
│   ├── 00_setup.py             ← setup (catálogo/esquemas/volume)
│   └── transformations/
│       ├── 01_bronze.py
│       ├── 02_silver.py
│       └── 03_gold.py
├── dashboard/
│   └── dashboard_gold.lvdash.json
├── data/                       ← los 12 archivos (para reproducibilidad)
└── README.md                   ← documentación (diccionarios + capas)
```

Pasos:
1. `databricks bundle init` (plantilla vacía o Default Python) **o** crear `databricks.yml` a mano con variables `catalog`, `bronze_schema`, `silver_schema`, `gold_schema` y targets `dev`/`prod`.
2. Rellenar `resources/pipeline.yml` (libraries → glob a `src/transformations/**`, catálogo, serverless, configuración) y `resources/job.yml` (task tipo pipeline + task setup).
3. Validar: `databricks bundle validate`.
4. Desplegar: `databricks bundle deploy -t dev`.
5. En la UI, verifica que el pipeline y el job aparezcan (prefijados `[dev tu_usuario]`). Borra el pipeline/job hechos a mano en fases 4–5 para no duplicar.
6. Ejecutar desde código: `databricks bundle run <pipeline_o_job>`.

> ✅ **Validación fase 7:** `databricks bundle deploy` OK y el pipeline del bundle corre en verde.

---

## FASE 8 — Publicar en GitHub

**Cubre requisito #15a y el entregable #5.**

1. En GitHub: crea un repo nuevo (p. ej. `ventas_retail_luisazana`), vacío.
2. En local, dentro de `proyecto_final_ed13/`:
   ```
   git init
   git add .
   git commit -m "Proyecto final: pipeline medallion ventas retail"
   git branch -M main
   git remote add origin https://github.com/<tu-usuario>/ventas_retail_luisazana.git
   git push -u origin main
   ```
3. Añade un `.gitignore` (excluir `.databricks/`, credenciales, `__pycache__/`).
4. **README.md** con (cubre #6 y #13):
   - Descripción y arquitectura (diagrama medallion).
   - **Diccionario de datos** por entidad (incluyendo la columna `audit_timestamp`).
   - Tabla **catálogo/esquema/tabla por capa**.
   - Instrucciones de despliegue (`databricks bundle deploy`) y del setup.

> 💡 Alternativa: conectar el repo como **Git folder** en el workspace (Workspace → Create → Git folder) para editar el código desde Databricks y versionar en el mismo repo.

> ✅ **Validación fase 8:** repo público/accesible en GitHub con todo el proyecto y README completo.

---

## FASE 9 — Evidencias y presentación

**Cubre requisito #15b.**

1. **Screenshot 1:** el Declarative Pipeline **desplegado y en ejecución** (grafo en verde, con nombres de tablas por capa visibles).
2. **Screenshot 2:** el **dashboard** con las ≥4 visualizaciones.
3. Arma una **presentación** breve: objetivo, arquitectura, capas y expectations, modelo estrella, + las 2 imágenes, + enlace al repo.

> ✅ **Validación fase 9:** presentación con las 2 evidencias y el enlace a GitHub.

---

## ✅ Checklist final de entrega

- [ ] Catálogo `proyecto_final` + esquemas landing/bronze/silver/gold + volume `raw_data` (Fase 1)
- [ ] 12 archivos en el Volume con la estructura por entidad (Fase 2)
- [ ] `01_bronze.py` — 4 STREAMING TABLES con STREAM (+ multiLine en JSON) (Fase 3)
- [ ] `02_silver.py` — 4 STREAMING TABLES con expectations de formato (Fase 3)
- [ ] `03_gold.py` — dim_cliente, dim_producto, dim_fecha (generada), fact_ventas + expectations de integridad (Fase 3)
- [ ] ≥1 expectation de cada severidad usada: **warn + drop + fail** (Fases 3–6)
- [ ] Pipeline corriendo en verde (Fase 4)
- [ ] Job orquestando el pipeline (Fase 5)
- [ ] Dashboard con ≥4 viz + `dashboard_gold.lvdash.json` exportado (Fase 6)
- [ ] Bundle `databricks bundle deploy` OK (Fase 7)
- [ ] `00_setup.py` incluido (Fase 1/7)
- [ ] Repo en GitHub + README con diccionarios y capas documentadas (Fase 8)
- [ ] Presentación con imagen del pipeline + imagen del dashboard (Fase 9)

---

## Puntos abiertos / a confirmar contigo

1. **Tipo de cuenta** (Free Edition vs trial en AWS/Azure/GCP) → afecta detalles de cómputo y permisos de `CREATE CATALOG`.
2. **Multi-schema publishing** del pipeline (publicar bronze/silver/gold desde un solo pipeline) → sintaxis exacta a validar en la primera corrida.
3. **¿Inyectar datos sucios?** Los datos provistos están limpios (integridad 100%), así que `drop`/`fail` no eliminarán nada visible. Si quieres evidencia de que las expectations actúan, podemos ensuciar 2–3 registros de un batch.
4. **API DLT:** uso `import dlt` (clásico, ampliamente documentado). Si tu runtime nuevo usa `from pyspark import pipelines`, ajustamos.
