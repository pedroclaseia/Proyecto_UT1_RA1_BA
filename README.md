# Proyecto_UT1_RA1_BA · Solución de ingestión, almacenamiento y reporte (UT1 · RA1)

Este repositorio contiene:
- **project/**: código reproducible (ingesta → clean → oro → reporte Markdown).
- **site/**: web pública con **Quartz 4** (GitHub Pages). El reporte UT1 se publica en `site/content/reportes/`.

## Ejecución rápida
```bash
# 1) Dependencias (elige uno)
python -m venv .venv
# Windows:
.venv\Scripts\activate
# Linux/Mac:
# source .venv/bin/activate
pip install -r project/requirements.txt
# o con Conda:
# conda env create -f project/environment.yml && conda activate ut1

# 2) (Opcional) Generar datos de ejemplo
python project/ingest/get_data.py

# 3) Pipeline fin-a-fin (ingesta→clean→oro→reporte.md)
python project/ingest/run.py

# 4) Copiar el reporte a la web Quartz
python project/tools/copy_report_to_site.py

# 5) (Opcional) Previsualizar la web en local
cd site
npx quartz build --serve   # abre http://localhost:8080

```

## 🌐 Publicación Web (GitHub Pages)

En **Settings → Pages** del repositorio, selecciona **“Source = GitHub Actions”**.
El workflow `.github/workflows/deploy-pages.yml` compila el sitio (`npx quartz build`) y despliega la carpeta `public` a Pages.

> **Importante:** Revisa la configuración de `baseUrl` en Quartz antes del primer deploy para que `sitemap` y `RSS` funcionen correctamente.

---

## 🌊 Flujo de Datos

**Bronce (raw) → Plata (clean) → Oro (analytics)**.

* **Ingesta:** *batch* desde CSV hacia `raw_*`, con metadatos de trazabilidad: `_ingest_ts`, `_source_file`, `_batch_id`.
* **Deduplicación “último gana”:** Se aplica por `_ingest_ts` sobre **claves naturales** (ventas: `fecha`, `id_cliente`, `id_producto`; clientes: `id_cliente`; productos: `id_producto`), usando `ON CONFLICT` en **SQLite**.
* **Reporte:** Generado en `project/output/reporte.md` y publicado en `site/content/reportes/reporte-UT1.md` para su visualización en **Quartz 4**.

---

## 💻 Esquema y SQL (`project/sql/`)

| Archivo | Descripción |
| :--- | :--- |
| **`00_schema.sql`** | Crea tablas `raw_`, `clean_` y `quarantine_*` por dominio con **PK** para habilitar `ON CONFLICT` en `clean_ventas`, `clean_clientes` y `clean_productos`. |
| **`10_upserts.sql`** | Tres sentencias independientes `INSERT…ON CONFLICT` (ventas, clientes, productos) con política **last-wins** por `_ingest_ts`; cada sentencia termina en “;” para ejecución fila a fila. |
| **`20_views.sql`** | **Vistas de reporte** y “oro”: `ventas_diarias_producto`, `ventas_diarias_categoria`, producto más vendido y más caro, y vistas por cliente (top por importe y por unidades). |

---

## 🧼 Ingesta y Limpieza

### Lectura Robusta de CSV
* Las líneas mal formadas por **conteo de columnas** se separan.
* Se envían a una cuarentena unificada por dominio (`ventas_quarantine.csv`, `productos_quarantine.csv`, `clientes_quarantine.csv`) y a tablas `quarantine_*` con `_row` crudo y motivo **`parse_error_bad_field_count`**.

### Validaciones por Dominio

* **ventas**:
    * `fecha` válida, `unidades ≥ 0`, `precio_unitario ≥ 0`.
    * Claves no vacías y validación **FK** contra `clean_productos` y `clean_clientes`.
    * En cuarentena se distinguen: `foreign_key_violation_product` y `foreign_key_violation_client`.
* **productos**:
    * `id_producto` no vacío, `unidades` y `precio` no negativos.
    * Tipado y dedupe **last-wins**.
* **clientes**:
    * `fecha` válida, `nombre` y `apellido` **alfabéticos**.
    * `id_cliente` con patrón **`CNNN`**.
    * Dedupe **last-wins** por `id_cliente`.

---

## 💎 Capa Plata (`clean`) y Mini‑DWH

| Tabla | Clave Primaria (PK) | Política de Actualización | Notas |
| :--- | :--- | :--- | :--- |
| **`clean_ventas`** | (`fecha`, `id_cliente`, `id_producto`) | UPSERT last-wins por `_ingest_ts` |
| **`clean_productos`** | `id_producto` | UPSERT last-wins |
| **`clean_clientes`** | `id_cliente` | UPSERT last-wins |
| **`dim_productos`** | `id_producto` | | (Parquet + tabla SQLite) |
| **`dim_clientes`** | `id_cliente` | | (Parquet + tabla SQLite) |
| **`fact_ventas`** | Compuesta | Actualización por conflicto (last-wins) | PK compuesta. Campos: `fecha`, `id_producto`, `id_cliente`, `unidades`, `precio_unitario`, `importe`. |

---

## 🥇 Capa Oro y Vistas (SQL)

* **`ventas_diarias_producto`**: `fecha`, `id_producto`, `nombre`, `categoría`, `unidades`, `importe`, `ticket_medio`. Se usa para **KPI diarios por producto**.
* **`ventas_diarias_categoria`**: Agregación por categoría y día con `ticket_medio` (`importe/unidades`, `NULL` si `unidades=0`).
* **`vw_producto_mas_vendido`**: Top 1 por **unidades acumuladas** a partir de `fact_ventas`, enriquecido con `nombre` desde dimensión.
* **`vw_producto_mas_caro`**: Top 1 por **`precio_unitario`** desde catálogo limpio (`dim_productos`).
* **`ventas_diarias_cliente`, `vw_cliente_top_importe`, `vw_cliente_top_unidades`**: Agregados y *rankings* por cliente (importe y unidades) a partir de `fact_ventas` y `dim_clientes`.

---

## 📥 Entradas (CSV)

| Archivo | Campos Principales | Notas sobre Calidad |
| :--- | :--- | :--- |
| **`ventas.csv`** | `fecha_venta`, `id_cliente`, `id_producto`, `unidades`, `precio_unitario` | Incluye casos **mal formados controlados** para pruebas de calidad y cuarentena. |
| **`productos.csv`** | `fecha_entrada`, `nombre_producto`, `id_producto`, `unidades`, `precio_unitario`, `categoria` | Contiene **valores no válidos** (fechas, IDs, negativos) para testear reglas. |
| **`clientes.csv`** | `fecha`, `nombre`, `apellido`, `id_cliente` | Incluye ejemplos **inválidos** de fecha, nombres con dígitos e IDs no estándar. |

---

## 📤 Salidas (SQLite + Parquet)

### SQLite
* Ruta: `project/output/ut1.db`
* Contiene: Tablas `raw_*`, `clean_*`, `quarantine_*`, `dim_productos`, `dim_clientes`, `fact_ventas` y **vistas de reporte**.

### Parquet
* Ruta: `project/output/parquet/`
* Archivos: `clean_ventas.parquet`, `clean_productos.parquet`, `clean_clientes.parquet`, `dim_productos.parquet`, `dim_clientes.parquet` y `fact_ventas.parquet`.
* *(Requiere `pyarrow` o `fastparquet`)*.

---

## 📊 KPIs y Supuestos

* **Importe**: Suma de $\text{unidades} \times \text{precio\_unitario}$; calculado en `fact_ventas` y re‑agregado en vistas.
* **Ticket medio**: $\frac{\text{importe}}{\text{unidades}}$ en el nivel de agregación; en división por cero retorna **NULL**.

> **Supuestos:**
> * El precio en ventas es el **precio efectivo de línea**.
> * El catálogo **no reescribe histórico**.
> * Las validaciones FK garantizan integridad en `fact_ventas` con respecto a las dimensiones.

---

## ⚙️ Requisitos

* **Python 3.10+** y `pandas`.
    * Para Parquet, instala `pyarrow` o `fastparquet` (recomendado **`pyarrow`**).
* **Node.js 18+** y `npm` para construir **Quartz 4** en local o vía GitHub Actions.

---

## 🛠️ Troubleshooting

| Problema | Solución |
| :--- | :--- |
| **No aparecen archivos Parquet** | Instala `pyarrow`: `pip install pyarrow`; vuelve a ejecutar el *pipeline*. |
| **Pages no publica el sitio** | Confirma que la acción compila Quartz (`npx quartz build`) y que Pages está configurado a desplegar el *artifact* `public` del *workflow*. |
# BDA_Proyecto_UT1_RA1


