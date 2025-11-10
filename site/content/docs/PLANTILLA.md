---
title: "Documento del Proyecto: Pipeline de Ingestión y Calidad de Datos"
tags: ["UT1","RA1","docs"]
version: "1.0.0"
owner: "equipo-alumno"
status: "draft"
---

# 1. Objetivo 🎯
El objetivo principal de este proyecto es implementar un **pipeline de datos (ETL)** que ingiera, limpie, valide y persista datos de ventas, clientes y productos. El propósito es transformar los datos de origen (capa **Bronce** - `raw`) en una capa de datos limpios y confiables (capa **Plata** - `clean`) para soportar análisis de negocio (capa **Oro** - `views`).

---

# 2. Alcance 🗺️
**Cubre:**
* La ingesta de archivos CSV para los dominios de Ventas, Clientes, y Productos.
* La aplicación de reglas de calidad de datos (validación de formatos, rangos, y nulos).
* La implementación de un mecanismo de **idempotencia** y **deduplicación** basado en clave primaria y marca de tiempo (`_ingest_ts`).
* La trazabilidad de los datos y el manejo de registros inválidos en una capa de **Cuarentena** (`quarantine_X`).
* La generación de un modelo analítico básico (vistas) para métricas clave (ej. `ventas_diarias`, `vw_producto_mas_vendido`).

**No Cubre:**
* La orquestación automática del proceso (se ejecuta manualmente vía `run.py`).
* La integración con sistemas de origen o destino en la nube (se utiliza SQLite y Parquet local).
* El manejo de cambios en el esquema (schema evolution) de los archivos fuente.

---

# 3. Decisiones / Reglas ⚙️

## Estrategia de Ingestión
* **Modo:** **Batch** a demanda, procesando todos los archivos CSV disponibles en la fuente.
* **Idempotencia:** Implementada mediante sentencias **`UPSERT`** (`INSERT INTO ... ON CONFLICT DO UPDATE`).
* **Deduplicación:** Se mantiene el registro con el **`_ingest_ts`** más reciente sobre la clave primaria, aplicando la política de "Último gana".

## Claves Naturales (PK en capa Plata - `clean`)
* **Ventas:** `(fecha, id_cliente, id_producto)`.
* **Clientes:** `(id_cliente)`.
* **Productos:** `(id_producto)`.

## Validaciones de Calidad (Cuarentena)
* **Malformación:** Filas con un número incorrecto de campos van a cuarentena con razón `parse_error_bad_field_count`.
* **Rangos:** `unidades` y `precio_unitario` deben ser numéricos y $\ge 0$.
* **Nulos/Formatos:** Campos obligatorios deben ser no nulos. Se valida el formato de fecha (ISO `YYYY-MM-DD`) y el formato del `id_cliente` (`^C\d{3}$`).
* **Estandarización:** Se aplica `TRIM` a todas las cadenas y `id_cliente` se normaliza a mayúsculas.

---

# 4. Procedimiento / Pasos 🛠️
Para reproducir la ejecución completa del pipeline, siga los siguientes pasos:

1.  **Preparación:** Asegúrese de que los archivos de esquema (`00_schema.sql`), UPSERTs (`10_upserts.sql`), Vistas (`20_views.sql`), el script (`run.py`), y los datos CSV (`ventas.csv`, `clientes.csv`, `productos.csv`) se encuentran en sus directorios esperados.
2.  **Ejecución:** Ejecutar el script principal desde la línea de comandos:
    ```bash
    python run.py
    ```
3.  **Resultado:** El script creará la base de datos **`ut1.db`** en la carpeta `/output` y los archivos Parquet/Cuarentena en `/output/parquet` y `/output/quality`.

---

# 5. Evidencias 📊
El script `run.py` proporciona los siguientes contadores de salida que sirven como evidencia de la ejecución y el control de calidad:

| Dominio | Filas RAW | Filas CLEAN (Plata) | Filas QUARANTINE | PK: Clean |
| :--- | :--- | :--- | :--- | :--- |
| **Ventas** | 120 | 119 | 1 | `(fecha, id_cliente, id_producto)` |
| **Clientes** | 125 | 119 | 6 | `(id_cliente)` |
| **Productos** | 128 | 118 | 10 | `(id_producto)` |

> **Evidencia de Vistas (Capa Oro):** Las vistas `ventas_diarias`, `vw_producto_mas_vendido`, y `vw_producto_mas_caro` están disponibles en `ut1.db` para su consulta.

---

# 6. Resultados (Métricas y Hallazgos) 📈

| Métrica Clave (Período Total) | Valor |
| :--- | :--- |
| **Ingresos Totales** | **€59,715** |
| **Transacciones Totales** | **119** |
| **Ticket Medio** | **€501.81** |

**Top Productos por Importe** (Igual contribución en la muestra): **P005** (Televisor Sony 55"), **P025** (Herramienta 25), **P085** (Equipo 85) con €3,500 cada uno.

**Hallazgos de Calidad:** Se ha detectado una tasa de cuarentena del **4.8% en Clientes** (por formato ID y fechas inválidas) y **7.8% en Productos** (por valores numéricos negativos y fechas).

---

# 7. Lecciones Aprendidas 🧠
* **Salió Bien:** La lógica de **`UPSERT`** en `10_upserts.sql` y el uso del **`_ingest_ts`** demostraron ser robustos para garantizar la deduplicación e idempotencia con la regla de "último gana". La separación de errores en parseo vs. validación es efectiva para el QA.
* **Mejorar/Distinto:** La validación de números (`to_float_money`, `pd.to_numeric`) fue necesaria, lo que indica un problema en el origen de los datos de `unidades` y `precio_unitario` (ej. valores con comas o mezclados en una sola columna como en `P015` de `productos.csv`). En el futuro, se podría añadir una validación de que `fecha_entrada` no sea posterior a la fecha actual para la tabla de productos.

---

# 8. Próximos Pasos ➡️
* **Acción 1: Corrección de Origen.** Revisar los procesos que generan los CSVs para evitar fechas inválidas (`2025-13-XX`, `2025-02-30`) y valores negativos/mal formateados, ya que causan la mayor parte de la cuarentena.
* **Acción 2: Finalizar Modelo Oro.** Crear la vista de **Top productos por Ingreso** (no solo por unidades) para tener un KPI completo.
* **Dueño:** Equipo de Datos / QA.