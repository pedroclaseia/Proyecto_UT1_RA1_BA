# Diseño de Ingestión

## Resumen
Este pipeline está diseñado para procesar archivos de datos de ventas, clientes y productos suministrados en formato CSV. La ingesta opera en modo **batch** a demanda a través del script `run.py`. La principal garantía es la **limpieza y validación** de los datos antes de su persistencia en la capa *Clean* (Plata). La **idempotencia** se logra mediante sentencias `UPSERT` en SQLite, priorizando el registro con el `_ingest_ts` más reciente.

---

## Fuente
- **Origen:** Archivos CSV ubicados en el directorio local, simulado como `data/drops/*.csv`.
- **Formato:** **CSV** (Comma Separated Values).
- **Frecuencia:** **A demanda/Manual** (ejecución de `run.py`).

---

## Estrategia
- **Modo:** **`batch`** (Procesamiento completo de los archivos disponibles en la fuente por ejecución).
- **Incremental:** **Full-refresh controlado por clave primaria y `_ingest_ts`**. Aunque el archivo fuente puede ser un *drop* completo, el `UPSERT` asegura que solo se actualice el registro si es más reciente, o si se añade un registro nuevo.
- **Particionado:** No aplica a nivel de almacenamiento de la fuente. La capa *Clean* se almacena en una única base de datos **SQLite** (`ut1.db`) y archivos **Parquet**.

---

## Idempotencia y Deduplicación
- **batch_id:** Se genera a partir del nombre base del archivo fuente (`f.stem.lower()`).
- **Clave Natural (Clave Primaria en la capa `clean`):**
    * **Ventas:** `(fecha, id_cliente, id_producto)`.
    * **Clientes:** `(id_cliente)`.
    * **Productos:** `(id_producto)`.
- **Política:** **"Último gana por `_ingest_ts`"**. La sentencia `UPSERT` actualiza un registro existente solo si el `_ingest_ts` del registro entrante es mayor que el que ya está en la tabla `clean`.

---

## Checkpoints y Trazabilidad
- **checkpoints/offset:** N/A (no es un sistema de streaming).
- **trazabilidad:** Se añaden metadatos de trazabilidad a todas las capas (`raw`, `clean`, `quarantine`):
    * `_ingest_ts`: Marca de tiempo ISO UTC de la ingestión.
    * `_source_file`: Nombre del archivo CSV de origen.
    * `_batch_id`: Identificador de la ejecución/archivo.
- **DLQ/quarantine:** Se manejan dos tipos de errores y se persisten en tablas específicas de SQLite (`quarantine_ventas`, `quarantine_clientes`, `quarantine_productos`) y se exportan a CSV:
    * **`parse_error_bad_field_count`**: Errores de malformación (diferente número de columnas en la línea).
    * **`validation_failed` / `validation_failed_clientes`**: Errores de calidad de datos (ej. fecha inválida, valores negativos, ID de cliente mal formateado).

---

## SLA
- **Disponibilidad:** **Definido por la hora de ejecución del batch.** Asumiendo que `run.py` se ejecuta diariamente, la capa *Clean* está disponible inmediatamente después de la finalización de la ejecución.
- **Alertas:** El script imprime un resumen al final (`Ventas (raw, clean, quar)`) que sirve como *check* de control de calidad.

---

## Riesgos / Antipatrones
- Batch con necesidad de segundos → **No es un riesgo**, el pipeline es puramente de batch y no requiere baja latencia.
- Falta de clave natural → **Mitigado**, se han definido explícitamente claves primarias únicas para cada tabla *Clean*.