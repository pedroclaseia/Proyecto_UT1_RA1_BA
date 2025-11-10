# Reglas de Limpieza y Calidad

## Tipos y Formatos

* **`fecha`** (Ventas) / **`fecha_entrada`** (Productos) / **`fecha`** (Clientes): Se transforma a formato **ISO `YYYY-MM-DD`** (`pd.to_datetime().dt.date`). Se aplica validación estricta de formato de fecha.
* **`unidades`**: Se convierte a tipo **`REAL`** (numérico, acepta decimales en la capa `clean` de SQLite y se valida que sea **$ \ge 0$**. La función de limpieza intenta convertir a numérico (`pd.to_numeric`).
* **`precio_unitario`**: Se convierte a tipo **`REAL`** (numérico) y se aplica una función personalizada (`to_float_money`) para manejar comas como separadores decimales (`str(x).replace(",", ".")`). Se valida que sea **$\ge 0$**.

---

## Nulos

* **Campos Obligatorios (Ventas)**: `fecha`, `unidades`, `precio_unitario`, `id_cliente`, `id_producto`.
* **Campos Obligatorios (Productos)**: `id_producto`, `unidades`, `precio_unitario`.
* **Campos Obligatorios (Clientes)**: `fecha` (fecha de alta), `nombre`, `apellido`, `id_cliente`.
* **Tratamiento (Nulos/Inválidos)**: Las filas que no cumplen las validaciones (incluyendo fechas inválidas o nulos en campos obligatorios) se marcan con `~valid` y se envían a la tabla de **cuarentena** (`quarantine_X`) con el motivo **`validation_failed`** o **`validation_failed_clientes`**.

---

## Rangos y Dominios

* **`unidades`**: Debe ser un valor numérico y **$ \ge 0$**.
* **`precio_unitario`**: Debe ser un valor numérico y **$ \ge 0$**.
* **`id_cliente` (Clientes)**: Debe ser un valor no nulo, normalizado a mayúsculas y *strip*. Debe coincidir con el patrón **`^C\d{3}$`** (p. ej., C001).
* **`nombre` y `apellido` (Clientes)**: Deben ser cadenas de texto no vacías que coincidan con un patrón regex que permite letras, espacios, tildes, diéresis, ñ y guiones (`^[A-Za-zÁÉÍÓÚÜÑáéíóúüñ\s'-]+$`).
* **`id_producto`**: Debe ser un valor no nulo.

---

## Deduplicación

* **Clave Natural (Ventas)**: `(fecha, id_cliente, id_producto)`.
* **Clave Natural (Clientes)**: `(id_cliente)`.
* **Clave Natural (Productos)**: `(id_producto)`.
* **Política de Resolución**: **Último gana** por `_ingest_ts`. La lógica en el `UPSERT` de SQL es: `WHERE excluded._ingest_ts > clean_X._ingest_ts`.

---

## Estandarización de Texto

* **`trim`**: Se aplica la eliminación de espacios iniciales y finales a todas las columnas de texto (`strip_strings` function).
* **Mayúsculas en Códigos**: El `id_cliente` se normaliza a **mayúsculas** (`.str.upper()`) antes de la validación y persistencia.

---

## Trazabilidad

* **Metadatos de Trazabilidad**: Se mantienen y persisten en las tablas `clean_X` y `quarantine_X`:
    * `_ingest_ts`
    * `_source_file`
    * `_batch_id`

---

## QA Rápida

| Dominio | Filas Raw | Filas Clean | Filas Quarantined | % Quarantined | Detalles |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Ventas** | [cite_start]120 [cite: 3] | [cite_start]119 [cite: 3] | [cite_start]1 [cite: 3] | **0.83%** | [cite_start]Un registro falló la validación/parseo (ej. fecha mal formada '2025-13-01')[cite: 3]. |
| **Clientes** | [cite_start]125 [cite: 4, 5, 6, 7, 8] | [cite_start]119 [cite: 4, 5, 6, 7, 8] | [cite_start]6 [cite: 4, 5, 6, 7, 8] | **4.80%** | [cite_start]Fallos por fecha inválida ('2025-13-05', '2025-01-40'), ID mal formado ('004', 'C-006'), o nombre/apellido nulo/inválido[cite: 4, 8]. |
| **Productos** | [cite_start]128 [cite: 1, 2] | [cite_start]118 [cite: 1, 2] | [cite_start]10 [cite: 1, 2] | **7.81%** | [cite_start]Fallos por precio/unidades negativos (`-5`, `-180`), nulos, fecha inválida ('2025-02-30', '2025-13-03'), o campos mezclados (ej. `P015` tiene `1,399,99` en unidades/precio)[cite: 1, 2]. |
*Nota: Los conteos se basan en el análisis de las filas inválidas en los CSV de origen tal como las gestiona `run.py`.*