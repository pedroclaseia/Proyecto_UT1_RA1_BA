---
title: "Definición de Métricas y Tablas Oro"
owner: "equipo-alumno"
periodicidad: "diaria"
version: "1.0.0"
---

# Modelo de Negocio (Capa Oro)

## Tablas Oro

La capa Oro de análisis se construye sobre la capa Plata (`clean_X`), utilizando **vistas SQL** para agregar y transformar los datos a la granularidad necesaria para los informes.

| Nombre | Tipo | Granularidad | Fuente | Descripción |
| :--- | :--- | :--- | :--- | :--- |
| **clean\_ventas** | Tabla Plata | **Línea de venta** | `raw_ventas` | Fuente de máxima granularidad, validada y deduplicada. |
| **ventas\_diarias** | Vista | **Día** | `clean_ventas` | Agregación diaria de ingresos y número de transacciones (líneas). |
| **vw\_producto\_mas\_vendido** | Vista | **Producto** | `clean_ventas`, `clean_productos` | Identifica el producto con el mayor número total de **unidades vendidas**. |
| **vw\_producto\_mas\_caro** | Vista | **Producto** | `clean_productos` | Identifica el producto con el **precio unitario** de catálogo más alto. |

---

## Métricas (KPI)

| Métrica | Definición | Vista/Consulta Fuente |
| :--- | :--- | :--- |
| **Ingresos netos** | Suma total de los importes de venta: $\sum(\text{unidades} \times \text{precio\_unitario})$. | `ventas_diarias` (columna `importe_total`) |
| **Líneas de Venta** | Conteo de transacciones por día: $COUNT(*)$ sobre líneas de venta. | `ventas_diarias` (columna `lineas`) |
| **Ticket medio** | Cálculo derivado: $\text{Ingresos netos} / \text{Líneas de Venta}$. | Derivado de `ventas_diarias` |
| **Top producto (por Unidades)** | El `id_producto` con el mayor $\sum(\text{unidades})$. | `vw_producto_mas_vendido` |
| **Top producto (por Ingreso)** | El `id_producto` con el mayor $\sum(\text{unidades} \times \text{precio\_unitario})$ en un periodo. | `clean_ventas` (Consulta conceptual) |

---

## Supuestos

* **Capa Base (Plata)**: La capa `clean_ventas` ya está **deduplicada** utilizando el criterio de **"último gana por `_ingest_ts`"** sobre la clave primaria `(fecha, id_cliente, id_producto)`.
* **Divisa/Valores**: Las métricas se calculan directamente como $\text{unidades} \times \text{precio\_unitario}$. Se asume que estos valores son **netos** (sin impuestos) y no están sujetos a descuentos adicionales fuera de la tabla `clean_ventas`.
* **`precio_unitario`**: En `clean_ventas` es el precio de la venta. En `clean_productos` es el precio de catálogo (usado para `vw_producto_mas_caro`).

---

## Consultas Base (SQL conceptual)

A continuación, se muestran las consultas que definen las principales vistas de análisis en `20_views.sql`:

```sql
-- Ingresos por día (Vista: ventas_diarias)
CREATE VIEW IF NOT EXISTS ventas_diarias AS
SELECT
  fecha AS fecha,
  SUM(unidades * precio_unitario) AS importe_total,
  COUNT(*) AS lineas
FROM clean_ventas
GROUP BY fecha;

-- Top producto por UNIDADES vendidas (Vista: vw_producto_mas_vendido)
WITH ventas_por_producto AS (
  SELECT
    v.id_producto,
    SUM(v.unidades) AS unidades_vendidas
  FROM clean_ventas v
  GROUP BY v.id_producto
)
SELECT id_producto, nombre_producto, unidades_vendidas
FROM (
    SELECT
      p.id_producto,
      cp.nombre_producto,
      vpp.unidades_vendidas,
      ROW_NUMBER() OVER (ORDER BY vpp.unidades_vendidas DESC, p.id_producto) AS rn
    FROM ventas_por_producto vpp
    JOIN clean_productos cp ON cp.id_producto = vpp.id_producto
)
WHERE rn = 1;