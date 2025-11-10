-- Ventas diarias (se mantiene por compatibilidad)
CREATE VIEW IF NOT EXISTS ventas_diarias AS
SELECT
  fecha AS fecha,
  SUM(unidades * precio_unitario) AS importe_total,
  COUNT(*) AS lineas
FROM clean_ventas
GROUP BY fecha;

-- Producto más vendido (por unidades totales)
CREATE VIEW IF NOT EXISTS vw_producto_mas_vendido AS
WITH ventas_por_producto AS (
  SELECT
    v.id_producto,
    SUM(v.unidades) AS unidades_vendidas
  FROM clean_ventas v
  GROUP BY v.id_producto
),
ranked AS (
  SELECT
    p.id_producto,
    cp.nombre_producto,
    vpp.unidades_vendidas,
    ROW_NUMBER() OVER (ORDER BY vpp.unidades_vendidas DESC, p.id_producto) AS rn
  FROM ventas_por_producto vpp
  JOIN clean_productos cp ON cp.id_producto = vpp.id_producto
  JOIN clean_productos p  ON p.id_producto  = vpp.id_producto
)
SELECT id_producto, nombre_producto, unidades_vendidas
FROM ranked
WHERE rn = 1;

-- Producto más caro (por precio_unitario en catálogo limpio)
CREATE VIEW IF NOT EXISTS vw_producto_mas_caro AS
WITH ranked AS (
  SELECT
    id_producto,
    nombre_producto,
    precio_unitario,
    ROW_NUMBER() OVER (ORDER BY precio_unitario DESC, id_producto) AS rn
  FROM clean_productos
)
SELECT id_producto, nombre_producto, precio_unitario
FROM ranked
WHERE rn = 1;
