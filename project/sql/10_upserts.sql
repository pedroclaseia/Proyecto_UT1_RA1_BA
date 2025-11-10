-- UPSERT para clean_ventas (PK: fecha,id_cliente,id_producto)
INSERT INTO clean_ventas (
    fecha, id_cliente, id_producto, unidades, precio_unitario, _ingest_ts
)
VALUES (:fecha, :idc, :idp, :u, :p, :ts)
ON CONFLICT(fecha, id_cliente, id_producto) DO UPDATE SET
    unidades = excluded.unidades,
    precio_unitario = excluded.precio_unitario,
    _ingest_ts = excluded._ingest_ts
WHERE excluded._ingest_ts > clean_ventas._ingest_ts;

-- UPSERT para clean_clientes (PK: id_cliente)
INSERT INTO clean_clientes (
    fecha, nombre, apellido, id_cliente, _ingest_ts
)
VALUES (:fecha, :nombre, :apellido, :idc, :ts)
ON CONFLICT(id_cliente) DO UPDATE SET
    fecha = excluded.fecha,
    nombre = excluded.nombre,
    apellido = excluded.apellido,
    _ingest_ts = excluded._ingest_ts
WHERE excluded._ingest_ts > clean_clientes._ingest_ts;

-- UPSERT para clean_productos (PK: id_producto)
INSERT INTO clean_productos (
    fecha_entrada, nombre_producto, id_producto, unidades, precio_unitario, categoria, _ingest_ts
)
VALUES (:fecha_entrada, :nombre_producto, :idp, :u, :p, :cat, :ts)
ON CONFLICT(id_producto) DO UPDATE SET
    fecha_entrada = excluded.fecha_entrada,
    nombre_producto = excluded.nombre_producto,
    unidades = excluded.unidades,
    precio_unitario = excluded.precio_unitario,
    categoria = excluded.categoria,
    _ingest_ts = excluded._ingest_ts
WHERE excluded._ingest_ts > clean_productos._ingest_ts;
