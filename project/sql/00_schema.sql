
-- 00_schema.sql — Esquema para pipeline (SQLite)

-- Bronce: raw (lo usa run.py)
CREATE TABLE IF NOT EXISTS raw_ventas(
  fecha TEXT,
  id_cliente TEXT,
  id_producto TEXT,
  unidades TEXT,
  precio_unitario TEXT,
  _ingest_ts TEXT,
  _source_file TEXT,
  _batch_id TEXT
);

-- Plata: clean (lo usa run.py y las vistas)
CREATE TABLE IF NOT EXISTS clean_ventas(
  fecha TEXT,
  id_cliente TEXT,
  id_producto TEXT,
  unidades REAL,
  precio_unitario REAL,
  _ingest_ts TEXT,
  PRIMARY KEY (fecha, id_cliente, id_producto)
);

-- Cuarentena para registros inválidos (exportada a CSV por run.py)
CREATE TABLE IF NOT EXISTS quarantine_ventas(
  _reason TEXT,
  _row TEXT,
  _ingest_ts TEXT,
  _source_file TEXT,
  _batch_id TEXT
);

--clientes
-- Bronce: raw
CREATE TABLE IF NOT EXISTS raw_clientes(
  fecha TEXT,
  nombre TEXT,
  apellido TEXT,
  id_cliente TEXT,
  _ingest_ts TEXT,
  _source_file TEXT,
  _batch_id TEXT
);

-- Plata: clean
CREATE TABLE IF NOT EXISTS clean_clientes(
  fecha TEXT,
  nombre TEXT,
  apellido TEXT,
  id_cliente TEXT,
  _ingest_ts TEXT,
  PRIMARY KEY (id_cliente)
);

-- Cuarentena
CREATE TABLE IF NOT EXISTS quarantine_clientes(
  _reason TEXT,
  _row TEXT,
  _ingest_ts TEXT,
  _source_file TEXT,
  _batch_id TEXT
);

-- Productos
-- Bronce: raw
CREATE TABLE IF NOT EXISTS raw_productos(
  fecha_entrada TEXT,
  nombre_producto TEXT,
  id_producto TEXT,
  unidades TEXT,
  precio_unitario TEXT,
  categoria TEXT,
  _ingest_ts TEXT,
  _source_file TEXT,
  _batch_id TEXT
);

-- Plata: clean
CREATE TABLE IF NOT EXISTS clean_productos(
  fecha_entrada TEXT,
  nombre_producto TEXT,
  id_producto TEXT,
  unidades REAL,
  precio_unitario REAL,
  categoria TEXT,
  _ingest_ts TEXT,
  PRIMARY KEY (id_producto)
);

-- Cuarentena
CREATE TABLE IF NOT EXISTS quarantine_productos(
  _reason TEXT,
  _row TEXT,
  _ingest_ts TEXT,
  _source_file TEXT,
  _batch_id TEXT
);
