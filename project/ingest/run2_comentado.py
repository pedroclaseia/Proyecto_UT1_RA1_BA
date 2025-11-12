from pathlib import Path
from datetime import datetime, timezone
import pandas as pd
import sqlite3
import re
from io import StringIO

# --- Configuración de Rutas y Estructura de Directorios ---
# Obtiene la ruta del directorio raíz del proyecto (un nivel por encima del script actual)
ROOT = Path(__file__).resolve().parents[1]
# Define el directorio de donde se tomarán los archivos CSV de entrada (drops)
DATA = ROOT / "data" / "drops"
# Define el directorio de salida principal
OUT = ROOT / "output"
# Define los subdirectorios para los archivos Parquet limpios y los reportes de calidad
PARQUET_DIR = OUT / "parquet"
QUALITY_DIR = OUT / "quality"

# Crea los directorios si no existen
OUT.mkdir(parents=True, exist_ok=True)
PARQUET_DIR.mkdir(parents=True, exist_ok=True)
QUALITY_DIR.mkdir(parents=True, exist_ok=True)
# Define la ruta de la base de datos SQLite de salida
DB = OUT / "ut1.db"

# --- Funciones de Utilidad ---

def to_float_money(x):
    """
    Convierte una cadena que representa dinero (con coma como separador decimal) a float.
    Si la conversión falla, devuelve None.
    """
    try:
        # Reemplaza la coma por el punto y convierte a float
        return float(str(x).replace(",", "."))
    except Exception:
        return None

def strip_strings(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpia los espacios en blanco iniciales y finales (strip) de:
    1. Los nombres de las columnas.
    2. Todas las celdas en columnas de tipo 'object' (cadenas).
    """
    df = df.copy()
    # Limpia los nombres de las columnas
    df.columns = df.columns.str.strip()
    
    # Itera sobre las columnas para limpiar valores de tipo string
    for c in df.columns:
        if pd.api.types.is_object_dtype(df[c]):
            # Convierte a string y aplica strip a las celdas
            df[c] = df[c].astype(str).str.strip()
    return df

def classify_file(fname: str) -> str | None:
    """
    Clasifica un archivo basándose en palabras clave en su nombre.
    Retorna el tipo de dominio ('ventas', 'clientes', 'productos') o None.
    """
    n = fname.lower()
    if any(k in n for k in ["ventas", "venta"]):
        return "ventas"
    if any(k in n for k in ["clientes", "cliente"]):
        return "clientes"
    if any(k in n for k in ["productos", "producto"]):
        return "productos"
    return None

def write_parquet(df: pd.DataFrame, path: Path, label: str):
    """
    Escribe un DataFrame en un archivo Parquet, imprimiendo el resultado o un aviso de error.
    """
    try:
        # Escribe el DataFrame al formato Parquet (index=False para no guardar el índice)
        df.to_parquet(path, index=False) 
        print(f"Parquet escrito: {path.name} ({len(df)} filas) para {label}")
    except ImportError as e:
        # Avisa si faltan librerías necesarias (pyarrow o fastparquet)
        print(f"[AVISO] No se pudo escribir {path.name} (instala 'pyarrow' o 'fastparquet'): {e}")

# --- Manejo de Calidad y Cuarentena ---

def append_quarantine(con: sqlite3.Connection, kind: str, reasons_rows: list[tuple[str, str, str, str, str]]):
    """
    Añade filas malformadas o inválidas a la tabla de cuarentena de la base de datos SQLite
    y al archivo CSV de cuarentena en el disco.
    """
    if not reasons_rows:
        return
        
    # Crea un DataFrame temporal con las filas en cuarentena trazabilidad
    dfq = pd.DataFrame(reasons_rows, columns=["_reason", "_row", "_ingest_ts", "_source_file", "_batch_id"])
    table = f"quarantine_{kind}"
    
    # 1. Persiste en SQLite (modo append)
    dfq.to_sql(table, con, if_exists="append", index=False)
    
    # 2. Persiste en CSV en el directorio de calidad
    out_csv = QUALITY_DIR / f"{kind}_quarantine.csv"
    mode = "a" if out_csv.exists() else "w"
    # Escribe al CSV, asegurando que si el archivo existe, no se escriba el header de nuevo
    dfq.to_csv(out_csv, index=False, mode=mode, header=not out_csv.exists())

def split_good_bad_lines(f: Path) -> tuple[list[str], list[str]]:
    """
    Lee un archivo de texto y separa las líneas en 'buenas' y 'malas'
    basándose en el conteo de separadores (comas).
    """
    # Lee todo el contenido y lo divide por líneas
    raw = f.read_text(encoding="utf-8").splitlines()
    if not raw:
        return [], []
        
    # La primera línea es la cabecera
    header = raw[0]
    # Determina el número esperado de columnas basado en el conteo de comas en el header
    expected_cols = header.count(",") + 1
    
    good = [header]
    bad = []
    
    # Itera sobre las líneas de datos (excluyendo la cabecera)
    for line in raw[1:]:
        # Calcula el número de columnas para la línea actual
        cols = line.count(",") + 1
        # Una línea es "buena" si el conteo de columnas coincide y no está vacía
        if cols == expected_cols and line.strip():
            good.append(line)
        else:
            # Si no coincide o está vacía, se marca como "mala"
            bad.append(line)
            
    # Devuelve las listas de líneas válidas y mal formadas
    return good, bad

# --- Ingesta de Archivos CSV ---

def ingest_one(f: Path, con: sqlite3.Connection, kind: str) -> pd.DataFrame:
    """
    Ingesta un único archivo CSV, maneja errores de parseo,
    lo carga en un DataFrame y añade metadatos de ingesta.
    """
    batch_id = f.stem.lower()
    
    # Separa líneas buenas de malas por error de parseo (conteo de comas)
    good_lines, bad_lines = split_good_bad_lines(f)
    
    # Si hay líneas malas, las añade a la cuarentena de 'parse_error_bad_field_count'
    if bad_lines:
        now = datetime.now(timezone.utc).isoformat()
        rows = [("parse_error_bad_field_count", bl, now, f.name, batch_id) for bl in bad_lines]
        append_quarantine(con, kind, rows)
        
    # Si solo queda la cabecera o menos, devuelve un DataFrame vacío
    if len(good_lines) <= 1:
        return pd.DataFrame()
        
    # Usa StringIO para tratar las líneas buenas como un archivo en memoria para pandas
    buf = StringIO("\n".join(good_lines))
    # Lee el CSV. Usamos dtype=str para importar todo como string y evitar coerciones tempranas
    df = pd.read_csv(buf, dtype=str, engine="python", on_bad_lines="skip")
    
    # Aplica la limpieza de espacios en blanco a columnas y valores
    df = strip_strings(df)
    
    # Estandarización de nombre de columna para ventas
    if "fecha_venta" in df.columns:
        df = df.rename(columns={"fecha_venta": "fecha"})
        
    # Añade columnas de metadatos de ingesta
    df["_source_file"] = f.name
    df["_ingest_ts"] = datetime.now(timezone.utc).isoformat()
    df["_batch_id"] = batch_id
    
    return df
# --- Ingesta Masiva de Archivos CSV --- bronce ingesta directa de los datos
def ingest_all_csvs_to_raw(con: sqlite3.Connection) -> dict:
    """
    Procesa todos los archivos CSV en el directorio DATA, los ingesta en DataFrames RAW
    y los persiste en las tablas RAW de SQLite.
    """
    counters = {"ventas": 0, "clientes": 0, "productos": 0}
    # Busca y ordena todos los archivos CSV en el directorio DATA
    detected = sorted(DATA.glob("*.csv"))
    print("CSV detectados:", [p.name for p in detected])
    
    for f in detected:
        # Clasifica el tipo de archivo
        kind = classify_file(f.name)
        
        if not kind:
            print("Ignorado (sin match):", f.name)
            continue
            
        # Ingesta el archivo y aplica la cuarentena de parseo
        df = ingest_one(f, con, kind)
        
        if kind == "ventas":
            # Define las columnas requeridas para el esquema RAW de ventas
            needed = ["fecha", "id_cliente", "id_producto", "unidades", "precio_unitario", "_ingest_ts", "_source_file", "_batch_id"]
            # Asegura que existan todas las columnas, añadiéndolas si faltan con valor None
            for c in needed:
                if c not in df.columns:
                    df[c] = None
            
            # Selecciona solo las columnas necesarias para el almacenamiento RAW
            df_raw = df[needed].copy()
            
            if not df_raw.empty:
                # Persiste en la tabla RAW de SQLite
                df_raw.to_sql("raw_ventas", con, if_exists="append", index=False)
                counters["ventas"] += len(df_raw)
                
        elif kind == "clientes":
            # Define las columnas requeridas para el esquema RAW de clientes
            cols = ["fecha", "nombre", "apellido", "id_cliente"]
            # Asegura que existan todas las columnas
            for c in cols:
                if c not in df.columns:
                    df[c] = None
            
            # Selecciona las columnas de datos y metadatos
            df_raw = df[cols + ["_ingest_ts", "_source_file", "_batch_id"]].copy()
            
            if not df_raw.empty:
                # Persiste en la tabla RAW de SQLite
                df_raw.to_sql("raw_clientes", con, if_exists="append", index=False)
                counters["clientes"] += len(df_raw)
                
        elif kind == "productos":
            # Define las columnas requeridas para el esquema RAW de productos
            cols = ["fecha_entrada", "nombre_producto", "id_producto", "unidades", "precio_unitario", "categoria"]
            # Asegura que existan todas las columnas
            for c in cols:
                if c not in df.columns:
                    df[c] = None
                    
            # Selecciona las columnas de datos y metadatos
            df_raw = df[cols + ["_ingest_ts", "_source_file", "_batch_id"]].copy()
            
            if not df_raw.empty:
                # Persiste en la tabla RAW de SQLite
                df_raw.to_sql("raw_productos", con, if_exists="append", index=False)
                counters["productos"] += len(df_raw)
                
    return counters

# --- Carga de Sentencias SQL de UPSERT plata ---

def load_upsert_sqls(path: Path) -> dict[str, str]:
    """
    Carga y parsea el archivo SQL para extraer las sentencias de UPSERT
    para cada tabla limpia ('clean_ventas', 'clean_clientes', 'clean_productos').
    """
    # Lee el contenido y elimina el Byte Order Mark (BOM) si existe
    raw = path.read_text(encoding="utf-8").replace("\ufeff", "")
    lines = []
    # Filtra líneas vacías y comentarios (líneas que empiezan por --)
    for line in raw.splitlines():
        line = line.split("--", 1)[0]
        if line.strip():
            lines.append(line)
    txt = "\n".join(lines)
    
    def extract_one(table: str) -> str:
        """ Función interna para extraer una sentencia INSERT de una tabla específica """
        # Busca el inicio del INSERT INTO {table} (case-insensitive)
        m = re.search(rf"(?is)\binsert\s+into\s+{table}\b", txt)
        if not m:
            raise ValueError(f"No se encontró INSERT INTO {table} en {path.name}")
        
        after = txt[m.start():]
        # Busca el final de la sentencia (punto y coma)
        semi = after.find(";")
        if semi == -1:
            raise ValueError(f"La sentencia INSERT de {table} no termina en ';' en {path.name}")
            
        stmt = after[:semi].strip()
        
        # Validación de la estructura del UPSERT
        if "values" not in stmt.lower() or "on conflict" not in stmt.lower():
            raise ValueError(f"INSERT de {table} incompleto en {path.name}")
        if "*" in stmt:
            raise ValueError(f"INSERT de {table} contiene '*', revisa {path.name}")
            
        return stmt
        
    # Retorna un diccionario con las sentencias SQL de UPSERT
    return {
        "clean_ventas": extract_one("clean_ventas"),
        "clean_clientes": extract_one("clean_clientes"),
        "clean_productos": extract_one("clean_productos"),
    }

# --- Validaciones Específicas ---

# Expresión regular para nombres y apellidos: permite letras, espacios, guiones y apóstrofes
NAME_RE = re.compile(r"^[A-Za-zÁÉÍÓÚÜÑáéíóúüñ\s'-]+$")
def validate_clientes(df: pd.DataFrame) -> pd.Series:
    """
    Aplica las reglas de validación de negocio para el dominio 'clientes'.
    Devuelve una serie booleana indicando True para filas válidas.
    """
    # 1. Fecha: Debe poder convertirse a fecha válida (notna)
    fecha_ok = pd.to_datetime(df["fecha"], errors="coerce").notna()
    # 2. Nombre: No nulo, longitud > 0 y coincide con el patrón de nombre
    nombre_ok = df["nombre"].fillna("").str.len().gt(0) & df["nombre"].fillna("").str.match(NAME_RE)
    # 3. Apellido: No nulo, longitud > 0 y coincide con el patrón de nombre
    apellido_ok = df["apellido"].fillna("").str.len().gt(0) & df["apellido"].fillna("").str.match(NAME_RE)
    # Normaliza el ID del cliente (mayúsculas, sin espacios)
    id_norm = df["id_cliente"].fillna("").str.upper().str.strip()
    # 4. ID Cliente: Debe tener el formato 'C###' (ej: C001)
    id_ok = id_norm.str.match(r"^C\d{3}$")
    
    # La fila es válida si todas las condiciones se cumplen
    return fecha_ok & nombre_ok & apellido_ok & id_ok

def serialize_row_csv_like(row: pd.Series, cols: list[str]) -> str:
    """
    Serializa una fila de pandas (Series) a un formato similar a CSV,
    utilizando comillas dobles para escapar comas o comillas internas.
    """
    values = []
    for c in cols:
        v = row.get(c, "")
        if v is None:
            v = ""
        s = str(v)
        # Aplica el escape si la cadena contiene comas o comillas dobles (CSV quoting)
        if "," in s or '"' in s:
            s = '"' + s.replace('"', '""') + '"'
        values.append(s)
    return ",".join(values)

# --- Limpieza y Persistencia del Dominio ---

# Limpieza: Ventas plata limpieza, validación y deduplicación
def clean_and_persist_ventas_from_raw(con: sqlite3.Connection, upsert_sql: str) -> tuple[int, int, int]:
    """
    Carga datos RAW de ventas, realiza limpieza/validación,
    persiste datos limpios en SQLite y Parquet, y registra inválidos en cuarentena.
    Retorna (filas_raw, filas_limpias, filas_cuarentena).
    """
    # 1. Cargar datos RAW desde SQLite
    df = pd.read_sql_query("SELECT * FROM raw_ventas", con)
    raw_rows = len(df)
    
    if df.empty:
        # Crea el archivo de cuarentena vacío si no hay datos
        (QUALITY_DIR / "ventas_quarantine.csv").touch(exist_ok=True)
        return 0, 0, 0
        
    # Limpieza inicial de espacios en blanco
    df = strip_strings(df)
    
    # Asegura la existencia de todas las columnas necesarias (para consistencia)
    for c in ["fecha", "id_cliente", "id_producto", "unidades", "precio_unitario", "_ingest_ts", "_source_file", "_batch_id"]:
        if c not in df.columns:
            df[c] = None
            
    # 2. Coerción de tipos
    df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce").dt.date
    df["unidades"] = pd.to_numeric(df["unidades"], errors="coerce")
    df["precio_unitario"] = df["precio_unitario"].apply(to_float_money)
    
    # 3. Validación de Negocio
    valid = (
        pd.notna(df["fecha"])                         # La fecha debe ser válida
        & df["unidades"].notna() & (df["unidades"] >= 0)       # Unidades debe ser número >= 0
        & df["precio_unitario"].notna() & (df["precio_unitario"] >= 0) # Precio debe ser número >= 0
        & df["id_cliente"].fillna("").ne("")          # ID Cliente no puede ser nulo/vacío
        & df["id_producto"].fillna("").ne("")         # ID Producto no puede ser nulo/vacío
    )
    
    # Divide el DataFrame en cuarentena (inválidas) y limpio (válidas)
    quarantine = df.loc[~valid].copy()
    clean = df.loc[valid].copy()
    
    # 4. Persistencia de Cuarentena
    if not quarantine.empty:
        cols_src = ["fecha", "id_cliente", "id_producto", "unidades", "precio_unitario"]
        now = datetime.now(timezone.utc).isoformat()
        rows = []
        for _, r in quarantine.iterrows():
            # Serializa la fila original y registra la razón 'validation_failed'
            rows.append(("validation_failed", serialize_row_csv_like(r, cols_src), now, r.get("_source_file", ""), r.get("_batch_id", "")))
        append_quarantine(con, "ventas", rows)
        
    # 5. Persistencia de Datos Limpios (Clean)
    if not clean.empty:
        # Deduplicación: Mantiene la fila más reciente (por _ingest_ts) para la clave única
        clean = clean.sort_values("_ingest_ts").drop_duplicates(subset=["fecha", "id_cliente", "id_producto"], keep="last")
        
        # Escribe Parquet
        write_parquet(clean, PARQUET_DIR / "clean_ventas.parquet", "ventas")
        
        # Persiste en SQLite usando UPSERT
        for _, r in clean.iterrows():
            con.execute(
                upsert_sql,
                {
                    "fecha": str(r["fecha"]),
                    "idc": r["id_cliente"],
                    "idp": r["id_producto"],
                    "u": float(r["unidades"]),
                    "p": float(r["precio_unitario"]),
                    "ts": r["_ingest_ts"],
                },
            )
        con.commit()
        
    return raw_rows, len(clean), len(quarantine)

# Limpieza: Clientes
def clean_and_persist_clientes_from_raw(con: sqlite3.Connection, upsert_sql: str) -> tuple[int, int, int]:
    """
    Carga datos RAW de clientes, realiza limpieza/validación,
    persiste datos limpios en SQLite y Parquet, y registra inválidos en cuarentena.
    Retorna (filas_raw, filas_limpias, filas_cuarentena).
    """
    # 1. Cargar datos RAW
    df = pd.read_sql_query("SELECT * FROM raw_clientes", con)
    raw_rows = len(df)
    
    if df.empty:
        (QUALITY_DIR / "clientes_quarantine.csv").touch(exist_ok=True)
        return 0, 0, 0
        
    df = strip_strings(df)
    
    # Asegura la existencia de columnas
    for c in ["fecha", "nombre", "apellido", "id_cliente", "_ingest_ts", "_source_file", "_batch_id"]:
        if c not in df.columns:
            df[c] = None
            
    # 2. Validación de Negocio (usando la función validate_clientes)
    valid = validate_clientes(df)
    
    # Divide el DataFrame en cuarentena y limpio
    quarantine = df.loc[~valid].copy()
    clean = df.loc[valid].copy()
    
    # 3. Persistencia de Cuarentena
    if not quarantine.empty:
        cols_src = ["fecha", "nombre", "apellido", "id_cliente"]
        now = datetime.now(timezone.utc).isoformat()
        rows = []
        for _, r in quarantine.iterrows():
            rows.append(("validation_failed_clientes", serialize_row_csv_like(r, cols_src), now, r.get("_source_file", ""), r.get("_batch_id", "")))
        append_quarantine(con, "clientes", rows)
        
    # 4. Persistencia de Datos Limpios (Clean)
    if not clean.empty:
        # Deduplicación: Mantiene la fila más reciente (por _ingest_ts) para la clave única (id_cliente)
        clean = clean.sort_values("_ingest_ts").drop_duplicates(subset=["id_cliente"], keep="last")
        
        # Escribe Parquet, seleccionando solo las columnas de interés
        write_parquet(clean[["id_cliente", "nombre", "apellido", "fecha"]], PARQUET_DIR / "clean_clientes.parquet", "clientes")
        
        # Persiste en SQLite usando UPSERT
        for _, r in clean.iterrows():
            con.execute(
                upsert_sql,
                {
                    "fecha": str(r["fecha"]) if pd.notna(r["fecha"]) else None,
                    "nombre": r["nombre"],
                    "apellido": r["apellido"],
                    # Normaliza el ID del cliente antes de persistir
                    "idc": r["id_cliente"].upper().strip(), 
                    "ts": r["_ingest_ts"],
                },
            )
        con.commit()
        
    return raw_rows, len(clean), len(quarantine)

# Limpieza: Productos
def clean_and_persist_productos_from_raw(con: sqlite3.Connection, upsert_sql: str) -> tuple[int, int, int]:
    """
    Carga datos RAW de productos, realiza limpieza/validación,
    persiste datos limpios en SQLite y Parquet, y registra inválidos en cuarentena.
    Retorna (filas_raw, filas_limpias, filas_cuarentena).
    """
    # 1. Cargar datos RAW
    df = pd.read_sql_query("SELECT * FROM raw_productos", con)
    raw_rows = len(df)
    
    if df.empty:
        (QUALITY_DIR / "productos_quarantine.csv").touch(exist_ok=True)
        return 0, 0, 0
        
    df = strip_strings(df)
    
    # Asegura la existencia de columnas
    for c in ["fecha_entrada", "nombre_producto", "id_producto", "unidades", "precio_unitario", "categoria", "_ingest_ts", "_source_file", "_batch_id"]:
        if c not in df.columns:
            df[c] = None
            
    # 2. Coerción de tipos
    df["fecha_entrada"] = pd.to_datetime(df["fecha_entrada"], errors="coerce").dt.date
    df["unidades"] = pd.to_numeric(df["unidades"], errors="coerce")
    df["precio_unitario"] = df["precio_unitario"].apply(to_float_money)
    
    # 3. Validación de Negocio
    valid = (
        df["id_producto"].fillna("").ne("")                     # ID Producto no puede ser nulo/vacío
        & df["precio_unitario"].notna() & (df["precio_unitario"] >= 0) # Precio debe ser número >= 0
        & df["unidades"].notna() & (df["unidades"] >= 0)               # Unidades debe ser número >= 0
    )
    
    # Divide el DataFrame en cuarentena y limpio
    quarantine = df.loc[~valid].copy()
    clean = df.loc[valid].copy()
    
    # 4. Persistencia de Cuarentena
    if not quarantine.empty:
        cols_src = ["fecha_entrada", "nombre_producto", "id_producto", "unidades", "precio_unitario", "categoria"]
        now = datetime.now(timezone.utc).isoformat()
        rows = []
        for _, r in quarantine.iterrows():
            rows.append(("validation_failed", serialize_row_csv_like(r, cols_src), now, r.get("_source_file", ""), r.get("_batch_id", "")))
        append_quarantine(con, "productos", rows)
        
    # 5. Persistencia de Datos Limpios (Clean)
    if not clean.empty:
        # Deduplicación: Mantiene la fila más reciente (por _ingest_ts) para la clave única (id_producto)
        clean = clean.sort_values("_ingest_ts").drop_duplicates(subset=["id_producto"], keep="last")
        
        # Escribe Parquet, seleccionando las columnas de interés
        write_parquet(
            clean[["id_producto", "nombre_producto", "categoria", "precio_unitario", "unidades", "fecha_entrada"]],
            PARQUET_DIR / "clean_productos.parquet",
            "productos",
        )
        
        # Persiste en SQLite usando UPSERT
        for _, r in clean.iterrows():
            con.execute(
                upsert_sql,
                {
                    "fecha_entrada": str(r["fecha_entrada"]) if pd.notna(r["fecha_entrada"]) else None,
                    "nombre_producto": r["nombre_producto"],
                    "idp": r["id_producto"],
                    "u": float(r["unidades"]),
                    "p": float(r["precio_unitario"]),
                    "cat": r["categoria"],
                    "ts": r["_ingest_ts"],
                },
            )
        con.commit()
        
    return raw_rows, len(clean), len(quarantine)

# --- Punto de Entrada del Script ---

if __name__ == "__main__":
    # Establece la conexión con la base de datos SQLite
    con = sqlite3.connect(DB)
    try:
        print("DB path:", (OUT / "ut1.db").resolve())
        
        # 1) Esquema: Crea las tablas RAW, CLEAN y de CUARENTENA
        con.executescript((ROOT / "sql" / "00_schema.sql").read_text(encoding="utf-8"))
        con.commit()
        print("Tablas tras esquema:", con.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;").fetchall())

        # 2) Ingesta RAW + Cuarentena de parseo (filas mal formadas)
        counters = ingest_all_csvs_to_raw(con)
        con.commit()
        print("RAW counters:", counters)

        # 3) Cargar UPSERTs: Obtiene las sentencias SQL para la limpieza/persistencia
        upserts = load_upsert_sqls(ROOT / "sql" / "10_upserts.sql")

        # 4) Limpieza + Persistencia + Parquet + Cuarentena unificada (reglas de negocio)
        rv = clean_and_persist_ventas_from_raw(con, upserts["clean_ventas"])
        rc = clean_and_persist_clientes_from_raw(con, upserts["clean_clientes"])
        rp = clean_and_persist_productos_from_raw(con, upserts["clean_productos"])
        
        # Reporte de resultados
        print("Ventas (raw, clean, quar):", rv)
        print("Clientes (raw, clean, quar):", rc)
        print("Productos (raw, clean, quar):", rp)

        # 5) Vistas: Crea las vistas para el consumo de datos (ej: producto más vendido) oro
        con.executescript((ROOT / "sql" / "20_views.sql").read_text(encoding="utf-8"))
        con.commit()
        print("Vistas finales:", con.execute("SELECT name FROM sqlite_master WHERE type='view' ORDER BY name;").fetchall())
        
    finally:
        # Cierra la conexión de la base de datos
        con.close() 