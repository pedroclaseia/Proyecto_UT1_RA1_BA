from pathlib import Path
from datetime import datetime, timezone
import pandas as pd
import sqlite3
import re
from io import StringIO

# Rutas base
ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data" / "drops"
OUT = ROOT / "output"
PARQUET_DIR = OUT / "parquet"
QUALITY_DIR = OUT / "quality"
OUT.mkdir(parents=True, exist_ok=True)
PARQUET_DIR.mkdir(parents=True, exist_ok=True)
QUALITY_DIR.mkdir(parents=True, exist_ok=True)
DB = OUT / "ut1.db"

# Utilidades
def to_float_money(x):
    try:
        return float(str(x).replace(",", "."))
    except Exception:
        return None

def strip_strings(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = df.columns.str.strip()
    for c in df.columns:
        if pd.api.types.is_object_dtype(df[c]):
            df[c] = df[c].astype(str).str.strip()
    return df

def write_parquet(df: pd.DataFrame, path: Path, label: str):
    try:
        df.to_parquet(path, index=False)  # pyarrow/fastparquet
        print(f"Parquet escrito: {path.name} ({len(df)} filas) para {label}")
    except ImportError as e:
        print(f"[AVISO] No se pudo escribir {path.name} (instala 'pyarrow' o 'fastparquet'): {e}")

def classify_file(fname: str) -> str | None:
    n = fname.lower()
    if any(k in n for k in ["ventas", "venta"]):
        return "ventas"
    if any(k in n for k in ["clientes", "cliente"]):
        return "clientes"
    if any(k in n for k in ["productos", "producto"]):
        return "productos"
    return None

# Cuarentena unificada (malformadas + inválidas) por dominio
def append_quarantine(con: sqlite3.Connection, kind: str, reasons_rows: list[tuple[str, str, str, str, str]]):
    if not reasons_rows:
        return
    dfq = pd.DataFrame(reasons_rows, columns=["_reason", "_row", "_ingest_ts", "_source_file", "_batch_id"])
    table = f"quarantine_{kind}"
    dfq.to_sql(table, con, if_exists="append", index=False)
    out_csv = QUALITY_DIR / f"{kind}_quarantine.csv"
    mode = "a" if out_csv.exists() else "w"
    dfq.to_csv(out_csv, index=False, mode=mode, header=not out_csv.exists())

# Detección de líneas mal formadas por conteo de separadores
def split_good_bad_lines(f: Path) -> tuple[list[str], list[str]]:
    raw = f.read_text(encoding="utf-8").splitlines()
    if not raw:
        return [], []
    header = raw[0]
    expected_cols = header.count(",") + 1
    good = [header]
    bad = []
    for line in raw[1:]:
        cols = line.count(",") + 1
        if cols == expected_cols and line.strip():
            good.append(line)
        else:
            bad.append(line)
    return good, bad

# Ingesta robusta por fichero con cuarentena de parseo
def ingest_one(f: Path, con: sqlite3.Connection, kind: str) -> pd.DataFrame:
    batch_id = f.stem.lower()
    good_lines, bad_lines = split_good_bad_lines(f)
    if bad_lines:
        now = datetime.now(timezone.utc).isoformat()
        rows = [("parse_error_bad_field_count", bl, now, f.name, batch_id) for bl in bad_lines]
        append_quarantine(con, kind, rows)
    if len(good_lines) <= 1:
        return pd.DataFrame()
    buf = StringIO("\n".join(good_lines))
    df = pd.read_csv(buf, dtype=str, engine="python", on_bad_lines="skip")
    df = strip_strings(df)
    if kind == "ventas" and "fecha_venta" in df.columns:
        df = df.rename(columns={"fecha_venta": "fecha"})
    df["_source_file"] = f.name
    df["_ingest_ts"] = datetime.now(timezone.utc).isoformat()
    df["_batch_id"] = batch_id
    return df

def ingest_all_csvs_to_raw(con: sqlite3.Connection) -> dict:
    counters = {"ventas": 0, "clientes": 0, "productos": 0}
    detected = sorted(DATA.glob("*.csv"))
    print("CSV detectados:", [p.name for p in detected])

    for f in detected:
        kind = classify_file(f.name)
        if not kind:
            print("Ignorado (sin match):", f.name)
            continue

        df = ingest_one(f, con, kind)

        if kind == "ventas":
            needed = ["fecha", "id_cliente", "id_producto", "unidades", "precio_unitario",
                      "_ingest_ts", "_source_file", "_batch_id"]
            for c in needed:
                if c not in df.columns:
                    df[c] = None
            df_raw = df[needed].copy()
            if not df_raw.empty:
                df_raw.to_sql("raw_ventas", con, if_exists="append", index=False)
                counters["ventas"] += len(df_raw)

        elif kind == "clientes":
            cols = ["fecha", "nombre", "apellido", "id_cliente"]
            for c in cols:
                if c not in df.columns:
                    df[c] = None
            df_raw = df[cols + ["_ingest_ts", "_source_file", "_batch_id"]].copy()
            if not df_raw.empty:
                df_raw.to_sql("raw_clientes", con, if_exists="append", index=False)
                counters["clientes"] += len(df_raw)

        elif kind == "productos":
            cols = ["fecha_entrada", "nombre_producto", "id_producto", "unidades",
                    "precio_unitario", "categoria"]
            for c in cols:
                if c not in df.columns:
                    df[c] = None
            df_raw = df[cols + ["_ingest_ts", "_source_file", "_batch_id"]].copy()
            if not df_raw.empty:
                df_raw.to_sql("raw_productos", con, if_exists="append", index=False)
                counters["productos"] += len(df_raw)

    return counters

# Carga de UPSERTs desde sql/10_upserts.sql (con fallback)
def load_upsert_sqls(path: Path) -> dict[str, str]:
    raw = path.read_text(encoding="utf-8").replace("\ufeff", "")
    lines = []
    for line in raw.splitlines():
        line = line.split("--", 1)[0]
        if line.strip():
            lines.append(line)
    txt = "\n".join(lines)

    def extract_one(table: str) -> str | None:
        m = re.search(rf"(?is)\binsert\s+into\s+{table}\b", txt)
        if not m:
            return None
        after = txt[m.start():]
        semi = after.find(";")
        if semi == -1:
            return None
        stmt = after[:semi].strip()
        if "values" not in stmt.lower() or "on conflict" not in stmt.lower():
            return None
        if "*" in stmt:
            return None
        return stmt

    up = {}
    # Ventas
    s = extract_one("clean_ventas")
    if s is None:
        raise ValueError("Falta UPSERT de clean_ventas en 10_upserts.sql")
    up["clean_ventas"] = s
    # Clientes (fallback si no está en archivo)
    s = extract_one("clean_clientes")
    if s is None:
        up["clean_clientes"] = (
            "INSERT INTO clean_clientes (fecha, nombre, apellido, id_cliente, _ingest_ts) "
            "VALUES (:fecha, :nombre, :apellido, :idc, :ts) "
            "ON CONFLICT(id_cliente) DO UPDATE SET "
            "fecha=excluded.fecha, nombre=excluded.nombre, apellido=excluded.apellido, _ingest_ts=excluded._ingest_ts "
            "WHERE excluded._ingest_ts > clean_clientes._ingest_ts"
        )
    else:
        up["clean_clientes"] = s
    # Productos (fallback si no está en archivo)
    s = extract_one("clean_productos")
    if s is None:
        up["clean_productos"] = (
            "INSERT INTO clean_productos (fecha_entrada, nombre_producto, id_producto, unidades, precio_unitario, categoria, _ingest_ts) "
            "VALUES (:fecha_entrada, :nombre_producto, :idp, :u, :p, :cat, :ts) "
            "ON CONFLICT(id_producto) DO UPDATE SET "
            "fecha_entrada=excluded.fecha_entrada, nombre_producto=excluded.nombre_producto, unidades=excluded.unidades, "
            "precio_unitario=excluded.precio_unitario, categoria=excluded.categoria, _ingest_ts=excluded._ingest_ts "
            "WHERE excluded._ingest_ts > clean_productos._ingest_ts"
        )
    else:
        up["clean_productos"] = s
    return up

# Validaciones de clientes
NAME_RE = re.compile(r"^[A-Za-zÁÉÍÓÚÜÑáéíóúüñ\s'-]+$")
def validate_clientes(df: pd.DataFrame) -> pd.Series:
    fecha_ok = pd.to_datetime(df["fecha"], errors="coerce").notna()
    nombre_ok = df["nombre"].fillna("").str.len().gt(0) & df["nombre"].fillna("").str.match(NAME_RE)
    apellido_ok = df["apellido"].fillna("").str.len().gt(0) & df["apellido"].fillna("").str.match(NAME_RE)
    id_norm = df["id_cliente"].fillna("").str.upper().str.strip()
    id_ok = id_norm.str.match(r"^C\d{3}$")
    return fecha_ok & nombre_ok & apellido_ok & id_ok

def serialize_row_csv_like(row: pd.Series, cols: list[str]) -> str:
    values = []
    for c in cols:
        v = row.get(c, "")
        if v is None:
            v = ""
        s = str(v)
        if "," in s or '"' in s:
            s = '"' + s.replace('"', '""') + '"'
        values.append(s)
    return ",".join(values)

# Limpieza: PRODUCTOS -> clean_productos y dim_productos
def clean_and_persist_productos_from_raw(con: sqlite3.Connection, upsert_sql: str) -> tuple[int, int, int]:
    df = pd.read_sql_query("SELECT * FROM raw_productos", con)
    raw_rows = len(df)
    if df.empty:
        (QUALITY_DIR / "productos_quarantine.csv").touch(exist_ok=True)
        return 0, 0, 0

    df = strip_strings(df)
    for c in ["fecha_entrada", "nombre_producto", "id_producto", "unidades", "precio_unitario",
              "categoria", "_ingest_ts", "_source_file", "_batch_id"]:
        if c not in df.columns:
            df[c] = None

    df["fecha_entrada"] = pd.to_datetime(df["fecha_entrada"], errors="coerce").dt.date
    df["unidades"] = pd.to_numeric(df["unidades"], errors="coerce")
    df["precio_unitario"] = df["precio_unitario"].apply(to_float_money)

    valid = (
        df["id_producto"].fillna("").ne("")
        & df["precio_unitario"].notna() & (df["precio_unitario"] >= 0)
        & df["unidades"].notna() & (df["unidades"] >= 0)
    )
    quarantine = df.loc[~valid].copy()
    clean = df.loc[valid].copy()

    if not quarantine.empty:
        cols_src = ["fecha_entrada", "nombre_producto", "id_producto", "unidades", "precio_unitario", "categoria"]
        now = datetime.now(timezone.utc).isoformat()
        rows = []
        for _, r in quarantine.iterrows():
            rows.append(("validation_failed", serialize_row_csv_like(r, cols_src), now, r.get("_source_file", ""), r.get("_batch_id", "")))
        append_quarantine(con, "productos", rows)

    if not clean.empty:
        clean = clean.sort_values("_ingest_ts").drop_duplicates(subset=["id_producto"], keep="last")

        # Parquet + tabla de dimensión
        dimp = clean[["id_producto", "nombre_producto", "categoria", "precio_unitario", "unidades", "fecha_entrada"]].copy()
        write_parquet(dimp, PARQUET_DIR / "dim_productos.parquet", "dim_productos")
        dimp.to_sql("dim_productos", con, if_exists="replace", index=False)

        # Upsert catálogo limpio
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

# Limpieza: CLIENTES -> clean_clientes y dim_clientes
def clean_and_persist_clientes_from_raw(con: sqlite3.Connection, upsert_sql: str) -> tuple[int, int, int]:
    df = pd.read_sql_query("SELECT * FROM raw_clientes", con)
    raw_rows = len(df)
    if df.empty:
        (QUALITY_DIR / "clientes_quarantine.csv").touch(exist_ok=True)
        return 0, 0, 0

    df = strip_strings(df)
    for c in ["fecha", "nombre", "apellido", "id_cliente", "_ingest_ts", "_source_file", "_batch_id"]:
        if c not in df.columns:
            df[c] = None

    valid = validate_clientes(df)
    quarantine = df.loc[~valid].copy()
    clean = df.loc[valid].copy()

    if not quarantine.empty:
        cols_src = ["fecha", "nombre", "apellido", "id_cliente"]
        now = datetime.now(timezone.utc).isoformat()
        rows = []
        for _, r in quarantine.iterrows():
            rows.append(("validation_failed_clientes", serialize_row_csv_like(r, cols_src), now, r.get("_source_file", ""), r.get("_batch_id", "")))
        append_quarantine(con, "clientes", rows)

    if not clean.empty:
        clean = clean.sort_values("_ingest_ts").drop_duplicates(subset=["id_cliente"], keep="last")

        # Parquet + tabla de dimensión
        dimc = clean[["id_cliente", "nombre", "apellido", "fecha"]].copy()
        write_parquet(dimc, PARQUET_DIR / "dim_clientes.parquet", "dim_clientes")
        dimc.to_sql("dim_clientes", con, if_exists="replace", index=False)

        # Upsert clean_clientes
        for _, r in clean.iterrows():
            con.execute(
                upsert_sql,
                {
                    "fecha": str(r["fecha"]) if pd.notna(r["fecha"]) else None,
                    "nombre": r["nombre"],
                    "apellido": r["apellido"],
                    "idc": r["id_cliente"].upper().strip(),
                    "ts": r["_ingest_ts"],
                },
            )
        con.commit()

    return raw_rows, len(clean), len(quarantine)

# Limpieza: VENTAS -> clean_ventas y fact_ventas validando id_producto e id_cliente
def clean_and_persist_ventas_from_raw(con: sqlite3.Connection, upsert_sql: str) -> tuple[int, int, int]:
    df = pd.read_sql_query("SELECT * FROM raw_ventas", con)
    raw_rows = len(df)
    if df.empty:
        (QUALITY_DIR / "ventas_quarantine.csv").touch(exist_ok=True)
        return 0, 0, 0

    df = strip_strings(df)
    for c in ["fecha", "id_cliente", "id_producto", "unidades", "precio_unitario",
              "_ingest_ts", "_source_file", "_batch_id"]:
        if c not in df.columns:
            df[c] = None

    df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce").dt.date
    df["unidades"] = pd.to_numeric(df["unidades"], errors="coerce")
    df["precio_unitario"] = df["precio_unitario"].apply(to_float_money)

    base_ok = (
        pd.notna(df["fecha"])
        & df["unidades"].notna() & (df["unidades"] >= 0)
        & df["precio_unitario"].notna() & (df["precio_unitario"] >= 0)
        & df["id_producto"].fillna("").ne("")
        & df["id_cliente"].fillna("").ne("")
    )

    # Valida FK contra catálogo y clientes limpios
    cat_prod = set(pd.read_sql_query("SELECT id_producto FROM clean_productos", con)["id_producto"].astype(str))
    cat_cli = set(pd.read_sql_query("SELECT id_cliente FROM clean_clientes", con)["id_cliente"].astype(str))
    fk_prod_ok = df["id_producto"].astype(str).isin(cat_prod)
    fk_cli_ok = df["id_cliente"].astype(str).isin(cat_cli)

    valid = base_ok & fk_prod_ok & fk_cli_ok

    # Cuarentena con motivos específicos
    now = datetime.now(timezone.utc).isoformat()
    cols_src = ["fecha", "id_cliente", "id_producto", "unidades", "precio_unitario"]
    rows = []
    for idx, r in df.loc[~valid].iterrows():
        if base_ok.loc[idx] and not fk_prod_ok.loc[idx]:
            reason = "foreign_key_violation_product"
        elif base_ok.loc[idx] and not fk_cli_ok.loc[idx]:
            reason = "foreign_key_violation_client"
        else:
            reason = "validation_failed"
        row_text = serialize_row_csv_like(r, cols_src)
        rows.append((reason, row_text, now, r.get("_source_file", ""), r.get("_batch_id", "")))
    append_quarantine(con, "ventas", rows)

    clean = df.loc[valid].copy()

    if not clean.empty:
        clean = clean.sort_values("_ingest_ts").drop_duplicates(
            subset=["fecha", "id_cliente", "id_producto"], keep="last"
        )

        # Upsert a clean_ventas
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

        # fact_ventas
        fact = clean.copy()
        fact["importe"] = fact["unidades"] * fact["precio_unitario"]
        write_parquet(
            fact[["fecha", "id_producto", "id_cliente", "unidades", "precio_unitario", "importe"]],
            PARQUET_DIR / "fact_ventas.parquet",
            "fact_ventas",
        )
        con.execute("""
            CREATE TABLE IF NOT EXISTS fact_ventas(
              fecha TEXT,
              id_producto TEXT,
              id_cliente TEXT,
              unidades REAL,
              precio_unitario REAL,
              importe REAL,
              _ingest_ts TEXT,
              PRIMARY KEY (fecha, id_producto, id_cliente)
            );
        """)
        con.executemany("""
            INSERT INTO fact_ventas(fecha, id_producto, id_cliente, unidades, precio_unitario, importe, _ingest_ts)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(fecha, id_producto, id_cliente) DO UPDATE SET
              unidades=excluded.unidades,
              precio_unitario=excluded.precio_unitario,
              importe=excluded.importe,
              _ingest_ts=excluded._ingest_ts;
        """, [
            (str(r["fecha"]), r["id_producto"], r["id_cliente"], float(r["unidades"]),
             float(r["precio_unitario"]), float(r["unidades"]*r["precio_unitario"]), r["_ingest_ts"])
            for _, r in fact.iterrows()
        ])
        con.commit()

    return raw_rows, len(clean), len(df) - len(clean)

if __name__ == "__main__":
    con = sqlite3.connect(DB)
    try:
        print("DB path:", (OUT / "ut1.db").resolve())
        # 1) Esquema base (raw/clean/quarantine)
        con.executescript((ROOT / "sql" / "00_schema.sql").read_text(encoding="utf-8"))
        con.commit()

        # 2) Ingesta RAW con cuarentena unificada de parseo
        counters = ingest_all_csvs_to_raw(con)
        con.commit()
        print("RAW counters:", counters)

        # 3) UPSERTs (ventas, clientes, productos) con fallback si faltan en archivo
        upserts = load_upsert_sqls(ROOT / "sql" / "10_upserts.sql")

        # 4) Limpiar catálogo/productos primero (dim + clean_productos)
        rp = clean_and_persist_productos_from_raw(con, upserts["clean_productos"])
        print("Productos (raw, clean, quar):", rp)

        # 5) Limpiar clientes (dim + clean_clientes)
        rc = clean_and_persist_clientes_from_raw(con, upserts["clean_clientes"])
        print("Clientes (raw, clean, quar):", rc)

        # 6) Limpiar ventas validando FK a producto y cliente (clean_ventas + fact_ventas)
        rv = clean_and_persist_ventas_from_raw(con, upserts["clean_ventas"])
        print("Ventas (raw, clean, quar):", rv)

        # 7) Vistas (oro + clientes)
        con.executescript((ROOT / "sql" / "20_views.sql").read_text(encoding="utf-8"))
        con.commit()
        print("Vistas finales:", con.execute("SELECT name FROM sqlite_master WHERE type='view' ORDER BY name;").fetchall())

    finally:
        con.close()
