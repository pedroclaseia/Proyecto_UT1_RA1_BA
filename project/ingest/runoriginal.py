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
        df.to_parquet(path, index=False)
        print(f"Parquet escrito: {path.name} ({len(df)} filas) para {label}")
    except ImportError as e:
        print(f"[AVISO] No se pudo escribir {path.name} (instala 'pyarrow' o 'fastparquet'): {e}")

def classify_file(fname: str) -> str | None:
    n = fname.lower()
    if any(k in n for k in ["ventas", "venta"]):
        return "ventas"
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
    counters = {"ventas": 0, "productos": 0}
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

# Carga de UPSERTs desde sql/10_upserts.sql (ventas/productos)
def load_upsert_sqls(path: Path) -> dict[str, str]:
    raw = path.read_text(encoding="utf-8").replace("\ufeff", "")
    lines = []
    for line in raw.splitlines():
        line = line.split("--", 1)[0]
        if line.strip():
            lines.append(line)
    txt = "\n".join(lines)

    def extract_one(table: str) -> str:
        m = re.search(rf"(?is)\binsert\s+into\s+{table}\b", txt)
        if not m:
            raise ValueError(f"No se encontró INSERT INTO {table} en {path.name}")
        after = txt[m.start():]
        semi = after.find(";")
        if semi == -1:
            raise ValueError(f"La sentencia INSERT de {table} no termina en ';' en {path.name}")
        stmt = after[:semi].strip()
        if "values" not in stmt.lower() or "on conflict" not in stmt.lower():
            raise ValueError(f"INSERT de {table} incompleto en {path.name}")
        if "*" in stmt:
            raise ValueError(f"INSERT de {table} contiene '*', revisa {path.name}")
        return stmt

    # Se requiere al menos ventas y productos para el DWH mínimo
    result = {"clean_ventas": extract_one("clean_ventas"),
              "clean_productos": extract_one("clean_productos")}
    return result

# Limpieza PRODUCTOS -> clean_productos y dim_productos
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
            rows.append(("validation_failed", ",".join(str(r[c]) if pd.notna(r[c]) else "" for c in cols_src),
                         now, r.get("_source_file", ""), r.get("_batch_id", "")))
        append_quarantine(con, "productos", rows)

    # last-wins sobre id_producto y persistencia
    if not clean.empty:
        clean = clean.sort_values("_ingest_ts").drop_duplicates(subset=["id_producto"], keep="last")

        # Parquet de dimensión
        write_parquet(
            clean[["id_producto", "nombre_producto", "categoria", "precio_unitario", "unidades", "fecha_entrada"]],
            PARQUET_DIR / "dim_productos.parquet",
            "dim_productos",
        )

        # Tabla de dimensión en SQLite
        clean[["id_producto", "nombre_producto", "categoria", "precio_unitario", "unidades", "fecha_entrada"]].to_sql(
            "dim_productos", con, if_exists="replace", index=False
        )

        # Upsert a clean_productos (catálogo limpio)
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

# Limpieza VENTAS -> clean_ventas y fact_ventas (validando id_producto en catálogo)
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

    # Reglas base
    base_ok = (
        pd.notna(df["fecha"])
        & df["unidades"].notna() & (df["unidades"] >= 0)
        & df["precio_unitario"].notna() & (df["precio_unitario"] >= 0)
        & df["id_producto"].fillna("").ne("")
    )

    quarantine_rows = []

    # Valida contra catálogo (clean_productos) antes de dedupe
    cat = pd.read_sql_query("SELECT id_producto FROM clean_productos", con)
    cat_set = set(cat["id_producto"].astype(str))

    fk_ok = df["id_producto"].astype(str).isin(cat_set)
    valid = base_ok & fk_ok

    # Cuarentena: FK inválida o validación base
    now = datetime.now(timezone.utc).isoformat()
    cols_src = ["fecha", "id_cliente", "id_producto", "unidades", "precio_unitario"]
    for _, r in df.loc[~valid].iterrows():
        reason = "foreign_key_violation_product" if (base_ok.loc[r.name] and not fk_ok.loc[r.name]) else "validation_failed"
        row_text = ",".join(str(r[c]) if pd.notna(r[c]) else "" for c in cols_src)
        quarantine_rows.append((reason, row_text, now, r.get("_source_file", ""), r.get("_batch_id", "")))
    append_quarantine(con, "ventas", quarantine_rows)

    clean = df.loc[valid].copy()

    if not clean.empty:
        # last-wins por (fecha, id_cliente, id_producto)
        clean = clean.sort_values("_ingest_ts").drop_duplicates(
            subset=["fecha", "id_cliente", "id_producto"], keep="last"
        )

        # Persistir clean_ventas con upsert
        for _, r in clean.iterrows():
            con.execute(
                upsert_sql,
                {
                    "fecha": str(r["fecha"]),
                    "idc": r.get("id_cliente", ""),
                    "idp": r["id_producto"],
                    "u": float(r["unidades"]),
                    "p": float(r["precio_unitario"]),
                    "ts": r["_ingest_ts"],
                },
            )
        con.commit()

        # Construir fact_ventas (fecha, id_producto, id_cliente, unidades, precio_unitario, importe)
        fact = clean.copy()
        fact["importe"] = fact["unidades"] * fact["precio_unitario"]
        write_parquet(
            fact[["fecha", "id_producto", "id_cliente", "unidades", "precio_unitario", "importe"]],
            PARQUET_DIR / "fact_ventas.parquet",
            "fact_ventas",
        )
        #Esto lo podriamos poner en el sql de upsert pero por claridad lo dejamos aqui.
        # Asegurar tabla fact_ventas y upsert-like por PK compuesta
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
        # last-wins: reemplazo por conflicto de PK
        con.executemany("""
            INSERT INTO fact_ventas(fecha, id_producto, id_cliente, unidades, precio_unitario, importe, _ingest_ts)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(fecha, id_producto, id_cliente) DO UPDATE SET
              unidades=excluded.unidades,
              precio_unitario=excluded.precio_unitario,
              importe=excluded.importe,
              _ingest_ts=excluded._ingest_ts;
        """, [
            (str(r["fecha"]), r["id_producto"], r.get("id_cliente", ""), float(r["unidades"]),
             float(r["precio_unitario"]), float(r["unidades"]*r["precio_unitario"]), r["_ingest_ts"])
            for _, r in fact.iterrows()
        ])
        con.commit()

    return raw_rows, len(clean), len(df) - len(clean)

if __name__ == "__main__":
    con = sqlite3.connect(DB)
    try:
        print("DB path:", (OUT / "ut1.db").resolve())

        # 1) Esquema base (raw/clean/quarantine) desde archivo
        con.executescript((ROOT / "sql" / "00_schema.sql").read_text(encoding="utf-8"))
        con.commit()

        # 2) Ingesta a RAW con cuarentena unificada de parseo
        counters = ingest_all_csvs_to_raw(con)
        con.commit()
        print("RAW counters:", counters)

        # 3) Cargar UPSERTs para ventas y productos
        upserts = load_upsert_sqls(ROOT / "sql" / "10_upserts.sql")

        # 4) Limpiar y persistir catálogo (dim + clean_productos)
        rp = clean_and_persist_productos_from_raw(con, upserts["clean_productos"])
        print("Productos (raw, clean, quar):", rp)

        # 5) Limpiar y persistir ventas (clean_ventas + fact_ventas) validando id_producto
        rv = clean_and_persist_ventas_from_raw(con, upserts["clean_ventas"])
        print("Ventas (raw, clean, quar):", rv)

        # 6) Vistas de reporte/KPI y oro
        con.executescript((ROOT / "sql" / "20_views.sql").read_text(encoding="utf-8"))
        con.commit()
        print("Vistas finales:", con.execute("SELECT name FROM sqlite_master WHERE type='view' ORDER BY name;").fetchall())

    finally:
        con.close()
