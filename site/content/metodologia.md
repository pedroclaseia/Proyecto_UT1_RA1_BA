# Metodología (bronce → plata → oro)

- **Bronce (raw):** datos tal cual + `_source_file`, `_ingest_ts`, `batch_id`.
- **Plata (clean):** coerción de tipos, rangos/dominos, dedupe “último gana”.
- **Oro (analytics):** derivados (`importe`) y agregaciones (por día/producto).

**Idempotencia:** reprocesar no duplica (UPSERT/dedupe por clave natural + `_ingest_ts`).  
**Reporte:** KPIs y tablas (sin gráficos en UT1).
