# Proyecto_UT1_RA1_BA · Solución de ingestión, almacenamiento y reporte (UT1 · RA1)

Este repositorio contiene:
- **project/**: código reproducible (ingesta → clean → oro → reporte Markdown).
- **site/**: web pública con **Quartz 4** (GitHub Pages). El reporte UT1 se publica en `site/content/reportes/`.

## Ejecución rápida
```bash
# 1) Dependencias (elige uno)
python -m venv .venv
.venv\Scripts\activate  # (o source .venv/bin/activate)
pip install -r project/requirements.txt
# o: conda env create -f project/environment.yml && conda activate ut1

# 2) (Opcional) Generar datos de ejemplo
python project/ingest/get_data.py

# 3) Pipeline fin-a-fin (ingesta→clean→oro→reporte.md)
python project/ingest/run.py

# 4) Copiar el reporte a la web Quartz
python project/tools/copy_report_to_site.py

# 5) (Opcional) Previsualizar la web en local
cd site
npx quartz build --serve   # abre http://localhost:8080
```

## Publicación web (GitHub Pages)
- En **Settings → Pages**, selecciona **Source = GitHub Actions**.
- El workflow `./.github/workflows/deploy-pages.yml` compila `site/` y despliega.

## Flujo de datos
Bronce (`raw`) → Plata (`clean`) → Oro (`analytics`).  
Idempotencia por `batch_id` (batch) o `event_id` (stream).  
Deduplicación “último gana” por `_ingest_ts`.  
Reporte Markdown: `project/output/reporte.md` → `site/content/reportes/reporte-UT1.md`.
# BDA_Proyecto_UT1_RA1


