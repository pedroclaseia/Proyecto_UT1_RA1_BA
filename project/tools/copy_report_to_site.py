from pathlib import Path
src = Path(__file__).resolve().parents[1] / "output" / "reporte.md"
dst = Path(__file__).resolve().parents[2] / "site" / "content" / "reportes" / "reporte-UT1.md"
dst.parent.mkdir(parents=True, exist_ok=True)
dst.write_text(src.read_text(encoding="utf-8"), encoding="utf-8")
print("Copiado:", dst)

