
#!/usr/bin/env python3

"""
sync_docs_to_site.py — Copia /docs → /site/content/docs para publicarlos en Quartz.

Uso:
  python project/tools/sync_docs_to_site.py            # copia todo
  python project/tools/sync_docs_to_site.py --dry-run  # simula
  python project/tools/sync_docs_to_site.py --clean    # borra destino antes de copiar
  python project/tools/sync_docs_to_site.py --only 10-diseno-ingesta.md 20-limpieza-calidad.md

Notas:
- No modifica tus ficheros; solo copia.
- Si un .md no tiene frontmatter, te avisará (Quartz funciona igual, pero es recomendable).
"""
from __future__ import annotations
import argparse
import shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "project" / "docs"
DST = ROOT / "site" / "content" / "docs"

IGNORE_EXT = {".zip", ".rar", ".7z", ".pdf", ".png", ".jpg", ".jpeg", ".gif"}
IGNORE_FILES = {".DS_Store", "Thumbs.db"}

def has_frontmatter(md_path: Path) -> bool:
    try:
        head = md_path.read_text(encoding="utf-8").lstrip()
    except Exception:
        return False
    return head.startswith("---\n") or head.startswith("---\r\n")

def ensure_dst():
    DST.mkdir(parents=True, exist_ok=True)

def list_sources(only: list[str] | None) -> list[Path]:
    if only:
        paths = []
        for name in only:
            p = SRC / name
            if p.exists():
                paths.append(p)
            else:
                print(f"[WARN] No existe en /docs: {name}")
        return paths
    return sorted([p for p in SRC.rglob("*") if p.is_file() and p.name not in IGNORE_FILES and p.suffix.lower() not in IGNORE_EXT])

def copy_file(src: Path, dst: Path, dry: bool):
    dst.parent.mkdir(parents=True, exist_ok=True)
    if dry:
        print(f"[DRY] Copiar {src.relative_to(SRC)} -> {dst.relative_to(DST)}")
        return
    shutil.copy2(src, dst)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="Simula la copia sin escribir")
    ap.add_argument("--clean", action="store_true", help="Borra el destino antes de copiar")
    ap.add_argument("--only", nargs="*", help="Lista de archivos dentro de /docs a copiar")
    args = ap.parse_args()

    if not SRC.exists():
        print(f"[ERROR] No existe el directorio de origen: {SRC}")
        return 1

    ensure_dst()

    if args.clean and not args.dry_run:
        print(f"[INFO] Limpiando destino: {DST}")
        shutil.rmtree(DST, ignore_errors=True)
        ensure_dst()

    sources = list_sources(args.only)
    if not sources:
        print("[INFO] No hay archivos que copiar.")
        return 0

    missing_front = []
    for s in sources:
        rel = s.relative_to(SRC)
        d = DST / rel
        if s.suffix.lower() == ".md" and not has_frontmatter(s):
            missing_front.append(str(rel))
        copy_file(s, d, args.dry_run)

    print(f"[OK] {'Simulación completada' if args.dry_run else 'Copia completada'}: {len(sources)} archivo(s).")
    if missing_front:
        print("\n[AVISO] Algunos .md no tienen frontmatter YAML (recomendado en Quartz):")
        for f in missing_front:
            print(" -", f)
        print("Sugerencia: añade al principio del archivo, por ejemplo:\n"
              '---\n'
              'title: "Título del documento"\n'
              'tags: ["UT1","docs"]\n'
              '---\n')

    print(f"\n[Destino] {DST}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
