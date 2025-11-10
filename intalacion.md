# Guía de instalación y publicación · Proyecto_UT1_RA1_BA

Esta guía explica cómo **clonar**, **ejecutar el pipeline** (ingesta → limpieza → Parquet/SQLite → reporte) y **publicar la web** con **Quartz 4** en **GitHub Pages**. Incluye solución a errores comunes del workflow.

---

## 0) Requisitos

- **Git** y cuenta de **GitHub**.
- **Python 3.11+**.
- **Node 22+** y **npm ≥ 10.9.2** (Quartz 4 lo exige).
  - Windows (nvm-windows):
    ```powershell
    nvm install 22
    nvm use 22
    npm i -g npm@^10.9.2
    ```
  - macOS/Linux (nvm):
    ```bash
    nvm install 22
    nvm use 22
    npm i -g npm@^10.9.2
    ```
- (Opcional) **Conda** si prefieres `environment.yml`.

---

## 1) Descargar el repositorio

```bash
git clone https://github.com/<TU_USUARIO>/Proyecto_UT1_RA1_BA.git
cd Proyecto_UT1_RA1_BA
```

> Si descargas el ZIP desde GitHub, simplemente descomprímelo y entra a la carpeta.

---

## 2) Preparar entorno Python y ejecutar el pipeline

### 2.1 Opción venv (recomendada si no usas conda)

```bash
# crear y activar entorno
python -m venv .venv
# Windows PowerShell
.\.venv\Scripts\Activate.ps1
# macOS/Linux
# source .venv/bin/activate

# dependencias
pip install -r project/requirements.txt

# generar datos de ejemplo y ejecutar pipeline
python project/ingest/get_data.py
python project/ingest/run.py
```

Se generan:
- `project/output/parquet/clean_ventas.parquet`
- `project/output/ut1.db`
- `project/output/reporte.md` (**reporte leído desde Parquet**)

### 2.2 Opción conda (alternativa)

```bash
conda env create -f project/environment.yml
conda activate ut1
python project/ingest/get_data.py
python project/ingest/run.py
```

---

## 3) Preparar la web (Quartz 4) en `site/`

Si ya tienes `site/` creado con Quartz, salta al paso 3.2.

### 3.1 Crear `site/` como proyecto Quartz

```bash
# desde la raíz del repo
rm -rf site             # (Windows: rmdir /s /q .\site)
npx create @jackyzha0/quartz site
cd site
npm i
```

Copia el contenido Markdown (si no estaba ya):
- `content/index.md`
- `content/metodologia.md`
- `content/reportes/reporte-UT1.md` (puedes copiar desde `project/output/reporte.md`).

### 3.2 Configurar la URL base

Edita `site/quartz.config.ts` y ajusta:

```ts
import { defineConfig } from "@jackyzha0/quartz"

export default defineConfig({
  site: {
    name: "Proyecto UT1 · RA1 · BA",
    baseUrl: "https://<TU_USUARIO>.github.io/Proyecto_UT1_RA1_BA", // <- ¡muy importante!
    description: "Ingesta · Almacenamiento · Reporte",
  },
})
```

### 3.3 Probar la web en local

```bash
npx quartz build --serve
# abre http://localhost:8080
```

---

## 4) Publicar en GitHub Pages (Actions)

### 4.1 Primer push

```bash
git add .
git commit -m "Proyecto UT1: pipeline + site Quartz"
git branch -M main
git remote add origin https://github.com/<TU_USUARIO>/Proyecto_UT1_RA1_BA.git
git push -u origin main
```

### 4.2 Activar Pages

En GitHub → **Settings → Pages** → **Build and deployment → Source = GitHub Actions**.

El repo debe contener `/.github/workflows/deploy-pages.yml`. Si no está, crea uno usando el YAML de abajo.

---

## 5) Workflow para Pages (elige una opción)

### 5.1 Opción simple (sin caché; no necesita `package-lock.json`)

```yaml
name: Deploy Quartz site

on:
  push:
    branches: [ "main" ]
  workflow_dispatch:

permissions:
  contents: read
  pages: write
  id-token: write

concurrency:
  group: "pages"
  cancel-in-progress: true

jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Setup Node 22
        uses: actions/setup-node@v4
        with:
          node-version: '22'    # Quartz 4 requiere Node >= 22

      - name: Upgrade npm
        run: npm i -g npm@^10.9.2

      - name: Install deps
        working-directory: site
        run: npm i              # sin 'ci' (no requiere lockfile)

      - name: Build Quartz
        working-directory: site
        run: npx quartz build

      - name: Upload artifact
        uses: actions/upload-pages-artifact@v3
        with:
          path: site/public

  deploy:
    needs: build
    runs-on: ubuntu-latest
    environment:
      name: github-pages
      url: ${{ steps.deployment.outputs.page_url }}
    steps:
      - id: deployment
        uses: actions/deploy-pages@v4
```

### 5.2 Opción con caché (requiere `site/package-lock.json`)

1) Genera y commitea el lockfile:
```bash
cd site
npm i
git add package-lock.json
git commit -m "Add lockfile for CI cache"
git push
```

2) Usa este YAML:

```yaml
- name: Setup Node 22
  uses: actions/setup-node@v4
  with:
    node-version: '22'
    cache: 'npm'
    cache-dependency-path: site/package-lock.json

- name: Upgrade npm
  run: npm i -g npm@^10.9.2

- name: Install deps
  working-directory: site
  run: npm ci
```

---

## 6) Verificar el despliegue

- GitHub → **Actions**: revisa el job “Deploy Quartz site” (debe estar en verde).
- URL final: `https://<TU_USUARIO>.github.io/Proyecto_UT1_RA1_BA`.
- Comprueba portada e informe **Reporte UT1** (`/reportes/reporte-UT1`).

---

## 7) Problemas comunes y soluciones

### A) `EBADENGINE` (motor no soportado)
- Quartz 4.5.x exige **Node ≥ 22** y **npm ≥ 10.9.2**.
- Solución:
  - Local: `nvm use 22 && npm i -g npm@^10.9.2`.
  - Actions: en el YAML, `node-version: '22'` y paso para subir `npm`.

### B) `Dependencies lock file is not found` en `setup-node`
- Sucede si usas **cache** o `npm ci` **sin** `package-lock.json`.
- Soluciones:
  - Opción simple: usar `npm i` y **no cachear** (workflow 5.1).
  - Opción con caché: generar `site/package-lock.json` y usar `cache-dependency-path` (workflow 5.2).

### C) `refusing to allow an OAuth App to create or update workflow ... without 'workflow' scope`
- Tu push usa credenciales sin permiso `workflow`.
- Soluciones:
  - `gh auth login` → acepta scopes `repo` y `workflow`.
  - O usa un **PAT (classic)** con `repo` + `workflow`.
  - O sube primero sin el workflow y créalo desde la UI de GitHub.

### D) `npm ERR! 404 @jackyzha0/quartz`
- Quartz 4 **no** se instala con `npm i @jackyzha0/quartz`. Debes **crear** el proyecto:
  ```bash
  npx create @jackyzha0/quartz site
  ```

### E) Web sin estilos o rutas rotas
- `baseUrl` mal en `quartz.config.ts`. Asegúrate de:
  ```
  https://<TU_USUARIO>.github.io/Proyecto_UT1_RA1_BA
  ```

### F) Repo privado y Pages
- Con **GitHub Free**, Pages necesita repos **públicos**.
- Con **Pro/Team**, puedes usar repo privado y poner Pages **Private** (solo miembros).

### G) Windows: no se activa el venv
- PowerShell puede bloquear scripts. Ejecuta una vez como admin:
  ```powershell
  Set-ExecutionPolicy -Scope CurrentUser RemoteSigned
  ```

---

## 8) Flujo de trabajo para alumnos (día a día)

1) Añadir/actualizar ficheros a `project/data/drops/`.
2) Ejecutar pipeline:
   ```bash
   python project/ingest/run.py
   python project/tools/copy_report_to_site.py  # si quieres publicar el reporte en la web
   ```
3) Probar la web:
   ```bash
   cd site
   npx quartz build --serve
   ```
4) Subir cambios:
   ```bash
   git add .
   git commit -m "Nuevos datos y reporte"
   git push
   ```

¡Listo! Con esto tienes el entorno reproducible, el pipeline técnico y la publicación en Pages funcionando.
