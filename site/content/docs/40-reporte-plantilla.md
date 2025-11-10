# Plantilla de Reporte (Resumen Ejecutivo)

> **Titular**: **Ingresos totales de €59.7K** concentrados en un solo día de ventas, con **P005** (Televisor Sony 55") y **P025** (Herramienta 25) como líderes por importe. Enfocarse en la validación de fechas de entrada/salida.

---

## 1) Métricas clave (Período Total Analizado)
- **Ingresos**: **€59,715**
- **Ticket medio**: **€501.81**
- **Transacciones**: **119**

---

## 2) Contribución por producto (Top 3 por Importe de Venta)

| Producto | Importe | % del Total |
| :--- | :--- | :--- |
| **P005** (Televisor Sony 55") | €3,500 | 5.86% |
| **P025** (Herramienta 25) | €3,500 | 5.86% |
| **P085** (Equipo 85) | €3,500 | 5.86% |

---

## 3) Evolución diaria
- [cite_start]El 100% de las transacciones (119 líneas) se registraron el **7 de julio de 2025**.
- Se necesita una **mayor distribución de fechas de venta** en los archivos fuente para evaluar la evolución diaria y la estacionalidad.
- El pico de ingresos (€59,715) se concentra en este único día.

---

## 4) Calidad de datos
- **Filas procesadas**:
    * **Bronce (raw)**: 120 (Ventas) + 125 (Clientes) + 128 (Productos) = **373**.
    * **Plata (clean)**: 119 (Ventas) + 119 (Clientes) + 118 (Productos) = **356**.
    * **Quarantine**: 1 (Ventas) + 6 (Clientes) + 10 (Productos) = **17**.
- **Motivos principales de quarantine**:
    * **Errores de Formato/Parseo (Malformación)**: Líneas con conteo incorrecto de campos (ej. en `clientes.csv` y `productos.csv`).
    * **Fallo de Validación (Clientes)**: Formato de `id_cliente` inválido (`004`, `C-006`) o valores nulos/inválidos en `fecha`, `nombre`, o `apellido`.
    * **Fallo de Validación (Productos)**: Valores negativos en `unidades` o `precio_unitario` (`-5`, `-180`), formato de fecha de entrada inválido (`2025-13-03`, `2025-02-30`), o nulos.

---

## 5) Próximos pasos
- **Revisar y Corregir Datos Cuarentenados**: Investigar la causa de los 17 registros en cuarentena, especialmente los errores de formato de fecha inválida (ej. `2025-13-03` en productos y clientes) y valores negativos, para corregir el origen de los datos.
- **Estandarizar IDs de Cliente**: Implementar una limpieza más rigurosa para normalizar IDs de cliente que no cumplen el patrón `C\d{3}` *antes* de la validación, para aumentar el porcentaje de éxito en `clean_clientes`.