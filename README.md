# PrecioJusto — Pipeline de datos (Sección 3)

Alerta temprana de órdenes de compra atípicas por Acuerdos Marco para la
**Municipalidad Provincial de Trujillo (MPT)**, Subgerencia de Abastecimiento.
Curso Big Data y Analítica de Datos — UPAO, 2026-20. Ruta A (on-premise).

Este repositorio contiene el pipeline completo: ingesta, limpieza, calidad,
extracción de PDF (con OCR), integración de fuentes, capa de consumo con
seudonimización, medición de optimizaciones y prueba de escalabilidad.
**Todos los datos son reales** (sin datos sintéticos); la única excepción es la
réplica de la prueba de escalabilidad, que vive en una zona separada y se borra.

## 1. Fuentes

| Fuente | Qué es | Formato | Acceso |
|---|---|---|---|
| 1 | Órdenes de compra por Catálogos Electrónicos (PERÚ COMPRAS), bienes, 2022-01 a 2026-09 | CSV mensual (57 archivos) | datosabiertos.gob.pe, dataset `30932ab9-2ee8-44d6-98c3-64628397ebc9` (ODC-BY) |
| 2a | Resoluciones de la MPT vinculadas a abastecimiento | PDF escaneado (OCR) | gob.pe/munitrujillo (buscador `busquedas.json`) |
| 2b | PDF de cada orden de compra de la MPT (detalle por ítem y precio unitario) | PDF con texto | enlaces `ORDEN_ELECTRÓNICA_GENERADA` del CSV |

## 2. Requisitos

* macOS o Linux, 8 GB de RAM como mínimo (probado con 16 GB en Apple Silicon); unos 2 GB libres
  (60 GB si se ejecuta la prueba x100).
* Python 3.12
* Java 17 (Spark 3.5 **no** funciona con Java ≥ 24). macOS: `brew install openjdk@17`
* Tesseract OCR con idioma español (solo para la Fuente 2a):
  `brew install tesseract` y luego
  `curl -L -o "$(brew --prefix)/share/tessdata/spa.traineddata" https://github.com/tesseract-ocr/tessdata_fast/raw/main/spa.traineddata`
  (Linux: `apt install tesseract-ocr tesseract-ocr-spa`)
* Conexión a internet (la ingesta descarga de los portales oficiales)

## 3. Instalación

```bash
git clone https://github.com/afefodev/bigdataproy.git && cd bigdataproy
python3.12 -m venv .venv
.venv/bin/pip install -r requirements.txt
cp config/config.example.yaml config/config.yaml   # rutas, Spark y fuentes
cp .env.example .env                                # JAVA_HOME y la sal secreta
# Generar la sal y pegarla en .env como PRECIOJUSTO_SALT=...
python3 -c "import secrets; print(secrets.token_hex(32))"
.venv/bin/python -m scripts.check_env               # prueba de humo de Spark
```

## 4. Ejecución

```bash
# Pipeline completo (≈ 30-40 min la primera vez; la mayor parte es descargar 1 271 PDF)
.venv/bin/python -m scripts.run_pipeline

# Solo lo necesario para el EDA y el modelo (sección 4): ≈ 4 min
.venv/bin/python -m scripts.run_pipeline --solo fuente1

# Relanzar desde una etapa (todas son idempotentes)
.venv/bin/python -m scripts.run_pipeline --desde integracion

# Mediciones del informe
.venv/bin/python -m scripts.benchmark_optimizacion --repeticiones 7   # 3.6
.venv/bin/python -m scripts.escalabilidad --factores 1 10 100        # 3.7
```

Orden de las etapas (`scripts/run_pipeline.py`):

| # | Etapa | Módulo | Salida |
|---|---|---|---|
| 1 | ingesta_f1 | `src/ingesta/fuente1_ordenes.py` | `raw/ordenes_compra/anio=/mes=/` + `_manifiesto.csv` |
| 2 | limpieza_f1 | `src/limpieza/fuente1_limpieza.py` | `processed/ordenes_compra/v1/`, `cuarentena/ordenes_compra/` |
| 3 | ingesta_f2_resoluciones | `src/ingesta/fuente2_resoluciones.py` | `raw/resoluciones_mpt/` |
| 4 | ingesta_f2_ordenes_pdf | `src/ingesta/fuente2_ordenes_pdf.py` | `raw/ordenes_pdf/` |
| 5 | extraccion_ordenes_pdf | `src/pdf/extraer_ordenes.py` | `processed/items_orden_mpt/`, `processed/ordenes_pdf_cabecera/` |
| 6 | extraccion_resoluciones | `src/pdf/extraer_resoluciones.py` | `processed/resoluciones_mpt/` |
| 7 | integracion | `src/integracion/integrar_fuentes.py` | `processed/ordenes_mpt_integradas/` |
| 8 | consumo | `src/consumo/construir_consumo.py` | `consumo/*` (sin datos personales) |

Cada ejecución deja su log en `logs/pipeline_<fecha>.log`. La evidencia de la
corrida completa está en `docs/evidencias/corrida_completa.log`.

## 5. Estructura

```
config/
  config.example.yaml           rutas (base_path local | hdfs:// | s3a://), Spark, fuentes
  reglas_calidad_ordenes.yaml   reglas de calidad declarativas (3.4)
  integracion.yaml              ventana de días y familias para la integración
  schemas/ordenes_compra.v1.json  contrato de esquema versionado de la zona procesada
src/
  ingesta/  limpieza/  calidad/  pdf/  integracion/  seguridad/  consumo/  common/
scripts/
  run_pipeline.py  benchmark_optimizacion.py  escalabilidad.py  check_env.py
docs/
  columnas_para_hugo.md   diccionario de las tablas para la sección 4
  inspeccion_fuente1.md   hallazgos de la inspección de los datos reales
  seccion3_borrador.md    borrador de las subsecciones 3.1 a 3.8
  evidencias/             métricas reales (CSV/JSON/log) citadas en el informe
datalake/                 (no versionado) raw / cuarentena / processed / consumo / escalabilidad
```

## 6. Cambiar de almacenamiento o de clúster

* **HDFS:** en `config/config.yaml`, `base_path: "hdfs://namenode:9000/datalake"`. Los jobs de
  Spark escriben con la API FileSystem de Hadoop, así que no cambian. La ingesta (Python) escribe
  en disco local; en HDFS se agrega un `hdfs dfs -put` de la zona cruda (no probado en este equipo).
* **YARN:** `spark.master: "yarn"`.

## 7. Datos personales

Los proveedores persona natural (RUC 10/15/17) se seudonimizan con HMAC-SHA256 en la
capa de consumo; la clave está en `.env` (nunca en el repositorio). Los PDF de órdenes
contienen nombres, DNI, correos y teléfonos: permanecen solo en la zona cruda y no se
extraen. No subir `datalake/` ni `.env` a ningún repositorio.
