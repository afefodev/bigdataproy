# 3. Implementación del pipeline de datos — BORRADOR

> Borrador generado a partir de ejecuciones reales del 11/10/2026. Cada cifra
> remite a un archivo de `docs/evidencias/` o a un log de `logs/`. Antes de
> pasarlo al informe: revisar redacción, numerar tablas y figuras con el formato
> APA del equipo e insertar las capturas listadas al final.

El pipeline se implementó en Python 3.12 con Apache Spark 3.5.8 (PySpark) en
modo local (`local[*]`, 10 núcleos, 16 GB de RAM), con la misma estructura de
zonas que tendría sobre HDFS. La raíz del data lake se define en un archivo de
configuración (`base_path`), de modo que pasar a `hdfs://` o `s3a://` no
requiere cambiar el código de los jobs. El código completo está en el
repositorio; aquí se incluyen solo los fragmentos que sustentan las decisiones.

---

## 3.1. Fuentes de datos y ficha de procedencia

**Tabla 3.1a.** Ficha de procedencia — Fuente 1 (estructurada)

| Campo | Valor |
|---|---|
| Nombre del conjunto | Órdenes de compra realizadas a través de los Catálogos Electrónicos [Central de Compras Públicas – PERÚ COMPRAS] (conjunto de bienes) |
| Origen | ☐ Cedido por la empresa ☒ Fuente pública institucional. Al filtrar por RUC 20175639391 constituye el registro de compras propio de la MPT |
| Entidad publicadora | Central de Compras Públicas – PERÚ COMPRAS (datosabiertos@perucompras.gob.pe), en la Plataforma Nacional de Datos Abiertos |
| Enlace o medio de entrega | https://www.datosabiertos.gob.pe — dataset `30932ab9-2ee8-44d6-98c3-64628397ebc9`, descargado mediante la API `package_show` del portal |
| Fecha de obtención | 11/10/2026 (registrada por archivo en `raw/ordenes_compra/_manifiesto.csv`) |
| Licencia o condiciones de uso | Open Data Commons Attribution License (ODC-BY) |
| Período cubierto y número de registros | Enero 2022 – septiembre 2026 (57 archivos mensuales); **511 782 registros**, de los cuales 1 231 corresponden a la MPT |
| Tamaño en disco y formato | **261 687 822 bytes (261,7 MB)**, CSV separado por `;`; 56 archivos en Windows-1252 y 1 en UTF-8 |
| Documentación | Diccionario de Datos oficial (PDF, conjunto de bienes), guardado en `raw/ordenes_compra/_metadatos/` |

**Tabla 3.1b.** Ficha de procedencia — Fuente 2a (no estructurada: resoluciones)

| Campo | Valor |
|---|---|
| Nombre del conjunto | Resoluciones de la Oficina General de Administración y Finanzas (OGAF) y de la Gerencia de Administración y Finanzas (GAF) de la MPT |
| Origen | ☐ Cedido por la empresa ☒ Fuente pública institucional (documentos propios de la entidad cliente) |
| Entidad publicadora | Municipalidad Provincial de Trujillo, en la Plataforma Digital Única del Estado (gob.pe/munitrujillo; munitrujillo.gob.pe redirige allí) |
| Enlace o medio de entrega | Buscador JSON de gob.pe (`/busquedas.json`, institución `munitrujillo`, contenido `normas`); PDF en cdn.www.gob.pe |
| Fecha de obtención | 11/10/2026 |
| Licencia o condiciones de uso | Información pública según la Ley N.° 27806, Ley de Transparencia y Acceso a la Información Pública |
| Período cubierto y número de registros | Catálogo completo de 1 665 normas de la MPT (1990–2026); **5 resoluciones** de abastecimiento entre 2024-09 y 2025-09 (206 páginas) |
| Tamaño en disco y formato | 23 089 267 bytes (23,1 MB), PDF escaneado (imagen, sin capa de texto) |

**Tabla 3.1c.** Ficha de procedencia — Fuente 2b (no estructurada: PDF de órdenes)

| Campo | Valor |
|---|---|
| Nombre del conjunto | Órdenes de compra electrónicas en PDF de la MPT (enlace `ORDEN_ELECTRÓNICA_GENERADA` de la Fuente 1) |
| Origen | ☒ Fuente pública institucional, enlazada desde el dataset de la Fuente 1 |
| Entidad publicadora | PERÚ COMPRAS (apps1.perucompras.gob.pe, sistema SISCATÁLOGO) |
| Fecha de obtención | 11/10/2026 (`raw/ordenes_pdf/_manifiesto.csv`) |
| Licencia o condiciones de uso | Documentos públicos enlazados por un dataset ODC-BY. Contienen datos personales (representantes y servidores), que no se extraen (sección 3.8) |
| Período cubierto y número de registros | Las 1 231 órdenes de la MPT (2022-01 a 2026-09): **1 231 PDF, 5 361 ítems** |
| Tamaño en disco y formato | 93 721 580 bytes (93,7 MB), PDF con texto nativo; además, dos muestras aleatorias de verificación: 56 órdenes digitalizadas (7.7 MB, escaneadas) y 58 documentos de estado (12.9 MB) |

**Volumen total ingerido:** 261,7 MB (CSV) + 23,1 MB + 93,7 MB + 20.6 MB (PDF) ≈ **399 MB**
y 511 782 registros estructurados. El requisito del curso (100 000 registros o 100 MB) se
cumple solo con la Fuente 1. *Nota: el avance v4 estimaba ≈165 MB a partir de un solo
archivo; la cifra medida es 261,7 MB.*

**Tabla 3.2.** Diccionario de las columnas relevantes (Fuente 1, tras la limpieza)

| Columna | Tipo | Origen | Uso |
|---|---|---|---|
| `orden_electronica` | string | ORDEN_ELECTRÓNICA | Clave única; clave de unión con la Fuente 2b |
| `ruc_entidad`, `entidad`, `es_mpt` | string, bool | RUC_ENTIDAD, ENTIDAD | Filtro de la MPT en la capa de consumo |
| `ruc_proveedor`, `proveedor`, `tipo_persona_proveedor` | string | RUC_PROVEEDOR, PROVEEDOR | Concentración de proveedores (seudonimizado si es persona natural) |
| `acuerdo_marco`, `codigo_acuerdo_marco` | string | ACUERDO_MARCO | Categoría de comparación (30 familias) y versión del acuerdo |
| `tipo_procedimiento` | string | TIPO_PROCEDIMIENTO | COMPRA ORDINARIA / GRAN COMPRA |
| `sub_total`, `igv`, `total`, `log10_total` | decimal(18,2), double | SUB_TOTAL, IGV, TOTAL | Variable principal del puntaje de atipicidad |
| `fecha_formalizacion`, `fecha_ultimo_estado`, `fecha_proceso` | timestamp | FECHA_* | Estacionalidad, tiempos de ciclo, ventana de observación |
| `estado_orden`, `flag_estado_adverso` | string, bool | ESTADO_ORDEN_ELECTRÓNICA | Estados adversos (RESUELTA, NULA) |
| `dias_observacion_hasta_corte` | int | derivada | Días entre la formalización y el corte del archivo |

Fuente 2b (`items_orden_mpt`): `orden_electronica`, `nro_item`, `producto`, `descripcion`,
`codigo_unico`, `cantidad`, `precio_unitario_sin_igv`, `igv_item`, `importe_item`. El
diccionario completo para la sección 4 está en `docs/columnas_para_hugo.md`.

**Diferencias entre el diccionario oficial y los datos reales** (verificadas en los 57 archivos):
el CSV usa `DOCUMENTO_ESTADO_OCAM` (el diccionario dice `DOCUMENTO_ESTADO`); las fechas
vienen como `yyyy-MM-dd HH:mm:ss` (el diccionario indica `DD/MM/YYYY`) salvo en 2024-06
(`d/MM/yyyy H:mm`); y `DESCRIPCIÓN_CESIÓN_DERECHOS` está vacía en el 100 % de los registros.

## 3.2. Ingesta y organización del data lake

La ingesta es un script de Python por fuente. No se escriben URLs a mano: para la
Fuente 1 se consulta la API del portal (`package_show`), que devuelve la lista oficial
de recursos. Esto es necesario porque el portal publica algunos archivos con sufijos
(`ReportePCBienes202302_0.csv`) que un patrón fijo de URL no encontraría.

Cada archivo se descarga a un temporal y se renombra al terminar (escritura atómica), y
se registra en un manifiesto con nombre, URL, bytes, SHA-256, filas, codificación y fecha
de descarga. Si el archivo ya existe y su hash coincide, se omite: **relanzar la ingesta
no duplica nada** (verificado: segunda corrida con 0 nuevos y 57 omitidos). Si el hash
no coincide, el job se detiene, porque la zona cruda debe ser inmutable.

```python
if destino.exists() and nombre in manifest:
    if sha256_file(destino) != manifest[nombre]["sha256"]:
        raise RuntimeError("El archivo crudo cambió: la zona cruda debe ser inmutable")
    omitidos += 1
    continue
```

**Estructura de directorios y convención de nombres** (raíz configurable `base_path`):

```
datalake/
├── raw/                                  inmutable, archivos tal como se descargan
│   ├── ordenes_compra/anio=AAAA/mes=MM/ReportePCBienesAAAAMM.csv   + _manifiesto.csv, _metadatos/
│   ├── resoluciones_mpt/anio=AAAA/mes=MM/<id>-resolucion-....pdf   + _manifiesto.csv, _catalogo/
│   └── ordenes_pdf/{generada,digitalizada,documento_estado}/anio=AAAA/mes=MM/OCAM-....pdf
├── cuarentena/ordenes_compra/            registros rechazados + motivos_rechazo
├── processed/                            Parquet (snappy)
│   ├── ordenes_compra/v1/anio=AAAA/mes=M/   esquema versionado (_esquema.json)
│   ├── items_orden_mpt/  ordenes_pdf_cabecera/  resoluciones_mpt/  resoluciones_mpt_paginas/
│   └── ordenes_mpt_integradas/  resoluciones_mpt_resumen/
├── consumo/                              sin datos personales; filtro MPT aplicado aquí
│   └── ordenes_nacional/anio=AAAA/  ordenes_mpt/  items_orden_mpt/  ordenes_mpt_integradas/ ...
└── escalabilidad/                        réplicas x10/x100 (temporales; nunca alimentan el análisis)
```

Convenciones: nombres de fuente en `snake_case`; particiones al estilo Hive (`anio=`,
`mes=`), que Spark, DuckDB y Athena reconocen; versión del esquema en la ruta de la zona
procesada (`v1`); archivos internos con prefijo `_`, que los lectores de Parquet ignoran.

**Corrida completa de extremo a extremo** (`scripts/run_pipeline.py`, evidencia en
`docs/evidencias/corrida_completa.log` y `corrida_completa_resumen.csv`): 9 etapas, todas OK,
**364,4 s** en total, en una ejecución sobre datos ya descargados. Las ingestas de CSV y de
PDF de órdenes solo verifican hashes (0,7 s cada una); la de resoluciones vuelve a recorrer
las 67 páginas del catálogo de gob.pe (127,5 s); la extracción de 1 231 PDF toma 147,1 s.

## 3.3. Procesamiento y limpieza

**Carga y verificación del esquema.** Antes de leer, el job compara la cabecera de cada
uno de los 57 archivos con las 19 columnas esperadas; si Perú Compras cambia una columna,
el job falla en lugar de mezclar datos. Todo se lee como texto (`StringType`) y se tipa
explícitamente después: si Spark infiriera tipos, una fecha en otro formato o un
tabulador al inicio convertirían en nulos columnas enteras sin avisar. Cada archivo se
lee con la codificación detectada en la ingesta.

**Valores nulos, duplicados y atípicos.**

* Nulos: tras normalizar, los 15 campos críticos tienen un 100 % de completitud
  (Tabla 3.4). Los campos opcionales se dejan nulos y se documentan: `descripcion_estado`
  (81,5 % nulo) y `codigo_acuerdo_marco` (42,1 % nulo, porque algunos meses no lo publican).
  No se imputa ningún valor.
* Duplicados: la regla R03 conserva una sola fila por `orden_electronica` (la del corte
  más reciente). En los datos reales hay 0 duplicados: cada orden se publica una sola vez,
  en el archivo de su mes de formalización.
* Atípicos de monto: **no se eliminan**. Detectar montos atípicos es el objetivo del
  proyecto, de modo que borrarlos destruiría la señal. Solo se marcan los casos
  inverosímiles (TOTAL < S/ 1: 11 órdenes; tasa de IGV distinta de 0 % y 18 %: 394).

**Tipos y normalización.**

* Fechas: se elige el formato según la forma del texto (ISO en 56 archivos, `d/MM/yyyy
  H:mm` en 2024-06); 0 fechas quedaron sin convertir.
* Montos: `DECIMAL(18,2)` y no `float`, para evitar errores de redondeo al sumar soles.
* Espacios: recorte de espacios y tabuladores (el archivo 2024-03 trae un tab delante de
  cada campo).
* Categorías: `TIPO_PROCEDIMIENTO` y estados en mayúsculas sin tildes. `ACUERDO_MARCO`
  tenía **72 variantes para 30 catálogos** (con o sin código `EXT-CE-2021-7`, con o sin
  tildes, con espacios dobles). Se separó el código de versión y se normalizó el nombre:

```python
patron = r"^((?:EXT|IM)-CE-\d{4}-\d+)\s+(.+)$"
F.when(tiene_codigo, F.regexp_extract(am, patron, 1)).alias("codigo_acuerdo_marco"),
sin_tildes_mayus(F.when(tiene_codigo, F.regexp_extract(am, patron, 2)).otherwise(am)).alias("acuerdo_marco"),
```

**Ingeniería de características.**

* `es_mpt`: marcador del cliente. El filtro se aplica en consumo porque el modelo se
  entrena con el histórico nacional.
* `log10_total`: los montos van de S/ 0,05 a S/ 44,6 millones; la escala logarítmica es
  la adecuada para comparar y detectar atípicos.
* `tipo_persona_proveedor`: según el prefijo del RUC (10/15/17 natural, 20 jurídica).
* `flag_estado_adverso`: estado RESUELTA o NULA.
* `dias_formalizacion_a_ultimo_estado` y `dias_observacion_hasta_corte`: la segunda
  existe porque el estado publicado es el estado **a la fecha de corte** del archivo
  (mediana: 17 días después de la formalización), no el estado final. Es una limitación
  clave para el modelo de la sección 4.
* `dia_semana`, `dia_mes`, `flag_igv_cero` y `tasa_igv`.

**Extracción de los PDF.**

*Órdenes (2b).* El PDF trae texto nativo, así que se lee con pdfplumber sin OCR. No tiene
tabla con bordes, por eso cada ítem se reconoce por la forma de su línea:
`<n> <descripción> <código> <cantidad> <precio unitario> <IGV> <importe>`.

Dos hallazgos obligaron a corregir el parser:
1. En las órdenes con entregas parciales, cada versión del PDF (`-1`, `-2`, `-3`) es una
   entrega y no una corrección, así que los ítems se suman sobre todas las entregas. Se
   detectó porque 9 órdenes no cuadraban con el TOTAL del CSV.
2. Las cantidades de 1 000 o más aparecen sin separador de miles ("3000"); el primer
   patrón numérico no las reconocía y 89 ítems quedaban sin precio unitario.

Tras ambas correcciones, las 1 231 órdenes se extraen bien. Los PDF se reparten entre
los núcleos con Spark (`parallelize(rutas).flatMap(parsear)`).

*Resoluciones (2a).* Son escaneos, así que se aplica OCR (Tesseract 5.5, idioma español,
200 ppp) solo a las páginas sin capa de texto, y Spark reparte las páginas. De cada
resolución se estructuran: número, tipo, unidad emisora, fecha de publicación, fecha de
emisión (leída del texto), tema, acción, expedientes citados y familias de bienes
mencionadas.

**Integración de las fuentes.** Antes de fijar la clave se revisó una muestra real. Las
resoluciones de abastecimiento disponibles son **bajas de bienes** y no compras; citan
expedientes de mesa de partes (`27789-2025-MPT`), mientras que las órdenes usan el
expediente SIAF (numeración distinta); ninguna cita un código de orden. Por eso se
implementaron tres cruces y se midió cada uno (Tabla 3.3):

**Tabla 3.3.** Resultado de la integración (`docs/evidencias/integracion_resumen.csv`)

| Cruce | Clave | Órdenes MPT que cruzan | No cruzan |
|---|---|---|---|
| CSV ⟷ PDF de la orden | `orden_electronica` (exacta) | **1 231** (100 %) | 0 |
| Órdenes ⟷ resoluciones | \|fecha de formalización − fecha de resolución\| ≤ 30 días | 192 (15,6 %) | 1 039 |
| Órdenes ⟷ resoluciones (temático) | ventana ±30 días **y** la resolución menciona la familia (acuerdo marco) de la orden | 63 (5,1 %) | 1 168 |
| Expediente | expediente citado = expediente SIAF | 0 | 1 231 |
| Código OCAM citado en la resolución | — | 0 | 1 231 |

La unión principal es la de clave exacta. Además sirve de control de veracidad entre
fuentes: el TOTAL del CSV coincide con el IMPORTE TOTAL del PDF en las 1 231 órdenes, y la
fecha de formalización también. El cruce por ventana con resoluciones se mantiene como
contexto ("en el mes de esta compra hubo una baja de bienes de la misma familia"), pero
se declara como relación débil. Los registros sin correspondencia **no se descartan**
(`left join`): quedan con `n_resoluciones_ventana = 0`.

```python
cruce = (ordenes.join(F.broadcast(resoluciones),
                      F.abs(F.datediff("fecha_formalizacion_dia", "fecha_resolucion")) <= 30, "inner")
                .withColumn("misma_familia", F.array_contains("familias_mencionadas", F.col("acuerdo_marco"))))
```

## 3.4. Control de calidad de los datos

Las reglas se declaran en `config/reglas_calidad_ordenes.yaml`, separadas del código, como
expresiones SQL. Un motor genérico las evalúa: un registro que incumple una regla de
**rechazo** va a `cuarentena/` con la lista de reglas incumplidas; uno que incumple una de
**advertencia** se conserva y se marca en `alertas_calidad`. Si una expresión da NULL,
cuenta como incumplimiento.

```yaml
- id: R09_total_cuadra
  descripcion: TOTAL = SUB_TOTAL + IGV con tolerancia de 5 céntimos (redondeo)
  severidad: rechazo
  expresion: "abs(total - (sub_total + igv)) <= 0.05"
```

**Tabla 3.4.** Resultado de las reglas sobre 511 782 registros (`docs/evidencias/calidad_reglas_fuente1.csv`)

| Regla | Severidad | Incumplen | % |
|---|---|---|---|
| R01 registro bien formado (19 columnas) | rechazo | 0 | 0 |
| R02 orden presente | rechazo | 0 | 0 |
| R03 orden única | rechazo | 0 | 0 |
| R04 RUC de entidad de 11 dígitos | rechazo | 0 | 0 |
| R05 RUC de proveedor de 11 dígitos | rechazo | 0 | 0 |
| R06 fecha de formalización válida | rechazo | 0 | 0 |
| R07 montos numéricos | rechazo | 0 | 0 |
| R08 TOTAL > 0 | rechazo | 0 | 0 |
| R09 TOTAL = SUB_TOTAL + IGV (±0,05) | rechazo | 0 | 0 |
| R10 tipo de procedimiento en dominio | rechazo | 0 | 0 |
| R11 acuerdo marco presente | rechazo | 0 | 0 |
| A01 estado en dominio | advertencia | 0 | 0 |
| A02 tasa de IGV 0 % o 18 % | advertencia | **394** | 0,077 |
| A03 TOTAL ≥ S/ 1 | advertencia | **11** | 0,002 |
| A04 último estado no anterior a la formalización | advertencia | 0 | 0 |
| A05 último estado no posterior al corte | advertencia | 0 | 0 |
| A06 mes de formalización = mes del archivo | advertencia | 0 | 0 |

Resultado: **511 782 leídos = 511 782 válidos + 0 en cuarentena**. La cuarentena vacía no
significa que no hubo limpieza: los problemas reales del archivo (tabuladores, dos
formatos de fecha, dos codificaciones, 72 variantes de categoría) se corrigieron en la
normalización, y gracias a ello ningún registro incumple las reglas de rechazo. La
cuarentena queda operativa para meses futuros.

**Tabla 3.5.** Completitud por campo crítico (`calidad_completitud_fuente1.csv`): los 15
campos (`orden_electronica`, `ruc_entidad`, `entidad`, `ruc_proveedor`, `proveedor`,
`tipo_procedimiento`, `acuerdo_marco`, `estado_orden`, `fecha_formalizacion`,
`fecha_ultimo_estado`, `sub_total`, `igv`, `total`, `url_orden_generada`,
`url_orden_digitalizada`) tienen un **100,0 %** de valores no nulos, tanto a la entrada como
a la salida.

**Comprobaciones de coherencia entre fuentes y de la extracción:**

| Comprobación | Resultado |
|---|---|
| TOTAL del CSV = IMPORTE TOTAL del PDF | 1 231 / 1 231 órdenes |
| Fecha de formalización del CSV = la del PDF | 1 231 / 1 231 |
| cantidad × precio unitario + IGV = importe (por ítem) | 5 361 / 5 361 ítems |
| suma de los ítems = IMPORTE TOTAL del PDF | 1 231 / 1 231 |
| Enlaces accesibles (generada / digitalizada / documento de estado) | 1 231/1 231, 56/56, 58/58 (sin un solo fallo) |
| Órdenes digitalizadas: OCR reconoce el TOTAL del CSV / el código de la orden | 54/56 / 5/56 (escaneo sin capa de texto; aporta firma y sello, no datos nuevos frente al PDF generado) |
| Confianza media del OCR, cuerpo de la resolución (págs. 1–3) | 70,0 – 94,2 % según la resolución |
| Confianza media del OCR, anexos tabulares | 27,6 – 56,25 % (inventarios de bienes en letra pequeña) |
| Fecha de emisión reconocida por OCR | 4 / 5 resoluciones (la 184-2025 tiene la cabecera ilegible) |

## 3.5. Almacenamiento de datos procesados

Los datos limpios se guardan en Parquet comprimido con Snappy en
`processed/ordenes_compra/v1/`, particionados por `anio` y `mes` de formalización (57
particiones, un archivo por partición). El contrato de esquema (nombre, tipo y
nulabilidad de las 39 columnas, clave y particiones) está versionado en
`config/schemas/ordenes_compra.v1.json` y se copia junto a los datos; si el job produjera
un esquema distinto, falla y obliga a crear una versión v2 en lugar de romper a los
consumidores.

| Formato | Tamaño |
|---|---|
| CSV crudo (57 archivos) | 261,7 MB |
| Parquet procesado (39 columnas, incluye derivadas) | **43,6 MB** (6,0 veces menos) |
| Parquet de consumo nacional (sin URL) | 27,4 MB |

**Por qué columnar.** (1) Las consultas analíticas leen pocas columnas de muchas filas:
Parquet lee solo las columnas pedidas, mientras que el CSV obliga a leer y parsear todas.
(2) Compresión por columna: los valores repetidos (entidad, acuerdo marco, estado) se
comprimen muy bien. (3) Guarda el esquema y los tipos, así que no hay que volver a
inferirlos. (4) Las particiones `anio=/mes=` permiten saltarse los meses que no se
consultan. El efecto se midió en la sección 3.6 (O1). El CSV se conserva solo en la zona
cruda como formato de intercambio y de evidencia.

## 3.6. Optimización del rendimiento

Se midieron tres optimizaciones sobre los datos reales (`scripts/benchmark_optimizacion.py`,
`docs/evidencias/optimizacion.csv`). Cada caso se repitió 7 veces **alternando el antes y
el después** en cada repetición, y se reporta la mediana. El equipo de pruebas es un
portátil compartido con otras aplicaciones (carga promedio de 17–19 durante la medición),
y al alternar, la variación de carga afecta a ambos casos por igual. En los tres casos se
verificó que el resultado sea idéntico antes y después.

**Tabla 3.6.** Optimizaciones medidas

| Optimización | Antes (mediana) | Después (mediana) | Aceleración |
|---|---|---|---|
| O1. Formato y particionado: compras de la MPT en 2025 por acuerdo marco, sobre CSV crudo frente a Parquet particionado | 3,880 s (lee 261,7 MB) | 0,509 s (lee 8,2 MB) | **7,6×** |
| O2. Expresiones de limpieza: regex por columna, prueba de todos los formatos de fecha y cadena de `withColumn`, frente a `btrim`, formato elegido por patrón y un `select` por etapa | 13,020 s | 5,623 s | **2,3×** |
| O3. Unión con tabla pequeña (58 grupos acuerdo × procedimiento): sort-merge join frente a broadcast join | 0,592 s | 0,214 s | **2,8×** |

Efecto de O2 sobre el **job completo** de limpieza (una corrida de cada versión, mismos 57
archivos; `docs/evidencias/optimizacion_job_limpieza.txt`): **151,6 s → 17,3 s**.

**Por qué funcionaron.**

* *O1:* el plan físico muestra `PartitionFilters: [isnotnull(anio#1209), (anio#1209 = 2025)]`
  (`optimizacion_plan_parquet.txt`), es decir, Spark solo abre las 12 particiones de 2025
  (8,2 MB) y de ellas solo las columnas usadas. Con el CSV debe leer y parsear los 261,7 MB.
* *O2:* tres causas.
  - Probar un formato de fecha que no corresponde hace que Java lance y capture una
    excepción por celda.
  - Un regex por cada una de las 19 columnas es más caro que `btrim`, que recorta por un
    conjunto de caracteres.
  - Una cadena larga de `withColumn` genera planes muy grandes que Spark analiza una y
    otra vez.
* *O3:* el sort-merge join reparte y ordena las 511 782 órdenes por la clave de unión
  (shuffle). El broadcast join copia la tabla pequeña a cada tarea y une sin mover las
  órdenes. En la configuración por defecto, Spark (con AQE) ya elige broadcast
  automáticamente para tablas pequeñas; aquí se forzó el sort-merge para medir la diferencia.

## 3.7. Prueba de escalabilidad

El conjunto real se replicó a 10 y 100 veces su tamaño (`scripts/escalabilidad.py`). Cada
CSV se copió con su mismo formato y solo se agregó un sufijo `-R###` a `ORDEN_ELECTRÓNICA`
en las copias, para que la regla de unicidad no mandara la réplica a cuarentena y el
trabajo medido fuera el real. Sobre cada réplica se ejecutó el pipeline completo de
procesamiento: lectura del CSV, verificación de esquema, normalización, 17 reglas de
calidad, escritura de Parquet particionado y capa de consumo con seudonimización. Cada
factor corrió en un proceso nuevo, y se muestreó cada 0,5 s la memoria residente de todo
el árbol de procesos (Python + JVM de Spark).

Las réplicas se guardaron en `datalake/escalabilidad/`, se borraron al terminar y **no se
usaron en el análisis ni en el modelo**.

**Tabla 3.7.** Resultados (`docs/evidencias/escalabilidad.csv`; equipo: 10 núcleos, 16 GB de
RAM, Spark local con 6 GB para el driver)

| Factor | Registros | CSV de entrada | Parquet de salida | Limpieza y calidad | Consumo | **Tiempo total** | Tiempo relativo | Registros/s | Pico de RAM | Carga del sistema |
|---|---|---|---|---|---|---|---|---|---|---|
| ×1 | 511 782 | 0,26 GB | 0,044 GB | 16,9 s | 5,7 s | **25,0 s** | 1,0 | 20 455 | 2 689 MB | 5,1 |
| ×10 | 5 117 820 | 2,64 GB | 0,229 GB | 135,7 s | 27,9 s | **166,7 s** | 6,7 | 30 706 | 5 242 MB | 12,3 |
| ×100 | 51 178 200 | 26,42 GB | 1,471 GB | 712,0 s | 177,2 s | **893,3 s** (14,9 min) | 35,7 | 57 289 | 4 934 MB | 28,3 |

En las tres corridas: 0 registros en cuarentena y 0 personas naturales en claro en consumo.

**¿El tiempo crece en proporción al volumen?** No: crece **menos que proporcionalmente**
hasta ×100. Con 10 veces más datos el tiempo se multiplicó por 6,7, y con 100 veces, por
35,7. El rendimiento pasó de 20 mil a 57 mil registros por segundo, por dos razones:

* Hay un costo fijo (arranque de la JVM, planificación de Spark, verificación de
  cabeceras) que en ×1 pesa mucho y en ×100 se diluye.
* Con ×1 hay solo 13 particiones de entrada para 10 núcleos, así que la máquina está
  subutilizada; con 5 700 archivos todos los núcleos trabajan.

No se observó degradación, aunque la corrida ×100 se hizo con el equipo más cargado por
otras aplicaciones (carga 28,3 frente a 5,1), lo que juega en su contra.

**Memoria.** El pico no crece con el volumen (2,7 GB, 5,2 GB y 4,9 GB). Spark procesa por
particiones y no carga el conjunto en memoria: el tope lo fija la memoria asignada al driver
(6 GB). Cuando no cabe, Spark derrama a disco. Por eso el job de limpieza usa un staging en
Parquet en lugar de `cache()`.

**Límite de la réplica.** El Parquet de ×100 ocupa 33 veces el de ×1, no 100 veces, porque
las copias repiten valores y comprimen mejor que datos nuevos. La escritura real de 100 veces
más datos distintos costaría algo más de lo medido.

**¿En qué punto deja de alcanzar una sola máquina?** El recurso que se agota primero es el
**disco**, no la CPU ni la memoria. Por cada múltiplo del volumen actual se necesitan
≈0,26 GB de CSV, el derrame temporal del shuffle (se observaron al menos 7,3 GB durante la
corrida ×100) y el staging y Parquet de salida, es decir, ≈0,35–0,4 GB por múltiplo. Con los
120 GB libres de este equipo, el techo práctico está alrededor de **×300** (≈150 millones de
órdenes). En tiempo, al ritmo medido de ~57 mil registros/s, ×300 tardaría unos 45 minutos:
aceptable para una carga mensual, pero ya sin margen.

Puesto en contexto, el histórico nacional crece unas 108 mil órdenes por año (511 782 en 4,75
años), de modo que ×100 equivale a varios siglos de crecimiento de esta fuente. Una sola
máquina basta para este problema hoy y en el horizonte previsible.

El procesamiento distribuido (HDFS + YARN con varios nodos) se justificaría si:
* se integraran fuentes mucho mayores (todas las contrataciones del SEACE, órdenes de
  servicio, ítems de todas las entidades y no solo de la MPT), o
* el volumen superara de forma sostenida unos 100 GB de entrada.

En ese escenario el código no cambia: basta con apuntar `base_path` a `hdfs://` y
`spark.master` a `yarn`.

## 3.8. Seguridad de la plataforma

*Implementado.*

* **Seudonimización:** los proveedores persona natural (RUC 10/15/17, cuyo RUC contiene
  el DNI) se seudonimizan con HMAC-SHA256 y clave secreta en la capa de consumo: 865 RUC
  distintos dan 865 seudónimos, sin colisiones. Se usa HMAC y no `sha256(ruc)` porque un
  hash sin clave de un DNI se revierte probando los ~30 millones de DNI posibles. El job
  verifica automáticamente que no quede ningún RUC ni nombre de persona natural en claro,
  y falla si lo encuentra.
* **Gestión de la clave:** en una variable de entorno (`.env`, excluido de Git); el job
  no corre sin ella.
* **Minimización:** la capa de consumo no incluye enlaces a PDF ni texto completo, y de
  los PDF de órdenes (que contienen nombres, DNI, correos y teléfonos) solo se extraen
  campos de la contratación y de los ítems.
* **Integridad:** SHA-256 por archivo crudo y escrituras atómicas.
* **Cifrado:** descargas solo por HTTPS; disco del equipo con FileVault activo.

```python
def token(ruc: str, clave: bytes) -> str:
    return "PN-" + hmac.new(clave, ruc.encode("utf-8"), hashlib.sha256).hexdigest()[:16]
```

*Previsto para producción (Ruta A).* Detalle completo en `docs/seguridad_permisos.md`:

* Autenticación Kerberos.
* Permisos HDFS por zona con mínimo privilegio: solo la ingesta escribe en `raw`; el
  pipeline lee `raw` y escribe `processed` y `consumo`; analistas y tablero **solo leen
  `consumo`**, la única zona sin datos personales.
* Instantáneas HDFS de la zona cruda.
* Zonas de cifrado HDFS con KMS, cifrado de RPC y de transferencia de bloques, y cifrado
  del shuffle de Spark.
* Clave del HMAC en un gestor de secretos con rotación anual.
* Auditoría con `hdfs-audit.log`.

---

## Capturas que debes tomar como evidencia

1. `scripts/check_env.py` ejecutado (versiones de Python, Spark y Java).
2. Árbol de `datalake/` (`find datalake -maxdepth 3 -type d | sort`) y `raw/ordenes_compra/_manifiesto.csv` abierto (primeras filas).
3. Segunda ejecución de la ingesta mostrando `nuevos: 0, omitidos: 57` (idempotencia).
4. Log de la limpieza con el resumen de reglas (`logs/limpieza_fuente1.log`).
5. `config/reglas_calidad_ordenes.yaml` y la tabla de resultados de reglas.
6. Salida de `scripts/benchmark_optimizacion.py` y el fragmento del plan con `PartitionFilters`.
7. Spark UI (http://localhost:4040) durante una corrida: pestaña *SQL* con el plan del broadcast join.
8. Tabla y log de la prueba de escalabilidad (`logs/escalabilidad.log`) y el Monitor de Actividad durante la corrida x100.
9. Un PDF de orden (con los datos personales tapados) junto a sus filas en `items_orden_mpt`.
10. Una página de resolución escaneada junto a su texto OCR.
11. `docs/evidencias/integracion_resumen.csv`.
12. Una consulta a `consumo/ordenes_mpt` donde se vean los seudónimos `PN-…`.
13. `docs/evidencias/corrida_completa.log` (corrida completa del pipeline, requisito de admisibilidad).
14. Historial de commits en GitHub.

*Ninguna captura debe mostrar nombres, DNI, correos ni teléfonos de personas naturales.*

## Herramientas de IA utilizadas (para el Anexo A)

| Herramienta | Uso que se le dio | Sección del informe | Verificación realizada (completar por el equipo) |
|---|---|---|---|
| Claude Code (modelo Claude Opus 5.5, Anthropic) | Inspección de los datos reales; escritura del código de ingesta, limpieza, calidad, extracción de PDF, integración, seudonimización, benchmark y escalabilidad; depuración de errores (codificación mixta, formatos de fecha, entregas parciales, alineación de columnas); redacción de este borrador | 3.1 – 3.8, README, docs/ | [p. ej.: se ejecutó el pipeline completo, se revisaron los conteos contra los CSV originales y se leyeron y entendieron todos los módulos] |

## Diferencias con el avance v4 que el equipo debe corregir en otras secciones

1. **Volumen** (1.7, 2.2, 2.4): son 261,7 MB y 511 782 registros, no ≈165 MB.
2. **Fuente 2** (1.5, 2.1, 3.1): en gob.pe solo hay 5 resoluciones de abastecimiento en
   2022–2026, todas sobre bajas de bienes. La fuente no estructurada con más valor resultó
   ser el PDF de cada orden, que trae el precio unitario. Esto **levanta parcialmente la
   limitación** declarada en 1.6, pero solo para la MPT (1 231 órdenes, 5 361 ítems).
3. **Clave de integración** (2.1): el documento dice que se fija "en la sección 3.4"; en la
   guía la integración va en 3.3, y la clave principal resultó ser `orden_electronica`.
4. **Estados adversos** (1.3, 1.5): la MPT tiene 0 órdenes RESUELTA o NULA, y a nivel
   nacional son el 0,066 %. Además, el estado es el estado a la fecha de corte, no el final.
5. **Diccionario oficial frente a los datos reales**: nombre de `DOCUMENTO_ESTADO_OCAM` y
   formato de fechas (ver 3.1).
6. **Entorno** (2.4): Spark 3.5.8 en modo local, Java 17, Python 3.12, Tesseract 5.5.3,
   pdfplumber 0.11.4 y PyArrow 17.0.0. No se levantó HDFS/YARN; la ruta base es configurable.
