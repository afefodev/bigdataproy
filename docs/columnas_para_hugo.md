# Datos limpios para la sección 4 (EDA y modelo base)

Hugo: esto es lo que necesitas para el análisis exploratorio y el modelo.
Todo sale de datos reales (sin datos sintéticos) y se reconstruye con el pipeline.

## 1. Qué tabla usar

| Uso | Ruta (relativa a la raíz del repo) | Filas | Notas |
|---|---|---|---|
| **EDA y entrenamiento del modelo (recomendada)** | `datalake/consumo/ordenes_nacional/` | 511 782 | Todas las entidades. Proveedores persona natural **seudonimizados**. Sin enlaces a PDF. Particionada por `anio` |
| Órdenes de la MPT (alerta / tablero) | `datalake/consumo/ordenes_mpt/` | 1 231 | Mismo esquema, solo `es_mpt = true` |
| Detalle por ítem de las órdenes MPT (precio unitario) | `datalake/consumo/items_orden_mpt/` | ver Paso 8 | Extraído de los PDF de cada orden |
| Zona procesada (uso interno) | `datalake/processed/ordenes_compra/v1/` | 511 782 | Contiene nombres y RUC de personas naturales: **no usar en notebooks ni capturas del informe** |

Para obtener los datos en tu máquina (unos 4 minutos; la mayor parte es la descarga):

```bash
.venv/bin/python -m scripts.run_pipeline --solo fuente1
```

Lectura:

```python
# Con PySpark
df = spark.read.parquet("datalake/consumo/ordenes_nacional")
# Con pandas (es pequeño: ~27 MB en Parquet)
import pandas as pd
df = pd.read_parquet("datalake/consumo/ordenes_nacional")
```

## 2. Diccionario de columnas (consumo/ordenes_nacional)

| Columna | Tipo | Descripción | Origen |
|---|---|---|---|
| `orden_electronica` | string | Código de la orden (`OCAM-AAAA-UE-N-v`). **Clave única** | ORDEN_ELECTRÓNICA |
| `fecha_formalizacion` | timestamp | Fecha y hora de aceptación de la orden (2024-06 solo trae fecha, hora 00:00) | FECHA_FORMALIZACIÓN |
| `fecha_formalizacion_dia` | date | Solo la fecha | derivada |
| `fecha_ultimo_estado` | timestamp | Fecha del último estado registrado **hasta el corte** | FECHA_ÚLTIMO_ESTADO |
| `fecha_proceso` | timestamp | Fecha de corte del archivo publicado | FECHA_PROCESO |
| `ruc_entidad`, `entidad` | string | Entidad compradora | RUC_ENTIDAD, ENTIDAD |
| `es_mpt` | boolean | `true` si la entidad es la MPT (RUC 20175639391) | derivada |
| `ruc_proveedor` | string | RUC del proveedor; para persona natural, seudónimo `PN-<hmac>` | RUC_PROVEEDOR |
| `proveedor` | string | Razón social; para persona natural, `PERSONA NATURAL PN-<hmac>` | PROVEEDOR |
| `tipo_persona_proveedor` | string | `NATURAL` (RUC 10/15/17) o `JURIDICA` (RUC 20) | derivada |
| `tipo_procedimiento` | string | `COMPRA ORDINARIA` o `GRAN COMPRA` | TIPO_PROCEDIMIENTO normalizado |
| `acuerdo_marco` | string | Catálogo normalizado (30 valores, mayúsculas sin tildes). **Categoría de comparación** | ACUERDO_MARCO sin código |
| `codigo_acuerdo_marco` | string | Versión del acuerdo (`EXT-CE-2021-7`); nulo cuando el mes no lo publicó (42 %) | ACUERDO_MARCO |
| `estado_orden` | string | ACEPTADA, PAGADA, RESUELTA, ORDEN DE COMPRA NULA, ACEPTADA C/ENTREGA PENDIENTE | ESTADO_ORDEN_ELECTRÓNICA |
| `descripcion_estado` | string | Motivo del estado (81,5 % nulo) | DESCRIPCIÓN_ESTADO |
| `flag_estado_adverso` | boolean | `estado_orden` ∈ {RESUELTA, ORDEN DE COMPRA NULA} | derivada |
| `flag_pagada` | boolean | `estado_orden` = PAGADA | derivada |
| `flag_documento_estado` | boolean | La orden tiene documento de sustento del cambio de estado | DOCUMENTO_ESTADO_OCAM |
| `sub_total`, `igv`, `total` | decimal(18,2) | Montos en soles | SUB_TOTAL, IGV, TOTAL |
| `total_double` | double | `total` como double (cómodo para pandas/sklearn) | derivada |
| `log10_total` | double | log10 del total. Los montos van de S/ 0,05 a S/ 44,6 M: **usa esta escala** para distribuciones y atípicos | derivada |
| `tasa_igv` | double | igv / sub_total (0,18 o 0) | derivada |
| `flag_igv_cero` | boolean | IGV = 0 (exonerada) — 19 217 órdenes | derivada |
| `dias_formalizacion_a_ultimo_estado` | int | Días entre formalización y último estado | derivada |
| `dias_observacion_hasta_corte` | int | Días entre formalización y corte (1–128, mediana 17). **Ver advertencia 1** | derivada |
| `dia_semana` | int | 1 = domingo … 7 = sábado | derivada |
| `dia_mes` | int | Día del mes de formalización | derivada |
| `alertas_calidad` | array<string> | Reglas de advertencia incumplidas (`A02_tasa_igv_coherente`, `A03_total_minimo`) | Paso 5 |
| `anio`, `mes` | int | Año y mes de formalización (partición) | derivada |

## 3. Advertencias que debes tener en cuenta (verificadas en los datos)

1. **El estado NO es el estado final.** Cada orden se publica una sola vez, en el archivo del mes de formalización, con el estado que tenía al día de corte (mediana: 17 días después). Una orden "ACEPTADA" puede haberse resuelto después y el dataset no lo refleja.
2. **Estados adversos muy escasos:** 340 de 511 782 (0,066 %): 328 RESUELTA y 12 NULA. **La MPT tiene 0.** Un clasificador de "terminar anulada" tendría muy pocos positivos y ninguno en la MPT. Recomendación: modelo base = **puntaje de atipicidad del monto** (`log10_total`) dentro de cada `acuerdo_marco` × `tipo_procedimiento` (p. ej. z-score robusto con mediana/MAD o percentiles), y dejar el estado adverso como análisis descriptivo nacional. Si entrenas un clasificador, usa métricas para clases desbalanceadas (PR-AUC, recall), no exactitud.
3. **Evita la fuga de información temporal:** si calculas medianas por acuerdo marco para puntuar una orden, usa solo órdenes anteriores a ella (o particiona entrenamiento/prueba por fecha, p. ej. entrenar 2022–2025 y probar 2026).
4. `GRAN COMPRA` y `COMPRA ORDINARIA` tienen escalas de monto muy distintas: compáralas por separado.
5. Algunas familias de `acuerdo_marco` tienen menos de 100 órdenes en 4,7 años (p. ej. CESPED SINTETICO: 31). Agrúpalas o exclúyelas del puntaje.
6. Hay 11 órdenes con TOTAL < S/ 1 y 394 con tasa de IGV atípica: siguen en la tabla, marcadas en `alertas_calidad`.

## 4. Cifras de referencia (para cuadrar tu EDA)

| Medida | Valor |
|---|---|
| Órdenes nacionales | 511 782 |
| Órdenes MPT | 1 231 |
| Familias de acuerdo marco | 30 |
| Compra ordinaria / gran compra | 475 630 / 36 152 |
| Proveedores persona natural (registros / distintos) | 77 735 / 865 |

Esquema versionado: `config/schemas/ordenes_compra.v1.json` (también copiado en `datalake/processed/ordenes_compra/v1/_esquema.json`).
