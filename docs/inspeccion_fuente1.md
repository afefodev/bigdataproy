# Inspección de la Fuente 1 (Paso 2) — hallazgos sobre los datos reales

Fecha de inspección: 2026-10-11. Fuente: datosabiertos.gob.pe, dataset
`30932ab9-2ee8-44d6-98c3-64628397ebc9` (PERÚ COMPRAS, conjunto de bienes).
Todas las cifras salen de los 57 archivos descargados (ver `datalake/raw/ordenes_compra/_manifiesto.csv`).

## Formato

| Aspecto | Hallazgo | Consecuencia en el pipeline |
|---|---|---|
| Separador | `;` | `sep=";"` |
| Codificación | **No es uniforme**: 56 archivos en cp1252 y 1 (2024-03) en UTF-8 | La ingesta detecta la codificación por archivo y la guarda en el manifiesto; la limpieza lee cada grupo con la suya |
| Columnas | Las mismas 19 columnas en los 57 archivos | Contrato de cabecera verificado antes de leer |
| Nombre de columna | El CSV trae `DOCUMENTO_ESTADO_OCAM`; el diccionario dice `DOCUMENTO_ESTADO` | Se acepta el nombre real; se documenta la diferencia |
| Formato de fecha | Diccionario: `DD/MM/YYYY hh:mm:ss`. Real: `yyyy-MM-dd HH:mm:ss` en 56 archivos y `d/MM/yyyy H:mm` (sin segundos; hora 00:00 en la mayoría) en 2024-06 | Conversión multiformato; 0 fechas sin convertir |
| Espacios | El archivo 2024-03 trae un **tabulador** al inicio de cada campo | Recorte de espacios y tabs en todas las columnas |
| `ACUERDO_MARCO` | 72 variantes para 30 catálogos: con y sin código (`EXT-CE-2021-7 ÚTILES…`), con y sin tildes, espacios dobles | Se separa `codigo_acuerdo_marco` y se normaliza `acuerdo_marco` (30 valores) |
| `DESCRIPCIÓN_CESIÓN_DERECHOS` | Vacía en el 100 % de los 511 782 registros | Se excluye de la zona procesada |

## Volumen

57 archivos (2022-01 a 2026-09), **511 782 registros**, **261 687 822 bytes (261,7 MB)**.
El documento v4 estimaba ≈165 MB a partir de un solo archivo (2026-08, de los más pequeños); la cifra real es mayor.

## Semántica que condiciona el modelo

1. **Cada orden aparece una sola vez**, en el archivo del mes en que se formalizó (0 duplicados de `ORDEN_ELECTRÓNICA`; 0 órdenes cuyo mes no coincide con el del archivo).
2. **El estado es el estado a la fecha de corte del archivo** (`FECHA_PROCESO`, normalmente el día 1 del mes siguiente), no el estado final. La ventana entre formalización y corte va de 1 a 128 días (mediana 17). Los archivos 2022-01 a 2022-04 se generaron juntos el 2022-05-12.
3. Estados observados: ACEPTADA 414 190 · PAGADA 97 250 · RESUELTA 328 · ORDEN DE COMPRA NULA 12 · ACEPTADA C/ENTREGA PENDIENTE 2. **Estados adversos: 340 (0,066 %)**.
4. IGV = 0 en 19 217 órdenes (coherente con exoneraciones, p. ej. Amazonía); 394 órdenes con tasa de IGV distinta de 0 % y 18 % (marcadas como advertencia).
5. Montos: TOTAL entre S/ 0,05 y S/ 44 580 302,67; TOTAL = SUB_TOTAL + IGV en el 100 % de los registros.

## Municipalidad Provincial de Trujillo

* `ENTIDAD` = `MUNICIPALIDAD PROVINCIAL DE TRUJILLO`, `RUC_ENTIDAD` = **20175639391** (un solo nombre para ese RUC en todo el período).
* Órdenes en 2026-08 (archivo inspeccionado): **13**. Total 2022-01 a 2026-09: **1 231** órdenes.
* Órdenes de la MPT en estado adverso a la fecha de corte: **0**.

## Enlaces a documentos (verificados)

* `ORDEN_ELECTRÓNICA_GENERADA` (apps1.perucompras.gob.pe): PDF con texto nativo; incluye **detalle por ítem con cantidad y precio unitario**, además de datos personales (representante legal, correo, teléfono, DNI del responsable de recepción).
* `ORDEN_DIGITALIZADA` (blob de Azure de Perú Compras): PDF escaneado (imagen); requiere OCR.
* Resultados de la verificación sistemática en `docs/evidencias/verificacion_enlaces.csv` (Paso 8).
