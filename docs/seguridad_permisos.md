# Seguridad de la plataforma — implementado y propuesto para producción (3.8)

Ruta A (on-premise). Lo **implementado** se ejecuta en este repositorio; lo
**propuesto** es cómo se configuraría en el clúster HDFS/YARN de producción.

## 1. Implementado en este proyecto

| Medida | Cómo | Evidencia |
|---|---|---|
| Seudonimización de personas naturales | HMAC-SHA256 con clave secreta sobre `ruc_proveedor` (RUC 10/15/17); el nombre se sustituye por `PERSONA NATURAL PN-<token>` | `src/seguridad/seudonimizacion.py`; `docs/evidencias/metricas_consumo.json`: 865 RUC → 865 seudónimos, 0 colisiones, 0 en claro |
| Verificación automática de fugas | El job de consumo **falla** si encuentra un RUC 10/15/17 o un nombre de persona natural en claro en cualquier tabla de consumo | `src/consumo/construir_consumo.py` |
| Minimización | La capa de consumo no incluye enlaces a PDF ni texto completo de resoluciones; de los PDF de órdenes solo se extraen campos de la contratación y de los ítems (no nombres, DNI, correos ni teléfonos) | `COLUMNAS_CONSUMO`, `src/pdf/extraer_ordenes.py` |
| Gestión de secretos | La clave del HMAC vive en `.env` (excluido por `.gitignore`); el job se niega a correr si la clave falta o tiene menos de 32 caracteres | `obtener_clave()` |
| Configuración fuera del código | Rutas, motor y fuentes en `config/config.yaml` (no versionado); plantilla en `config.example.yaml` | `src/config.py` |
| Integridad de la zona cruda | SHA-256 de cada archivo en el manifiesto; la ingesta **aborta** si un archivo crudo cambió | `_manifiesto.csv`, `fuente1_ordenes.py` |
| Integridad entre fuentes | TOTAL del CSV = IMPORTE TOTAL del PDF en las 1 231 órdenes MPT | `docs/evidencias/integracion_resumen.csv` |
| Escrituras atómicas | Temporal + renombrado: nunca queda un Parquet a medio escribir | `src/common/spark_io.py` |
| Cifrado en tránsito (ingesta) | Todas las descargas por HTTPS (datosabiertos.gob.pe, gob.pe, perucompras.gob.pe) | URLs en los manifiestos |
| Cifrado en reposo (equipo de desarrollo) | Disco del equipo anfitrión con FileVault (macOS) | Configuración del sistema operativo |

### Riesgo residual de reidentificación (para la sección 5.1)

* El seudónimo es estable: permite seguir a un mismo proveedor en el tiempo (necesario para medir concentración) y, combinado con fecha, entidad y monto, alguien con acceso al CSV público original podría volver a enlazarlo. La protección evita exponer el dato en **nuestros** productos (tablero, informe, capturas); no puede deshacer una publicación que ya hizo Perú Compras.
* Si la clave se filtra, se puede comprobar por fuerza bruta qué DNI corresponde a cada token. Mitigación: rotar la clave (cambia todos los seudónimos; se regenera la capa de consumo desde la zona procesada).

## 2. Propuesta para producción (HDFS + YARN)

### 2.1 Identidades y grupos

| Identidad | Tipo | Función |
|---|---|---|
| `svc_ingesta` | cuenta de servicio | Ejecuta las etapas de ingesta |
| `svc_pipeline` | cuenta de servicio | Ejecuta limpieza, extracción, integración y consumo |
| `grp_ingenieria` | grupo | Ingenieros de datos del equipo |
| `grp_analitica` | grupo | Analistas y modelado (sección 4) |
| `svc_tablero` | cuenta de servicio | Tablero de la Subgerencia de Abastecimiento |

Autenticación con **Kerberos** (`hadoop.security.authentication=kerberos`): sin
ella, HDFS confía en el nombre de usuario que declara el cliente.

### 2.2 Permisos por zona (principio de mínimo privilegio)

| Zona | `svc_ingesta` | `svc_pipeline` | `grp_ingenieria` | `grp_analitica` | `svc_tablero` | Contiene datos personales |
|---|---|---|---|---|---|---|
| `/datalake/raw` | lectura y escritura (solo agrega) | lectura | lectura | — | — | **Sí** (PDF de órdenes, nombres en CSV) |
| `/datalake/cuarentena` | — | lectura y escritura | lectura | — | — | Sí |
| `/datalake/processed` | — | lectura y escritura | lectura | — | — | Sí (nombres y RUC de personas naturales) |
| `/datalake/consumo` | — | lectura y escritura | lectura | **lectura** | **lectura** | No (seudonimizado) |
| `/datalake/escalabilidad` | — | lectura y escritura | lectura y escritura | — | — | Sí (réplica temporal; se borra) |

Comandos de referencia:

```bash
hdfs dfs -mkdir -p /datalake/{raw,cuarentena,processed,consumo,escalabilidad}
hdfs dfs -chown -R svc_ingesta:grp_ingenieria /datalake/raw
hdfs dfs -chown -R svc_pipeline:grp_ingenieria /datalake/{cuarentena,processed,consumo,escalabilidad}
hdfs dfs -chmod -R 750 /datalake/{raw,cuarentena,processed}   # dueño rwx, grupo r-x, otros nada
hdfs dfs -chmod -R 750 /datalake/consumo
hdfs dfs -setfacl -m user:svc_pipeline:r-x /datalake/raw               # el pipeline lee la zona cruda
hdfs dfs -setfacl -R -m group:grp_analitica:r-x /datalake/consumo      # analistas: solo consumo
hdfs dfs -setfacl -R -m user:svc_tablero:r-x /datalake/consumo         # tablero: solo consumo
hdfs dfs -setfacl -R -m default:group:grp_analitica:r-x /datalake/consumo  # se hereda a archivos nuevos
```

Inmutabilidad de la zona cruda: además de los permisos, **instantáneas** HDFS
(`hdfs dfsadmin -allowSnapshot /datalake/raw` y `hdfs dfs -createSnapshot`) tras
cada ingesta, para poder demostrar que el dato original no cambió.

### 2.3 Cifrado

| Capa | Medida | Configuración |
|---|---|---|
| En reposo (HDFS) | Zonas de cifrado transparente con Hadoop KMS para `raw`, `cuarentena` y `processed` | `hadoop key create pj_key`; `hdfs crypto -createZone -keyName pj_key -path /datalake/raw` |
| En tránsito (HDFS) | Cifrado de RPC y de transferencia de bloques | `hadoop.rpc.protection=privacy`, `dfs.encrypt.data.transfer=true` |
| En tránsito (Spark) | Autenticación y cifrado entre procesos de Spark | `spark.authenticate=true`, `spark.network.crypto.enabled=true` |
| En reposo (Spark) | Cifrado de los archivos temporales de shuffle y de derrame | `spark.io.encryption.enabled=true` |
| Fuentes externas | Solo HTTPS con verificación de certificado (valor por defecto de `requests`) | — |

### 2.4 Credenciales

* La clave del HMAC y cualquier credencial pasan de `.env` a un gestor de secretos (HashiCorp Vault o el KMS del clúster), entregados como variable de entorno solo al proceso `svc_pipeline`.
* Keytabs de Kerberos con permisos `400`, solo legibles por su cuenta de servicio.
* Rotación anual de la clave de seudonimización (o inmediata ante sospecha de filtración), seguida de la regeneración de la capa de consumo.

### 2.5 Auditoría

* Registro de auditoría del NameNode (`hdfs-audit.log`): quién leyó o escribió cada ruta.
* Los logs del pipeline (`logs/pipeline_*.log`) y los manifiestos con SHA-256 permiten rastrear un valor del tablero hasta su archivo de origen (linaje, sección 5.2).

### 2.6 Equivalencia en la Ruta B (referencia)

Zonas = prefijos de un bucket S3 con cifrado SSE-KMS; permisos con políticas IAM por rol
(el rol del tablero solo tiene `s3:GetObject` sobre `consumo/*`); la clave del HMAC en
AWS Secrets Manager; auditoría con CloudTrail.
