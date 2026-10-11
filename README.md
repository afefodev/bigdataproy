# PrecioJusto — Pipeline de datos (Sección 3)

Alerta temprana de órdenes de compra atípicas por Acuerdos Marco para la
Municipalidad Provincial de Trujillo (MPT). Curso Big Data y Analítica de Datos,
UPAO 2026-20.

> README en construcción: se completa en el Paso 11 con el orden de ejecución.

## Requisitos verificados (Paso 1)
- Python 3.12 · PySpark 3.5.8 · PyArrow 17.0.0
- Java 17 (`brew install openjdk@17`). Java ≥ 24 no es compatible con Spark 3.5.
- Spark en modo local (`local[*]`); en el clúster del curso cambiar `spark.master` a `yarn`.

## Instalación
```bash
python3.12 -m venv .venv
.venv/bin/pip install -r requirements.txt
cp .env.example .env                       # completar JAVA_HOME y PRECIOJUSTO_SALT
cp config/config.example.yaml config/config.yaml
.venv/bin/python -m scripts.check_env      # prueba de humo
```

## Estructura
```
src/        código del pipeline (jobs)
scripts/    utilidades y orquestación
config/     configuración (rutas, Spark, fuentes) y reglas de calidad
notebooks/  exploración
docs/       documentación y borradores del informe
datalake/   raw / cuarentena / processed / consumo / escalabilidad  (no versionado)
```
