from pyspark.sql import SparkSession
from delta import *
import re

# ===============================
# CONFIGURACIÓN SPARK + DELTA
# ===============================
master_url = "spark://spark-master:7077"

builder = (
    SparkSession.builder
    .appName("Lab_SECOP_Bronze")
    .master(master_url)
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.executor.memory", "1g")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# ===============================
# LECTURA CSV
# ===============================
print("Leyendo CSV crudo...")

df_raw = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("file:/app/data/SECOP_II_Contratos_Electronicos.csv")
)

# ===============================
# LIMPIEZA DE COLUMNAS
# ===============================
def clean_columns(df):
    for c in df.columns:
        clean_c = re.sub(r"[ ,;{}()\n\t=]", "_", c)
        df = df.withColumnRenamed(c, clean_c.lower())
    return df

df_raw = clean_columns(df_raw)

# ===============================
# ESCRITURA DELTA (BRONZE)
# ===============================
print("Escribiendo en capa Bronce...")

output_path = "/app/data/lakehouse/bronze/secop"

(
    df_raw.write
    .format("delta")
    .mode("overwrite")
    .option("delta.columnMapping.mode", "name")
    .save(output_path)
)

print(f"Ingesta completada. Registros procesados: {df_raw.count()}")
