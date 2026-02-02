from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from delta import configure_spark_with_delta_pip

# ---------------------------
# Spark + Delta configuration
# ---------------------------
builder = (
    SparkSession.builder
    .appName("Lab_SECOP_Silver")
    .master("spark://spark-master:7077")
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# ---------------------------
# Read Bronze
# ---------------------------
print("Leyendo capa Bronce...")
df_bronze = spark.read.format("delta").load(
    "/app/data/lakehouse/bronze/secop"
)

print(f"Registros en Bronce: {df_bronze.count()}")

# ---------------------------
# Quality Gate
# Reglas:
# - precio_base > 0
# - fecha_de_firma no nula
# ---------------------------
print("Aplicando Quality Gate...")

valid_condition = (
    col("precio_base").isNotNull() &
    (col("precio_base") > 0) &
    col("fecha_de_firma").isNotNull()
)

df_valid = df_bronze.filter(valid_condition)

df_invalid = df_bronze.filter(~valid_condition) \
    .withColumn(
        "motivo_rechazo",
        lit("precio_base <= 0 o fecha_de_firma es nula")
    )

print(f"Registros válidos (Silver): {df_valid.count()}")
print(f"Registros inválidos (Quarantine): {df_invalid.count()}")

# ---------------------------
# Transformaciones Silver
# ---------------------------
print("Transformando datos Silver...")
df_silver = df_valid.dropDuplicates()

# ---------------------------
# Write Silver
# ---------------------------
print("Escribiendo capa Silver...")
df_silver.write.format("delta").mode("overwrite").save(
    "/app/data/lakehouse/silver/secop"
)

# ---------------------------
# Write Quarantine
# ---------------------------
print("Escribiendo capa Quarantine...")
df_invalid.write.format("delta").mode("overwrite").save(
    "/app/data/lakehouse/quarantine/secop_errors"
)

print("Transformación Silver + Quarantine completada")




