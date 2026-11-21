
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import functions as F

SILVER_TABLE = "lakehouse.silver.products"

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

# Configurar catálogo Iceberg
spark.conf.set(
    "spark.sql.catalog.lakehouse",
    "org.apache.iceberg.spark.SparkCatalog"
)
spark.conf.set("spark.sql.catalog.lakehouse.type", "hadoop")
spark.conf.set(
    "spark.sql.catalog.lakehouse.warehouse",
    "s3a://ucv-data-platform/products"
)

# Configurar conexión S3 con Floci
hconf = sc._jsc.hadoopConfiguration()
hconf.set("fs.s3a.endpoint", "http://floci:4566")
hconf.set("fs.s3a.path.style.access", "true")
hconf.set("fs.s3a.connection.ssl.enabled", "false")
hconf.set("fs.s3a.access.key", "test")
hconf.set("fs.s3a.secret.key", "test")
hconf.set(
    "fs.s3a.aws.credentials.provider",
    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
)

# Leer tabla Silver
silver = spark.table(SILVER_TABLE)

# Validar identificadores nulos
null_ids = silver.filter(
    F.col("product_id").isNull()
).count()

# Validar precios nulos o menores/iguales a cero
invalid_prices = silver.filter(
    F.col("price").isNull() | (F.col("price") <= 0)
).count()

print("=== VALIDACION SILVER ===")
print(f"Total products: {silver.count()}")
print(f"Null product_id: {null_ids}")
print(f"Invalid prices: {invalid_prices}")

print(
    "VALIDATION PASSED"
    if null_ids == 0 and invalid_prices == 0
    else "VALIDATION FAILED"
)

spark.stop()

