
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import functions as F

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

spark.conf.set(
    "spark.sql.catalog.lakehouse",
    "org.apache.iceberg.spark.SparkCatalog"
)
spark.conf.set("spark.sql.catalog.lakehouse.type", "hadoop")
spark.conf.set(
    "spark.sql.catalog.lakehouse.warehouse",
    "s3a://ucv-data-platform/products"
)

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

gold = spark.table("lakehouse.gold.product_category_summary")

invalid_count = gold.filter(
    F.col("product_count").isNull() |
    (F.col("product_count") <= 0)
).count()

print("=== VALIDACION PRODUCT_COUNT ===")
print(f"Total categories: {gold.count()}")
print(f"Invalid product_count: {invalid_count}")

print(
    "VALIDATION PASSED"
    if invalid_count == 0
    else "VALIDATION FAILED"
)

spark.stop()
