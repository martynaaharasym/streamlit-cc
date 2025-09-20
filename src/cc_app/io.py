from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split
from pyspark.sql.types import IntegerType

def load_edge_list_from_gz(spark: SparkSession, path_str: str):
    df_text = spark.read.text(path_str)
    df_clean = df_text.filter(~col("value").startswith("#"))
    parts = split(col("value"), r"\s+")
    return (
        df_clean
        .withColumn("key", parts.getItem(0).cast(IntegerType()))
        .withColumn("val", parts.getItem(1).cast(IntegerType()))
        .select("key", "val")
    )
