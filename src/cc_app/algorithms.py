from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, collect_set, array_min, least, array, explode, concat,
    min as spark_min, size, when
)
from pyspark.sql.types import IntegerType

def ccf_iterate_mapper_df(df: DataFrame) -> DataFrame:
    flipped = df.select(col("val").alias("key"), col("key").alias("val"))
    return df.unionByName(flipped)

def ccf_iterate_reducer_df(df: DataFrame) -> DataFrame:
    g = df.groupBy("key").agg(collect_set("val").alias("valset"))
    min_label = least(
        col("key"),
        when(size(col("valset")) > 0, array_min(col("valset"))).otherwise(col("key"))
    ).alias("min_label")

    df_g = g.select("key", "valset", min_label)

    expanded = (
        df_g
        .select(
            col("min_label"),
            concat(array(col("key")), col("valset")).alias("valueList")
        )
        .withColumn("valueList", explode("valueList"))
        .filter(col("min_label") != col("valueList"))
        .select(col("min_label").alias("key"), col("valueList").alias("val"))
    )
    return expanded

def compute_cc_df(df: DataFrame, max_iters: int = 50, dedup: bool = False) -> DataFrame:
    prev_df = (
        df.select(
            col("key").cast(IntegerType()).alias("key"),
            col("val").cast(IntegerType()).alias("val"),
        ).distinct()
    )

    for _ in range(max_iters):
        mapped = ccf_iterate_mapper_df(prev_df)
        reduced = ccf_iterate_reducer_df(mapped)
        nxt = reduced.select("key", "val").distinct()

        if dedup:
            nxt = nxt.filter(col("key") > col("val")).distinct()

        # New rows in nxt that weren't in prev_df
        delta = (
            nxt.alias("n")
               .join(prev_df.alias("p"), on=["key", "val"], how="left_anti")
               .count()
        )

        if delta == 0:
            # At convergence, pairs are (min_label -> node).
            # Build node->component:
            labels_from_edges = nxt.select(col("val").alias("node"), col("key").alias("label"))
            self_labels = nxt.select(col("key").alias("node"), col("key").alias("label"))
            all_labels = labels_from_edges.unionByName(self_labels)

            return (
                all_labels.groupBy("node")
                          .agg(spark_min(col("label")).alias("component"))
                          .withColumnRenamed("node", "key")
                          .orderBy("key")
            )

        prev_df = nxt

    # Fallback if not converged: same mapping logic as above
    labels_from_edges = prev_df.select(col("val").alias("node"), col("key").alias("label"))
    self_labels = prev_df.select(col("key").alias("node"), col("key").alias("label"))
    all_labels = labels_from_edges.unionByName(self_labels)
    return (
        all_labels.groupBy("node")
                  .agg(spark_min(col("label")).alias("component"))
                  .withColumnRenamed("node", "key")
                  .orderBy("key")
    )
