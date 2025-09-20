from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, StructType, StructField
from cc_app.algorithms import compute_cc_df

def test_compute_cc_df_two_components():
    spark = (SparkSession.builder.master("local[*]").appName("test").getOrCreate())
    try:
        schema = StructType([StructField("key", IntegerType()), StructField("val", IntegerType())])
        data = [(1,2),(2,3),(10,11)]
        df = spark.createDataFrame(data, schema)
        comp = compute_cc_df(df, max_iters=20)
        # nodes 1,2,3 should share the same component; 10,11 another
        rows = comp.collect()
        comps = {}
        for r in rows:
            comps.setdefault(r["component"], set()).add(r["key"])
        assert {1,2,3} in comps.values()
        assert {10,11} in comps.values()
        assert len(comps) == 2
    finally:
        spark.stop()
