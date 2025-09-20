import gzip
from pathlib import Path
from pyspark.sql import SparkSession
from cc_app.io import load_edge_list_from_gz

def test_load_edge_list_from_gz(tmp_path: Path):
    # create a tiny gz file
    p = tmp_path / "edges.txt.gz"
    with gzip.open(p, "wt") as f:
        f.write("# comment\n1 2\n2 3\n")
    spark = (SparkSession.builder.master("local[*]").appName("test").getOrCreate())
    try:
        df = load_edge_list_from_gz(spark, str(p))
        assert df.count() == 2
        assert set(df.columns) == {"key", "val"}
    finally:
        spark.stop()
