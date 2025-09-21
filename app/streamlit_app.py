# app/streamlit_app.py

import os, sys
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

import time
from pathlib import Path
import gzip

import streamlit as st
st.set_page_config(page_title="Connected Components (PySpark)", layout="wide")  # MUST be first Streamlit call

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, split, collect_set, array_min, least, array, explode, concat, min as spark_min
)
from pyspark.sql.types import IntegerType

# ---------------------------
# Spark setup (local)
# ---------------------------
@st.cache_resource
def get_spark():
    return (
        SparkSession.builder
        .appName("ConnectedComponentsApp")
        .master("local[*]")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )

spark = get_spark()

# ---------------------------
# CCF helpers (PySpark DataFrame based)
# ---------------------------
def ccf_iterate_mapper_df(df):
    flipped = df.select(col("val").alias("key"), col("key").alias("val"))
    return df.unionByName(flipped)

def ccf_iterate_reducer_df(df):
    df_g = (
        df.groupBy("key")
          .agg(collect_set("val").alias("valset"))
          .withColumn("min", least(col("key"), array_min(col("valset"))))
          .filter(col("key") != col("min"))
    )
    new_df = (
        df_g
        .select(
            col("min").alias("a_min"),
            concat(array(col("key")), col("valset")).alias("valueList")
        )
        .withColumn("valueList", explode("valueList"))
        .filter(col("a_min") != col("valueList"))
        .select(col("a_min").alias("key"), col("valueList").alias("val"))
    )
    return new_df

def ccf_dedup_df(df):
    return df.filter(col("key") > col("val"))

def compute_cc_df(df, max_iters=50, dedup=False, verbose=True):
    df = df.select(col("key").cast(IntegerType()).alias("key"),
                   col("val").cast(IntegerType()).alias("val")).distinct()
    prev_df = df
    start = time.time()

    for it in range(1, max_iters + 1):
        iter_start = time.time()
        mapped = ccf_iterate_mapper_df(prev_df)
        reduced = ccf_iterate_reducer_df(mapped)
        nxt = reduced.distinct()
        if dedup:
            nxt = ccf_dedup_df(nxt).distinct()

        delta = nxt.exceptAll(prev_df).count()
        if verbose:
            st.write(f"Iteration {it}: Δ = {delta} new pairs (took {time.time() - iter_start:.2f}s)")
        if delta == 0:
            final_df = nxt.groupBy("key").agg(spark_min(col("val")).alias("component")).orderBy("key")
            if verbose:
                st.write(f"Converged in {it} iterations ({time.time() - start:.2f}s total).")
            return final_df
        prev_df = nxt

    st.warning("Reached max iterations without full convergence; returning current labels.")
    return prev_df.groupBy("key").agg(spark_min(col("val")).alias("component")).orderBy("key")

# ---------------------------
# File loading
# ---------------------------
def load_edge_list_from_gz(spark, path_str: str):
    """
    Reads a .gz text edge list with lines like: "u v" (ints), ignores lines starting with '#'.
    Returns DataFrame with columns: key (int), val (int).
    """
    df_text = spark.read.text(path_str)
    df_clean = df_text.filter(~col("value").startswith("#"))
    parts = split(col("value"), r"\s+")
    df_edges = (
        df_clean
        .withColumn("key", parts.getItem(0).cast(IntegerType()))
        .withColumn("val", parts.getItem(1).cast(IntegerType()))
        .select("key", "val")
    )
    return df_edges

# ---------------------------
# Streamlit UI
# ---------------------------
st.title("Connected Components on Prebundled Graphs")

st.markdown("""
This app demonstrates how to find **connected components** in graphs using **PySpark**.

---

### 🔍 What is a connected component?
A connected component in an **undirected graph** is a set of nodes where each node is reachable from any other node in the same set, and there are **no edges** connecting to nodes outside the set.

- Example: If we have edges `1-2-3` and separately `10-11`, then `{1,2,3}` is one component and `{10,11}` is another.

---

### How does this app work?
1. **Reads a graph** stored as an edge list (pairs of integers `u v`) from one of the bundled `.txt.gz` files.  
2. **Builds a Spark DataFrame** of edges and iteratively propagates the smallest node ID (label) across neighbors.  
3. Repeats until all nodes in the same connected component share the same label.  
4. Returns a mapping: **node → component ID**.

---

### Why is this useful?
- In **social networks**, connected components reveal isolated communities.  
- In **web graphs**, they identify clusters of pages that link to each other.  
- In **mobility/transport networks**, components can show disconnected areas.  

Understanding components is a fundamental step in graph analysis.

---

### How to use the app
- Select one of the three bundled datasets from the sidebar.  
- Click **Run connected components** to process the graph with Spark.  
- The app will display:
  - The number of edges and nodes loaded  
  - Sample rows of the edge list
  - Iteration progress until convergence  
  - The number of connected components detected  
  - A preview of the mapping (node → component)  
  - Option to download the full mapping as CSV  

---
""")

st.markdown("Pick one of the preconfigured `.gz` files containing an edge list (`u v` per line), then run CC.")

# Resolve data dir relative to repo root
REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = REPO_ROOT / "data"

DEFAULT_FILES = {
    "Mid A (chain + star, 2 components)": str(DATA_DIR / "mid_a.txt.gz"),
    "Mid B (ring + chain, 2 components)": str(DATA_DIR / "mid_b.txt.gz"),
    "Mid C (ladder + triangle + chain, 3 components)": str(DATA_DIR / "mid_c.txt.gz"),
}

choice = st.radio("Choose an input file", list(DEFAULT_FILES.keys()), index=0)
path_chosen = DEFAULT_FILES[choice]
st.code(f"Selected path: {path_chosen}")

if st.button("Run connected components"):
    if not Path(path_chosen).exists():
        st.error("Selected path does not exist. Put your file under the `data/` folder or choose another.")
    else:
        with st.spinner("Reading edges and computing components..."):
            t0 = time.time()
            edges_df = load_edge_list_from_gz(spark, path_chosen)
            n_edges = edges_df.count()
            n_nodes = edges_df.select("key").union(edges_df.select("val")).distinct().count()
            st.write(f"Loaded **{n_edges}** edges over **{n_nodes}** nodes.")
            st.dataframe(edges_df.limit(10).toPandas())

            comp_df = compute_cc_df(edges_df, max_iters=50, dedup=False, verbose=True)
            n_components = comp_df.select("component").distinct().count()
            st.success(f"Found **{n_components}** connected components in {time.time() - t0:.2f}s.")

            st.subheader("Sample of node → component mapping")
            st.dataframe(comp_df.limit(100).toPandas())

            try:
                pdf = comp_df.toPandas()
                st.download_button(
                    "Download full components as CSV",
                    data=pdf.to_csv(index=False).encode("utf-8"),
                    file_name="components.csv",
                    mime="text/csv",
                )
            except Exception as e:
                st.info("Dataset seems large; skipping auto CSV conversion.")
                st.exception(e)
