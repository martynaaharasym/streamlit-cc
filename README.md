# Connected Components Streamlit App (PySpark)

This project demonstrates how to compute **connected components** in undirected graphs using **PySpark**.  
It comes with a simple **Streamlit web app** and three **bundled graph datasets**.  
The whole project is containerized with Docker and tested with pytest + GitHub Actions CI.

---

## 📖 What is this about?

A **connected component** in a graph is a set of nodes where each node is reachable from any other node in the same set, and there are no edges to nodes outside the set.  
This project:

- Loads a graph from a bundled `.txt.gz` edge list (`u v` pairs).  
- Runs a Spark-based **label propagation algorithm** to find connected components.  
- Shows results in a Streamlit interface, including progress, statistics, and a downloadable mapping (node → component).  

Three synthetic datasets are provided under `data/`:

- **mid_a.txt.gz** — chain + star → 2 components  
- **mid_b.txt.gz** — ring + chain → 2 components  
- **mid_c.txt.gz** — ladder + triangle + chain → 3 components  

---

## 🚀 Quickstart

```bash
git clone https://github.com/YOUR_USERNAME/streamlit-cc.git
cd streamlit-cc
pip install -r requirements.txt
streamlit run app/streamlit_app.py
```
Open the app at http://localhost:8501.

The code below runs unit tests on the dataset loaders and connected components algorithm.

Troubleshooting: If you see “Python in worker has different version …”, ensure you activated the venv and that we set PYSPARK_PYTHON/PYSPARK_DRIVER_PYTHON to the current interpreter (already handled in tests/conftest.py and in the app).


```bash
python -c "import sys; print(sys.version)"
python -c "import pyspark, sys; print('PySpark', pyspark.__version__, 'Python', sys.executable)"
pytest -q
```
