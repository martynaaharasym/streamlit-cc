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

## ✅ Requirements
- **Python 3.11** (recommended)
- **Java 17** (required by PySpark)  
  Check with: `java -version`
  
---

## 🚀 Quickstart

```bash
git clone https://github.com/martynaaharasym/streamlit-cc.git
cd streamlit-cc
python -m venv .venv
```

Activate the virtual environment:

**macOS/Linux:**
```bash
source .venv/bin/activate
```
**Windows (PowerShell):**
```bash
.venv\Scripts\Activate.ps1
```

```bash
pip install -r requirements.txt
streamlit run app/streamlit_app.py
```
Open the app at http://localhost:8501.

## 🧪 Run tests
The code below runs unit tests on the dataset loaders and connected components algorithm.

```bash
pytest -q
```
## 🔧 Troubleshooting

If you see “Python in worker has different version …”, ensure you activated the venv and that PYSPARK_PYTHON/PYSPARK_DRIVER_PYTHON is set to the current interpreter (already handled in tests/conftest.py and in the app).

## 🐳 Docker
```bash
docker build -t streamlit-cc:local .
docker run --rm -p 8501:8501 streamlit-cc:local
```


