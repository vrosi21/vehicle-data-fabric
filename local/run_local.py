"""Run notebooks/vehicle_medallion.ipynb on a laptop before uploading it to Fabric.

Uses local PySpark, reads data/raw/ instead of the lakehouse, and registers temp views
instead of writing Delta tables. Exits non-zero if the notebook's quality gate fails.

    python local/run_local.py
"""
import json
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession

ROOT = Path(__file__).resolve().parents[1]
NOTEBOOK = ROOT / "notebooks" / "vehicle_medallion.ipynb"

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
spark = (SparkSession.builder.master("local[2]").appName("vehicle-local")
         .config("spark.sql.shuffle.partitions", "4")
         .config("spark.ui.enabled", "false").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")


def display(df, n=40):
    df.show(n, truncate=False)


env = {"spark": spark, "display": display}
cells = [c for c in json.loads(NOTEBOOK.read_text(encoding="utf-8"))["cells"] if c["cell_type"] == "code"]
for i, cell in enumerate(cells):
    src = "".join(cell["source"])
    if i == 0:  # parameter cell: point at local data, skip Delta writes
        src = f'BRONZE_PATH = "{(ROOT / "data" / "raw").as_posix()}"\nLOCAL_TEST = True'
    print(f"\n--- cell {i} ---")
    exec(compile(src, f"cell{i}", "exec"), env)

spark.stop()
