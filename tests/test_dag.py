import os
os.environ.setdefault("AIRFLOW_VAR_SENSOR_TIMEOUT_SEC", "600")

import re
import unittest
from pathlib import Path

from airflow.models import DagBag

DAG_FOLDER = "/opt/airflow/dags"
DAG_ID = "retail_etl"


class TestRetailEtlDag(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.bag = DagBag(dag_folder=DAG_FOLDER)
        cls.dag = cls.bag.get_dag(DAG_ID)

    def test_dag_parses(self):
        errors = {k: v for k, v in self.bag.import_errors.items() if "retail_etl_dag" in k}
        self.assertEqual(errors, {}, f"Import errors: {errors}")
        self.assertIsNotNone(self.dag, "DAG retail_etl was not found")

    def test_no_cycles(self):
        graph = {t.task_id: set(t.downstream_task_ids) for t in self.dag.tasks}
        indegree = {n: 0 for n in graph}
        for targets in graph.values():
            for t in targets:
                indegree[t] += 1
        queue = [n for n, d in indegree.items() if d == 0]
        seen = 0
        while queue:
            node = queue.pop()
            seen += 1
            for t in graph[node]:
                indegree[t] -= 1
                if indegree[t] == 0:
                    queue.append(t)
        self.assertEqual(seen, len(graph), "The DAG contains a cycle")

    def test_retries_set(self):
        for task in self.dag.tasks:
            self.assertGreaterEqual(task.retries, 1, f"{task.task_id} has no retries")

    def test_expected_stages_exist(self):
        ids = {t.task_id for t in self.dag.tasks}
        for needed in ("wait_for_files", "load_bronze", "silver_sales", "gold_dim_item",
                       "gold_dim_locn", "gold_fct_sales", "gold_fct_inventory", "reconcile"):
            self.assertIn(needed, ids)

    def test_no_hardcoded_credentials(self):
        text = Path(DAG_FOLDER, "retail_etl_dag.py").read_text(encoding="utf-8")
        self.assertIsNone(re.search(r"password\s*=", text, re.I))
        self.assertNotIn("/opt/airflow/", text.replace("# ", ""))


if __name__ == "__main__":
    unittest.main()