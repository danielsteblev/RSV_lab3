import os
import signal
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pytest
from pymongo import MongoClient

ROOT = Path(__file__).resolve().parents[1]
WORKER_MAIN = ROOT / "worker_py" / "main.py"
DEFAULT_TEST_URL = "mongodb://labuser:labpass@127.0.0.1:27017/labdb_test?authSource=admin"


def _mongo_available(mongo_url: str) -> bool:
    try:
        client = MongoClient(mongo_url, serverSelectionTimeoutMS=1500)
        client.admin.command("ping")
        client.close()
        return True
    except Exception:
        return False


@pytest.mark.integration
def test_no_duplicates():
    mongo_url = os.getenv("MONGO_TEST_URL", DEFAULT_TEST_URL)
    if not _mongo_available(mongo_url):
        pytest.skip("MongoDB test instance is not available")

    db_name = f"labdb_test_{uuid.uuid4().hex[:8]}"
    isolated_url = mongo_url.replace("/labdb_test", f"/{db_name}")
    client = MongoClient(isolated_url)
    db = client.get_default_database()

    db.social_nodes.insert_many(
        [
            {"node_id": "a", "community": "x"},
            {"node_id": "b", "community": "x"},
            {"node_id": "c", "community": "y"},
            {"node_id": "d", "community": "y"},
        ]
    )
    db.social_edges.insert_many(
        [
            {"source": "a", "target": "b"},
            {"source": "b", "target": "a"},
            {"source": "b", "target": "c"},
            {"source": "c", "target": "b"},
            {"source": "c", "target": "d"},
            {"source": "d", "target": "c"},
        ]
    )
    db.graph_tasks.insert_many(
        [
            {
                "payload": {"root_node": "a", "depth_limit": 2, "community_hint": "x"},
                "status": "pending",
                "worker_id": None,
                "processing_until": None,
                "attempts": 0,
                "priority": 1,
                "created_at": datetime.now(timezone.utc),
                "updated_at": datetime.now(timezone.utc),
            },
            {
                "payload": {"root_node": "b", "depth_limit": 2, "community_hint": "x"},
                "status": "pending",
                "worker_id": None,
                "processing_until": None,
                "attempts": 0,
                "priority": 1,
                "created_at": datetime.now(timezone.utc),
                "updated_at": datetime.now(timezone.utc),
            },
            {
                "payload": {"root_node": "c", "depth_limit": 3, "community_hint": "y"},
                "status": "pending",
                "worker_id": None,
                "processing_until": None,
                "attempts": 0,
                "priority": 1,
                "created_at": datetime.now(timezone.utc),
                "updated_at": datetime.now(timezone.utc),
            },
        ]
    )

    processes = []
    metrics_files = []
    try:
        for worker_id in ("test-w1", "test-w2", "test-w3"):
            env = os.environ.copy()
            metrics_file = ROOT / f"{worker_id}.metrics.log"
            metrics_files.append(metrics_file)
            env.update(
                {
                    "DB_URL": isolated_url,
                    "WORKER_ID": worker_id,
                    "EMPTY_POLLS_BEFORE_EXIT": "3",
                    "POLL_INTERVAL": "0.2",
                    "METRICS_FILE": str(metrics_file),
                }
            )
            processes.append(subprocess.Popen([sys.executable, str(WORKER_MAIN)], env=env, cwd=str(ROOT / "worker_py")))

        deadline = time.time() + 25
        while time.time() < deadline:
            done_count = db.graph_tasks.count_documents({"status": "done"})
            if done_count == 3:
                break
            time.sleep(0.5)

        for process in processes:
            process.wait(timeout=20)

        assert db.graph_tasks.count_documents({"status": "done"}) == 3
        results = list(db.graph_results.find({}, {"_id": 0, "task_id": 1}))
        task_ids = [str(result["task_id"]) for result in results]
        assert len(task_ids) == 3
        assert len(task_ids) == len(set(task_ids))
    finally:
        for process in processes:
            if process.poll() is None:
                process.send_signal(signal.SIGTERM)
                process.wait(timeout=10)
        for metrics_file in metrics_files:
            if metrics_file.exists():
                metrics_file.unlink()
        client.drop_database(db_name)
        client.close()
