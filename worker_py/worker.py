import json
import logging
import os
import signal
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from pymongo import ASCENDING, DESCENDING, MongoClient, ReturnDocument
from pymongo.database import Database

load_dotenv()
load_dotenv(Path(__file__).resolve().parents[1] / ".env")

DEFAULT_WORKER_ID = os.getenv("HOSTNAME") or os.getenv("COMPUTERNAME") or "py-w1"
WORKER_ID = os.getenv("WORKER_ID", DEFAULT_WORKER_ID)
DB_URL = os.getenv("DB_URL", "mongodb://labuser:labpass@localhost:27017/labdb?authSource=admin")
LEASE_SECONDS = int(os.getenv("LEASE_SECONDS", "30"))
POLL_INTERVAL = float(os.getenv("POLL_INTERVAL", "1"))
MAX_DEPTH_DEFAULT = int(os.getenv("MAX_DEPTH_DEFAULT", "2"))
EMPTY_POLLS_BEFORE_EXIT = int(os.getenv("EMPTY_POLLS_BEFORE_EXIT", "0"))
METRICS_FILE = os.getenv("METRICS_FILE", os.path.join(os.getcwd(), "metrics.log"))

logging.basicConfig(
    level=logging.INFO,
    format=f"%(asctime)s [{WORKER_ID}] %(levelname)s %(message)s",
)
LOGGER = logging.getLogger("graph-worker")

shutdown_requested = False


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def emit_metric(event: str, **fields: Any) -> None:
    payload = {"ts": utc_now().isoformat(), "worker_id": WORKER_ID, "event": event, **fields}
    with open(METRICS_FILE, "a", encoding="utf-8") as metrics_file:
        metrics_file.write(json.dumps(payload, ensure_ascii=True) + "\n")


def make_bson_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): make_bson_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [make_bson_safe(item) for item in value]
    if isinstance(value, tuple):
        return [make_bson_safe(item) for item in value]
    return value


def handle_signal(signum: int, _frame: Any) -> None:
    global shutdown_requested
    shutdown_requested = True
    LOGGER.info("Received signal %s, worker will stop after current task", signum)
    emit_metric("signal", signal=signum)



# подключение к БД
def connect_to_db() -> tuple[MongoClient, Database]:
    client = MongoClient(DB_URL, tz_aware=True)
    db = client.get_default_database()
    return client, db


def ensure_indexes(db: Database) -> None:
    db.graph_tasks.create_index(
        [("status", ASCENDING), ("processing_until", ASCENDING), ("priority", DESCENDING), ("created_at", ASCENDING)]
    )
    db.graph_tasks.create_index([("worker_id", ASCENDING), ("updated_at", DESCENDING)])
    db.graph_results.create_index([("task_id", ASCENDING)], unique=True)
    db.social_nodes.create_index([("node_id", ASCENDING)], unique=True)
    db.social_edges.create_index([("source", ASCENDING), ("target", ASCENDING)], unique=True)


def fetch_task_atomic(db: Database) -> dict[str, Any] | None:
    now = utc_now()
    deadline = now + timedelta(seconds=LEASE_SECONDS)
    task = db.graph_tasks.find_one_and_update(
        {
            "$or": [
                {"status": "pending"},
                {"status": "processing", "processing_until": {"$lt": now}},
            ]
        },
        {
            "$set": {
                "status": "processing",
                "worker_id": WORKER_ID,
                "processing_until": deadline,
                "started_at": now,
                "updated_at": now,
            },
            "$inc": {"attempts": 1},
            "$unset": {"last_error": ""},
        },
        sort=[("priority", DESCENDING), ("created_at", ASCENDING), ("_id", ASCENDING)],
        return_document=ReturnDocument.AFTER,
    )
    return task


def load_task_subgraph(db: Database, payload: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[int, int]]:
    root_node = payload["root_node"]
    depth_limit = int(payload.get("depth_limit", MAX_DEPTH_DEFAULT))

    pipeline = [
        {"$match": {"node_id": root_node}},
        {
            "$graphLookup": {
                "from": "social_edges",
                "startWith": "$node_id",
                "connectFromField": "target",
                "connectToField": "source",
                "as": "reachable_edges",
                "maxDepth": max(depth_limit - 1, 0),
                "depthField": "depth",
            }
        },
        {
            "$project": {
                "_id": 0,
                "node_id": 1,
                "reachable_edges": 1,
            }
        },
    ]

    aggregate_result = list(db.social_nodes.aggregate(pipeline))
    if not aggregate_result:
        raise ValueError(f"Root node '{root_node}' not found in social_nodes")

    reachable_edges = aggregate_result[0]["reachable_edges"]
    node_ids = {root_node}
    depth_summary: dict[int, int] = defaultdict(int)

    for edge in reachable_edges:
        node_ids.add(edge["source"])
        node_ids.add(edge["target"])
        depth_summary[int(edge.get("depth", 0))] += 1

    node_docs = list(db.social_nodes.find({"node_id": {"$in": sorted(node_ids)}}, {"_id": 0, "node_id": 1, "community": 1}))
    return node_docs, reachable_edges, dict(sorted(depth_summary.items()))


def normalize_undirected_edges(edge_docs: list[dict[str, Any]]) -> list[tuple[str, str]]:
    unique_edges: set[tuple[str, str]] = set()
    for edge in edge_docs:
        source = edge["source"]
        target = edge["target"]
        if source == target:
            continue
        unique_edges.add(tuple(sorted((source, target))))
    return sorted(unique_edges)


def build_adjacency(node_ids: set[str], edges: list[tuple[str, str]]) -> dict[str, set[str]]:
    adjacency = {node_id: set() for node_id in sorted(node_ids)}
    for source, target in edges:
        adjacency.setdefault(source, set()).add(target)
        adjacency.setdefault(target, set()).add(source)
    return adjacency


def find_bridges(adjacency: dict[str, set[str]]) -> list[list[str]]:
    timer = 0
    visited: set[str] = set()
    tin: dict[str, int] = {}
    low: dict[str, int] = {}
    bridges: list[list[str]] = []

    def dfs(node: str, parent: str | None) -> None:
        nonlocal timer
        visited.add(node)
        tin[node] = timer
        low[node] = timer
        timer += 1

        for neighbor in sorted(adjacency[node]):
            if neighbor == parent:
                continue
            if neighbor in visited:
                low[node] = min(low[node], tin[neighbor])
                continue
            dfs(neighbor, node)
            low[node] = min(low[node], low[neighbor])
            if low[neighbor] > tin[node]:
                bridges.append(sorted([node, neighbor]))

    for node in sorted(adjacency):
        if node not in visited:
            dfs(node, None)

    bridges.sort()
    return bridges


def compute_graph_metrics(
    node_docs: list[dict[str, Any]],
    edge_docs: list[dict[str, Any]],
    root_node: str,
    depth_limit: int,
    depth_summary: dict[int, int] | None = None,
) -> dict[str, Any]:
    node_ids = {doc["node_id"] for doc in node_docs}
    edges = normalize_undirected_edges(edge_docs)
    adjacency = build_adjacency(node_ids, edges)

    node_count = len(adjacency)
    edge_count = len(edges)
    denominator = max(node_count - 1, 1)
    degree_centrality = {
        node_id: round(len(neighbors) / denominator, 4) for node_id, neighbors in sorted(adjacency.items())
    }
    density = 0.0
    if node_count > 1:
        density = round((2 * edge_count) / (node_count * (node_count - 1)), 4)

    community_sizes: dict[str, int] = defaultdict(int)
    for doc in node_docs:
        community_sizes[doc.get("community", "unknown")] += 1

    influencers = [
        {"node_id": node_id, "degree_centrality": score}
        for node_id, score in sorted(degree_centrality.items(), key=lambda item: (-item[1], item[0]))[:3]
    ]

    return {
        "status": "done",
        "analysis_type": "connectivity",
        "root_node": root_node,
        "depth_limit": depth_limit,
        "node_count": node_count,
        "edge_count": edge_count,
        "density": density,
        "degree_centrality": degree_centrality,
        "bridges": find_bridges(adjacency),
        "influencers": influencers,
        "community_sizes": dict(sorted(community_sizes.items())),
        "depth_edge_counts": depth_summary or {},
    }


def process_task(db: Database, payload: dict[str, Any]) -> dict[str, Any]:
    root_node = payload["root_node"]
    depth_limit = int(payload.get("depth_limit", MAX_DEPTH_DEFAULT))

    node_docs, edge_docs, depth_summary = load_task_subgraph(db, payload)
    result = compute_graph_metrics(
        node_docs=node_docs,
        edge_docs=edge_docs,
        root_node=root_node,
        depth_limit=depth_limit,
        depth_summary=depth_summary,
    )
    result["community_hint"] = payload.get("community_hint")
    return result


def commit_result(db: Database, task: dict[str, Any], result: dict[str, Any], elapsed_ms: float) -> None:
    now = utc_now()
    task_id = task["_id"]
    safe_result = make_bson_safe(result)
    db.graph_results.update_one(
        {"task_id": task_id},
        {
            "$set": {
                "task_id": task_id,
                "worker_id": WORKER_ID,
                "root_node": safe_result["root_node"],
                "result": safe_result,
                "elapsed_ms": round(elapsed_ms, 2),
                "updated_at": now,
            },
            "$setOnInsert": {"created_at": now},
        },
        upsert=True,
    )
    db.graph_tasks.update_one(
        {"_id": task_id},
        {
            "$set": {
                "status": "done",
                "processing_until": None,
                "result_ref": task_id,
                "worker_id": WORKER_ID,
                "updated_at": now,
                "completed_at": now,
            }
        },
    )


def mark_failed(db: Database, task: dict[str, Any] | None, error: str) -> None:
    if not task or "_id" not in task:
        return
    now = utc_now()
    db.graph_tasks.update_one(
        {"_id": task["_id"]},
        {
            "$set": {
                "status": "failed",
                "last_error": error,
                "updated_at": now,
                "processing_until": now + timedelta(seconds=LEASE_SECONDS),
            }
        },
    )


def log_status_snapshot(db: Database) -> None:
    statuses = db.graph_tasks.aggregate(
        [
            {"$group": {"_id": "$status", "count": {"$sum": 1}}},
            {"$sort": {"_id": 1}},
        ]
    )
    snapshot = {row["_id"]: row["count"] for row in statuses}
    LOGGER.info("Task status snapshot: %s", snapshot)
    emit_metric("status_snapshot", statuses=snapshot)


def worker_loop(db: Database) -> None:
    LOGGER.info("Worker started")
    emit_metric("worker_started")

    empty_polls = 0
    processed_count = 0
    error_count = 0

    while not shutdown_requested:
        task = fetch_task_atomic(db)
        if task is None:
            empty_polls += 1
            if EMPTY_POLLS_BEFORE_EXIT > 0 and empty_polls >= EMPTY_POLLS_BEFORE_EXIT:
                LOGGER.info("No tasks left, exiting after %s empty polls", empty_polls)
                emit_metric("worker_exited_empty", empty_polls=empty_polls)
                break
            time.sleep(POLL_INTERVAL)
            continue

        empty_polls = 0
        task_id = str(task["_id"])
        start_time = time.perf_counter()
        LOGGER.info("Processing task %s for root %s", task_id, task["payload"]["root_node"])

        try:
            result = process_task(db, task["payload"])
            elapsed_ms = (time.perf_counter() - start_time) * 1000
            commit_result(db, task, result, elapsed_ms)
            processed_count += 1
            LOGGER.info("Task %s completed in %.2f ms", task_id, elapsed_ms)
            emit_metric(
                "task_completed",
                task_id=task_id,
                root_node=result["root_node"],
                elapsed_ms=round(elapsed_ms, 2),
                processed_count=processed_count,
            )
        except Exception as exc:
            error_count += 1
            LOGGER.exception("Task %s failed", task_id)
            mark_failed(db, task, str(exc))
            emit_metric("task_failed", task_id=task_id, error=str(exc), error_count=error_count)

        if processed_count and processed_count % 5 == 0:
            log_status_snapshot(db)

    emit_metric("worker_stopped", processed_count=processed_count, error_count=error_count)


def main() -> None:
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    client = None
    try:
        client, db = connect_to_db()
        ensure_indexes(db)
        worker_loop(db)
    finally:
        if client is not None:
            client.close()
        LOGGER.info("Worker shutdown complete")


if __name__ == "__main__":
    main()
