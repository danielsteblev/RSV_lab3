import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
WORKER_DIR = ROOT / "worker_py"
sys.path.insert(0, str(WORKER_DIR))

from worker import compute_graph_metrics


def test_process_logic():
    node_docs = [
        {"node_id": "u1", "community": "alpha"},
        {"node_id": "u2", "community": "alpha"},
        {"node_id": "u3", "community": "beta"},
        {"node_id": "u4", "community": "beta"},
    ]
    edge_docs = [
        {"source": "u1", "target": "u2"},
        {"source": "u2", "target": "u1"},
        {"source": "u2", "target": "u3"},
        {"source": "u3", "target": "u2"},
        {"source": "u3", "target": "u4"},
        {"source": "u4", "target": "u3"},
    ]

    result = compute_graph_metrics(
        node_docs=node_docs,
        edge_docs=edge_docs,
        root_node="u2",
        depth_limit=2,
        depth_summary={0: 2, 1: 2},
    )

    assert result["status"] == "done"
    assert result["node_count"] == 4
    assert result["edge_count"] == 3
    assert result["density"] == 0.5
    assert result["degree_centrality"]["u2"] == 0.6667
    assert result["bridges"] == [["u1", "u2"], ["u2", "u3"], ["u3", "u4"]]
    assert result["influencers"][0]["node_id"] == "u2"
    assert result["depth_edge_counts"] == {0: 2, 1: 2}


def test_bson_safe_conversion():
    from worker import make_bson_safe

    payload = {"depth_edge_counts": {0: 2, 1: {"inner": {2: 3}}}, "bridges": [("u1", "u2")]}

    safe_payload = make_bson_safe(payload)

    assert safe_payload == {
        "depth_edge_counts": {"0": 2, "1": {"inner": {"2": 3}}},
        "bridges": [["u1", "u2"]],
    }
