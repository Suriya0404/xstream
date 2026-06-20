"""
Unit tests for pipeline_service: topological sort.
"""
import pytest
from services.pipeline_service import topo_sort


def test_topo_sort_simple_chain():
    nodes = {"a": {}, "b": {}, "c": {}}
    edges = [
        {"source_id": "a", "target_id": "b"},
        {"source_id": "b", "target_id": "c"},
    ]
    result = topo_sort(nodes, edges)
    assert result.index("a") < result.index("b") < result.index("c")


def test_topo_sort_fan_in():
    nodes = {"s1": {}, "s2": {}, "sink": {}}
    edges = [
        {"source_id": "s1", "target_id": "sink"},
        {"source_id": "s2", "target_id": "sink"},
    ]
    result = topo_sort(nodes, edges)
    assert result.index("s1") < result.index("sink")
    assert result.index("s2") < result.index("sink")


def test_topo_sort_ignores_unknown_nodes():
    nodes = {"a": {}, "b": {}}
    edges = [
        {"source_id": "a", "target_id": "b"},
        {"source_id": "missing", "target_id": "b"},  # dangling edge
    ]
    result = topo_sort(nodes, edges)
    assert "a" in result
    assert "b" in result
    assert "missing" not in result


def test_topo_sort_no_edges():
    nodes = {"x": {}, "y": {}, "z": {}}
    result = topo_sort(nodes, [])
    assert set(result) == {"x", "y", "z"}


def test_topo_sort_single_node():
    assert topo_sort({"only": {}}, []) == ["only"]
