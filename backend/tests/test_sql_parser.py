"""
Unit tests for sql_parser: field extraction and Flink SQL generation.
"""
import pytest
from sql_parser import parse_fields, generate_flink_sql, Field


# ── parse_fields ─────────────────────────────────────────────────────────────

@pytest.mark.parametrize("sql,expected", [
    (
        "CREATE TABLE orders (order_id STRING, amount DOUBLE, name STRING) WITH ('connector'='kafka');",
        [Field("order_id", "STRING"), Field("amount", "DOUBLE"), Field("name", "STRING")],
    ),
    (
        "SELECT order_id, amount, name FROM orders",
        [Field("order_id", "STRING"), Field("amount", "STRING"), Field("name", "STRING")],
    ),
    (
        # Multi-line with PRIMARY KEY constraint — regex parser would fail here
        """CREATE TABLE merged (
          `symbol` STRING,
          `price` DOUBLE,
          PRIMARY KEY (`symbol`) NOT ENFORCED
        ) WITH ('connector'='cassandra');""",
        [Field("symbol", "STRING"), Field("price", "DOUBLE")],
    ),
    ("", []),
])
def test_parse_fields(sql: str, expected: list[Field]):
    assert parse_fields(sql) == expected


# ── generate_flink_sql — Kafka ────────────────────────────────────────────────

_GLOBAL_CFG = {
    "kafka":      {"bootstrap_servers": "localhost:9092"},
    "scylladb":   {"contact_points": ["localhost"], "port": 9042, "keyspace": "xstream"},
    "clickhouse": {"host": "localhost", "port": 9000, "database": "xstream"},
}

_KAFKA_NODE = {
    "id": "k1",
    "node_type": "kafka",
    "label": "kafka-orders",
    "flink_sql": "",
    "properties": {
        "topic": "orders",
        "format": "json",
        "_handles": [["order_id", "STRING"], ["amount", "DOUBLE"]],
    },
}

def test_kafka_ddl_contains_connector():
    sql = generate_flink_sql(_KAFKA_NODE, [], {"k1": _KAFKA_NODE}, _GLOBAL_CFG)
    assert "'connector' = 'kafka'" in sql
    assert "'topic' = 'orders'" in sql
    assert "`order_id` STRING" in sql


def test_kafka_ddl_escapes_single_quotes():
    node = {**_KAFKA_NODE, "properties": {**_KAFKA_NODE["properties"], "topic": "my'topic"}}
    sql = generate_flink_sql(node, [], {"k1": node}, _GLOBAL_CFG)
    assert "'topic' = 'my''topic'" in sql


# ── generate_flink_sql — ScyllaDB ─────────────────────────────────────────────

_SCYLLA_NODE = {
    "id": "s1",
    "node_type": "scylladb",
    "label": "scylla-sink",
    "flink_sql": "",
    "properties": {
        "keyspace": "xstream",
        "table": "trades",
        "_handles": [["symbol", "STRING"], ["price", "DOUBLE"]],
    },
}

def test_scylla_ddl_contains_cassandra_connector():
    sql = generate_flink_sql(_SCYLLA_NODE, [], {"s1": _SCYLLA_NODE}, _GLOBAL_CFG)
    assert "'connector' = 'cassandra'" in sql
    assert "PRIMARY KEY" in sql


def test_scylla_generates_insert_from_kafka():
    nodes = {"k1": _KAFKA_NODE, "s1": _SCYLLA_NODE}
    edges = [{"id": "e1", "source_id": "k1", "target_id": "s1"}]
    sql = generate_flink_sql(_SCYLLA_NODE, edges, nodes, _GLOBAL_CFG)
    assert "INSERT INTO" in sql
    assert "`scylla-sink`" in sql


# ── generate_flink_sql — ClickHouse ──────────────────────────────────────────

_CH_NODE = {
    "id": "c1",
    "node_type": "clickhouse",
    "label": "clickhouse-reports",
    "flink_sql": "",
    "properties": {
        "database": "xstream",
        "table": "reports",
        "_handles": [["order_id", "STRING"], ["amount", "DOUBLE"]],
    },
}

def test_clickhouse_ddl_contains_jdbc_connector():
    sql = generate_flink_sql(_CH_NODE, [], {"c1": _CH_NODE}, _GLOBAL_CFG)
    assert "'connector' = 'jdbc'" in sql
    assert "ClickHouseDriver" in sql


# ── SQL injection prevention ───────────────────────────────────────────────────

def test_label_strips_injection_chars():
    # The identifier sanitizer removes quotes, semicolons, and spaces.
    # The resulting name is backtick-quoted, so no statement terminator can escape.
    malicious = {**_KAFKA_NODE, "label": "table'); DROP TABLE orders;--"}
    sql = generate_flink_sql(malicious, [], {"k1": malicious}, _GLOBAL_CFG)
    # No unescaped single-quote or semicolon should appear inside a WITH string literal
    assert "'); DROP" not in sql
    # The identifier must be wrapped in backticks — no raw statement break
    assert "CREATE TABLE IF NOT EXISTS `table" in sql


def test_string_value_escapes_quotes():
    node = {**_KAFKA_NODE, "properties": {**_KAFKA_NODE["properties"], "topic": "x'y"}}
    sql = generate_flink_sql(node, [], {"k1": node}, _GLOBAL_CFG)
    assert "x''y" in sql
