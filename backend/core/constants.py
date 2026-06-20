from enum import StrEnum


class NodeType(StrEnum):
    KAFKA = "kafka"
    SCYLLADB = "scylladb"
    CLICKHOUSE = "clickhouse"


NON_SOURCE_TYPES: frozenset[str] = frozenset({NodeType.SCYLLADB, NodeType.CLICKHOUSE})
