import { useEffect, useRef, useState } from 'react';
import type { KeyboardEvent } from 'react';
import { FiX, FiSave, FiCode, FiSettings } from 'react-icons/fi';
import type { TurboNodeData } from './TurboNode';

export type NodeType = 'kafka' | 'scylladb' | 'clickhouse';

export interface NodeProperties {
  // Kafka
  topic?: string;
  group_id?: string;
  format?: string;
  bootstrap_servers?: string;
  // ScyllaDB
  keyspace?: string;
  table?: string;
  host?: string;
  // ClickHouse
  database?: string;
  // Primary key fields (scylladb / clickhouse) — supports composite keys
  primary_key?: string[];
  // Field-to-field mappings: index → { source_node_id, source_field }
  field_mappings?: Record<string, { source_node_id: string; source_field: string }>;
  // Shared
  [key: string]: unknown;
}

/** One upstream node whose fields can be mapped into this sink node. */
export interface ConnectedSource {
  nodeId: string;
  nodeLabel: string;
  fields: string[][];  // [[fieldName, type], ...]
}

export interface EditableNodeData extends TurboNodeData {
  nodeType: NodeType;
  properties: NodeProperties;
  flinkSql: string;
}

interface Props {
  nodeId: string;
  data: EditableNodeData;
  connectedSources: ConnectedSource[];
  onSave: (nodeId: string, data: EditableNodeData) => void;
  onClose: () => void;
}

const NODE_TYPE_LABELS: Record<NodeType, string> = {
  kafka: 'Kafka Topic',
  scylladb: 'ScyllaDB',
  clickhouse: 'ClickHouse',
};

const NODE_TYPE_COLOR: Record<NodeType, string> = {
  kafka: '#e92a67',
  scylladb: '#2a8af6',
  clickhouse: '#f6a82a',
};

const SINK_TYPES: NodeType[] = ['scylladb', 'clickhouse'];

function PropertiesPanel({ nodeType, props, handles, connectedSources, onChange }: {
  nodeType: NodeType;
  props: NodeProperties;
  handles: string[][];
  connectedSources: ConnectedSource[];
  onChange: (p: NodeProperties) => void;
}) {
  const field = (label: string, key: string, placeholder = '') => (
    <div className="modal-field" key={key}>
      <label className="modal-label">{label}</label>
      <input
        className="modal-input"
        value={(props[key] as string) ?? ''}
        placeholder={placeholder}
        onChange={e => onChange({ ...props, [key]: e.target.value })}
      />
    </div>
  );

  const isSink = SINK_TYPES.includes(nodeType);

  // Field mappings — always show for sink nodes that have fields
  const fieldMappings = isSink && handles.length > 0 && (
    <div className="modal-field-mappings" key="field_mappings">
      <label className="modal-label">Field Mappings</label>
      {connectedSources.length === 0 && (
        <p className="modal-hint fmr-no-sources">
          No upstream nodes connected yet. Draw edges from source fields to populate this section.
        </p>
      )}
      {connectedSources.length > 0 && (
        <>
          <div className="fmr-header">
            <span className="fmr-col-target">This field</span>
            <span className="fmr-col-arrow" />
            <span className="fmr-col-source">Mapped from</span>
          </div>
          {handles.map(([fieldName], idx) => {
            const name = fieldName ?? '';
            const mappingKey = String(idx);
            const current = props.field_mappings?.[mappingKey];
            const currentVal = current ? `${current.source_node_id}:${current.source_field}` : '';
            const pkFields = (props.primary_key as string[] | undefined) ?? [];
            const isPk = pkFields.includes(name);
            return (
              <div key={idx} className={`fmr-row${isPk ? ' fmr-pk' : ''}`}>
                <span className="fmr-target">
                  {isPk && <span className="pk-badge-sm">PK</span>}
                  {name}
                </span>
                <span className="fmr-arrow">←</span>
                <select
                  className="modal-input fmr-source"
                  value={currentVal}
                  onChange={e => {
                    const val = e.target.value;
                    const newMappings = { ...(props.field_mappings ?? {}) };
                    if (!val) {
                      delete newMappings[mappingKey];
                    } else {
                      const colonIdx = val.indexOf(':');
                      const source_node_id = val.slice(0, colonIdx);
                      const source_field   = val.slice(colonIdx + 1);
                      newMappings[mappingKey] = { source_node_id, source_field };
                    }
                    onChange({ ...props, field_mappings: newMappings });
                  }}
                >
                  <option value="">— not mapped —</option>
                  {connectedSources.flatMap(src =>
                    src.fields.map(([srcField]) => (
                      <option
                        key={`${src.nodeId}:${srcField}`}
                        value={`${src.nodeId}:${srcField}`}
                      >
                        {src.nodeLabel}.{srcField}
                      </option>
                    ))
                  )}
                </select>
              </div>
            );
          })}
        </>
      )}
    </div>
  );

  if (nodeType === 'kafka') return (
    <>
      {field('Topic', 'topic', 'my-topic')}
      {field('Bootstrap Servers', 'bootstrap_servers', 'localhost:9092')}
      {field('Consumer Group ID', 'group_id', 'xstream-group')}
      {field('Format', 'format', 'json')}
    </>
  );

  if (nodeType === 'scylladb') return (
    <>
      {field('Host', 'host', 'localhost')}
      {field('Keyspace', 'keyspace', 'xstream')}
      {field('Table Name', 'table', 'my_table')}
      {fieldMappings}
    </>
  );

  if (nodeType === 'clickhouse') return (
    <>
      {field('Host', 'host', 'localhost')}
      {field('Database', 'database', 'xstream')}
      {field('Table Name', 'table', 'my_table')}
      {fieldMappings}
    </>
  );

  return null;
}

export default function NodeEditModal({ nodeId, data, connectedSources, onSave, onClose }: Props) {
  const [draft, setDraft] = useState<EditableNodeData>(data);
  const [tab, setTab] = useState<'props' | 'sql'>('props');
  const [parsedFields, setParsedFields] = useState<Array<{ name: string; data_type: string }>>([]);
  const [sqlError, setSqlError] = useState('');
  const sqlRef = useRef<HTMLTextAreaElement>(null);
  const paneRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    paneRef.current?.focus();
    if (draft.flinkSql) parseSql(draft.flinkSql);
  }, []);

  const parseSql = async (sql: string) => {
    setSqlError('');
    try {
      const res = await fetch('http://localhost:8000/api/parse-sql', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sql }),
      });
      if (res.ok) {
        const json = await res.json();
        setParsedFields(json.fields ?? []);
      }
    } catch {
      const cols: Array<{ name: string; data_type: string }> = [];
      const re = /^\s*`?(\w+)`?\s+([A-Z][A-Z0-9_<>(), ]*)/gim;
      let m;
      while ((m = re.exec(sql)) !== null) {
        const kw = ['PRIMARY', 'WATERMARK', 'CONSTRAINT', 'CREATE', 'TABLE', 'WITH', 'SELECT'];
        if (!kw.includes(m[1]!.toUpperCase())) {
          cols.push({ name: m[1]!, data_type: m[2]!.trim().replace(/,$/, '') });
        }
      }
      setParsedFields(cols);
    }
  };

  const handleSqlChange = (sql: string) => {
    setDraft(d => ({ ...d, flinkSql: sql }));
    if (sql.length > 10) parseSql(sql);
  };

  const handleSave = () => {
    const handles = parsedFields.length > 0
      ? parsedFields.map(f => [f.name, f.data_type])
      : draft.handles;
    onSave(nodeId, { ...draft, handles });
  };

  const handleKeyDown = (e: KeyboardEvent) => {
    if (e.key === 'Escape') onClose();
    if ((e.metaKey || e.ctrlKey) && e.key === 's') { e.preventDefault(); handleSave(); }
  };

  const color = NODE_TYPE_COLOR[draft.nodeType];

  return (
    <div
      ref={paneRef}
      className="side-pane"
      tabIndex={-1}
      onKeyDown={handleKeyDown}
      style={{ borderTop: `3px solid ${color}` }}
    >
        {/* Header */}
        <div className="modal-header">
          <div className="modal-header-left">
            <span className="modal-badge" style={{ background: color }}>
              {NODE_TYPE_LABELS[draft.nodeType]}
            </span>
            <input
              className="modal-title-input"
              value={draft.title}
              onChange={e => setDraft(d => ({ ...d, title: e.target.value }))}
              placeholder="Node label"
            />
          </div>
          <div className="modal-header-right">
            <select
              className="modal-type-select"
              value={draft.nodeType}
              onChange={e => setDraft(d => ({ ...d, nodeType: e.target.value as NodeType }))}
            >
              {Object.entries(NODE_TYPE_LABELS).map(([k, v]) => (
                <option key={k} value={k}>{v}</option>
              ))}
            </select>
            <button className="modal-icon-btn" onClick={onClose} title="Close (Esc)">
              <FiX size={18} />
            </button>
          </div>
        </div>

        {/* Tabs */}
        <div className="modal-tabs">
          <button
            className={`modal-tab ${tab === 'props' ? 'active' : ''}`}
            onClick={() => setTab('props')}
          >
            <FiSettings size={13} /> Properties
          </button>
          <button
            className={`modal-tab ${tab === 'sql' ? 'active' : ''}`}
            onClick={() => setTab('sql')}
          >
            <FiCode size={13} /> Flink SQL
          </button>
        </div>

        {/* Body */}
        <div className="modal-body">
          {tab === 'props' && (
            <PropertiesPanel
              nodeType={draft.nodeType}
              props={draft.properties}
              handles={draft.handles}
              connectedSources={connectedSources}
              onChange={p => setDraft(d => ({ ...d, properties: p }))}
            />
          )}

          {tab === 'sql' && (
            <div className="modal-sql-panel">
              <p className="modal-hint">
                Write a Flink SQL <code>CREATE TABLE</code> statement. Field names will
                become the node's connectable handles.
              </p>
              <textarea
                ref={sqlRef}
                className="modal-sql-editor"
                value={draft.flinkSql}
                onChange={e => handleSqlChange(e.target.value)}
                placeholder={SQL_PLACEHOLDER[draft.nodeType]}
                spellCheck={false}
              />
              {sqlError && <p className="modal-error">{sqlError}</p>}
              {parsedFields.length > 0 && (
                <div className="modal-parsed-fields">
                  <p className="modal-hint">Detected fields (will become node handles):</p>
                  <div className="modal-field-chips">
                    {parsedFields.map(f => (
                      <span key={f.name} className="modal-chip">
                        <span className="chip-name">{f.name}</span>
                        <span className="chip-type">{f.data_type}</span>
                      </span>
                    ))}
                  </div>
                </div>
              )}
            </div>
          )}
        </div>

        {/* Footer */}
        <div className="modal-footer">
          <span className="modal-hint">⌘S to save · Esc to cancel</span>
          <div style={{ display: 'flex', gap: 8 }}>
            <button className="toolbar-btn" onClick={onClose}>Cancel</button>
            <button className="toolbar-btn" style={{ background: color }} onClick={handleSave}>
              <FiSave size={13} /> Save
            </button>
          </div>
        </div>
    </div>
  );
}

const SQL_PLACEHOLDER: Record<NodeType, string> = {
  kafka: `CREATE TABLE orders (
  order_id   STRING,
  customer_id STRING,
  amount     DOUBLE,
  ts         TIMESTAMP(3),
  PRIMARY KEY (order_id) NOT ENFORCED,
  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic'     = 'orders',
  'format'    = 'json'
);`,

  scylladb: `CREATE TABLE merged_orders (
  order_id    STRING,
  customer_id STRING,
  amount      DOUBLE,
  name        STRING,
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
  'connector' = 'cassandra',
  'host'      = 'scylladb',
  'keyspace'  = 'xstream',
  'table'     = 'merged_orders'
);`,

  clickhouse: `CREATE TABLE reports (
  order_id    STRING,
  amount      DOUBLE,
  name        STRING,
  report_date DATE
) WITH (
  'connector'  = 'jdbc',
  'url'        = 'jdbc:clickhouse://clickhouse:9000/xstream',
  'table-name' = 'reports'
);`,
};
