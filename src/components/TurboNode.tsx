import { Fragment, memo, useCallback, useLayoutEffect, useRef, useState } from 'react';
import type { ReactNode } from 'react';
import { Handle, Position, useReactFlow } from 'reactflow';
import type { NodeProps } from 'reactflow';
import { FiEdit, FiPlus, FiTrash2 } from 'react-icons/fi';
import type { NodeType } from './NodeEditModal';
import { useNodeEdit } from '../context/NodeEditContext';
import { useFlinkHealth } from '../context/FlinkHealthContext';

export type TurboNodeData = {
  title: string;
  icon?: ReactNode;
  subline?: string;
  handles: string[][];
  nodeType?: NodeType;
  properties?: Record<string, unknown>;
  flinkSql?: string;
};

const NODE_TYPE_ACCENT: Record<string, string> = {
  kafka:      '#e92a67',
  scylladb:   '#2a8af6',
  clickhouse: '#f6a82a',
  mongodb:    '#13aa52',
};

export default memo(({ id, data }: NodeProps<TurboNodeData>) => {
  const { setNodes, setEdges } = useReactFlow();
  const { openPanel } = useNodeEdit();
  const health = useFlinkHealth();
  const fieldsRef = useRef<HTMLDivElement>(null);
  const [handleTops, setHandleTops] = useState<number[]>([]);

  const accent = NODE_TYPE_ACCENT[data.nodeType ?? 'scylladb'] ?? '#2a8af6';

  // Measure each field row's vertical center relative to the ReactFlow node element.
  // Uses offsetTop traversal (zoom-independent) so handle positions match at any scale.
  useLayoutEffect(() => {
    const section = fieldsRef.current;
    if (!section) return;
    const nodeEl = section.closest('.react-flow__node') as HTMLElement | null;
    if (!nodeEl) return;
    const rows = section.querySelectorAll<HTMLElement>('.field-row');
    const tops = Array.from(rows).map(row => {
      let top = row.offsetTop + row.offsetHeight / 2;
      let el: HTMLElement | null = row.offsetParent as HTMLElement | null;
      while (el && el !== nodeEl) {
        top += el.offsetTop;
        el = el.offsetParent as HTMLElement | null;
      }
      return top;
    });
    setHandleTops(tops);
  }, [data.handles.length]);

  const updateData = useCallback(
    (updater: (d: TurboNodeData) => TurboNodeData) => {
      setNodes(nodes =>
        nodes.map(n => (n.id === id ? { ...n, data: updater(n.data as TurboNodeData) } : n))
      );
    },
    [id, setNodes]
  );

  const updateTitle = useCallback(
    (value: string) => updateData(d => ({ ...d, title: value })),
    [updateData]
  );

  const addField = useCallback(
    () => updateData(d => ({ ...d, handles: [...d.handles, ['field', 'STRING']] })),
    [updateData]
  );

  const removeField = useCallback(
    (index: number) => updateData(d => ({ ...d, handles: d.handles.filter((_, i) => i !== index) })),
    [updateData]
  );

  const updateField = useCallback(
    (index: number, col: number, value: string) =>
      updateData(d => ({
        ...d,
        handles: d.handles.map((row, i) =>
          i === index ? row.map((v, c) => (c === col ? value : v)) : row
        ),
      })),
    [updateData]
  );

  const togglePrimaryKey = useCallback(
    (fieldName: string) =>
      updateData(d => {
        const current = (d.properties?.primary_key as string[] | undefined) ?? [];
        const next = current.includes(fieldName)
          ? current.filter(k => k !== fieldName)
          : [...current, fieldName];
        return { ...d, properties: { ...(d.properties ?? {}), primary_key: next } };
      }),
    [updateData]
  );

  // Highlight edges connected to the hovered field; pass null to clear all highlights.
  const highlightFieldEdges = useCallback(
    (fieldIdx: number | null) => {
      setEdges(eds => eds.map(e => {
        const connected = fieldIdx !== null && (
          (e.source === id && e.sourceHandle === `out-${fieldIdx}`) ||
          (e.target === id && e.targetHandle === `in-${fieldIdx}`)
        );
        const already = (e.className ?? '').includes('edge-highlighted');
        if (connected === already) return e;
        return {
          ...e,
          className: connected
            ? ((e.className ?? '') + ' edge-highlighted').trim()
            : (e.className ?? '').replace('edge-highlighted', '').trim(),
        };
      }));
    },
    [id, setEdges]
  );

  const handleEditClick = useCallback(() => {
    // Pass empty connectedSources — Workspace.openPanel computes them from edges
    openPanel(id, {
      title:      data.title,
      handles:    data.handles,
      nodeType:   data.nodeType ?? 'scylladb',
      properties: data.properties ?? {},
      flinkSql:   data.flinkSql ?? '',
    }, []);
  }, [id, data, openPanel]);

  return (
    <>
      {/* Per-field handles — outside .wrapper so overflow:hidden doesn't clip them.
          Positioned absolute relative to .react-flow__node-turbo at measured field centers. */}
      {data.handles.map((_, index) => (
        <Fragment key={index}>
          <Handle
            type="target"
            position={Position.Left}
            id={`in-${index}`}
            style={{
              top: handleTops[index] ?? '50%',
              background: accent,
              transform: 'translateY(-50%)',
            }}
          />
          <Handle
            type="source"
            position={Position.Right}
            id={`out-${index}`}
            style={{
              top: handleTops[index] ?? '50%',
              background: accent,
              transform: 'translateY(-50%)',
            }}
          />
        </Fragment>
      ))}

      {/* Flink job heartbeat indicator */}
      {health !== 'unknown' && (
        <div
          className={`node-health-dot ${health}`}
          title={health === 'healthy' ? 'Flink job running' : 'Flink job not healthy'}
        />
      )}

      {/* Edit badge */}
      <div
        className="cloud gradient"
        style={{ cursor: 'pointer' }}
        title="Edit node properties"
        onClick={handleEditClick}
      >
        <div><FiEdit /></div>
      </div>

      {/* Node card — no Handle components inside; handles are siblings above */}
      <div className="wrapper gradient" style={{ borderColor: accent }}>
        <div className="inner">
          <div className="body">

            <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginBottom: 4 }}>
              <span className="node-type-pill" style={{ background: accent }}>
                {data.nodeType ?? 'node'}
              </span>
            </div>
            <input
              className="node-title nodrag"
              value={data.title}
              onChange={e => updateTitle(e.target.value)}
              placeholder="Node title"
            />

            <div
              className="fields-section"
              ref={fieldsRef}
              onMouseLeave={() => highlightFieldEdges(null)}
            >
              {(() => {
                const isSink = data.nodeType === 'scylladb' || data.nodeType === 'clickhouse';
                const pkFields = (data.properties?.primary_key as string[] | undefined) ?? [];
                return (
                  <>
                    <div className="field-header-row">
                      {isSink && <span className="fh-pk" title="Primary Key">PK</span>}
                      <span className="fh-name">Field</span>
                      <span className="fh-type">Type</span>
                      <span style={{ width: 18 }} />
                    </div>

                    {data.handles.map((row, index) => {
                      const fieldName = row[0] ?? '';
                      const isPk = isSink && pkFields.includes(fieldName);
                      return (
                        <div
                          key={index}
                          className={`field-row${isPk ? ' field-row-pk' : ''}`}
                          onMouseEnter={() => highlightFieldEdges(index)}
                        >
                          {isSink && (
                            <input
                              type="checkbox"
                              className="field-pk-checkbox nodrag"
                              checked={isPk}
                              title="Primary Key"
                              onChange={() => togglePrimaryKey(fieldName)}
                            />
                          )}
                          <input
                            className="field-input fi-name nodrag"
                            value={fieldName}
                            onChange={e => updateField(index, 0, e.target.value)}
                            placeholder="field"
                          />
                          <input
                            className="field-input fi-type nodrag"
                            value={row[1] ?? ''}
                            onChange={e => updateField(index, 1, e.target.value)}
                            placeholder="STRING"
                          />
                          <button
                            className="field-remove-btn nodrag"
                            onClick={() => removeField(index)}
                            title="Remove field"
                          >
                            <FiTrash2 size={10} />
                          </button>
                        </div>
                      );
                    })}
                  </>
                );
              })()}
            </div>

            <button className="add-field-btn nodrag" onClick={addField} style={{ marginTop: 6 }}>
              <FiPlus size={12} /> Add Field
            </button>

          </div>
        </div>
      </div>
    </>
  );
});
