import React, { useCallback, useEffect, useLayoutEffect, useRef, useState } from 'react';
import ReactFlow, { Controls, useNodesState, useEdgesState, addEdge, Panel } from 'reactflow';
import type { Node, Edge, Connection } from 'reactflow';
import {
  FiPlay, FiSave, FiCheckCircle, FiAlertCircle,
  FiLoader, FiHome, FiEdit2, FiSun, FiMoon,
  FiTerminal, FiChevronDown, FiChevronUp, FiSquare,
} from 'react-icons/fi';
import { useTheme } from '../context/ThemeContext';
import TurboNode from './TurboNode';
import type { TurboNodeData } from './TurboNode';
import TurboEdge from './TurboEdge';
import FunctionIcon from './FunctionIcon';
import NodeEditModal from './NodeEditModal';
import type { EditableNodeData, ConnectedSource } from './NodeEditModal';
import { NodeEditContext } from '../context/NodeEditContext';
import SaveDialog from './SaveDialog';
import ChatAgent from './ChatAgent';
import { useAutoSave } from '../hooks/useAutoSave';
import { usePipelineRun } from '../hooks/usePipelineRun';
import type { RunStatus } from '../hooks/usePipelineRun';

const BACKEND = 'http://localhost:8000';

const nodeTypes = { turbo: TurboNode };
const edgeTypes  = { turbo: TurboEdge };

const SINK_TYPES = new Set(['scylladb', 'clickhouse']);

/** Extract the numeric index from a handle id like 'out-3' or 'in-0'. */
const parseHandleIdx = (handle: string | null | undefined): number => {
  const n = parseInt((handle ?? '').split('-').pop() ?? '0', 10);
  return isNaN(n) ? 0 : n;
};
const defaultEdgeOptions = {
  type: 'turbo',
  markerEnd: 'edge-circle',
  markerStart: 'edge-circle',
};

interface Props {
  initialPipelineName: string;
  onHome: () => void;
}

export default function Workspace({ initialPipelineName, onHome }: Props) {
  const [nodes, setNodes, onNodesChange] = useNodesState<TurboNodeData>([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState([]);
  const [pipelineName, setPipelineName] = useState(initialPipelineName);
  const [showSaveDialog, setShowSaveDialog] = useState(false);
  const [backendOnline, setBackendOnline] = useState(false);
  const [activePanel, setActivePanel] = useState<{ id: string; data: EditableNodeData; connectedSources: ConnectedSource[] } | null>(null);
  const logBodyRef = useRef<HTMLPreElement>(null);
  const loadedRef  = useRef(false);
  const justLoadedRef = useRef(false);

  // Stable refs — useLayoutEffect keeps them synchronous with state before browser paint,
  // so any click handler that fires after paint always reads current values.
  const nodesRef = useRef(nodes);
  const edgesRef = useRef(edges);
  useLayoutEffect(() => { nodesRef.current = nodes; }, [nodes]);
  useLayoutEffect(() => { edgesRef.current = edges; }, [edges]);

  const { theme, toggleTheme } = useTheme();

  // ── Node/edge serialisers ──────────────────────────────
  const nodePayload = useCallback((n: Node<TurboNodeData>) => {
    const d = n.data as TurboNodeData;
    return {
      id: n.id,
      node_type: d.nodeType ?? 'scylladb',
      label: d.title,
      pos_x: n.position.x,
      pos_y: n.position.y,
      flink_sql: d.flinkSql ?? '',
      properties: { ...(d.properties ?? {}), _handles: d.handles ?? [] },
    };
  }, []);

  const edgePayload = useCallback((e: Edge) => ({
    id: e.id,
    source_id: e.source,
    target_id: e.target,
    source_handle: e.sourceHandle,
    target_handle: e.targetHandle,
  }), []);

  // ── Explicit save (may prompt for name) ───────────────
  const savePipeline = useCallback(async (nameOverride?: string): Promise<boolean> => {
    const name = nameOverride ?? pipelineName;
    if (!name) {
      setShowSaveDialog(true);
      return false;
    }
    try {
      await fetch(`${BACKEND}/api/pipeline`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          name,
          nodes: nodes.map(nodePayload),
          edges: edges.map(edgePayload),
        }),
      });
      return true;
    } catch (err) {
      return false;
    }
  }, [pipelineName, nodes, edges, nodePayload, edgePayload]);

  // ── Auto-save (debounced) ──────────────────────────────
  const { autoSaved } = useAutoSave({
    nodes, edges, pipelineName, loadedRef, justLoadedRef, nodePayload, edgePayload,
  });

  // ── Pipeline run + polling ─────────────────────────────
  const {
    runStatus, setRunStatus,
    runLog, setRunLog,
    showLog, setShowLog,
    logCollapsed, setLogCollapsed,
    runPipeline,
    stopPipeline,
  } = usePipelineRun({ pipelineName, savePipeline });

  // Auto-scroll log pane when content arrives
  useEffect(() => {
    if (logBodyRef.current && showLog && !logCollapsed) {
      logBodyRef.current.scrollTop = logBodyRef.current.scrollHeight;
    }
  }, [runLog, showLog, logCollapsed]);

  // ── Backend health check ───────────────────────────────
  useEffect(() => {
    fetch(`${BACKEND}/health`).then(() => setBackendOnline(true)).catch(() => {});
  }, []);

  // ── Load pipeline on mount ─────────────────────────────
  useEffect(() => {
    if (!initialPipelineName) {
      loadedRef.current = true;
      return;
    }
    fetch(`${BACKEND}/api/pipeline/${encodeURIComponent(initialPipelineName)}`)
      .then(r => r.json())
      .then((data: { nodes?: any[]; edges?: any[] }) => {
        if (data.nodes && data.nodes.length > 0) {
          const rfNodes: Node<TurboNodeData>[] = data.nodes.map((n: any) => {
            const { _handles, ...cleanProps } = n.properties ?? {};
            return {
              id: n.id,
              type: 'turbo',
              position: { x: n.pos_x ?? 0, y: n.pos_y ?? 0 },
              data: {
                icon: <FunctionIcon />,
                title: n.label,
                nodeType: n.node_type,
                handles: _handles ?? [['id', 'STRING']],
                properties: cleanProps,
                flinkSql: n.flink_sql ?? '',
              },
            };
          });
          const rfEdges: Edge[] = (data.edges ?? []).map((e: any) => ({
            id: e.id,
            source: e.source_id,
            target: e.target_id,
            sourceHandle: e.source_handle ?? undefined,
            targetHandle: e.target_handle ?? undefined,
            type: 'turbo',
            markerEnd: 'edge-circle',
            markerStart: 'edge-circle',
          }));
          justLoadedRef.current = true;
          setNodes(rfNodes);
          setEdges(rfEdges);
        }
      })
      .catch(() => {})
      .finally(() => { loadedRef.current = true; });
  }, []);

  // ── Edge ↔ field_mappings sync ─────────────────────────
  // Whenever edges change (drag-connect or delete), recompute field_mappings on sink nodes.
  useEffect(() => {
    if (!loadedRef.current) return;
    setNodes(nds => {
      let anyChanged = false;
      const updated = nds.map(node => {
        const d = node.data as TurboNodeData;
        if (!SINK_TYPES.has(d.nodeType ?? '')) return node;

        const newMappings: Record<string, { source_node_id: string; source_field: string }> = {};
        for (const edge of edges) {
          if (edge.target !== node.id) continue;
          const tIdx = parseHandleIdx(edge.targetHandle);
          const srcNode = nds.find(n => n.id === edge.source);
          if (!srcNode) continue;
          const sIdx = parseHandleIdx(edge.sourceHandle);
          const srcField = (srcNode.data as TurboNodeData).handles[sIdx]?.[0] ?? `field_${sIdx}`;
          newMappings[String(tIdx)] = { source_node_id: edge.source, source_field: srcField };
        }

        const existing = (d.properties?.field_mappings ?? {}) as Record<string, unknown>;
        if (JSON.stringify(newMappings) === JSON.stringify(existing)) return node;

        anyChanged = true;
        return { ...node, data: { ...d, properties: { ...(d.properties ?? {}), field_mappings: newMappings } } };
      });
      return anyChanged ? updated : nds;
    });
  }, [edges]); // eslint-disable-line react-hooks/exhaustive-deps

  // ── Panel callbacks ────────────────────────────────────
  const openPanel = useCallback((nodeId: string, data: EditableNodeData) => {
    const incomingEdges = edgesRef.current.filter(e => e.target === nodeId);
    const currentNodes = nodesRef.current;

    // Build connectedSources (one entry per unique upstream node)
    const seen = new Set<string>();
    const connectedSources: ConnectedSource[] = [];
    for (const edge of incomingEdges) {
      if (seen.has(edge.source)) continue;
      seen.add(edge.source);
      const srcNode = currentNodes.find(n => n.id === edge.source);
      if (srcNode) {
        connectedSources.push({
          nodeId: srcNode.id,
          nodeLabel: srcNode.data.title,
          fields: srcNode.data.handles,
        });
      }
    }

    // Compute field_mappings directly from edges — bypasses any pending sync-effect render cycle
    const freshMappings: Record<string, { source_node_id: string; source_field: string }> = {};
    for (const edge of incomingEdges) {
      const tIdx = parseHandleIdx(edge.targetHandle);
      const srcNode = currentNodes.find(n => n.id === edge.source);
      if (!srcNode) continue;
      const sIdx = parseHandleIdx(edge.sourceHandle);
      const srcField = (srcNode.data as TurboNodeData).handles[sIdx]?.[0] ?? `field_${sIdx}`;
      freshMappings[String(tIdx)] = { source_node_id: edge.source, source_field: srcField };
    }

    const freshData: EditableNodeData = {
      ...data,
      properties: { ...data.properties, field_mappings: freshMappings },
    };

    setActivePanel({ id: nodeId, data: freshData, connectedSources });
  }, []);

  const handlePanelSave = useCallback((nodeId: string, saved: EditableNodeData) => {
    setNodes(nds => nds.map(n =>
      n.id === nodeId
        ? { ...n, data: { ...n.data, title: saved.title, handles: saved.handles, nodeType: saved.nodeType, properties: saved.properties, flinkSql: saved.flinkSql } }
        : n
    ));

    // For sink nodes: rebuild edges from the new field_mappings so the canvas reflects dropdown changes.
    if (SINK_TYPES.has(saved.nodeType)) {
      const fieldMappings = (saved.properties?.field_mappings ?? {}) as Record<string, { source_node_id: string; source_field: string }>;
      setEdges(currentEdges => {
        const otherEdges = currentEdges.filter(e => e.target !== nodeId);
        const newEdges: Edge[] = [];
        for (const [tIdxStr, mapping] of Object.entries(fieldMappings)) {
          if (!mapping.source_node_id || !mapping.source_field) continue;
          const srcNode = nodesRef.current.find(n => n.id === mapping.source_node_id);
          if (!srcNode) continue;
          const sFieldIdx = (srcNode.data as TurboNodeData).handles.findIndex(h => h[0] === mapping.source_field);
          const srcHandle = sFieldIdx >= 0 ? `out-${sFieldIdx}` : 'out-0';
          newEdges.push({
            id: `fmap-${mapping.source_node_id}-${sFieldIdx}-${nodeId}-${tIdxStr}`,
            source: mapping.source_node_id,
            target: nodeId,
            sourceHandle: srcHandle,
            targetHandle: `in-${tIdxStr}`,
            type: 'turbo',
            markerEnd: 'edge-circle',
            markerStart: 'edge-circle',
          });
        }
        return [...otherEdges, ...newEdges];
      });
    }

    setActivePanel(null);
  }, [setNodes, setEdges]);

  const handleSaveDialogConfirm = useCallback(async (name: string) => {
    setPipelineName(name);
    setShowSaveDialog(false);
    await savePipeline(name);
  }, [savePipeline]);

  // ── Add / delete nodes ─────────────────────────────────
  const onConnect = useCallback(
    (params: Connection) => setEdges(eds => addEdge(params, eds)),
    [setEdges]
  );

  const addNode = useCallback((nodeType: TurboNodeData['nodeType'] = 'scylladb') => {
    const id = String(Date.now());
    setNodes(nds => [...nds, {
      id,
      position: { x: 200 + Math.random() * 400, y: 200 + Math.random() * 400 },
      type: 'turbo',
      data: {
        icon: <FunctionIcon />,
        title: `${nodeType}-${id.slice(-4)}`,
        handles: [['id', 'STRING'], ['value', 'STRING']],
        nodeType,
        properties: {},
        flinkSql: '',
      },
    }]);
  }, [setNodes]);

  const deleteSelected = useCallback(() => {
    setNodes(nds => nds.filter(n => !n.selected));
    setEdges(eds => eds.filter(e => !e.selected));
  }, [setNodes, setEdges]);

  // ── Status icon ────────────────────────────────────────
  const StatusIcon = () => {
    if (runStatus === 'running' || runStatus === 'saving') return <FiLoader size={14} className="spin" />;
    if (runStatus === 'success') return <FiCheckCircle size={14} color="#4caf50" />;
    if (runStatus === 'failed')  return <FiAlertCircle size={14} color="#e92a67" />;
    return null;
  };

  const runLabel: Record<RunStatus, string> = {
    idle: 'Run Pipeline', saving: 'Saving…', running: 'Running…',
    success: 'Run ✓', failed: 'Failed — Retry', cancelled: 'Run Pipeline',
  };

  return (
    <NodeEditContext.Provider value={{ openPanel }}>
      {showSaveDialog && (
        <SaveDialog
          initialName={pipelineName || 'My Workflow'}
          onSave={handleSaveDialogConfirm}
          onCancel={() => setShowSaveDialog(false)}
        />
      )}

      <div style={{ display: 'flex', flexDirection: 'column', width: '100vw', height: '100vh' }}>

        {/* ── Workflow header bar ───────────────────────── */}
        <div className="workflow-header">
          <button className="wh-back-btn" onClick={onHome} title="Back to workflows">
            <FiHome size={13} /> Home
          </button>
          <div className="wh-divider" />
          <span className="workflow-title-text">
            {pipelineName || <span style={{ color: '#555' }}>Untitled Workflow</span>}
          </span>
          <button
            className="wh-rename-btn"
            onClick={() => setShowSaveDialog(true)}
            title="Rename workflow"
          >
            <FiEdit2 size={12} />
          </button>
          <button
            className="theme-toggle"
            onClick={toggleTheme}
            title={`Switch to ${theme === 'dark' ? 'light' : 'dark'} mode`}
          >
            {theme === 'dark' ? <FiSun size={15} /> : <FiMoon size={15} />}
          </button>
        </div>

        {/* ── Canvas + right panel + log pane ─────────────── */}
        <div style={{ flex: 1, display: 'flex', flexDirection: 'column', minHeight: 0 }}>
        <div style={{ flex: 1, display: 'flex', minHeight: 0 }}>

          <div style={{ flex: 1, minWidth: 0 }}>
            <ReactFlow
              nodes={nodes}
              edges={edges}
              onNodesChange={onNodesChange}
              onEdgesChange={onEdgesChange}
              onConnect={onConnect}
              fitView
              nodeTypes={nodeTypes}
              edgeTypes={edgeTypes}
              defaultEdgeOptions={defaultEdgeOptions}
            >
              {/* Left toolbar */}
              <Panel position="top-left">
                <div className="toolbar">
                  <div className="toolbar-group">
                    <span className="toolbar-label">Add</span>
                    {(['kafka', 'scylladb', 'clickhouse'] as const).map(t => (
                      <button
                        key={t}
                        className="toolbar-btn"
                        style={{ background: { kafka: '#e92a67', scylladb: '#2a8af6', clickhouse: '#f6a82a' }[t] }}
                        onClick={() => addNode(t)}
                      >
                        + {t}
                      </button>
                    ))}
                  </div>
                  <div className="toolbar-divider" />
                  <button className="toolbar-btn danger" onClick={deleteSelected}>Delete</button>
                  <span className="toolbar-count">{nodes.length} node{nodes.length !== 1 ? 's' : ''}</span>
                </div>
              </Panel>

              {/* Right toolbar */}
              <Panel position="top-right">
                <div className="toolbar">
                  {autoSaved && <span className="toolbar-autosaved">✓ saved</span>}
                  {!backendOnline && <span className="toolbar-offline">backend offline</span>}
                  <button className="toolbar-btn" onClick={() => savePipeline()} disabled={runStatus === 'running'}>
                    <FiSave size={13} /> Save
                  </button>
                  {runStatus === 'running' && (
                    <button
                      className="toolbar-btn stop-btn"
                      onClick={stopPipeline}
                      title="Stop pipeline"
                    >
                      <FiSquare size={13} /> Stop
                    </button>
                  )}
                  <button
                    className={`toolbar-btn run-btn ${runStatus}`}
                    onClick={runStatus === 'running' ? () => { setShowLog(true); setLogCollapsed(false); } : runPipeline}
                    disabled={runStatus === 'saving'}
                  >
                    <StatusIcon />
                    <FiPlay size={13} />
                    {runLabel[runStatus]}
                  </button>
                  <button
                    className={`toolbar-btn${showLog ? ' active' : ''}`}
                    onClick={() => { setShowLog(v => !v); setLogCollapsed(false); }}
                    title="Toggle log pane"
                  >
                    <FiTerminal size={13} /> Logs
                  </button>
                </div>
              </Panel>

              <Controls showInteractive={false} />

              <svg>
                <defs>
                  <linearGradient id="edge-gradient">
                    <stop offset="0%"   stopColor="#ae53ba" />
                    <stop offset="100%" stopColor="#2a8af6" />
                  </linearGradient>
                  <marker
                    id="edge-circle"
                    viewBox="-5 -5 10 10"
                    refX="0" refY="0"
                    markerUnits="strokeWidth"
                    markerWidth="10" markerHeight="10"
                    orient="auto"
                  >
                    <circle stroke="#2a8af6" strokeOpacity="0.75" r="2" cx="0" cy="0" />
                  </marker>
                </defs>
              </svg>
            </ReactFlow>
          </div>

          {/* Right edit panel */}
          <div className={`right-panel${activePanel ? ' open' : ''}`}>
            {activePanel && (
              <NodeEditModal
                nodeId={activePanel.id}
                data={activePanel.data}
                connectedSources={activePanel.connectedSources}
                onSave={handlePanelSave}
                onClose={() => setActivePanel(null)}
              />
            )}
          </div>

        </div>{/* end canvas row */}

        {/* ── Bottom log pane ────────────────────────────── */}
        {showLog && (
          <div className={`log-bottom-pane${logCollapsed ? ' collapsed' : ''}`}>
            <div className="log-bottom-header" onClick={() => setLogCollapsed(v => !v)}>
              <span className="log-bottom-title">
                <FiTerminal size={13} />
                Pipeline Logs
                {runStatus === 'running'   && <FiLoader size={12} className="spin" style={{ marginLeft: 6 }} />}
                {runStatus === 'success'   && <FiCheckCircle size={12} color="#4caf50" style={{ marginLeft: 6 }} />}
                {runStatus === 'failed'    && <FiAlertCircle size={12} color="#e92a67" style={{ marginLeft: 6 }} />}
                {runStatus === 'cancelled' && <FiSquare size={12} color="#ff9800" style={{ marginLeft: 6 }} />}
              </span>
              <div className="log-bottom-actions" onClick={e => e.stopPropagation()}>
                <button
                  className="log-bottom-btn"
                  onClick={() => setLogCollapsed(v => !v)}
                  title={logCollapsed ? 'Expand' : 'Collapse'}
                >
                  {logCollapsed ? <FiChevronUp size={13} /> : <FiChevronDown size={13} />}
                </button>
                <button
                  className="log-bottom-btn"
                  onClick={() => setShowLog(false)}
                  title="Close"
                >
                  ×
                </button>
              </div>
            </div>
            {!logCollapsed && (
              <pre ref={logBodyRef} className="log-bottom-body">
                {runLog || '(no output yet)'}
              </pre>
            )}
          </div>
        )}

        </div>{/* end outer column */}
      </div>

      {/* Claude chat agent — fixed bottom-right, outside the layout flow */}
      <ChatAgent
        nodes={nodes}
        edges={edges}
        setNodes={setNodes}
        setEdges={setEdges}
        pipelineName={pipelineName}
      />
    </NodeEditContext.Provider>
  );
}
