import { useCallback, useEffect, useRef, useState } from 'react';

const BACKEND = 'http://localhost:8000';
const SUMMARY_REFRESH_MS = 60_000; // "latest status of the nodes every one minute"
const POLL_MS = 3_000;

export type RunStatus = 'idle' | 'saving' | 'running' | 'success' | 'failed' | 'cancelled';
export type NodeStatus = 'pending' | 'running' | 'success' | 'failed';

export const SUMMARY_TAB = '__summary__';

interface LogEntry {
  id: number;
  level: string;
  message: string;
  timestamp: string;
  node_id: string | null;
  node_label: string | null;
}

export interface NodeSummary {
  node_id: string;
  node_label: string;
  node_type: string;
  status: NodeStatus;
  last_level: string | null;
  last_message: string | null;
  timestamp: string | null;
  log_count: number;
  error_count: number;
}

interface RunSummary {
  run_id: number;
  run_status: string | null;
  flink_job_id: string | null;
  nodes: NodeSummary[];
}

interface UsePipelineRunOptions {
  pipelineName: string;
  savePipeline: (nameOverride?: string) => Promise<boolean>;
}

const PIPELINE_KEY = '__pipeline__';

function formatLogEntries(entries: LogEntry[]): string {
  return entries
    .map(e => {
      const ts = new Date(e.timestamp).toLocaleTimeString();
      return `[${ts}] ${e.level.padEnd(7)} ${e.message}`;
    })
    .join('\n');
}

const RUN_TO_STATUS: Record<string, RunStatus> = {
  pending: 'running', running: 'running',
  success: 'success', failed: 'failed', cancelled: 'cancelled',
};

export function usePipelineRun({ pipelineName, savePipeline }: UsePipelineRunOptions) {
  const [runStatus, setRunStatus] = useState<RunStatus>('idle');
  const [showLog, setShowLog] = useState(false);
  const [logCollapsed, setLogCollapsed] = useState(false);

  const [summary, setSummary] = useState<NodeSummary[]>([]);
  const [summaryLog, setSummaryLog] = useState(''); // pipeline-level messages
  const [nodeLogs, setNodeLogs] = useState<Record<string, string>>({});
  const [openTabs, setOpenTabs] = useState<Array<{ id: string; label: string }>>([]);
  const [activeTab, setActiveTab] = useState<string>(SUMMARY_TAB);

  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const summaryTimerRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const runIdRef = useRef<number | null>(null);

  const stopPolling = useCallback(() => {
    if (pollRef.current) {
      clearInterval(pollRef.current);
      pollRef.current = null;
    }
  }, []);

  // Fetch all logs + summary in one refresh, then split logs per node.
  const refresh = useCallback(async (runId: number) => {
    try {
      const [logsRes, summaryRes] = await Promise.all([
        fetch(`${BACKEND}/api/pipeline/${encodeURIComponent(pipelineName)}/runs/${runId}/logs`),
        fetch(`${BACKEND}/api/pipeline/${encodeURIComponent(pipelineName)}/runs/${runId}/summary`),
      ]);
      const entries: LogEntry[] = await logsRes.json();
      const sum: RunSummary = await summaryRes.json();

      if (Array.isArray(sum.nodes)) setSummary(sum.nodes);
      const mapped = sum.run_status ? RUN_TO_STATUS[sum.run_status] : undefined;
      if (mapped) {
        setRunStatus(mapped);
        if (mapped !== 'running') stopPolling();
      }

      if (Array.isArray(entries)) {
        const grouped: Record<string, LogEntry[]> = {};
        for (const e of entries) {
          const key = e.node_id ?? PIPELINE_KEY;
          (grouped[key] ??= []).push(e);
        }
        const formatted: Record<string, string> = {};
        for (const [key, list] of Object.entries(grouped)) {
          formatted[key] = formatLogEntries(list);
        }
        setNodeLogs(formatted);
        setSummaryLog(formatted[PIPELINE_KEY] ?? '');
      }
    } catch { /* ignore transient fetch errors */ }
  }, [pipelineName, stopPolling]);

  const startPolling = useCallback((runId: number) => {
    stopPolling();
    runIdRef.current = runId;
    refresh(runId);
    pollRef.current = setInterval(() => refresh(runId), POLL_MS);
  }, [refresh, stopPolling]);

  // 1-minute summary heartbeat: refreshes node statuses whenever the log pane is
  // open and a run exists — covers the period after fast polling has stopped.
  useEffect(() => {
    if (summaryTimerRef.current) {
      clearInterval(summaryTimerRef.current);
      summaryTimerRef.current = null;
    }
    if (!showLog) return;
    summaryTimerRef.current = setInterval(() => {
      if (runIdRef.current != null) refresh(runIdRef.current);
    }, SUMMARY_REFRESH_MS);
    return () => {
      if (summaryTimerRef.current) clearInterval(summaryTimerRef.current);
    };
  }, [showLog, refresh]);

  useEffect(() => () => stopPolling(), [stopPolling]);

  // On mount: resume tracking if a job is genuinely still running (cross-checked
  // against Flink health to avoid a stale DB "running" row after a backend restart).
  useEffect(() => {
    if (!pipelineName) return;
    (async () => {
      try {
        const runsRes = await fetch(`${BACKEND}/api/pipeline/${encodeURIComponent(pipelineName)}/runs`);
        const runs: Array<{ id: number; status: string }> = await runsRes.json();
        if (!Array.isArray(runs) || runs.length === 0) return;
        const latest = runs[0];
        if (!latest) return;

        // Always load the latest run's logs/summary so the pane isn't empty,
        // but only resume live polling if the Flink job is actually alive.
        runIdRef.current = latest.id;
        await refresh(latest.id);

        if (latest.status === 'running' || latest.status === 'pending') {
          try {
            const healthRes = await fetch(`${BACKEND}/api/pipeline/${encodeURIComponent(pipelineName)}/health`);
            const health = await healthRes.json();
            if (health.healthy === true) {
              setRunStatus('running');
              setShowLog(true);
              setLogCollapsed(false);
              startPolling(latest.id);
            }
          } catch { /* unreachable health — don't assume running */ }
        }
      } catch { /* ignore */ }
    })();
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [pipelineName]);

  const openNodeTab = useCallback((nodeId: string, label: string) => {
    setOpenTabs(tabs => (tabs.some(t => t.id === nodeId) ? tabs : [...tabs, { id: nodeId, label }]));
    setActiveTab(nodeId);
    if (runIdRef.current != null) refresh(runIdRef.current);
  }, [refresh]);

  const closeNodeTab = useCallback((nodeId: string) => {
    setOpenTabs(tabs => tabs.filter(t => t.id !== nodeId));
    setActiveTab(prev => (prev === nodeId ? SUMMARY_TAB : prev));
  }, []);

  const runPipeline = useCallback(async () => {
    const saved = await savePipeline();
    if (!saved) return;

    setRunStatus('running');
    setSummaryLog('Submitting pipeline to Flink…');
    setNodeLogs({});
    setSummary([]);
    setActiveTab(SUMMARY_TAB);
    setShowLog(true);
    setLogCollapsed(false);

    try {
      const res = await fetch(
        `${BACKEND}/api/pipeline/${encodeURIComponent(pipelineName)}/run`,
        { method: 'POST' }
      );
      const json = await res.json();
      if (!res.ok) {
        setSummaryLog(json.detail ?? JSON.stringify(json));
        setRunStatus('failed');
        return;
      }
      startPolling(json.run_id);
    } catch (err) {
      setSummaryLog(String(err));
      setRunStatus('failed');
    }
  }, [savePipeline, pipelineName, startPolling]);

  const stopPipeline = useCallback(async () => {
    try {
      const res = await fetch(
        `${BACKEND}/api/pipeline/${encodeURIComponent(pipelineName)}/stop`,
        { method: 'POST' }
      );
      const json = await res.json();
      stopPolling();
      setRunStatus('cancelled');
      if (json.run_id) await refresh(json.run_id);
    } catch (err) {
      stopPolling();
      setRunStatus('cancelled');
      setSummaryLog(`Stop request failed: ${String(err)}`);
    }
  }, [pipelineName, stopPolling, refresh]);

  return {
    runStatus, setRunStatus,
    showLog, setShowLog,
    logCollapsed, setLogCollapsed,
    summary, summaryLog, nodeLogs,
    openTabs, activeTab, setActiveTab,
    openNodeTab, closeNodeTab,
    runPipeline,
    stopPipeline,
  };
}
