import { useCallback, useEffect, useRef, useState } from 'react';

const BACKEND = 'http://localhost:8000';

export type RunStatus = 'idle' | 'saving' | 'running' | 'success' | 'failed' | 'cancelled';

interface UsePipelineRunOptions {
  pipelineName: string;
  savePipeline: (nameOverride?: string) => Promise<boolean>;
}

export function usePipelineRun({ pipelineName, savePipeline }: UsePipelineRunOptions) {
  const [runStatus, setRunStatus] = useState<RunStatus>('idle');
  const [runLog, setRunLog] = useState('');
  const [showLog, setShowLog] = useState(false);
  const [logCollapsed, setLogCollapsed] = useState(false);
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const stopPolling = useCallback(() => {
    if (pollRef.current) {
      clearInterval(pollRef.current);
      pollRef.current = null;
    }
  }, []);

  // Cancel polling on unmount to prevent state updates on an unmounted component
  useEffect(() => {
    return () => stopPolling();
  }, [stopPolling]);

  const startPolling = useCallback((runId: number) => {
    stopPolling();
    pollRef.current = setInterval(async () => {
      try {
        const runsRes = await fetch(
          `${BACKEND}/api/pipeline/${encodeURIComponent(pipelineName)}/runs`
        );
        const runs: Array<{ id: number; status: string; logs?: string }> =
          await runsRes.json();
        const run = runs.find(r => r.id === runId);
        if (!run) return;
        setRunLog(run.logs ?? run.status);
        if (run.status === 'success') {
          setRunStatus('success');
          stopPolling();
        } else if (run.status === 'cancelled') {
          setRunStatus('cancelled');
          stopPolling();
        } else if (run.status === 'failed') {
          setRunStatus('failed');
          stopPolling();
        } else if (run.status === 'running') {
          setRunStatus('running');
        }
      } catch { /* ignore transient fetch errors */ }
    }, 3000);
  }, [pipelineName, stopPolling]);

  // On mount: detect if this pipeline already has a running job and resume status tracking
  useEffect(() => {
    if (!pipelineName) return;
    fetch(`${BACKEND}/api/pipeline/${encodeURIComponent(pipelineName)}/runs`)
      .then(r => r.json())
      .then((runs: Array<{ id: number; status: string; logs?: string }>) => {
        if (!Array.isArray(runs) || runs.length === 0) return;
        const latest = runs[0];
        if (!latest) return;
        if (latest.status === 'running') {
          setRunStatus('running');
          setRunLog(latest.logs ?? 'Pipeline is running…');
          setShowLog(true);
          setLogCollapsed(false);
          startPolling(latest.id);
        }
      })
      .catch(() => {});
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [pipelineName]);

  const runPipeline = useCallback(async () => {
    const saved = await savePipeline();
    if (!saved) return;

    setRunStatus('running');
    setRunLog('Submitting pipeline to Flink…');
    setShowLog(true);
    setLogCollapsed(false);

    try {
      const res = await fetch(
        `${BACKEND}/api/pipeline/${encodeURIComponent(pipelineName)}/run`,
        { method: 'POST' }
      );
      const json = await res.json();
      if (!res.ok) {
        setRunLog(json.detail ?? JSON.stringify(json));
        setRunStatus('failed');
        return;
      }
      const runId: number = json.run_id;
      setRunLog(`Run queued (id=${runId}). Polling status…`);
      startPolling(runId);
    } catch (err) {
      setRunLog(String(err));
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
      setRunLog(`Pipeline stopped (run_id=${json.run_id ?? '?'})`);
    } catch (err) {
      stopPolling();
      setRunStatus('cancelled');
      setRunLog(`Stop request failed: ${String(err)}`);
    }
  }, [pipelineName, stopPolling]);

  return {
    runStatus, setRunStatus,
    runLog, setRunLog,
    showLog, setShowLog,
    logCollapsed, setLogCollapsed,
    runPipeline,
    stopPipeline,
  };
}
