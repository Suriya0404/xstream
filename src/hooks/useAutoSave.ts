import { useCallback, useEffect, useRef, useState } from 'react';

const BACKEND = 'http://localhost:8000';

interface UseAutoSaveOptions {
  nodes: any[];
  edges: any[];
  pipelineName: string;
  loadedRef: React.MutableRefObject<boolean>;
  justLoadedRef: React.MutableRefObject<boolean>;
  nodePayload: (n: any) => any;
  edgePayload: (e: any) => any;
}

export function useAutoSave({
  nodes,
  edges,
  pipelineName,
  loadedRef,
  justLoadedRef,
  nodePayload,
  edgePayload,
}: UseAutoSaveOptions) {
  const [autoSaved, setAutoSaved] = useState(false);
  const timerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const silentSave = useCallback(async () => {
    if (!loadedRef.current || !pipelineName) return;
    try {
      await fetch(`${BACKEND}/api/pipeline`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          name: pipelineName,
          nodes: nodes.map(nodePayload),
          edges: edges.map(edgePayload),
        }),
      });
      setAutoSaved(true);
      setTimeout(() => setAutoSaved(false), 2500);
    } catch { /* silent */ }
  }, [nodes, edges, nodePayload, edgePayload, pipelineName, loadedRef]);

  // Debounce auto-save: fire 1.5s after the last change
  useEffect(() => {
    if (!loadedRef.current || !pipelineName) return;
    if (justLoadedRef.current) {
      justLoadedRef.current = false;
      return;
    }
    if (timerRef.current) clearTimeout(timerRef.current);
    timerRef.current = setTimeout(silentSave, 1500);
    return () => {
      if (timerRef.current) clearTimeout(timerRef.current);
    };
  }, [nodes, edges, silentSave, pipelineName, loadedRef, justLoadedRef]);

  // Flush on unmount so we don't leave a dangling timer
  useEffect(() => {
    return () => {
      if (timerRef.current) clearTimeout(timerRef.current);
    };
  }, []);

  return { autoSaved, silentSave };
}
