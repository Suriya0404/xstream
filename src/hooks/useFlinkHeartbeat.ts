import { useEffect, useRef, useState } from 'react';

const BACKEND = 'http://localhost:8000';
const HEARTBEAT_MS = 6000;

export type FlinkHealth = 'healthy' | 'unhealthy' | 'unknown';

/** Polls the pipeline's Flink job heartbeat periodically. 'unknown' means no job has run yet. */
export function useFlinkHeartbeat(pipelineName: string): FlinkHealth {
  const [health, setHealth] = useState<FlinkHealth>('unknown');
  const timerRef = useRef<ReturnType<typeof setInterval> | null>(null);

  useEffect(() => {
    if (!pipelineName) {
      setHealth('unknown');
      return;
    }

    let cancelled = false;
    const check = async () => {
      try {
        const res = await fetch(
          `${BACKEND}/api/pipeline/${encodeURIComponent(pipelineName)}/health`
        );
        const data: { healthy: boolean | null } = await res.json();
        if (cancelled) return;
        setHealth(data.healthy === null ? 'unknown' : data.healthy ? 'healthy' : 'unhealthy');
      } catch {
        if (!cancelled) setHealth('unhealthy');
      }
    };

    check();
    timerRef.current = setInterval(check, HEARTBEAT_MS);
    return () => {
      cancelled = true;
      if (timerRef.current) clearInterval(timerRef.current);
    };
  }, [pipelineName]);

  return health;
}
