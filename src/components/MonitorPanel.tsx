import { useCallback, useEffect, useRef, useState } from 'react';
import { FiActivity, FiRefreshCw, FiStopCircle, FiZap, FiSun, FiMoon, FiArrowLeft } from 'react-icons/fi';
import { useTheme } from '../context/ThemeContext';

const BACKEND = 'http://localhost:8000';
const AUTO_REFRESH_MS = 5000;

interface FlinkJob {
  jid: string;
  name: string;
  state: string;
  'start-time': number;
  'end-time': number;
  duration: number;
  tasks?: {
    total: number;
    running: number;
    finished: number;
    failed: number;
    canceled: number;
  };
}

const STATE_CFG: Record<string, { label: string; cls: string; pulse?: boolean }> = {
  RUNNING:    { label: 'Running',    cls: 'mon-state-running',   pulse: true  },
  FINISHED:   { label: 'Finished',   cls: 'mon-state-finished'               },
  FAILED:     { label: 'Failed',     cls: 'mon-state-failed'                 },
  CANCELED:   { label: 'Canceled',   cls: 'mon-state-canceled'               },
  RESTARTING: { label: 'Restarting', cls: 'mon-state-running',   pulse: true  },
  CREATED:    { label: 'Created',    cls: 'mon-state-pending'                },
  SUSPENDED:  { label: 'Suspended',  cls: 'mon-state-pending'                },
};

function formatDuration(ms: number): string {
  if (ms < 0) return '—';
  const s = Math.floor(ms / 1000);
  if (s < 60)   return `${s}s`;
  const m = Math.floor(s / 60);
  if (m < 60)   return `${m}m ${s % 60}s`;
  const h = Math.floor(m / 60);
  return `${h}h ${m % 60}m`;
}

function formatTime(ms: number): string {
  if (ms <= 0) return '—';
  return new Date(ms).toLocaleTimeString();
}

function shortId(jid: string): string {
  return jid.slice(0, 8) + '…';
}

interface Props {
  onBack: () => void;
}

export default function MonitorPanel({ onBack }: Props) {
  const [jobs, setJobs]         = useState<FlinkJob[]>([]);
  const [loading, setLoading]   = useState(true);
  const [error, setError]       = useState<string | null>(null);
  const [autoRefresh, setAutoRefresh] = useState(true);
  const [cancelling, setCancelling]   = useState<Set<string>>(new Set());
  const timerRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const { theme, toggleTheme }  = useTheme();

  const fetchJobs = useCallback(async () => {
    try {
      const r = await fetch(`${BACKEND}/api/flink/jobs`);
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      const data = await r.json();
      setJobs(Array.isArray(data.jobs) ? data.jobs : []);
      setError(null);
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : 'Failed to reach Flink');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchJobs();
  }, [fetchJobs]);

  useEffect(() => {
    if (autoRefresh) {
      timerRef.current = setInterval(fetchJobs, AUTO_REFRESH_MS);
    } else if (timerRef.current) {
      clearInterval(timerRef.current);
      timerRef.current = null;
    }
    return () => { if (timerRef.current) clearInterval(timerRef.current); };
  }, [autoRefresh, fetchJobs]);

  async function cancelJob(jid: string) {
    setCancelling(prev => new Set(prev).add(jid));
    try {
      await fetch(`${BACKEND}/api/flink/jobs/${jid}`, { method: 'DELETE' });
      await fetchJobs();
    } finally {
      setCancelling(prev => { const s = new Set(prev); s.delete(jid); return s; });
    }
  }

  const running = jobs.filter(j => j.state === 'RUNNING').length;

  return (
    <div className="home-screen">

      {/* ── Top nav ──────────────────────────────────────── */}
      <div className="home-topbar">
        <div className="home-topbar-brand">
          <FiZap size={18} className="home-zap" />
          <span className="home-brand-name">x-stream</span>
          <span className="home-brand-tag">Flink Monitor</span>
        </div>
        <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
          <button className="mon-back-btn" onClick={onBack}>
            <FiArrowLeft size={14} /> Pipelines
          </button>
          <button className="theme-toggle" onClick={toggleTheme} title="Toggle theme">
            {theme === 'dark' ? <FiSun size={17} /> : <FiMoon size={17} />}
          </button>
        </div>
      </div>

      {/* ── Main content ─────────────────────────────────── */}
      <div className="home-content">
        <div className="cl-card">

          {/* ── Header ─────────────────────────────────── */}
          <div className="cl-card-header">
            <div className="cl-card-header-left">
              <FiActivity size={15} style={{ opacity: 0.7, marginRight: 6 }} />
              <span className="cl-card-title">Flink Jobs</span>
              {!loading && !error && (
                <span className="cl-card-count">
                  {running} running · {jobs.length} total
                </span>
              )}
            </div>
            <div className="cl-card-tools">
              <label className="mon-auto-toggle">
                <input
                  type="checkbox"
                  checked={autoRefresh}
                  onChange={e => setAutoRefresh(e.target.checked)}
                />
                Auto-refresh
              </label>
              <button
                className="cl-new-btn"
                onClick={() => { setLoading(true); fetchJobs(); }}
                disabled={loading}
              >
                <FiRefreshCw size={13} className={loading ? 'mon-spin' : ''} />
                Refresh
              </button>
            </div>
          </div>

          {/* ── Body ───────────────────────────────────── */}
          <div className="cl-table-wrap">
            {error ? (
              <div className="cl-state mon-error">
                <FiActivity size={20} style={{ marginBottom: 6 }} />
                <div>Cannot reach Flink JobManager</div>
                <div className="mon-error-sub">{error}</div>
                <div className="mon-error-sub" style={{ marginTop: 4 }}>
                  Make sure Flink is running at <code>http://localhost:8082</code>
                </div>
              </div>
            ) : loading ? (
              <div className="cl-state">Connecting to Flink…</div>
            ) : jobs.length === 0 ? (
              <div className="cl-state">
                <FiActivity size={20} style={{ marginBottom: 6, opacity: 0.4 }} />
                <div>No Flink jobs found</div>
                <div style={{ fontSize: 12, color: 'var(--text-muted)', marginTop: 4 }}>
                  Run a pipeline from the Workflows screen to submit a job.
                </div>
              </div>
            ) : (
              <table className="cl-table">
                <thead>
                  <tr className="cl-thead-row">
                    <th className="cl-th">Job ID</th>
                    <th className="cl-th">Name</th>
                    <th className="cl-th">Status</th>
                    <th className="cl-th">Started</th>
                    <th className="cl-th">Duration</th>
                    <th className="cl-th cl-th-center">Tasks</th>
                    <th className="cl-th cl-th-right">Action</th>
                  </tr>
                </thead>
                <tbody>
                  {jobs.map(job => {
                    const st = STATE_CFG[job.state] ?? { label: job.state, cls: 'mon-state-pending' };
                    const live = job['end-time'] <= 0;
                    const dur = live
                      ? Date.now() - job['start-time']
                      : job.duration;
                    return (
                      <tr key={job.jid} className="cl-row">
                        <td className="cl-td cl-td-meta">
                          <code className="mon-jid" title={job.jid}>{shortId(job.jid)}</code>
                        </td>
                        <td className="cl-td">
                          <span className="cl-name-primary">{job.name || '—'}</span>
                        </td>
                        <td className="cl-td">
                          <span className={`cl-badge ${st.cls}${st.pulse ? ' cl-badge-pulse' : ''}`}>
                            <span className="cl-badge-dot" />
                            {st.label}
                          </span>
                        </td>
                        <td className="cl-td cl-td-meta">{formatTime(job['start-time'])}</td>
                        <td className="cl-td cl-td-meta">{formatDuration(dur)}</td>
                        <td className="cl-td cl-td-center">
                          {job.tasks ? (
                            <span className="mon-tasks">
                              <span className="mon-t-run">{job.tasks.running}</span>
                              {' / '}
                              <span className="mon-t-total">{job.tasks.total}</span>
                            </span>
                          ) : '—'}
                        </td>
                        <td className="cl-td cl-td-action">
                          {job.state === 'RUNNING' && (
                            <button
                              className="mon-cancel-btn"
                              onClick={() => cancelJob(job.jid)}
                              disabled={cancelling.has(job.jid)}
                              title="Cancel job"
                            >
                              <FiStopCircle size={12} />
                              {cancelling.has(job.jid) ? 'Stopping…' : 'Cancel'}
                            </button>
                          )}
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            )}
          </div>

          {!loading && !error && jobs.length > 0 && (
            <div className="cl-card-footer">
              {autoRefresh ? `Auto-refreshing every ${AUTO_REFRESH_MS / 1000}s` : 'Auto-refresh paused'}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
