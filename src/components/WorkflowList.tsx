import { useEffect, useState } from 'react';
import { FiPlus, FiSearch, FiZap, FiSun, FiMoon, FiExternalLink, FiActivity } from 'react-icons/fi';
import { useTheme } from '../context/ThemeContext';

const BACKEND = 'http://localhost:8000';

interface WorkflowMeta {
  id: number;
  name: string;
  created_at: string;
  updated_at: string;
  node_count: number;
  last_run_status: string | null;
  last_run_at: string | null;
}

const STATUS_CFG: Record<string, { label: string; cls: string; pulse?: boolean }> = {
  running:   { label: 'Running',    cls: 'cl-badge-running',   pulse: true },
  success:   { label: 'Success',    cls: 'cl-badge-success'  },
  failed:    { label: 'Failed',     cls: 'cl-badge-failed'   },
  pending:   { label: 'Pending',    cls: 'cl-badge-pending'  },
  cancelled: { label: 'Cancelled',  cls: 'cl-badge-cancelled'},
};

const NODE_COLORS = ['#e92a67', '#a853ba', '#2a8af6', '#f6a82a', '#22bb6e', '#5bc0de'];

function avatarColor(name: string): string {
  let h = 0;
  for (let i = 0; i < name.length; i++) h = (h * 31 + name.charCodeAt(i)) >>> 0;
  return NODE_COLORS[h % NODE_COLORS.length] ?? '#a853ba';
}

function initials(name: string): string {
  return name
    .split(/[\s\-_]+/)
    .filter(Boolean)
    .slice(0, 2)
    .map(w => (w[0] ?? '').toUpperCase())
    .join('');
}

function formatAge(iso: string | null): string {
  if (!iso) return '—';
  const ms = Date.now() - new Date(iso).getTime();
  const mins = Math.floor(ms / 60000);
  if (mins < 1)  return 'just now';
  if (mins < 60) return `${mins}m ago`;
  const hrs = Math.floor(mins / 60);
  if (hrs < 24)  return `${hrs}h ago`;
  return `${Math.floor(hrs / 24)}d ago`;
}

interface Props {
  onOpen: (name: string) => void;
  onNew: () => void;
  onMonitor: () => void;
}

export default function WorkflowList({ onOpen, onNew, onMonitor }: Props) {
  const [workflows, setWorkflows] = useState<WorkflowMeta[]>([]);
  const [loading, setLoading]     = useState(true);
  const [query, setQuery]         = useState('');
  const { theme, toggleTheme }    = useTheme();

  useEffect(() => {
    fetch(`${BACKEND}/api/pipelines`)
      .then(r => r.json())
      .then(d => setWorkflows(Array.isArray(d) ? d : []))
      .catch(() => setWorkflows([]))
      .finally(() => setLoading(false));
  }, []);

  const filtered = query.trim()
    ? workflows.filter(w => w.name.toLowerCase().includes(query.toLowerCase()))
    : workflows;

  return (
    <div className="home-screen">

      {/* ── Top nav ─────────────────────────────────────── */}
      <div className="home-topbar">
        <div className="home-topbar-brand">
          <FiZap size={18} className="home-zap" />
          <span className="home-brand-name">x-stream</span>
          <span className="home-brand-tag">Workflow Designer</span>
        </div>
        <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
          <button className="mon-back-btn" onClick={onMonitor} title="Flink Monitor">
            <FiActivity size={14} /> Monitor
          </button>
          <button className="theme-toggle" onClick={toggleTheme} title={`Switch to ${theme === 'dark' ? 'light' : 'dark'} mode`}>
            {theme === 'dark' ? <FiSun size={17} /> : <FiMoon size={17} />}
          </button>
        </div>
      </div>

      {/* ── Main content ─────────────────────────────────── */}
      <div className="home-content">
        <div className="cl-card">

          {/* ── Card header (gradient) ─────────────────── */}
          <div className="cl-card-header">
            <div className="cl-card-header-left">
              <span className="cl-card-title">Pipelines</span>
              {!loading && (
                <span className="cl-card-count">
                  {filtered.length} workflow{filtered.length !== 1 ? 's' : ''}
                </span>
              )}
            </div>
            <div className="cl-card-tools">
              <div className="cl-search-wrap">
                <FiSearch size={13} className="cl-search-icon" />
                <input
                  className="cl-search"
                  placeholder="Search pipelines…"
                  value={query}
                  onChange={e => setQuery(e.target.value)}
                />
              </div>
              <button className="cl-new-btn" onClick={onNew}>
                <FiPlus size={14} /> New Pipeline
              </button>
            </div>
          </div>

          {/* ── Table ─────────────────────────────────── */}
          <div className="cl-table-wrap">
            {loading ? (
              <div className="cl-state">Loading…</div>
            ) : filtered.length === 0 ? (
              <div className="cl-state">
                {query
                  ? `No pipelines matching "${query}"`
                  : <><span>No pipelines yet.</span><button className="home-link-btn" onClick={onNew}> Create your first one →</button></>
                }
              </div>
            ) : (
              <table className="cl-table">
                <thead>
                  <tr className="cl-thead-row">
                    <th className="cl-th">#</th>
                    <th className="cl-th">Pipeline</th>
                    <th className="cl-th cl-th-center">Nodes</th>
                    <th className="cl-th">Last Updated</th>
                    <th className="cl-th">Last Run</th>
                    <th className="cl-th">Status</th>
                    <th className="cl-th cl-th-right">Action</th>
                  </tr>
                </thead>
                <tbody>
                  {filtered.map((w, i) => {
                    const st = w.last_run_status ? STATUS_CFG[w.last_run_status] : null;
                    const bg = avatarColor(w.name);
                    return (
                      <tr key={w.id} className="cl-row" onClick={() => onOpen(w.name)}>
                        <td className="cl-td cl-td-num">{i + 1}</td>
                        <td className="cl-td">
                          <div className="cl-name-cell">
                            <span className="cl-avatar" style={{ background: bg }}>
                              {initials(w.name)}
                            </span>
                            <div className="cl-name-text">
                              <span className="cl-name-primary">{w.name}</span>
                              <span className="cl-name-sub">
                                Created {formatAge(w.created_at)}
                              </span>
                            </div>
                          </div>
                        </td>
                        <td className="cl-td cl-td-center">
                          <span className="cl-nodes-badge">{w.node_count}</span>
                        </td>
                        <td className="cl-td cl-td-meta">{formatAge(w.updated_at)}</td>
                        <td className="cl-td cl-td-meta">{formatAge(w.last_run_at)}</td>
                        <td className="cl-td">
                          {st ? (
                            <span className={`cl-badge ${st.cls}${st.pulse ? ' cl-badge-pulse' : ''}`}>
                              <span className="cl-badge-dot" />
                              {st.label}
                            </span>
                          ) : (
                            <span className="cl-badge cl-badge-none">Not run</span>
                          )}
                        </td>
                        <td className="cl-td cl-td-action">
                          <button
                            className="cl-open-btn"
                            onClick={e => { e.stopPropagation(); onOpen(w.name); }}
                          >
                            <FiExternalLink size={12} /> Open
                          </button>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            )}
          </div>

          {/* ── Card footer ───────────────────────────── */}
          {!loading && filtered.length > 0 && (
            <div className="cl-card-footer">
              Showing {filtered.length} of {workflows.length} pipeline{workflows.length !== 1 ? 's' : ''}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
