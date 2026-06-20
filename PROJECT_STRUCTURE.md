# x-stream — Complete Project Structure

The platform is split into two repositories:

| Repo | Purpose |
|------|---------|
| **x-stream** | UI (React + Vite), FastAPI backend, Docker Compose, Kubernetes manifests for all infrastructure |
| **x-stream-jobs** | Generated PyFlink job files, workflow YAML, Flink-specific config, Docker image, K8s manifests for Flink |

---

## x-stream (UI + Backend)

```
x-stream/
├── config.yaml                        # Central config (MySQL, Kafka, Flink, ScyllaDB, ClickHouse,
│                                      #   Anthropic, Finnhub, CORS, jobs_repo path)
│
├── backend/                           # FastAPI backend
│   ├── main.py                        # App factory: mounts routes, starts FlinkJobManager/SQLGateway
│   ├── config.py                      # load_config() with env-var overrides (JOBS_REPO_PATH, etc.)
│   ├── models.py                      # SQLAlchemy: Pipeline, Node, Edge, PipelineRun, PipelineRunLog
│   ├── database.py                    # SQLAlchemy engine + Session factory
│   ├── flink_job_generator.py         # Generates PyFlink .py + workflow .yaml on every pipeline save
│   │                                  # → writes to x-stream-jobs/ (resolved via JOBS_REPO_PATH or config)
│   ├── flink_runner.py                # FlinkSQLGateway (submit SQL) + FlinkJobManager (REST API)
│   ├── sql_parser.py                  # Parse CREATE TABLE SQL → field list
│   ├── Dockerfile                     # Backend container image
│   ├── requirements.txt
│   ├── api/
│   │   └── routes/
│   │       ├── pipelines.py           # CRUD + run/stop + /api/flink/jobs + /api/flink/jobs/{id}
│   │       ├── chat.py                # AI assistant route (Anthropic Claude)
│   │       └── health.py             # /health liveness probe
│   ├── services/
│   │   └── pipeline_service.py        # execute_pipeline() background task
│   ├── core/
│   │   └── constants.py
│   └── tests/
│       ├── test_flink_runner.py
│       ├── test_pipeline_service.py
│       └── test_sql_parser.py
│
├── src/                               # React + TypeScript frontend (Vite)
│   ├── main.jsx                       # Entry point
│   ├── App.tsx                        # Root: routes between home | workspace | monitor views
│   ├── index.css                      # Global styles, dark-mode theme, node/edge/modal/monitor CSS
│   ├── components/
│   │   ├── WorkflowList.tsx           # Home screen: pipeline list + Monitor nav button
│   │   ├── Workspace.tsx              # ReactFlow canvas: drag-drop nodes, edges, field handles,
│   │   │                              #   bidirectional field-mapping sync, primary-key checkboxes
│   │   ├── TurboNode.tsx              # Custom ReactFlow node: field rows, PK checkboxes,
│   │   │                              #   edge highlight on hover, per-field source/target handles
│   │   ├── TurboEdge.tsx              # Custom animated edge
│   │   ├── NodeEditModal.tsx          # Slide-in edit panel: props + Flink SQL + field mappings
│   │   ├── MonitorPanel.tsx           # Flink job monitor: live job list, status, cancel, auto-refresh
│   │   ├── SaveDialog.tsx             # Pipeline name dialog
│   │   ├── ChatAgent.tsx              # AI chat side panel
│   │   └── FunctionIcon.tsx           # Node type icons
│   ├── context/
│   │   ├── NodeEditContext.tsx        # Shared state for the edit panel (open/close, activePanel)
│   │   └── ThemeContext.tsx           # Dark/light theme toggle
│   └── hooks/
│       ├── useAutoSave.ts             # Debounced auto-save on canvas change
│       └── usePipelineRun.ts          # Run / stop / poll status
│
├── producer/                          # Finnhub real-time data producer
│   ├── finnhub_producer.py            # WebSocket trades + REST quotes → 3 Kafka topics
│   ├── scylla_consumer.py             # Kafka → ScyllaDB consumer
│   ├── metrics.py                     # Prometheus counters
│   ├── log_setup.py
│   ├── Dockerfile
│   └── requirements.txt
│
├── docker/
│   ├── docker-compose.yml             # All services: Zookeeper, Kafka, Schema Registry, ScyllaDB,
│   │                                  #   MySQL, Flink (JM + TM), ClickHouse, backend, producer
│   │                                  #   Mounts x-stream-jobs/ → /jobs in backend container
│   ├── config-docker.yaml             # docker-compose config override (container hostnames)
│   ├── flink/
│   │   ├── Dockerfile                 # Custom Flink image with PyFlink + connectors
│   │   ├── entrypoint.sh
│   │   └── flink-conf.yaml
│   └── init/
│       └── mysql-init.sql             # Schema bootstrap
│
├── k8s/                               # Kubernetes manifests for non-Flink services
│   ├── namespace.yaml
│   ├── kafka.yaml
│   └── scylladb.yaml
│
├── scripts/
│   └── start-sandbox.sh               # One-shot local dev startup script
│
├── index.html
├── package.json
├── vite.config.js
└── tsconfig.json
```

---

## x-stream-jobs (Generated Artifacts + Flink Infra)

```
x-stream-jobs/
├── flink-jobs/                        # Auto-generated by backend on every pipeline save
│   └── <pipeline-name>/
│       └── <label>_<node-id>_job.py  # PyFlink job per node (CREATE TABLE + connector config)
│
├── workflows/                         # Auto-generated by backend on every pipeline save
│   └── <pipeline-name>.yaml          # Full pipeline topology: nodes, edges, field mappings
│
├── config/
│   ├── connectors.yaml                # Kafka, ScyllaDB, ClickHouse connection settings
│   ├── flink.yaml                     # Cluster URLs, parallelism, checkpointing, state backend
│   └── jobs.yaml                      # Per-job submission defaults (restart strategy, watermarks)
│
├── docker/
│   ├── Dockerfile                     # Custom Flink image (PyFlink + connectors)
│   ├── entrypoint.sh
│   └── flink-conf.yaml
│
├── k8s/
│   └── flink.yaml                     # Flink JobManager + TaskManager K8s manifests
│
├── requirements.txt                   # apache-flink==1.19.0, pyyaml
├── .gitignore
└── README.md
```

---

## Key Data Flows

### Pipeline Save → Artifact Generation
```
UI (Workspace.tsx)
  → POST /api/pipeline
    → Save nodes + edges to MySQL
    → flink_job_generator.generate_pipeline_artifacts()
      → resolves output path: JOBS_REPO_PATH env > config.yaml jobs_repo.path > project root
      → writes x-stream-jobs/flink-jobs/<pipeline>/<label>_<id>_job.py  (one per node)
      → writes x-stream-jobs/workflows/<pipeline>.yaml
```

### Field Mapping (Canvas ↔ Edit Panel)
```
Draw edge between field handles
  → Workspace.tsx useEffect([edges])
    → syncs field_mappings into sink node properties (ScyllaDB / ClickHouse)

Open edit panel
  → Workspace.tsx openPanel()
    → recomputes field_mappings fresh from current edges (avoids stale state)
    → passes connectedSources (upstream nodes + fields) to NodeEditModal

Change dropdown in edit panel + Save
  → Workspace.tsx handlePanelSave()
    → rebuilds canvas edges from field_mappings (bidirectional sync)
```

### Flink Job Monitoring
```
MonitorPanel.tsx
  → GET /api/flink/jobs         → FlinkJobManager → Flink REST /jobs/overview
  → GET /api/flink/jobs/{id}    → FlinkJobManager → Flink REST /jobs/{id}
  → DELETE /api/flink/jobs/{id} → FlinkJobManager.cancel_job() → PATCH /jobs/{id}?mode=cancel
  Auto-refreshes every 5 seconds while panel is open
```

---

## Environment Variables

| Variable             | Default (config.yaml)         | Description                             |
|----------------------|-------------------------------|-----------------------------------------|
| `CONFIG_PATH`        | `../config.yaml`              | Path to config.yaml                     |
| `JOBS_REPO_PATH`     | `../x-stream-jobs`            | Where to write generated Flink artifacts|
| `MYSQL_PASSWORD`     | `xstream123`                  | MySQL root password                     |
| `ANTHROPIC_API_KEY`  | _(empty)_                     | Claude AI assistant                     |
| `FINNHUB_API_KEY`    | _(empty)_                     | Finnhub WebSocket producer              |
| `SCYLLADB_USERNAME`  | _(empty)_                     | ScyllaDB credentials                    |
| `SCYLLADB_PASSWORD`  | _(empty)_                     | ScyllaDB credentials                    |
| `CLICKHOUSE_USER`    | `default`                     | ClickHouse credentials                  |
| `CLICKHOUSE_PASSWORD`| _(empty)_                     | ClickHouse credentials                  |
| `ALLOWED_ORIGINS`    | `http://localhost:5173`       | CORS allowed origins (comma-separated)  |

---

## Local Development

```bash
# 1. Start all infrastructure (Kafka, Flink, ScyllaDB, MySQL, ClickHouse)
cd x-stream/docker
docker compose up -d

# 2. Start backend (reads config.yaml, writes jobs to ../x-stream-jobs)
cd x-stream/backend
pip install -r requirements.txt
uvicorn main:app --reload --port 8000

# 3. Start frontend
cd x-stream
npm install
npm run dev        # → http://localhost:5173
```

The backend auto-generates files into `../x-stream-jobs` (relative to `x-stream/`) on every pipeline save, matching the `jobs_repo.path` setting in `config.yaml`.
