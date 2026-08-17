-- x-stream pipeline configuration schema

CREATE TABLE IF NOT EXISTS pipelines (
  id            INT AUTO_INCREMENT PRIMARY KEY,
  name          VARCHAR(255)       NOT NULL DEFAULT 'untitled',
  description   TEXT,
  created_at    DATETIME           DEFAULT CURRENT_TIMESTAMP,
  updated_at    DATETIME           DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS nodes (
  id            VARCHAR(64)        PRIMARY KEY,
  pipeline_id   INT                NOT NULL,
  node_type     ENUM('kafka','scylladb','clickhouse','mongodb') NOT NULL,
  label         VARCHAR(255)       NOT NULL,
  pos_x         FLOAT              NOT NULL DEFAULT 0,
  pos_y         FLOAT              NOT NULL DEFAULT 0,
  flink_sql     LONGTEXT,
  properties    JSON,
  created_at    DATETIME           DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (pipeline_id) REFERENCES pipelines(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS edges (
  id            VARCHAR(64)        PRIMARY KEY,
  pipeline_id   INT                NOT NULL,
  source_id     VARCHAR(64)        NOT NULL,
  target_id     VARCHAR(64)        NOT NULL,
  source_handle VARCHAR(64),
  target_handle VARCHAR(64),
  FOREIGN KEY (pipeline_id) REFERENCES pipelines(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
  id            INT AUTO_INCREMENT PRIMARY KEY,
  pipeline_id   INT                NOT NULL,
  status        ENUM('pending','running','success','failed','cancelled') DEFAULT 'pending',
  flink_job_id  VARCHAR(255),
  logs          LONGTEXT,
  started_at    DATETIME,
  finished_at   DATETIME,
  FOREIGN KEY (pipeline_id) REFERENCES pipelines(id)
);

CREATE TABLE IF NOT EXISTS pipeline_run_logs (
  id            INT AUTO_INCREMENT PRIMARY KEY,
  run_id        INT                NOT NULL,
  node_id       VARCHAR(64),
  node_label    VARCHAR(255),
  level         ENUM('DEBUG','INFO','WARNING','ERROR') DEFAULT 'INFO',
  message       TEXT               NOT NULL,
  timestamp     DATETIME           DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (run_id) REFERENCES pipeline_runs(id) ON DELETE CASCADE
);

INSERT INTO pipelines (name, description) VALUES ('default', 'Default pipeline');
