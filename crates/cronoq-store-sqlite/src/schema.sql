-- cronoq SQLite schema v1 (idempotent). Times are wall-clock Unix nanoseconds.

CREATE TABLE IF NOT EXISTS cronoq_leader (
    id               INTEGER PRIMARY KEY CHECK (id = 1),
    epoch            INTEGER NOT NULL,
    -- High-water mark of issued task ids, so pruning can never cause id reuse.
    max_id           INTEGER NOT NULL DEFAULT 0,
    -- The process holding leadership, and when its lease runs out.
    holder           TEXT,
    expires_unix_ns  INTEGER NOT NULL DEFAULT 0
);
INSERT INTO cronoq_leader (id, epoch) VALUES (1, 0) ON CONFLICT (id) DO NOTHING;

CREATE TABLE IF NOT EXISTS cronoq_tasks (
    id                  INTEGER PRIMARY KEY,
    seq                 INTEGER NOT NULL,
    arrival_unix_ns     INTEGER NOT NULL,
    pool                TEXT NOT NULL,
    kind                TEXT NOT NULL,
    priority            INTEGER NOT NULL,
    key                 TEXT,
    payload             BLOB NOT NULL,
    deadline_unix_ns    INTEGER,
    fixed_at_unix_ns    INTEGER,
    fixed_tolerance_ns  INTEGER,
    fixed_on_miss       TEXT,
    duration_hint_ns    INTEGER,
    timeout_ns          INTEGER,
    retry               TEXT,
    deps                TEXT NOT NULL,
    resources           TEXT NOT NULL,
    tags                TEXT NOT NULL,
    -- Worker labels the task requires.
    labels              TEXT NOT NULL DEFAULT '[]',
    -- Tenant, for fair sharing.
    tenant              TEXT,
    status              TEXT NOT NULL,
    terminal            INTEGER NOT NULL DEFAULT 0,
    attempt             INTEGER NOT NULL DEFAULT 0,
    updated_unix_ns     INTEGER NOT NULL,
    message             TEXT,
    output              BLOB
);

-- Keys are unique among live tasks (the engine also deduplicates against retained ones).
CREATE UNIQUE INDEX IF NOT EXISTS cronoq_tasks_live_key ON cronoq_tasks (key) WHERE key IS NOT NULL AND terminal = 0;
CREATE INDEX IF NOT EXISTS cronoq_tasks_key ON cronoq_tasks (key, id) WHERE key IS NOT NULL;
-- Recovery scans live tasks; retention and pruning scan terminal ones by age.
CREATE INDEX IF NOT EXISTS cronoq_tasks_live ON cronoq_tasks (id) WHERE terminal = 0;
CREATE INDEX IF NOT EXISTS cronoq_tasks_terminal_age ON cronoq_tasks (updated_unix_ns) WHERE terminal = 1;

CREATE TABLE IF NOT EXISTS cronoq_recurring (
    name              TEXT PRIMARY KEY,
    spec              TEXT NOT NULL,
    template_payload  BLOB NOT NULL,
    next_unix_ns      INTEGER,
    -- The occurrence a queued schedule chains to.
    last_task_id      INTEGER
);
