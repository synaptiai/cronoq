-- cronoq schema v1 (idempotent). Times are wall-clock Unix nanoseconds.

CREATE TABLE IF NOT EXISTS cronoq_leader (
    id      SMALLINT PRIMARY KEY CHECK (id = 1),
    epoch   INT NOT NULL
);
INSERT INTO cronoq_leader (id, epoch) VALUES (1, 0) ON CONFLICT (id) DO NOTHING;

CREATE TABLE IF NOT EXISTS cronoq_tasks (
    id                  BIGINT PRIMARY KEY,
    seq                 BIGINT NOT NULL,
    arrival_unix_ns     BIGINT NOT NULL,
    pool                TEXT NOT NULL,
    kind                TEXT NOT NULL,
    priority            SMALLINT NOT NULL,
    key                 TEXT,
    payload             BYTEA NOT NULL,
    deadline_unix_ns    BIGINT,
    fixed_at_unix_ns    BIGINT,
    fixed_tolerance_ns  BIGINT,
    fixed_on_miss       TEXT,
    duration_hint_ns    BIGINT,
    timeout_ns          BIGINT,
    retry               JSONB,
    deps                JSONB NOT NULL,
    resources           JSONB NOT NULL,
    tags                JSONB NOT NULL,
    status              TEXT NOT NULL,
    terminal            BOOLEAN NOT NULL DEFAULT FALSE,
    attempt             INT NOT NULL DEFAULT 0,
    updated_unix_ns     BIGINT NOT NULL,
    message             TEXT,
    output              BYTEA
);

-- Keys are unique among live tasks (the engine also deduplicates against retained ones).
CREATE UNIQUE INDEX IF NOT EXISTS cronoq_tasks_live_key ON cronoq_tasks (key) WHERE key IS NOT NULL AND NOT terminal;
CREATE INDEX IF NOT EXISTS cronoq_tasks_key ON cronoq_tasks (key, id) WHERE key IS NOT NULL;
-- Recovery scans live tasks; retention and pruning scan terminal ones by age.
CREATE INDEX IF NOT EXISTS cronoq_tasks_live ON cronoq_tasks (id) WHERE NOT terminal;
CREATE INDEX IF NOT EXISTS cronoq_tasks_terminal_age ON cronoq_tasks (updated_unix_ns) WHERE terminal;

CREATE TABLE IF NOT EXISTS cronoq_recurring (
    name              TEXT PRIMARY KEY,
    spec              JSONB NOT NULL,
    template_payload  BYTEA NOT NULL,
    next_unix_ns      BIGINT
);
