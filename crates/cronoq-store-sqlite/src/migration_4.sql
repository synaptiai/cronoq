-- cronoq SQLite schema v4: ordering keys. Tasks of one pool that share an ordering key run
-- one at a time, in submission order (`seq`). The key and what a failure does to it are
-- stored with each task; a key's own state (class floor, halted) has a row only while it
-- differs from the default.

ALTER TABLE cronoq_tasks ADD COLUMN ordering_key TEXT;
ALTER TABLE cronoq_tasks ADD COLUMN halt_on_failure INTEGER NOT NULL DEFAULT 0;
ALTER TABLE cronoq_pending ADD COLUMN ordering_key TEXT;
ALTER TABLE cronoq_pending ADD COLUMN halt_on_failure INTEGER NOT NULL DEFAULT 0;

CREATE TABLE IF NOT EXISTS cronoq_ordering (
    pool     TEXT NOT NULL,
    key      TEXT NOT NULL,
    -- Class floor of the key's head (0 = critical), or NULL.
    floor    INTEGER,
    -- The floor is kept while the key has no tasks.
    persist  INTEGER NOT NULL DEFAULT 0,
    -- A task that asked for it failed: nothing of the key is offered until it is resumed.
    halted   INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (pool, key)
) WITHOUT ROWID;
