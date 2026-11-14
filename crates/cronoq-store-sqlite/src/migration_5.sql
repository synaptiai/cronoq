-- cronoq SQLite schema v5: pauses. A pool, one of its classes or one of its ordering keys
-- is paused while it has a row here; resuming removes the row.

CREATE TABLE IF NOT EXISTS cronoq_paused (
    pool    TEXT NOT NULL,
    -- 0: the pool; 1: a class; 2: an ordering key.
    kind    INTEGER NOT NULL,
    -- '' for the pool, the class (0 = critical) or the ordering key.
    name    TEXT NOT NULL,
    reason  TEXT NOT NULL,
    PRIMARY KEY (pool, kind, name)
) WITHOUT ROWID;
