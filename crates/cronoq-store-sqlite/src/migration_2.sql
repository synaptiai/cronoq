-- cronoq SQLite schema v2: submissions stored to run after a live task that has started
-- (`replace_queued`). One row per live task at most; the row goes when the task ends.

CREATE TABLE IF NOT EXISTS cronoq_pending (
    task_id           INTEGER PRIMARY KEY,
    seq               INTEGER NOT NULL,
    arrival_unix_ns   INTEGER NOT NULL,
    -- The successor takes the place in line of this submission, not of the task.
    renew             INTEGER NOT NULL DEFAULT 0,
    pool              TEXT NOT NULL,
    kind              TEXT NOT NULL,
    priority          INTEGER NOT NULL,
    key               TEXT,
    payload           BLOB NOT NULL,
    deadline_unix_ns  INTEGER,
    duration_hint_ns  INTEGER,
    timeout_ns        INTEGER,
    retry             TEXT,
    deps              TEXT NOT NULL,
    resources         TEXT NOT NULL,
    tags              TEXT NOT NULL,
    labels            TEXT NOT NULL DEFAULT '[]',
    tenant            TEXT
);
