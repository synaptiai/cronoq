-- cronoq SQLite schema v6: releases. An attempt that its executor gave back (a release) does
-- not count against the retry policy: `releases` says how many of a task's attempts were
-- released. `max_releases` is the task's own limit, if it set one.

ALTER TABLE cronoq_tasks ADD COLUMN releases INTEGER NOT NULL DEFAULT 0;
ALTER TABLE cronoq_tasks ADD COLUMN max_releases INTEGER;
ALTER TABLE cronoq_pending ADD COLUMN max_releases INTEGER;
