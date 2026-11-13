-- cronoq SQLite schema v3: the fingerprint a task was submitted with (16 bytes, big-endian),
-- compared when a later submission of its key asks for conflicts to be reported.

ALTER TABLE cronoq_tasks ADD COLUMN fingerprint BLOB;
ALTER TABLE cronoq_pending ADD COLUMN fingerprint BLOB;
