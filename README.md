# ProAct-Log

## Ignored files

This repository intentionally ignores local database files and large dataset folders.

- `backend/activity_logs.db` and `activity_logs.db` (SQLite database files created at runtime)
- `bigdata/` (local large data files)

These are added to `.gitignore` because they are runtime artifacts or large binaries that should not be committed.