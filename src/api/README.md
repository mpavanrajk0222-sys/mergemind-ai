# FastAPI Service

The API adapts existing MergeMind pipeline, validation, review, quarantine, and SQLite functions. It does not duplicate matching or survivorship rules. The complete current route and schema reference is [../../API_DOCUMENTATION.md](../../API_DOCUMENTATION.md); the runtime OpenAPI schema is served at `/openapi.json` and Swagger UI at `/docs`.

## Start on Windows

From the repository root:

```powershell
.\.venv\Scripts\python.exe -m uvicorn src.api.app:app --reload --host 127.0.0.1 --port 8000
```

The API uses `data/output/mergemind.db` unless `MERGEMIND_DATABASE_PATH` is set. `MERGEMIND_REPORT_DIRECTORY` can override the default `reports/` directory.

If the selected database file already exists, API startup applies the backend's additive schema setup: missing current tables are created and the nullable `input_fingerprint` column is added when needed. Existing database records are not deleted. The API's review and quarantine endpoints require these tables to be present; an empty table is distinct from an unavailable database.

The Vite development server proxies `/api` to `http://127.0.0.1:8000`. Set `VITE_API_PROXY_TARGET` before starting Vite to use another local API port.

## Operational Notes

- Uploads use generated filenames in an OS temporary directory, are limited to 25 MiB and 100,000 parsed rows, and are staged in a process-local registry for up to two hours.
- Pipeline execution is synchronous; no background-job or progress-polling endpoint is implemented.
- Review and quarantine changes call the existing audited Python operations. They do not automatically merge records or reprocess corrected data.
- The API does not expose arbitrary paths or SQL. Report paths are derived from validated run IDs, and report responses omit local filesystem paths.
