# ODS Lineage Dashboard

Read-only visualiser for `pipeline.lineage_link` + `pipeline.lineage_edge`.

- `api/` — FastAPI service (Python). Queries control tables, returns
  graph-shaped JSON for React Flow.
- `web/` — Vite + React + React Flow front-end. Lists recent write events
  and renders the target→raw chain for the selected event.

## Run

Two terminals.

**API**

```
cd ops/lineage_dashboard/api
python -m venv .venv && .venv\Scripts\activate           # or source .venv/bin/activate
pip install -r requirements.txt
uvicorn main:app --reload --port 8765
```

Env (defaults match the local docker stack):

| var | default |
|-----|---------|
| POSTGRES_HOST | localhost |
| POSTGRES_PORT | 5440 |
| POSTGRES_DB   | ods_dev |
| POSTGRES_USER | ods |
| POSTGRES_PASSWORD | ods |

**Web**

```
cd ops/lineage_dashboard/web
npm install
npm run dev
```

Open http://localhost:5180. Vite proxies `/api` → `http://localhost:8765`.

## Endpoints

- `GET /api/health`
- `GET /api/lineage/links?limit=50` — recent write events with consumer-run metadata
- `GET /api/lineage/trace/{lineage_link_id}` — graph payload:

```json
{
  "lineage_link": { ... },
  "nodes": [{ "id": "...", "kind": "raw_file|upstream_run|write_event|consumer_run|target",
              "label": "...", "data": { ... } }],
  "edges": [{ "id": "...", "source": "...", "target": "...",
              "kind": "produced|contributed|emitted_by|wrote_to",
              "data": { "slot_name": "core", "record_count": 3 } }]
}
```

## Graph model

```
raw_file ──produced──▶ upstream_run ──contributed[slot]──▶ write_event ──wrote_to──▶ target
                                                            │
                                                            └─emitted_by─▶ consumer_run
```

Single-file direct-PG path → one raw_file, one upstream_run (ingestion).
Multi-file merge path → N raw_files + N upstream_runs (one per slot) all
contributing to one write_event, with `slot_name` on the contributing edges.
