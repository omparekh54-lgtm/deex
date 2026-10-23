# DEEX local Design Experience snapshot

DEEX can run from a baked, read-only PostgreSQL custom-format dump at:

`/app/snapshot/pms.dump`

At startup, `scripts/start_deex.sh` detects the dump, initializes a local PostgreSQL 17 instance under `/tmp`, restores the snapshot, sets the local database to read-only by default, rewrites `PMS_DATABASE_URL` to the localhost database, and then starts the FastAPI service.

The dump must contain the Design Experience `pms` schema and any public dependency tables required by that schema. Database credentials must never be committed alongside the dump.

If the dump is absent, DEEX falls back to the externally configured `PMS_DATABASE_URL` so the current service stays usable while a snapshot is being produced.
