# `02_set_passwords.sh` — Service Role Password Bootstrap

## 1. What this file is

`02_set_passwords.sh` is a short shell script that runs once during PostgreSQL container initialisation. Its sole job is to read five password values from environment variables and write them into the database as `ALTER ROLE ... PASSWORD` statements. Without it, every service role created by `01_schema.sql` would have no password and could not authenticate — Kafka Connect, dbt, the FastAPI reader, Debezium, and Airflow would all fail to connect even though their roles exist.

This file is why passwords are never hardcoded anywhere in the repository. The roles live in SQL; the credentials live in `.env`; this script is the bridge that joins them at startup.

---

## 2. Where it fits

```ascii
.env                          ← developer sets passwords here (gitignored)
    │
    ▼
docker-compose.yml            ← passes env vars into the postgres container environment
    │
    ▼
docker-entrypoint-initdb.d/   ← postgres runs all scripts here on first start, alphabetically
    ├── 01_schema.sql         ← creates roles (kafka_connect, dbt_runner, etc.) — no passwords yet
    ├── 02_set_passwords.sh   ← YOU ARE HERE — reads env vars, sets passwords on those roles
    └── 03_stored_procedures.sql
    │
    ▼
Roles are now login-capable with credentials
Kafka Connect / dbt / FastAPI / Debezium / Airflow can authenticate
```

The numeric prefix `02` is not cosmetic — PostgreSQL's initdb mechanism executes scripts in strict alphabetical order. This script must run after `01` (which creates the roles) and before `03` (which grants privileges those roles will use). Swap the order and the `ALTER ROLE` statements fail on roles that do not yet exist.

---

## 3. Prerequisites

Before this script can succeed:

- The `pgdata` volume must be empty (initdb only runs on a fresh cluster — it is skipped on restart)
- `01_schema.sql` must have completed successfully, creating all five service roles
- All five password environment variables must be set in `.env` and passed through `docker-compose.yml`

If any variable is missing, `docker compose up` will abort before any container starts, printing a clear error that names the missing variable. This validation happens in `docker-compose.yml` via the `:?error` syntax — not inside this script. See section 6 for why that boundary is intentional.

---

## 4. Core concepts

**`docker-entrypoint-initdb.d`** is a directory that the official PostgreSQL Docker image processes exactly once — when the data directory is initialised for the first time. Any `.sql` or `.sh` file placed there is executed in alphabetical order. On every subsequent container start (restart, `docker compose up` after `down` without `-v`), this directory is ignored entirely. This is why the initdb scripts are safe to leave in place — they will not re-run unless the volume is deleted.

**`ALTER ROLE ... PASSWORD`** stores a salted, hashed form of the password in `pg_authid`. The plain-text value never persists in the database. The hash algorithm is determined by the `password_encryption` PostgreSQL setting (default: `scram-sha-256` in PostgreSQL 16).

**`set -eu`** is the most important line in the script after the shebang. `-e` causes the script to abort immediately if any command returns a non-zero exit code. `-u` causes the script to abort if any variable is referenced but unset. Together they turn what would otherwise be silent partial failures into hard stops with a clear error message.

**Heredoc with `psql`** (`<<-EOSQL`) is the mechanism used to pass the `ALTER ROLE` SQL into the database without writing it to a temporary file. The shell expands `${KAFKA_CONNECT_PASSWORD}` and friends before psql ever sees the text — this is precisely what makes shell scripting necessary here. A `.sql` file processed by psql has no access to shell environment variables.

---

## 5. Component walkthrough

### Shebang — `#!/usr/bin/env sh`

Uses `sh` rather than `bash` because the PostgreSQL Alpine image ships with `sh` (busybox ash) but not necessarily `bash`. Writing to the lowest common denominator makes the script portable across any base image without modification.

### `set -eu`

Sets strict error behaviour. Any unset variable or failed command causes an immediate exit. This is the safety net that prevents a half-configured database from silently being passed to the next initdb step as if it were healthy.

### No presence check loop — intentional

An earlier version of this script contained an explicit validation loop that checked each password variable before running psql, exiting with an error if any were missing. That loop was removed. The validation responsibility was moved upstream to `docker-compose.yml` using the `:?error` syntax:

```yaml
KAFKA_CONNECT_PASSWORD:  ${KAFKA_CONNECT_PASSWORD:?Set KAFKA_CONNECT_PASSWORD in .env}
DBT_RUNNER_PASSWORD:     ${DBT_RUNNER_PASSWORD:?Set DBT_RUNNER_PASSWORD in .env}
API_READER_PASSWORD:     ${API_READER_PASSWORD:?Set API_READER_PASSWORD in .env}
DEBEZIUM_PASSWORD:       ${DEBEZIUM_PASSWORD:?Set DEBEZIUM_PASSWORD in .env}
AIRFLOW_RUNNER_PASSWORD: ${AIRFLOW_RUNNER_PASSWORD:?Set AIRFLOW_RUNNER_PASSWORD in .env}
```

This stops `docker compose up` before a single container starts — no volume is created, no initdb fires, nothing needs cleaning up. The in-script loop caught the problem too late: the container was already running, initdb had already started, the pgdata volume already existed, and a `docker compose down -v` was required to recover. The compose-level check costs nothing and gives a cleaner failure mode. See section 6 for the full reasoning.

### `psql` heredoc block

```sh
psql -v ON_ERROR_STOP=1 \
     --username "${POSTGRES_USER}" \
     --dbname   "${POSTGRES_DB}" \
     <<-EOSQL
    ALTER ROLE kafka_connect  PASSWORD '${KAFKA_CONNECT_PASSWORD}';
    ALTER ROLE dbt_runner     PASSWORD '${DBT_RUNNER_PASSWORD}';
    ALTER ROLE api_reader     PASSWORD '${API_READER_PASSWORD}';
    ALTER ROLE debezium       PASSWORD '${DEBEZIUM_PASSWORD}';
    ALTER ROLE airflow_runner PASSWORD '${AIRFLOW_RUNNER_PASSWORD}';
EOSQL
```

`-v ON_ERROR_STOP=1` tells psql to exit with a non-zero code if any SQL statement fails. Without this, psql would print an error and continue, which combined with `set -e` would still abort the script — but with `ON_ERROR_STOP=1` the exit is immediate and the error message is cleaner. `${POSTGRES_USER}` and `${POSTGRES_DB}` are set by the postgres Docker image itself from the `POSTGRES_USER` and `POSTGRES_DB` compose environment variables. The password variables are expanded by the shell before psql receives the string, which is why they land in the SQL as plain-text literals — this is fine because the connection is local and the value is immediately hashed by PostgreSQL on ingestion.

---

## 6. Key design decisions

**Why a shell script instead of a SQL file?** psql, when used to execute `.sql` initdb files, does not perform shell variable expansion. Writing `PASSWORD '${KAFKA_CONNECT_PASSWORD}'` inside a `.sql` file would insert the literal string `${KAFKA_CONNECT_PASSWORD}` as the password. Only a shell script can read environment variables and inject them into a SQL statement at runtime.

**Why not use `POSTGRES_PASSWORD` for all roles?** Sharing a single password across all service roles collapses the access control model. Each role has a distinct privilege set (write-only for Kafka Connect, read-only for FastAPI, replication for Debezium). A single password means a compromised FastAPI credential can be used to write to raw tables or trigger replication. Separate passwords enforce least-privilege at the authentication layer, not just the authorisation layer.

**Why validate variables in `docker-compose.yml` rather than in this script?** Validating inside the script catches the problem too late. By the time the script runs, the postgres container is already up, a temporary initdb server process is running, and the `pgdata` volume already exists. When the script exits with an error, the postgres Docker entrypoint does not propagate that failure code to the container's own exit code — the real server starts anyway, silently, on a half-initialised database. Recovery requires `docker compose down -v` to destroy the volume before initdb will fire again. The `:?error` syntax in `docker-compose.yml` catches missing variables before `docker compose up` creates anything, produces a clear terminal error naming the missing variable, and leaves no cleanup work behind. The right place to enforce that required variables exist is at the boundary where they enter the system — the compose file — not inside the container after that boundary has already been crossed.

**Why `sh` and not `bash`?** The postgres Alpine base image is minimal. Depending on `bash` would require it to be installed in the Dockerfile, adding an unnecessary layer to a container whose only job is to run a database. `sh` is always present.

---

## 7. How to run / trigger

This script is never run directly by a developer. It is triggered automatically by the postgres container entrypoint on first start. The correct mental model is:

```bash
# Trigger initdb (which runs this script as step 2 of 3):
docker compose up --build

# Confirm it ran successfully — check container logs:
docker logs afyabima-postgres | grep "\[initdb\]"

# Expected output:
# [initdb] Setting service role passwords...
# [initdb] Service role passwords set.
```

To verify the passwords were applied correctly, connect and inspect `pg_authid`:

```sql
-- Run inside psql as afyabima_admin
SELECT rolname, rolcanlogin, rolpassword IS NOT NULL AS has_password
FROM pg_authid
WHERE rolname IN (
    'kafka_connect', 'dbt_runner', 'api_reader',
    'debezium', 'airflow_runner'
)
ORDER BY rolname;
```

All five roles should show `rolcanlogin = true` and `has_password = true`. The actual hash is not visible regardless of privilege level.

To test a specific role's credentials:

```bash
docker exec -it afyabima-postgres \
  psql -U kafka_connect -d afyabima
# Enter password when prompted — use KAFKA_CONNECT_PASSWORD from .env
```

---

## 8. If something goes wrong

**`docker compose up` fails with `variable is not set`** — a password variable is missing from `.env` or not declared in the `environment` block of `docker-compose.yml`. The error message names the exact variable. Add it to `.env` and run `docker compose up --build` — no volume reset needed because no container was created.

**`ERROR: role "kafka_connect" does not exist`** — `01_schema.sql` did not complete successfully before this script ran. Check `docker logs afyabima-postgres` for the error from the schema step. Fix the schema error, then do a full reset with `docker compose down -v`.

**Password was set but service cannot authenticate** — the most common cause is that the password in `.env` contains special characters that were not quoted correctly when passed to the container. Single quotes inside a shell heredoc are interpreted literally, but if the password itself contains a single quote, it will break the SQL string. Use passwords that avoid single quotes, or escape them as `''` in the variable value.

---

## 9. What this connects to

Once this script completes, all five service roles are login-capable with their respective credentials. `03_stored_procedures.sql` runs next, creating the `ops` schema and granting `EXECUTE` privileges on its procedures to `airflow_runner` and `dbt_runner`. From there, each service connects using the role and password set here:

- `kafka_connect` → inserts into `raw.*` via Kafka Connect JDBC Sink or `load_csv.sh`
- `dbt_runner` → reads `raw.*`, writes `staging/intermediate/marts`
- `api_reader` → reads `marts.*` only (FastAPI)
- `debezium` → reads the WAL via logical replication for CDC
- `airflow_runner` → calls `ops.*` procedures on schedule

---

## 10. Things worth carrying forward

The existence of this file is itself a design statement: credentials and schema are intentionally separated. The schema defines what roles exist and what they can do; this script defines who can claim to be those roles. Keeping them in different files means the schema can be read, reviewed, and version-controlled without any secrets ever appearing in it.

The `set -eu` pattern is worth memorising as the standard opening for any shell script that runs in an automated context. Silent failures in initdb scripts are particularly dangerous because the container appears healthy while the database is misconfigured.

The `:?error` compose syntax is worth knowing as a general pattern for any service that requires environment variables to be set before it can start safely. It is the compose-native equivalent of a pre-flight check — free, readable, and impossible to accidentally bypass.
