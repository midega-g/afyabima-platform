# syntax=docker/dockerfile:1

# =============================================================================
# AfyaBima — PostgreSQL 16 (Alpine)
# =============================================================================
#
# Build context: project root (afyabima_platform/)
# This Dockerfile is at: docker/postgres/Dockerfile
#
# It COPYs init scripts directly from database_scripts/ — no separate
# initdb/ folder to maintain. database_scripts/ IS the init source.
#
# Init script execution order (alphabetical, so numeric prefix controls it):
#   01_schema.sql              creates extensions, roles, schemas, all tables
#   02_set_passwords.sh        ALTER ROLE ... PASSWORD from env vars
#   03_stored_procedures.sql   creates ops.* functions
#
# The password script must run after schema because it ALTER ROLEs that
# schema creates. Hence 02 not 00.
#
# WAL level:
#   wal_level=logical is required by Debezium and must be baked in at cluster
#   init time via POSTGRES_INITDB_ARGS. Changing it post-init requires a full
#   cluster restart with volume manipulation.
#
# Non-root:
#   The official postgres image drops to uid 999 (postgres) before starting
#   the server. Adding USER here would break initdb. Nothing extra needed.
# =============================================================================

FROM postgres:16-alpine

LABEL org.opencontainers.image.title="afyabima-postgres" \
      org.opencontainers.image.description="AfyaBima PostgreSQL 16 — fraud detection platform" \
      org.opencontainers.image.version="1.0.0"

# ---------------------------------------------------------------------------
# Copy init scripts from database_scripts/ (at project root).
# Runs once on first start; skipped on every subsequent restart.
# docker-compose sets build context to project root so these paths resolve.
# ---------------------------------------------------------------------------
COPY database_scripts/01_schema.sql             /docker-entrypoint-initdb.d/01_schema.sql
COPY database_scripts/02_set_passwords.sh       /docker-entrypoint-initdb.d/02_set_passwords.sh
COPY database_scripts/03_stored_procedures.sql  /docker-entrypoint-initdb.d/03_stored_procedures.sql

# 0644 → sql files readable by all; 0755 → shell script executable by all.
# The postgres entrypoint runs as uid 999 (postgres), not root — every file
# in initdb.d must be readable (and executable for .sh) by world, not just owner.
RUN chmod 0644 /docker-entrypoint-initdb.d/01_schema.sql \
               /docker-entrypoint-initdb.d/03_stored_procedures.sql \
 && chmod 0755 /docker-entrypoint-initdb.d/02_set_passwords.sh

HEALTHCHECK \
    --interval=10s \
    --timeout=5s \
    --start-period=30s \
    --retries=5 \
    CMD pg_isready -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" || exit 1
