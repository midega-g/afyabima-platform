# syntax=docker/dockerfile:1

# =============================================================================
# AfyaBima — Python Data Generator (Alpine, multi-stage)
# =============================================================================
#
# Build context: project root (afyabima_platform/)
# This Dockerfile is at: docker/generator/Dockerfile
#
# Two stages:
#   builder  — resolves deps and installs the project; never lands in final image
#   runtime  — copies .venv only; uv binary and all caches are discarded
#
# Layer caching strategy (two-step uv sync):
#
#   Step 1 — deps only (--no-install-project)
#     Uses bind mounts so pyproject.toml and uv.lock are readable without being
#     COPYed into a layer. Cache mount keeps downloaded packages across builds.
#     This layer is only invalidated when pyproject.toml or uv.lock change.
#
#   Step 2 — install the project itself
#     COPYs source code then runs uv sync again (no bind mount needed — files
#     are already present). This creates .venv/bin/generate (the console_script
#     entry point defined in pyproject.toml [project.scripts]).
#     Only invalidated when source code changes — deps layer stays cached.
#
# Why two steps instead of one?
#   --no-install-project installs third-party packages (faker, numpy, pandas)
#   but does NOT install the data_generation package or create console scripts.
#   The second sync installs the project into the already-populated venv,
#   which is fast (no downloads) and creates .venv/bin/generate.
# =============================================================================

# ---------------------------------------------------------------------------
# Stage 1 — builder
# ---------------------------------------------------------------------------
FROM python:3.11-alpine AS builder

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

# Step 1: install third-party dependencies only.
# bind mount: pyproject.toml and uv.lock are read but NOT written into the layer.
# cache mount: downloaded packages persist across builds on this host.
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    uv sync \
        --frozen \
        --no-install-project \
        --no-dev

# Step 2: copy source and install the project itself.
# This creates .venv/bin/generate from the [project.scripts] entry in pyproject.toml.
# Kept as a separate layer so source-only changes don't re-run the slow deps step.
COPY data_generation/ ./data_generation/
COPY README.md pyproject.toml uv.lock ./

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync \
        --frozen \
        --no-dev \
        --no-editable

# ---------------------------------------------------------------------------
# Stage 2 — runtime
# ---------------------------------------------------------------------------
FROM python:3.11-alpine AS runtime

RUN addgroup --system --gid 1001 appgroup && \
    adduser  --system --uid 1001 \
             --ingroup appgroup \
             --no-create-home \
             --disabled-password \
             appuser

WORKDIR /app

# Copy the fully resolved venv — includes console scripts and all dependencies.
# The uv binary is NOT copied; it lives only in the builder stage.
COPY --from=builder --chown=appuser:appgroup /app/.venv /app/.venv

# data_generation/ is already installed into the venv as a package.
# No need to COPY it again — it was baked in during builder step 2.

RUN mkdir -p /app/data && chown appuser:appgroup /app/data

ENV PATH="/app/.venv/bin:${PATH}" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

USER appuser

CMD ["generate", "--output-dir", "/app/data"]
