FROM ghcr.io/astral-sh/uv:0.9.4-trixie-slim

RUN /usr/sbin/useradd --create-home --shell /bin/bash --user-group python
USER python

WORKDIR /app
COPY --chown=python:python .python-version pyproject.toml uv.lock ./
RUN /usr/local/bin/uv sync --frozen

ENV PATH="/app/.venv/bin:${PATH}" \
    PYTHONDONTWRITEBYTECODE="1" \
    PYTHONUNBUFFERED="1" \
    TZ="Etc/UTC"

COPY --chown=python:python get-content-usage-history.py get-content-view-history.py get-library-content-versions.py ./
COPY --chown=python:python get-library-contents.py get-search-history.py get-users.py get-workspace-content-versions.py ./
COPY --chown=python:python get-workspace-contents.py seismic.py ./

ENTRYPOINT ["uv", "run"]

LABEL org.opencontainers.image.authors="William Jackson <wjackson@informatica.com>" \
      org.opencontainers.image.source="https://github.com/informatica-na-presales-ops/seismic-api"
