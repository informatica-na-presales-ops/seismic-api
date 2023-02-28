FROM python:3.11.2-alpine3.17

RUN /sbin/apk add --no-cache libpq
RUN /usr/sbin/adduser -g python -D python

USER python
RUN /usr/local/bin/python -m venv /home/python/venv

COPY --chown=python:python requirements.txt /home/python/seismic-api/requirements.txt
RUN /home/python/venv/bin/pip install --no-cache-dir --requirement /home/python/seismic-api/requirements.txt

ENV PATH="/home/python/venv/bin:${PATH}" \
    PYTHONDONTWRITEBYTECODE="1" \
    PYTHONUNBUFFERED="1" \
    TZ="Etc/UTC"

COPY --chown=python:python get-content-view-history.py /home/python/seismic-api/get-content-view-history.py

ENTRYPOINT ["/home/python/venv/bin/python"]

LABEL org.opencontainers.image.authors="William Jackson <wjackson@informatica.com>" \
      org.opencontainers.image.source="https://bitb.informatica.com/projects/TS/repos/seismic-api"
