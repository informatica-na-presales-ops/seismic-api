FROM python:3.11.2-alpine3.17

RUN /sbin/apk add --no-cache libpq
RUN /usr/sbin/adduser -g python -D python

USER python
RUN /usr/local/bin/python -m venv /home/python/venv

COPY --chown=python:python requirements.txt /home/python/seismic-api/requirements.txt
RUN /home/python/venv/bin/pip install --no-cache-dir --requirement /home/python/seismic-api/requirements.txt

ENV APP_VERSION="2020.1" \
    PATH="/home/python/venv/bin:${PATH}" \
    PYTHONDONTWRITEBYTECODE="1" \
    PYTHONUNBUFFERED="1" \
    TZ="Etc/UTC"

COPY --chown=python:python seismic-etl.py /home/python/seismic-api/seismic-etl.py

ENTRYPOINT ["/home/python/venv/bin/python"]
CMD ["/home/python/seismic-api/seismic-etl.py"]

LABEL org.opencontainers.image.authors="William Jackson <wjackson@informatica.com>" \
      org.opencontainers.image.source="https://github.com/informatica-na-presales-ops/seismic-api" \
      org.opencontainers.image.version="${APP_VERSION}"
