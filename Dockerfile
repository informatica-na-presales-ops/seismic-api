FROM python:3.9.4-alpine3.13

COPY requirements.txt /seismic-etl/requirements.txt

RUN /usr/local/bin/pip install --no-cache-dir --requirement /seismic-etl/requirements.txt

ENV APP_VERSION="2020.1" \
    PYTHONUNBUFFERED="1"

ENTRYPOINT ["/usr/local/bin/python"]
CMD ["/seismic-etl/seismic-etl.py"]

LABEL org.opencontainers.image.authors="William Jackson <wjackson@informatica.com>" \
      org.opencontainers.image.version="${APP_VERSION}"

COPY seismic-etl.py /seismic-etl/seismic-etl.py
