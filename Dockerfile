FROM astrocrpublic.azurecr.io/runtime:3.1-7

USER root
COPY requirements-kg-rdf.txt /usr/local/airflow/requirements-kg-rdf.txt
RUN python -m pip install --no-cache-dir -r /usr/local/airflow/requirements-kg-rdf.txt
USER astro

