FROM python:3.11-slim
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates   && rm -rf /var/lib/apt/lists/*

COPY . /app

RUN pip install --no-cache-dir -r requirements.airflow.txt || pip install --no-cache-dir -r requirements.txt
RUN chmod +x deploy/azure_fabric/jobs/kg_export_validate_load/run.sh

ENTRYPOINT ["deploy/azure_fabric/jobs/kg_export_validate_load/run.sh"]
