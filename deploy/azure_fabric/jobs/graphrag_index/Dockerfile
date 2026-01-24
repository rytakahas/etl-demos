FROM python:3.11-slim
WORKDIR /app
COPY . /app
RUN pip install --no-cache-dir -r requirements.airflow.txt || pip install --no-cache-dir -r requirements.txt
RUN chmod +x deploy/azure_fabric/jobs/graphrag_index/run.sh
ENTRYPOINT ["deploy/azure_fabric/jobs/graphrag_index/run.sh"]
