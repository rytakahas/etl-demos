FROM python:3.11-slim
WORKDIR /app
COPY deploy/azure_fabric/jobs/job_trigger_service/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt
COPY deploy/azure_fabric/jobs/job_trigger_service/app.py /app/app.py
EXPOSE 8080
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8080"]
