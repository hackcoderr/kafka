FROM python:3.11-slim
WORKDIR /app
COPY lab/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY lab/ .
COPY README.md ./README.md
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
