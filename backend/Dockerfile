FROM python:3.12-slim
WORKDIR /app
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY dfs ./dfs
COPY config ./config
CMD ["python", "-m", "dfs.controller", "--config", "config/config.yaml"]
