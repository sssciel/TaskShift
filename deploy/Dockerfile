FROM python:3.14-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY requirements.txt ./
RUN pip install --upgrade pip \
    && pip install -r requirements.txt

COPY taskshift ./taskshift
COPY src ./src
COPY configs ./configs

RUN sed -i 's/\r$//' /app/taskshift \
    && chmod +x /app/taskshift \
    && cp /app/configs/scheduler.example.yaml /app/configs/scheduler.yaml \
    && cp /app/configs/cluster.example.yaml /app/configs/cluster.yaml \
    && cp /app/configs/server.example.yaml /app/configs/server.yaml \
    && mkdir -p /app/logs /app/exports

EXPOSE 8000
STOPSIGNAL SIGTERM

ENTRYPOINT ["/app/taskshift"]
CMD ["schedule", "--with-web-panel"]
