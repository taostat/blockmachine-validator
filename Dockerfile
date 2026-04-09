FROM python:3.13-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libffi-dev \
    && rm -rf /var/lib/apt/lists/*

COPY validator/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY validator/ validator/

ENV PYTHONUNBUFFERED=1

CMD ["python", "-m", "validator.main"]
