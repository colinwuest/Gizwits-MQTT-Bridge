FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY dhw_mqtt.py .

# Runs as non-root for security
RUN useradd -r -u 1001 appuser
USER appuser

CMD ["python3", "-u", "dhw_mqtt.py"]
