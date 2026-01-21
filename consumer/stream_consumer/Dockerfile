FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy consumer code
COPY consumer/stream_consumer /app

# Copy storage package so "from storage.router import ..." works
COPY storage /app/storage

CMD ["python", "app.py"]
