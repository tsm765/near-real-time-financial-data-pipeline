FROM python:3.11-slim

WORKDIR /app

# Copy dependency list
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy just the producer code
COPY producers/prices_producer /app

# Default command
CMD ["python", "app.py"]
