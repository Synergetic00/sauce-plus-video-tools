FROM python:3.14.0-slim

RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg nodejs && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

VOLUME ["/app/downloads", "/app/encoded", "/app/credentials"]

CMD ["python", "main.py"]
