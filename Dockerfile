FROM python:3.12-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends pngquant && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir Pillow==11.1.0 boto3==1.36.0

COPY compress.py /app/compress.py
COPY upload.py /app/upload.py

WORKDIR /app

CMD ["python", "compress.py"]
