FROM python:3.12-slim

COPY requirements.txt /

RUN pip install -r /requirements.txt --no-cache-dir

WORKDIR /app

COPY ./app /app

ENTRYPOINT ["/usr/local/bin/python3", "main.py"]