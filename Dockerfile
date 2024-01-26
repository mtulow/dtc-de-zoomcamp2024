FROM python:3.9.1

WORKDIR /app

COPY .env .env
COPY elt.py elt.py
COPY requirements.txt requirements.txt

RUN apt-get update && \
    apt-get install wget && \
    pip install --upgrade pip && \
    pip install -r requirements.txt

ENTRYPOINT [ "python", "elt.py" ]
