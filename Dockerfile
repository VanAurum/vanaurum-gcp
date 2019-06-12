# Dockerfile
FROM ubuntu:16.04

RUN apt-get update && apt-get install -y python3-pip curl
RUN apt-get install tree
RUN pip3 install -U setuptools
RUN pip3 install --upgrade pip
COPY requirements.txt .
RUN pip3 install -r requirements.txt

RUN echo "deb http://packages.cloud.google.com/apt cloud-sdk-zesty main" | \
    tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && \
    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add - && \
    apt-get update && apt-get install -y google-cloud-sdk

WORKDIR /app
COPY  worker.py vanaurum-a6b766a3f2d7.json ./
COPY config/ /app/config/
COPY helpers/ /app/helpers/
COPY ingest/ /app/ingest/
RUN tree /
ENV GOOGLE_APPLICATION_CREDENTIALS=/app/vanaurum-a6b766a3f2d7.json
RUN gcloud auth activate-service-account --key-file=$GOOGLE_APPLICATION_CREDENTIALS
CMD python3 worker.py