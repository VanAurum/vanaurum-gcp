# Dockerfile
FROM ubuntu:16.04
WORKDIR /app
COPY ./app
RUN apt-get update && apt-get install -y python-pip curl
RUN pip install --trusted-host pypi.python.org -r requirements.txt

RUN echo "deb http://packages.cloud.google.com/apt cloud-sdk-xenial main" | \
    tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && \
    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add - && \
    apt-get update && apt-get install -y google-cloud-sdk

COPY worker.py service-key.json ./
ENV GOOGLE_APPLICATION_CREDENTIALS=/app/vanaurum-a6b766a3f2d7.json
RUN gcloud auth activate-service-account --key-file=$GOOGLE_APPLICATION_CREDENTIALS
CMD python worker.py