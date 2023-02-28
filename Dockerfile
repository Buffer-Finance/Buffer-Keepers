# Creating image based on official python3 image
FROM python:3.8.8
ENV LANG=C.UTF-8 LC_ALL=C.UTF-8

# Your contact, so can people blame you afterwards
LABEL maintainer="heisenberg@buffer.finance"

ENV APP_HOME=/home/app/web
RUN mkdir -p $APP_HOME
WORKDIR $APP_HOME
# WORKDIR /
# RUN apt-get update && apt-get install -f -y postgresql-client

# Installing all python dependencies
COPY requirements.txt ./

RUN pip install --upgrade pip
RUN pip install --no-warn-conflicts --no-cache-dir -r requirements.txt

COPY app/ ./

