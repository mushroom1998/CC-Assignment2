#
# Small server program for use with Cloud Computing 2024 Homework 2,
# LIACS, Leiden University.
#

FROM python:3.10-slim
ENV APP_HOME /app
RUN apt-get update && apt-get install -y \
    libgl1 \
    libgl1-mesa-glx \
    libglib2.0-0 && \
    rm -rf /var/lib/apt/lists/*
WORKDIR $APP_HOME
COPY . ./
RUN pip install Flask gunicorn opencv-python Pillow google-cloud-storage google-cloud-datastore
RUN pip3 install google-cloud-pubsub
CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 app:app