#
# Small server program for use with Cloud Computing 2024 Homework 2,
# LIACS, Leiden University.
#

FROM python:3.10-slim
ENV APP_HOME /app
WORKDIR $APP_HOME
COPY . ./
RUN pip install Flask gunicorn opencv-python Pillow
COPY watermark.py ./
CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 app:app
