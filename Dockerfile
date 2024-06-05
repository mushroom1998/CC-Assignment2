# For process.py
FROM python:3.10-slim
ENV APP_HOME /app
ENV PYTHONUNBUFFERED=1
RUN apt-get update && apt-get install -y \
    libgl1 \
    libgl1-mesa-glx \
    libglib2.0-0 && \
    rm -rf /var/lib/apt/lists/*
WORKDIR $APP_HOME
COPY . ./
RUN pip install google-cloud-storage google-cloud-datastore google-cloud-pubsub numpy opencv-python
RUN pip3 install google-cloud-pubsub
CMD ["python", "process.py"]
