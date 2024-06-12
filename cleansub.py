import logging
from flask import Flask, request, jsonify, send_from_directory
import cv2
import os
import uuid
import math
import json
from google.cloud import pubsub_v1, datastore, storage
import time
import re

project_id = "thinking-banner-421414"
decompose_topic_id = "decompose-video"
decompose_subscription_paths = []
process_subscription_id = "process-video-sub"

publisher = pubsub_v1.PublisherClient()
subscriber = pubsub_v1.SubscriberClient()
decompose_topic_path = publisher.topic_path(project_id, decompose_topic_id)
process_subscription_path = subscriber.subscription_path(project_id, process_subscription_id)
def callback(message):
    print(f"Dropping message: {message.data}")
    message.ack()


def clear_subscription(subscription_path):
    future = subscriber.subscribe(subscription_path, callback)
    try:
        future.result(timeout=10)
    except Exception as e:
        future.cancel()
        print(f"Subscription cleared: {subscription_path}")

for i in range(25,75):
    decompose_subscription_path = subscriber.subscription_path(project_id, "decompose-video-sub-" + str(i))
    clear_subscription(decompose_subscription_path)
clear_subscription(process_subscription_path)