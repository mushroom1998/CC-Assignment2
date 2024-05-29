from PIL import Image
from google.cloud import pubsub_v1
import os
import json
from google.cloud import datastore
import time

project_id = "thinking-banner-421414"

process_topic_id = "process-video"
publisher = pubsub_v1.PublisherClient()
process_topic_path = publisher.topic_path(project_id, process_topic_id)

decompose_subscription_id = "decompose-video-sub"
decompose_topic_id = "decompose-video"
subscriber = pubsub_v1.SubscriberClient()
decompose_subscription_path = subscriber.subscription_path(project_id, decompose_subscription_id)
decompose_topic_path = publisher.topic_path(project_id, decompose_topic_id)


def checkTopicSub():
    '''create the second Pub/Sub topic and the first subscription if not exist'''
    try:
        publisher.get_topic(topic=process_topic_path)
    except:
        publisher.create_topic(name=process_topic_path)
    try:
        subscriber.get_subscription(subscription=decompose_subscription_path)
    except:
        subscriber.create_subscription(name=decompose_subscription_path, topic=decompose_topic_path)


def getImagePath(task_id):
    datastore_client = datastore.Client()
    task_key = datastore_client.key("Status", task_id)
    task = datastore.Entity(key=task_key)
    return task['image_path']


def update_table(progress, task_id):
    datastore_client = datastore.Client()
    task_key = datastore_client.key("Status", task_id)
    task = datastore.Entity(key=task_key)
    task['status'] = progress
    task["update_time"] = time.ctime()


def addWatermark(message):
    '''add watermark to every frames'''
    data = json.loads(message.data)
    task_id = data['task_id']
    fps = data['fps']
    image_path = getImagePath(task_id)

    logo = Image.open(image_path)
    size = (500, 100)
    logo.thumbnail(size)
    i = 0
    for img_name in os.listdir('/tmp/frames'):
        img = Image.open('/tmp/frames/' + img_name)
        width, height = img.size
        transparent = Image.new('RGBA', (width, height), (0,0,0,0))
        transparent.paste(img, (0,0))
        transparent.paste(logo, (0,0), mask=logo)
        transparent.convert('RGB').save('/tmp/frames/' + img_name)
        i += 1
        progress = "processing {:.2f}%".format(100*i/len(os.listdir('/tmp/frames')))
        update_table(progress, task_id)

    process_message = {
        'task_id': task_id,
        'fps': fps
    }
    publisher.publish(process_topic_path, json.dumps(process_message).encode('utf-8'))
    subscriber.delete_subscription(subscription=decompose_subscription_path)
    publisher.delete_topic(topic=decompose_topic_path)
    message.ack()


if __name__ == "__main__":
    checkTopicSub()
    streaming_pull_future = subscriber.subscribe(decompose_subscription_path, callback=addWatermark)
    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()