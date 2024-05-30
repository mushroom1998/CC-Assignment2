from PIL import Image
from google.cloud import pubsub_v1
import os
import json
from google.cloud import datastore
import time
import logging

project_id = "thinking-banner-421414"
os.environ["GCLOUD_PROJECT"] = "thinking-banner-421414"

process_topic_id = "process-video"
publisher = pubsub_v1.PublisherClient()
process_topic_path = publisher.topic_path(project_id, process_topic_id)

decompose_subscription_id = "decompose-video-sub"
decompose_topic_id = "decompose-video"
subscriber = pubsub_v1.SubscriberClient()
decompose_subscription_path = subscriber.subscription_path(project_id, decompose_subscription_id)
decompose_topic_path = publisher.topic_path(project_id, decompose_topic_id)


def getImagePath(task_id):
    client = datastore.Client()
    key = client.key('Status', task_id)
    task = client.get(key)
    return task['image_path']


def update_table(progress, task_id):
    client = datastore.Client()
    key = client.key('Status', task_id)
    task = client.get(key)
    if task:
        task['status'] = progress
        task['update_time'] = time.ctime()
    client.put(task)
    logging.warning(progress)


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
        i += 1
        progress = "processing {:.2f}%".format(100*i/len(os.listdir('/tmp/frames')))
        update_table(progress, task_id)
        try:
            img = Image.open('/tmp/frames/' + img_name)
        except:
            os.remove('/tmp/frames/' + img_name)
            continue
        width, height = img.size
        transparent = Image.new('RGBA', (width, height), (0,0,0,0))
        transparent.paste(img, (0,0))
        transparent.paste(logo, (0,0), mask=logo)
        transparent.convert('RGB').save('/tmp/frames/' + img_name)

    process_message = {
        'task_id': task_id,
        'fps': fps
    }
    publisher.publish(process_topic_path, json.dumps(process_message).encode('utf-8'))
    message.ack()


if __name__ == "__main__":
    streaming_pull_future = subscriber.subscribe(decompose_subscription_path, callback=addWatermark)
    with subscriber:
        try:
            streaming_pull_future.result()
        except KeyboardInterrupt:
            streaming_pull_future.cancel()
