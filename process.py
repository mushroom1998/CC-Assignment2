import logging
from PIL import Image
from google.cloud import pubsub_v1
import os
import json

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
    try:
        publisher.get_topic(topic=process_topic_path)
    except:
        publisher.create_topic(name=process_topic_path)
    try:
        subscriber.get_subscription(subscription=decompose_subscription_path)
    except:
        subscriber.create_subscription(name=decompose_subscription_path, topic=decompose_topic_path)


def addWatermark(message):
    '''add watermark to every frames'''
    data = json.loads(message.data)
    fps = data['fps']
    video_name = data['video_name']
    video_path = data['video_path']
    image_path = data['image_path']
    output_path = data['output_path']

    logo = Image.open(image_path)
    size = (500, 100)
    logo.thumbnail(size)
    for img_name in os.listdir('/tmp/frames'):
        img = Image.open('/tmp/frames/' + img_name)
        width, height = img.size
        transparent = Image.new('RGBA', (width, height), (0,0,0,0))
        transparent.paste(img, (0,0))
        transparent.paste(logo, (0,0), mask=logo)
        transparent.convert('RGB').save('/tmp/frames/' + img_name)

    process_message = {
        'fps': fps,
        'video_name': video_name,
        'video_path': video_path,
        'image_path': image_path,
        'output_path': output_path,
        'status': 'pending'
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