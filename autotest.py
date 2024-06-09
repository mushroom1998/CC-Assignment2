from google.cloud import pubsub_v1, datastore
from kubernetes import client, config
import uuid
import json
import time


publisher = pubsub_v1.PublisherClient()
subscriber = pubsub_v1.SubscriberClient()
project_id = "thinking-banner-421414"
process_subscription_id = "process-video-sub"
process_subscription_path = subscriber.subscription_path(project_id, process_subscription_id)
decompose_topic_id = "decompose-video"
decompose_topic_path = publisher.topic_path(project_id, decompose_topic_id)

def getPodCount():
    config.load_kube_config()
    v1 = client.CoreV1Api()
    pods = v1.list_namespaced_pod(namespace='default')
    return len(pods.items)


def clear_subscription(subscription_path):
    def callback(message):
        print(f"Dropping message: {message.data}")
        message.ack()
    future = subscriber.subscribe(subscription_path, callback)
    try:
        future.result(timeout=10)
    except Exception as e:
        future.cancel()
        print(f"Subscription cleared: {subscription_path}")


def checkSubPub():
    '''create the Pub/Sub if not exist'''
    try:
        publisher.get_topic(topic=decompose_topic_path)
    except:
        publisher.create_topic(name=decompose_topic_path)


def create_table(task_id):
    '''create NoSQL table to store task status'''
    client = datastore.Client(project='thinking-banner-421414')
    task_key = client.key("Status", task_id)
    task = datastore.Entity(key=task_key)

    task["status"] = "Preprocessing"
    task["update_time"] = time.ctime()

    client.put(task)


if __name__ == "__main__":
    image_urls = ['https://storage.googleapis.com/thinking-banner-421414_cloudbuild/Logo.png',
                  'image_urls']
    video_urls = ['https://storage.googleapis.com/thinking-banner-421414_cloudbuild/fatfat.mp4',
                  'video_url']

    pod_num = getPodCount()
    for i in range(pod_num):
        decompose_subscription_path = subscriber.subscription_path(project_id, "decompose-video-sub-" + str(i))
        clear_subscription(decompose_subscription_path)
    clear_subscription(process_subscription_path)
    checkSubPub()

    for i in range(len(image_urls)):
        task_id = str(uuid.uuid4())
        create_table(task_id)
        message = {
            'task_id': task_id,
            'video_url': video_urls[i],
            'image_url': image_urls[i],
            'pod_num': pod_num
        }
        publisher.publish(decompose_topic_path, json.dumps(message).encode('utf-8'))
        filename = video_urls[i].split('/')[-1]
        print(filename + ", task_id is " + task_id)
