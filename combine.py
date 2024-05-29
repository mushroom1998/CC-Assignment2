from google.cloud import pubsub_v1
import os
import json
import cv2
from google.cloud import storage
from google.cloud import datastore
import time

bucket_name = 'thinking-banner-421414_cloudbuild'
os.environ["GCLOUD_PROJECT"] = "thinking-banner-421414"

project_id = "thinking-banner-421414"
topic_id = "process-video"
subscription_id = "process-video-sub"
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)


def checkSub():
    try:
        subscriber.get_subscription(subscription=subscription_path)
    except:
        subscriber.create_subscription(name=subscription_path, topic=topic_path)


def getInformation(task_id):
    datastore_client = datastore.Client()
    task_key = datastore_client.key("Status", task_id)
    task = datastore.Entity(key=task_key)
    return task['video_name'], task['video_path'], task['image_path'], task['output_path']


def update_table(progress, task_id):
    datastore_client = datastore.Client()
    task_key = datastore_client.key("Status", task_id)
    task = datastore.Entity(key=task_key)
    task['status'] = progress
    task['update_time'] = time.ctime()


def createVideo(message: pubsub_v1.subscriber.message.Message) -> None:
    '''combine all watermarked frames to a new video'''
    data = json.loads(message.data)
    task_id = data['task_id']
    fps = data['fps']
    video_name, video_path, image_path, output_path = getInformation(task_id)

    update_table('merging', task_id)
    image_folder = '/tmp/frames'
    images = [img for img in os.listdir(image_folder)]
    frame = cv2.imread(os.path.join(image_folder, images[0]))
    height, width, layers = frame.shape
    fourcc = cv2.VideoWriter_fourcc(*'mp4v')
    video = cv2.VideoWriter(output_path, fourcc, fps, (width, height))
    images.sort()

    for image in images:
        video.write(cv2.imread(os.path.join(image_folder, image)))

    cv2.destroyAllWindows()
    video.release()
    download_url = upload_blob(output_path, video_name)
    os.system('rm -rf /tmp/frames')
    os.system('rm -f ' + video_path)
    os.system('rm -f ' + output_path)
    os.system('rm -f ' + image_path)
    subscriber.delete_subscription(subscription=subscription_path)
    publisher.delete_topic(topic=topic_path)
    status = "Task finish! Download URL: " + download_url + ('\n. You can download by running command: '
                                                             'curl -X GET -H "Authorization: Bearer '
                                                             '$(gcloud auth print-access-token)" -o '
                                                             '"LOCAL_FILENAME" "DOWNLOAD_URL"')
    update_table(status, task_id)
    message.ack()


def upload_blob(source_file_name, destination_blob_name):
    '''upload file to bucket'''
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)
    download_url = "https://storage.googleapis.com/" + bucket_name + "/" + destination_blob_name
    return download_url


if __name__ == "__main__":
    checkSub()
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=createVideo)
    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()