from google.cloud import pubsub_v1
import os
import json
import cv2
from google.cloud import storage

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


def createVideo(message: pubsub_v1.subscriber.message.Message) -> None:
    '''combine all watermarked frames to a new video'''
    data = json.loads(message.data)
    fps = data['fps']
    video_name = data['video_name']
    video_path = data['video_path']
    image_path = data['image_path']
    output_path = data['output_path']

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
    upload_blob(bucket_name, output_path, video_name)
    os.system('rm -rf /tmp/frames')
    os.system('rm -f ' + video_path)
    os.system('rm -f ' + output_path)
    os.system('rm -f ' + image_path)
    subscriber.delete_subscription(subscription=subscription_path)
    publisher.delete_topic(topic=topic_path)
    message.ack()


def upload_blob(bucket_name, source_file_name, destination_blob_name):
    '''upload file to bucket'''
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)
    # public_url = "https://console.cloud.google.com/storage/browser/_details/" + bucket_name + "/" + destination_blob_name


if __name__ == "__main__":
    checkSub()
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=createVideo)
    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()