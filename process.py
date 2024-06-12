from google.cloud import pubsub_v1, datastore, storage
import os
import cv2
import json
import time
import numpy as np

project_id = "thinking-banner-421414"
job_id = os.environ.get("HOSTNAME").split("-")[-1]
bucket_name = 'thinking-banner-421414_cloudbuild'

decompose_topic_id = "decompose-video"
decompose_subscription_id = "decompose-video-sub-" + job_id
process_topic_id = "process-video"
process_subscription_id = "process-video-sub"

publisher = pubsub_v1.PublisherClient()
subscriber = pubsub_v1.SubscriberClient()
decompose_topic_path = publisher.topic_path(project_id, decompose_topic_id)
decompose_subscription_path = subscriber.subscription_path(project_id, decompose_subscription_id)
process_topic_path = publisher.topic_path(project_id, process_topic_id)
process_subscription_path = subscriber.subscription_path(project_id, process_subscription_id)


def checkSubPub():
    '''create the Pub/Sub if not exist'''
    try:
        publisher.get_topic(topic=process_topic_path)
    except:
        publisher.create_topic(name=process_topic_path)
    try:
        subscriber.get_subscription(subscription=decompose_subscription_path)
    except:
        subscriber.create_subscription(name=decompose_subscription_path, topic=decompose_topic_path)


def addWatermark(message):
    '''add watermark to video clips'''
    data = json.loads(message.data)
    message.ack()
    task_id = data['task_id']
    print(job_id, task_id)
    video_name = data['video_name']
    image_name = data['image_name']
    pod_num = data['pod_num']
    video_url = "https://storage.googleapis.com/" + bucket_name + "/" + video_name.split('.')[0] + job_id + ".mp4"
    image_url = "https://storage.googleapis.com/" + bucket_name + "/" + image_name
    video_name = download_blob(video_url)
    image_name = download_blob(image_url)
    if video_name == None or image_name == None:
        return
    output_name = task_id + "_video" + job_id + ".mp4"

    cap = cv2.VideoCapture(video_name)
    fps = cap.get(cv2.CAP_PROP_FPS)
    width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    fourcc = cv2.VideoWriter_fourcc(*'mp4v')
    out = cv2.VideoWriter(output_name, fourcc, fps, (width, height))

    image = cv2.imread(image_name, cv2.IMREAD_UNCHANGED)
    image_height, image_width = image.shape[:2]
    success, frame = cap.read()
    frame_height, frame_width = frame.shape[:2]
    scale = min(frame_width / image_width, frame_height / image_height)
    image_resized = cv2.resize(image, (int(image_width * scale), int(image_height * scale)))
    image_height, image_width = image_resized.shape[:2]

    # transform to BGR if watermark file is BGRA
    if image_resized.shape[2] == 4:
        image_rgb = cv2.cvtColor(image_resized, cv2.COLOR_BGRA2BGR)
        image_alpha = image_resized[:, :, 3] / 255.0
    else:
        image_rgb = image_resized
        image_alpha = np.ones((image_height, image_width))

    x_loc = frame_width - image_width
    y_loc = frame_height - image_height

    while cap.isOpened():
        success, frame = cap.read()
        if not success:
            break

        watermark_loc = frame[y_loc: y_loc + image_height, x_loc: x_loc + image_width]
        for c in range(3):
            watermark_loc[:, :, c] = watermark_loc[:, :, c] * (1 - image_alpha) + image_rgb[:, :, c] * image_alpha
        frame[y_loc: y_loc + image_height, x_loc: x_loc + image_width] = watermark_loc

        out.write(frame)
    cap.release()
    out.release()
    cv2.destroyAllWindows()
    print('add finish')

    upload_blob(output_name, output_name)
    print('upload')
    process_message = {
        'task_id': task_id,
        'pod_num': pod_num
    }
    future = publisher.publish(process_topic_path, json.dumps(process_message).encode('utf-8'))
    os.remove(video_name)
    os.remove(image_name)
    os.remove(output_name)
    print(task_id, future.result())


def upload_blob(source_file_name, destination_blob_name):
    '''upload file to bucket'''
    storage_client = storage.Client(project='thinking-banner-421414')
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)
    download_url = "https://storage.googleapis.com/" + bucket_name + "/" + destination_blob_name
    return download_url


def download_blob(url):
    '''save the file in url at /tmp/file and return filename'''
    filename = url.split('/')[-1]
    storage_client = storage.Client(project='thinking-banner-421414')
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(filename)
    try:
        blob.download_to_filename(filename)
        return filename
    except:
        return None


if __name__ == "__main__":
    checkSubPub()
    streaming_pull_future = subscriber.subscribe(decompose_subscription_path, callback=addWatermark)
    with subscriber:
        try:
            streaming_pull_future.result()
        except KeyboardInterrupt:
            streaming_pull_future.cancel()
