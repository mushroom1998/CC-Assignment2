from google.cloud import pubsub_v1, datastore, storage
import os
import cv2
import json
import time
import numpy as np
import logging

project_id = "thinking-banner-421414"
job_id = int(os.environ.get("HOSTNAME").split("-")[-1])
bucket_name = 'thinking-banner-421414_cloudbuild'

decompose_topic_id = "decompose-video"
decompose_subscription_id = "decompose-video-sub"
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


def getProgress(task_id):
    client = datastore.Client(project='thinking-banner-421414')
    key = client.key('Status', task_id)
    task = client.get(key)
    return 0 if task['status'] == 'Decomposing' else int(task['status'][:-1])


def update_table(task_id):
    client = datastore.Client(project='thinking-banner-421414')
    key = client.key('Status', task_id)
    task = client.get(key)
    if task:
        progress = 0 if task['status'] == 'Decomposing' else int(task['status'][:-1])
        task['status'] = "{:.0f}%".format(progress + 25)
        task['update_time'] = time.ctime()
    client.put(task)


def addWatermark(message):
    '''add watermark to videos'''
    data = json.loads(message.data)
    print(data)
    task_id = data['task_id']
    print(task_id)
    video_url = data['video_url']
    image_url = data['image_url']
    video_name = download_blob(video_url)
    image_name = download_blob(image_url)
    output_name = "video"+str(job_id)+".mp4"

    cap = cv2.VideoCapture(video_name)
    fps = cap.get(cv2.CAP_PROP_FPS)
    width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    segment_frames = total_frames // 4
    fourcc = cv2.VideoWriter_fourcc(*'mp4v')
    out = cv2.VideoWriter(output_name, fourcc, fps, (width, height))

    logo = cv2.imread(image_name, cv2.IMREAD_UNCHANGED)
    logo_height, logo_width = logo.shape[:2]


    start_frame = job_id * segment_frames
    end_frame = start_frame + segment_frames
    cap.set(cv2.CAP_PROP_POS_FRAMES, start_frame)
    while cap.isOpened() and cap.get(cv2.CAP_PROP_POS_FRAMES) < end_frame:
        ret, frame = cap.read()
        if not ret:
            break
        frame_height, frame_width = frame.shape[:2]
        scale = min(frame_width / logo_width, frame_height / logo_height)
        logo_resized = cv2.resize(logo, (int(logo_width * scale), int(logo_height * scale)))
        logo_height, logo_width = logo_resized.shape[:2]

        if logo_resized.shape[2] == 4:
            logo_rgb = cv2.cvtColor(logo_resized, cv2.COLOR_BGRA2BGR)
            logo_alpha = logo_resized[:, :, 3] / 255.0
        else:
            logo_rgb = logo_resized
            logo_alpha = np.ones((logo_height, logo_width))

        x_offset = frame_width - logo_width
        y_offset = frame_height - logo_height
        roi = frame[y_offset: y_offset + logo_height, x_offset: x_offset + logo_width]

        for c in range(3):
            roi[:, :, c] = roi[:, :, c] * (1 - logo_alpha) + logo_rgb[:, :, c] * logo_alpha

        frame[y_offset: y_offset + logo_height, x_offset: x_offset + logo_width] = roi

        out.write(frame)
    cap.release()
    out.release()
    cv2.destroyAllWindows()

    update_table(task_id)
    upload_blob(output_name, output_name)
    process_message = {
        'task_id': task_id
    }
    publisher.publish(process_topic_path, json.dumps(process_message).encode('utf-8'))
    message.ack()


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
    bucket_name = url.split('/')[-2]
    storage_client = storage.Client(project='thinking-banner-421414')
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(filename)
    destination_file_name = filename
    blob.download_to_filename(destination_file_name)
    return filename


if __name__ == "__main__":
    checkSubPub()
    streaming_pull_future = subscriber.subscribe(decompose_subscription_path, callback=addWatermark)
    with subscriber:
        try:
            streaming_pull_future.result()
        except KeyboardInterrupt:
            streaming_pull_future.cancel()
