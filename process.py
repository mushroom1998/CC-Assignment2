from PIL import Image
from google.cloud import pubsub_v1, datastore, storage
import os
import cv2
import json
import time
import logging

project_id = "thinking-banner-421414"
os.environ["GCLOUD_PROJECT"] = "thinking-banner-421414"
job_id = int(os.environ.get("HOSTNAME").split("-")[-1])

process_topic_id = "process-video"
publisher = pubsub_v1.PublisherClient()
process_topic_path = publisher.topic_path(project_id, process_topic_id)

decompose_subscription_id = "decompose-video-sub"
decompose_topic_id = "decompose-video"
subscriber = pubsub_v1.SubscriberClient()
decompose_subscription_path = subscriber.subscription_path(project_id, decompose_subscription_id)
decompose_topic_path = publisher.topic_path(project_id, decompose_topic_id)


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
        task['status'] = "{:.2f}%".format(progress + 25)
        task['update_time'] = time.ctime()
    client.put(task)


def addWatermark(message):
    '''add watermark to videos'''
    data = json.loads(message.data)
    task_id = data['task_id']
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
    scale_factor = 0.1
    new_width = int(width * scale_factor)
    new_height = int(logo_height * (new_width / logo_width))
    logo = cv2.resize(logo, (new_width, new_height))

    start_frame = job_id * segment_frames
    end_frame = start_frame + segment_frames
    cap.set(cv2.CAP_PROP_POS_FRAMES, start_frame)
    while cap.isOpened() and cap.get(cv2.CAP_PROP_POS_FRAMES) < end_frame:
        ret, frame = cap.read()
        if ret:
            overlay = frame.copy()
            overlay[-logo_height:, -logo_width:] = logo
            frame = cv2.addWeighted(overlay, 0.5, frame, 0.5, 0)
            out.write(frame)
        else:
            break

    update_table(task_id)
    upload_blob(output_name, output_name[:-4])
    process_message = {
        'task_id': task_id
    }
    publisher.publish(process_topic_path, json.dumps(process_message).encode('utf-8'))
    message.ack()


def download_folder(bucket_name, source_folder_path, local_folder_path):
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=source_folder_path)
    for blob in blobs:
        if blob.name[-1] == '/':
            continue
        blob.download_to_filename(os.path.join(local_folder_path, blob.name.split('/')[-1]))


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
    streaming_pull_future = subscriber.subscribe(decompose_subscription_path, callback=addWatermark)
    with subscriber:
        try:
            streaming_pull_future.result()
        except KeyboardInterrupt:
            streaming_pull_future.cancel()
