import os
from google.cloud import pubsub_v1
import json
import threading
import cv2
from google.cloud import storage
from google.cloud import datastore
import time

bucket_name = 'thinking-banner-421414_cloudbuild'
message_count = {}
lock = threading.Lock()

project_id = "thinking-banner-421414"
process_subscription_id = "process-video-sub"
subscriber = pubsub_v1.SubscriberClient()
process_subscription_path = subscriber.subscription_path(project_id, process_subscription_id)

publisher = pubsub_v1.PublisherClient()
process_topic_id = "process-video"
process_topic_path = publisher.topic_path(project_id, process_topic_id)


def checkSubPub():
    '''create the Pub/Sub if not exist'''
    try:
        subscriber.get_subscription(subscription=process_subscription_path)
    except:
        subscriber.create_subscription(name=process_subscription_path, topic=process_topic_path)


def update_table(progress, task_id):
    '''update the progress of task'''
    client = datastore.Client(project='thinking-banner-421414')
    key = client.key('Status', task_id)
    task = client.get(key)
    if task:
        task['status'] = progress
        task['update_time'] = time.ctime()
    client.put(task)


def combineVideo(message):
    '''combine all watermarked video clips to a new video'''
    global message_count
    data = json.loads(message.data)
    message.ack()
    task_id = data['task_id']
    pod_num = data['pod_num']

    with lock:
        print(message_count)
        if task_id not in message_count:
            message_count[task_id] = 0
        message_count[task_id] += 1
        print(message_count[task_id], task_id)
        update_table("{:.2f}%".format(message_count[task_id] * 100 / pod_num), task_id)

    if message_count[task_id] == pod_num:  # perform combine operations when all workers finish
        video_files = []
        for i in range(pod_num):
            download_blob("https://storage.googleapis.com/" + bucket_name + "/" + task_id + "_video" + str(i) + ".mp4")
            video_files.append(task_id + '_video' + str(i) + '.mp4')

        print("start combining")
        cap = cv2.VideoCapture(video_files[0])
        fps = int(cap.get(cv2.CAP_PROP_FPS))
        width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
        height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
        cap.release()
        fourcc = cv2.VideoWriter_fourcc(*'mp4v')
        out = cv2.VideoWriter(task_id+".mp4", fourcc, fps, (width, height))

        for video_file in video_files:
            cap = cv2.VideoCapture(video_file)
            while True:
                success, frame = cap.read()
                if not success:
                    break
                out.write(frame)
            cap.release()
            os.remove(video_file)
        out.release()

        print("uploading...")
        download_url = upload_blob(task_id+".mp4", task_id+".mp4")
        status = "Task finish! Download URL: " + download_url + ('\n. You can download by running command: '
                                                                 'curl -X GET -H "Authorization: Bearer '
                                                                 '$(gcloud auth print-access-token)" -o '
                                                                 '"LOCAL_FILENAME" "DOWNLOAD_URL"')
        print(task_id + " finish time: " + time.ctime())
        update_table(status, task_id)
        os.remove(task_id+".mp4")


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
    blob.download_to_filename(filename)
    return filename


if __name__ == "__main__":
    checkSubPub()
    streaming_pull_future = subscriber.subscribe(process_subscription_path, callback=combineVideo)
    with subscriber:
        try:
            streaming_pull_future.result()
        except KeyboardInterrupt:
            streaming_pull_future.cancel()