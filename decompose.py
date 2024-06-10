import logging
from flask import Flask, request, jsonify, send_from_directory
import cv2
import os
import uuid
import math
import json
from google.cloud import pubsub_v1, datastore, storage
import time
from kubernetes import client, config
import re

app = Flask(__name__)
os.environ["GCLOUD_PROJECT"] = "thinking-banner-421414"
bucket_name = 'thinking-banner-421414_cloudbuild'
task_id = str(uuid.uuid4())

project_id = "thinking-banner-421414"
decompose_topic_id = "decompose-video"
decompose_subscription_paths = []
process_subscription_id = "process-video-sub"

publisher = pubsub_v1.PublisherClient()
subscriber = pubsub_v1.SubscriberClient()
decompose_topic_path = publisher.topic_path(project_id, decompose_topic_id)
process_subscription_path = subscriber.subscription_path(project_id, process_subscription_id)


def callback(message):
    print(f"Dropping message: {message.data}")
    message.ack()


def clear_subscription(subscription_path):
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


def getPodCount():
    config.load_kube_config()
    v1 = client.CoreV1Api()
    pods = v1.list_namespaced_pod(namespace='default')
    return len(pods.items)


def split_video(input_file, pod_num):
    cap = cv2.VideoCapture(input_file)
    fps = cap.get(cv2.CAP_PROP_FPS)
    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    frames_per_split = math.ceil(total_frames / pod_num)
    fourcc = cv2.VideoWriter_fourcc(*'mp4v')

    for i in range(pod_num):
        # 创建视频写入器
        output_file = input_file.split('.')[0] + str(i) + '.mp4'
        out = cv2.VideoWriter(output_file, fourcc, fps, (width, height))
        for j in range(frames_per_split):
            ret, frame = cap.read()
            if not ret:
                break
            out.write(frame)
        out.release()

        if not ret:
            break
    cap.release()


def create_table(video_path, image_path):
    '''create NoSQL table to store task status'''
    client = datastore.Client(project='thinking-banner-421414')
    task_key = client.key("Status", task_id)
    task = datastore.Entity(key=task_key)

    task['image_path'] = image_path
    task['video_path'] = video_path
    task["status"] = "Preprocessing"
    task["update_time"] = time.ctime()

    client.put(task)


def getProgress(task_id):
    client = datastore.Client(project='thinking-banner-421414')
    key = client.key('Status', task_id)
    task = client.get(key)
    return task['status']


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


@app.route('/')
def index():
    return send_from_directory('static', 'web.html')


@app.route('/upload1', methods=['POST'])
def videoProcess():
    video = request.files['videoFile']
    image = request.files['watermarkFile']

    video_path = video.filename
    image_path = image.filename

    video.save(video_path)
    image.save(image_path)
    split_video(video_path, pod_num)

    create_table(video_path, image_path)
    upload_blob(image_path, image_path)
    for i in range(pod_num):
        upload_blob(video_path.split('.')[0] + str(i) + '.mp4', video_path.split('.')[0] + str(i) + '.mp4')
    message = {
        'task_id': task_id,
        'video_name': video_path,
        'image_name': image_path,
        'pod_num': pod_num
    }
    publisher.publish(decompose_topic_path, json.dumps(message).encode('utf-8'))
    logging.warning(message)
    return jsonify({"message": "Task_id: " + task_id})


@app.route('/upload2', methods=['POST'])
def urlProcess():
    video_url = request.form['videoURL']
    image_url = request.form['watermarkURL']

    pattern = r'^https://storage\.googleapis\.com/([^/]+)/([^/]+)\.([^/]+)$'
    video_match = re.match(pattern, video_url)
    image_match = re.match(pattern, image_url)
    if video_match is None or image_match is None:
        return jsonify({"message": "Wrong URL input. Please check your URL form."}), 500

    video_path = download_blob(video_url)
    image_path = download_blob(image_url)
    split_video(video_path, pod_num)

    create_table(video_path, image_path)
    upload_blob(image_path, image_path)
    for i in range(pod_num):
        upload_blob(video_path.split('.')[0] + str(i) + '.mp4', video_path.split('.')[0] + str(i) + '.mp4')
    message = {
        'task_id': task_id,
        'video_name': video_path,
        'image_name': image_path,
        'pod_num': pod_num
    }
    publisher.publish(decompose_topic_path, json.dumps(message).encode('utf-8'))
    logging.warning(message)
    return jsonify({"message": "task_id is " + task_id})


@app.route('/tokenProcess', methods=['GET'])
def tokenProcess():
    try:
        input_task_id = request.args.get('taskID')
        progress = getProgress(input_task_id)
        return jsonify({"message": progress})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    pod_num = getPodCount()
    for i in range(pod_num):
        decompose_subscription_path = subscriber.subscription_path(project_id, "decompose-video-sub-" + str(i))
        clear_subscription(decompose_subscription_path)
    clear_subscription(process_subscription_path)
    checkSubPub()
    app.run(debug=True, host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))