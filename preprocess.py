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
bucket_name = 'thinking-banner-421414_cloudbuild'
project_id = "thinking-banner-421414"
decompose_topic_id = "decompose-video"
decompose_subscription_paths = []
process_subscription_id = "process-video-sub"

publisher = pubsub_v1.PublisherClient()
subscriber = pubsub_v1.SubscriberClient()
decompose_topic_path = publisher.topic_path(project_id, decompose_topic_id)
process_subscription_path = subscriber.subscription_path(project_id, process_subscription_id)


def checkSubPub():
    '''create the Pub/Sub if not exist'''
    try:
        publisher.get_topic(topic=decompose_topic_path)
    except:
        publisher.create_topic(name=decompose_topic_path)


def getPodCount():
    '''Get the total number of pods in GKE'''
    try:
        config.load_kube_config()
    except:
        return 1
    v1 = client.CoreV1Api()
    pods = v1.list_namespaced_pod(namespace='default')
    return len(pods.items)


def splitVideo(input_file, pod_num):
    '''split the original video to pod_num clips'''
    cap = cv2.VideoCapture(input_file)
    fps = cap.get(cv2.CAP_PROP_FPS)
    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    frames_per_split = math.floor(total_frames / pod_num)
    fourcc = cv2.VideoWriter_fourcc(*'mp4v')

    for i in range(pod_num):
        output_file = input_file.split('.')[0] + str(i) + '.mp4'
        out = cv2.VideoWriter(output_file, fourcc, fps, (width, height))
        for j in range(frames_per_split):
            success, frame = cap.read()
            if not success:
                break
            out.write(frame)
        out.release()

        if not success:
            break
        logging.warning("video" + str(i) + " split.")
    cap.release()


def create_table(video_path, image_path, task_id):
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
    '''get the status of task by task_id'''
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
    '''save the file in url at local directory and return filename'''
    filename = url.split('/')[-1]
    storage_client = storage.Client(project='thinking-banner-421414')
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(filename)
    blob.download_to_filename(filename)
    return filename


@app.route('/')
def index():
    return send_from_directory('static', 'web.html')


@app.route('/upload1', methods=['POST'])
def videoProcess():
    '''deal with file input'''
    task_id = str(uuid.uuid4())

    video = request.files['videoFile']
    image = request.files['watermarkFile']
    video_path = video.filename
    image_path = image.filename

    # format checking
    if image_path.split('.')[-1].lower() not in ['png', 'jpg', 'jpeg']:
        return jsonify({"message": "Not an image file. Please check your URL form."}), 500
    if video_path.split('.')[-1].lower() not in ['mov', 'mp4', 'wmv', 'avi']:
        return jsonify({"message": "Not a video file. Please check your URL form."}), 500

    video.save(video_path)
    image.save(image_path)
    splitVideo(video_path, pod_num)

    create_table(video_path, image_path, task_id)
    # upload video clips and watermark to GCS
    upload_blob(image_path, image_path)
    for i in range(pod_num):
        upload_blob(video_path.split('.')[0] + str(i) + '.mp4', video_path.split('.')[0] + str(i) + '.mp4')
        os.remove(video_path.split('.')[0] + str(i) + '.mp4')

    message = {
        'task_id': task_id,
        'video_name': video_path,
        'image_name': image_path,
        'pod_num': pod_num
    }
    publisher.publish(decompose_topic_path, json.dumps(message).encode('utf-8'))
    logging.warning(message)
    os.remove(video_path)
    os.remove(image_path)
    return jsonify({"message": "Task_id: " + task_id})


@app.route('/upload2', methods=['POST'])
def urlProcess():
    '''deal with URL input'''
    task_id = str(uuid.uuid4())

    video_url = request.form['videoURL']
    image_url = request.form['watermarkURL']

    # format check
    pattern = r'^https://storage\.googleapis\.com/([^/]+)/([^/]+)\.([^/]+)$'
    video_match = re.match(pattern, video_url)
    image_match = re.match(pattern, image_url)
    if video_match is None or image_match is None:
        return jsonify({"message": "Wrong URL input. Please check your URL form."}), 500
    if image_url.split('.')[-1].lower() not in ['png', 'jpg', 'jpeg']:
        return jsonify({"message": "Not an image file. Please check your URL form."}), 500
    if video_url.split('.')[-1].lower() not in ['mov', 'mp4', 'wmv', 'avi']:
        return jsonify({"message": "Not a video file. Please check your URL form."}), 500

    video_path = download_blob(video_url)
    image_path = image_url.split('/')[-1]
    splitVideo(video_path, pod_num)

    create_table(video_path, image_path, task_id)
    for i in range(pod_num):
        upload_blob(video_path.split('.')[0] + str(i) + '.mp4', video_path.split('.')[0] + str(i) + '.mp4')
        os.remove(video_path.split('.')[0] + str(i) + '.mp4')
    message = {
        'task_id': task_id,
        'video_name': video_path,
        'image_name': image_path,
        'pod_num': pod_num
    }
    publisher.publish(decompose_topic_path, json.dumps(message).encode('utf-8'))
    logging.warning(message)
    os.remove(video_path)
    return jsonify({"message": "task_id is " + task_id})


@app.route('/tokenProcess', methods=['GET'])
def tokenProcess():
    '''get the status of task using id'''
    try:
        input_task_id = request.args.get('taskID')
        progress = getProgress(input_task_id)
        return jsonify({"message": progress})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    pod_num = getPodCount()
    checkSubPub()
    app.run(debug=True, host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))