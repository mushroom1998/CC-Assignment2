import logging

import cv2
from flask import Flask, request, jsonify, send_from_directory
import os
import uuid
import json
from google.cloud import pubsub_v1, datastore, storage
import time

app = Flask(__name__)
os.environ["GCLOUD_PROJECT"] = "thinking-banner-421414"
bucket_name = 'thinking-banner-421414_cloudbuild'
task_id = str(uuid.uuid4())

project_id = "thinking-banner-421414"
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
        publisher.get_topic(topic=decompose_topic_path)
    except:
        publisher.create_topic(name=decompose_topic_path)
    try:
        publisher.get_topic(topic=process_topic_path)
    except:
        publisher.create_topic(name=process_topic_path)
    try:
        subscriber.get_subscription(subscription=decompose_subscription_path)
    except:
        subscriber.create_subscription(name=decompose_subscription_path, topic=decompose_topic_path)
    try:
        subscriber.get_subscription(subscription=process_subscription_path)
    except:
        subscriber.create_subscription(name=process_subscription_path, topic=process_topic_path)


def create_table():
    '''create NoSQL table to store task status'''
    client = datastore.Client(project='thinking-banner-421414')
    task_key = client.key("Status", task_id)
    task = datastore.Entity(key=task_key)

    task["status"] = "Decomposing"
    task["update_time"] = time.ctime()

    client.put(task)


def getProgress(task_id):
    client = datastore.Client(project='thinking-banner-421414')
    key = client.key('Status', task_id)
    task = client.get(key)
    return task['status']


def splitWorker(video_path):
    '''decompose the original video to frames'''
    output_video_paths = ['video_1.mp4', 'video_2.mp4', 'video_3.mp4', 'video_4.mp4']
    cap = cv2.VideoCapture(video_path)
    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    fps = cap.get(cv2.CAP_PROP_FPS)
    segment_frames = total_frames // 4
    if not os.path.isdir('/tmp/videos'):
        os.mkdir('/tmp/videos')
    for i, output_video_path in enumerate(output_video_paths):
        start_frame = i * segment_frames
        end_frame = start_frame + segment_frames

        # 设置视频的编解码器和帧大小
        fourcc = cv2.VideoWriter_fourcc(*'mp4v')
        width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
        height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
        out = cv2.VideoWriter(output_video_path, fourcc, fps, (width, height))

        # 读取并写入视频帧
        cap.set(cv2.CAP_PROP_POS_FRAMES, start_frame)
        while cap.isOpened() and cap.get(cv2.CAP_PROP_POS_FRAMES) < end_frame:
            ret, frame = cap.read()
            if ret:
                out.write(frame)
            else:
                break

        # 释放资源
        out.release()
    cap.release()
    cv2.destroyAllWindows()
    # upload_folder(bucket_name, '/videos')


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
    destination_file_name = "/tmp/" + filename
    blob.download_to_filename(destination_file_name)
    return filename


@app.route('/')
def index():
    return send_from_directory('static', 'web.html')


@app.route('/upload1', methods=['POST'])
def videoProcess():
    video = request.files['videoFile']
    image = request.files['watermarkFile']

    video_name = video.filename
    video_path = "/tmp/" + video_name
    image_path = "/tmp/" + image.filename

    video.save(video_path)
    image.save(image_path)

    create_table()
    video_url = upload_blob(video_path, video_name)
    image_url = upload_blob(image_path, image.filename)
    message = {
        'task_id': task_id,
        'video_url': video_url,
        'image_url': image_url
    }
    publisher.publish(decompose_topic_path, json.dumps(message).encode('utf-8'))
    return jsonify({"message": "Task_id: " + task_id})


@app.route('/upload2', methods=['POST'])
def urlProcess():
    video_url = request.form['videoURL']
    image_url = request.form['watermarkURL']

    required_string = ['cloud', 'storage', 'google', '.com', 'https://']

    if not (all(substr in video_url for substr in required_string) and
            all(substr in image_url for substr in required_string)):
        return jsonify({"message": "Wrong URL input. Please check your URL form."}), 500

    create_table()
    # fps = decomposeFrame(video_path)
    message = {
        'task_id': task_id,
        'video_url': video_url,
        'image_url': image_url
    }
    publisher.publish(decompose_topic_path, json.dumps(message).encode('utf-8'))
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
    checkSubPub()
    app.run(debug=True, host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))