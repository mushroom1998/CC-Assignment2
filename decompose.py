import cv2
from flask import Flask, request, jsonify, send_from_directory
import os
import uuid
import json
from google.cloud import pubsub_v1
from google.cloud import datastore
from google.cloud import storage
import time

app = Flask(__name__)
os.environ["GCLOUD_PROJECT"] = "thinking-banner-421414"
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


def create_table(video_name, video_path, image_path, output_path):
    '''create NoSQL table to store task status'''
    datastore_client = datastore.Client()
    task_key = datastore_client.key("Status", task_id)
    task = datastore.Entity(key=task_key)

    task["video_name"] = video_name
    task["video_path"] = video_path
    task["image_path"] = image_path
    task["output_path"] = output_path
    task["status"] = "Decomposing"
    task["update_time"] = time.ctime()

    datastore_client.put(task)


def getProgress(task_id):
    client = datastore.Client()
    key = client.key('Status', task_id)
    task = client.get(key)
    return task['status']


def decomposeFrame(video_path):
    '''decompose the original video to frames'''
    vidcap = cv2.VideoCapture(video_path)
    fps = vidcap.get(cv2.CAP_PROP_FPS)
    if not os.path.isdir('/tmp/frames'):
        os.mkdir('/tmp/frames')
    if vidcap.isOpened():
        success, frame = vidcap.read()
        count = 0
        while success:
            success, frame = vidcap.read()
            if not success:
                break
            cv2.imwrite("/tmp/frames/frame%s.jpg" % str(count).zfill(5), frame)
            count += 1
        vidcap.release()
    cv2.destroyAllWindows()
    return fps


def download_blob(url):
    '''save the file in url at /tmp/file and return filename'''
    filename = url.split('/')[-1]
    bucket_name = url.split('/')[-2]
    storage_client = storage.Client()
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
    output_path = "/tmp/watermarked_" + video_name

    video.save(video_path)
    image.save(image_path)

    create_table(video_name, video_path, image_path, output_path)
    fps = decomposeFrame(video_path)
    message = {
        'task_id': task_id,
        'fps': fps
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

    video_name = download_blob(video_url)
    image_name = download_blob(image_url)
    video_path = "/tmp/" + video_name
    image_path = "/tmp/" + image_name
    output_path = "/tmp/watermarked_" + video_name

    create_table(video_name, video_path, image_path, output_path)
    fps = decomposeFrame(video_path)
    message = {
        'task_id': task_id,
        'fps': fps
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