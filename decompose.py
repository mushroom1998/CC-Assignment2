import logging
import cv2
from flask import Flask, request, jsonify, send_from_directory
import os
import re
import uuid
import json
from google.cloud import pubsub_v1

app = Flask(__name__)
os.environ["GCLOUD_PROJECT"] = "thinking-banner-421414"
task_id = str(uuid.uuid4())

project_id = "thinking-banner-421414"
decompose_topic_id = "decompose-video"

publisher = pubsub_v1.PublisherClient()
decompose_topic_path = publisher.topic_path(project_id, decompose_topic_id)


def checkTopicSub():
    try:
        publisher.get_topic(topic=decompose_topic_path)
    except:
        publisher.create_topic(name=decompose_topic_path)


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
    command = ('curl -X GET -H "Authorization: Bearer $(gcloud auth print-access-token)" -o "/tmp/' + filename
               + '" "' + url + '"')
    os.system(command)
    return filename


def call(image_path, video_path, output_path, video_name, fps):
    fps = decomposeFrame(video_path)
    message = {
        'fps': fps,
        'video_name': video_name,
        'video_path': video_path,
        'image_path': image_path,
        'output_path': output_path,
        'status': 'pending'
    }
    publisher.publish(decompose_topic_path, json.dumps(message).encode('utf-8'))


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

    fps = decomposeFrame(video_path)
    logging.warning(task_id)
    call(image_path, video_path, output_path, video_name, fps)

    return jsonify({"message": "task_id is " + task_id})


@app.route('/upload2', methods=['POST'])
def urlProcess():
    video_url = request.form['videoURL']
    image_url = request.form['watermarkURL']

    if not (re.match(r'https://storage\.cloud\.google\.com/.*/.*', video_url) and
            re.match(r'https://storage\.cloud\.google\.com/.*/.*', image_url)):
        return jsonify({"message": "Wrong URL input. Please check your URL form."}), 500

    video_name = download_blob(video_url)
    image_name = download_blob(image_url)
    video_path = "/tmp/" + video_name
    image_path = "/tmp/" + image_name
    output_path = "/tmp/watermarked_" + video_name

    fps = decomposeFrame(video_path)
    call(image_path, video_path, output_path, video_name, fps)

    return jsonify({"message": "task_id is " + task_id})


@app.route('/token', methods=['POST'])
def tokenProcess():
    try:

        return jsonify({
            "message": ""
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    checkTopicSub()
    app.run(debug=True, host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))