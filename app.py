import cv2
from PIL import Image
from flask import Flask, request, jsonify, send_from_directory
from google.cloud import storage
import os
import re

app = Flask(__name__)

bucket_name = 'thinking-banner-421414_cloudbuild'

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
            cv2.imwrite("/tmp/frames/frame%s.jpg" % str(count).zfill(4), frame)
            count += 1
        vidcap.release()
    cv2.destroyAllWindows()
    return fps


def addWatermark(logo, img_name):
    '''add watermark to every frames'''
    img = Image.open('/tmp/frames/' + img_name)
    width, height = img.size
    transparent = Image.new('RGBA', (width, height), (0,0,0,0))
    transparent.paste(img, (0,0))
    transparent.paste(logo, (0,0), mask=logo)
    transparent.convert('RGB').save('/tmp/frames/' + img_name)


def createVideo(output_path, fps):
    '''combine all watermarked frames to a new video'''
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


def download_blob(url):
    '''save the file in url at /tmp/file and return filename'''
    filename = url.split('/')[-1]
    command = ('curl -X GET -H "Authorization: Bearer $(gcloud auth print-access-token)" -o "/tmp/' + filename
               + '" "' + url + '"')
    os.system(command)
    return filename


def upload_blob(bucket_name, source_file_name, destination_blob_name):
    '''upload file to bucket'''
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    generation_match_precondition = 0
    blob.upload_from_filename(source_file_name, if_generation_match=generation_match_precondition)
    # public_url = "https://console.cloud.google.com/storage/browser/_details/" + bucket_name + "/" + destination_blob_name


def process_video(image_path, video_path, output_path, video_name):
    '''function to perform video processing procedure'''
    logo = Image.open(image_path)
    fps = decomposeFrame(video_path)
    size = (500, 100)
    logo.thumbnail(size)
    for img_name in os.listdir('/tmp/frames'):
        addWatermark(logo, img_name)
    createVideo(output_path, fps)

    upload_blob(bucket_name, output_path, video_name)
    os.system('rm -rf /tmp/frames')
    os.system('rm -f ' + video_path)
    os.system('rm -f ' + output_path)
    os.system('rm -f ' + image_path)


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

    try:
        process_video(image_path, video_path, output_path, video_name)

        return jsonify({
            "message": "Job token: "
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/upload2', methods=['POST'])
def urlProcess():
    video_url = request.form['videoURL']
    image_url = request.form['watermarkURL']

    if not (re.match(r'https://storage\.cloud\.google\.com/.*/.*', video) and
            re.match(r'https://storage\.cloud\.google\.com/.*/.*', image)):
        return jsonify({"message": "Wrong URL input. Please check your URL form."}), 500

    video_name = download_blob(video_url)
    image_name = download_blob(image_url)
    video_path = "/tmp/" + video_name
    image_path = "/tmp/" + image_name
    output_path = "/tmp/watermarked_" + video_name

    try:
        process_video(image_path, video_path, output_path, video_name)

        return jsonify({
            "message": ""
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/token', methods=['POST'])
def tokenProcess():
    try:

        return jsonify({
            "message": ""
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))