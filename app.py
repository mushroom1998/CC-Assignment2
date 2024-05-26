import cv2
from PIL import Image
import os
from flask import Flask, request, jsonify, send_from_directory
from google.cloud import storage
import subprocess

app = Flask(__name__)


def decomposeFrame(video_path):
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


def addWatermark(logo):
    for img_name in os.listdir('/tmp/frames'):
        img = Image.open('/tmp/frames/' + img_name)
        width, height = img.size
        transparent = Image.new('RGBA', (width, height), (0,0,0,0))
        transparent.paste(img, (0,0))
        transparent.paste(logo, (0,0), mask=logo)
        transparent.convert('RGB').save('/tmp/frames/' + img_name)


def createVideo(output_path, fps):
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


def storeWatermarkedFile(file_path, bucket_name, destination_blob_name):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(file_path)
    return blob.public_url


@app.route('/')
def index():
    return send_from_directory('static', 'web.html')


@app.route('/upload', methods=['POST'])
def videoProcess():
    if 'videoFile' not in request.files or 'watermarkFile' not in request.files:
        return jsonify({'error': 'Lost file'}), 400

    if 'videoURL' not in request.form or 'watermarkURL' not in request.form:
        return jsonify({'error': 'Lost file'}), 400

    print(request.files)
    print(request.form)
    if request.files['videoFile'] != '':
        video = request.files['videoFile']
        image = request.files['watermarkFile']
    else:
        video = request.form['videoURL']
        image = request.form['watermarkURL']

    video_path = f"/tmp/{video.filename}"
    image_path = f"/tmp/{image.filename}"
    output_path = f"/tmp/watermarked_{video.filename}"

    video.save(video_path)
    image.save(image_path)

    try:
        logo = Image.open(image_path)
        fps = decomposeFrame(video_path)
        size = (500, 100)
        logo.thumbnail(size)
        addWatermark(logo)
        createVideo(output_path, fps)

        # bucket_name = 'your-bucket-name'
        # destination_blob_name = f"processed/{os.path.basename(output_path)}"
        # public_url = storeWatermarkedFile(output_path, bucket_name, destination_blob_name)
        # subprocess.run(['rm', '-rf', '/tmp/frames'], check=True)
        # subprocess.run(['rm', '-f', video_path], check=True)
        # subprocess.run(['rm', '-f', output_path], check=True)
        # subprocess.run(['rm', '-f', image_path], check=True)


        return jsonify({
            # "url": public_url,
            "message": "Video processed and uploaded successfully"
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500



if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))