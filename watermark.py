import cv2
import os
from PIL import Image
import matplotlib.pyplot as plt

def decomposeFrame(video, video_name):
    vidcap = cv2.VideoCapture(video)
    if not os.path.exists(video_name):
        os.makedirs(video_name)
    fps = vidcap.get(cv2.CAP_PROP_FPS)
    if vidcap.isOpened():
        success, frame = vidcap.read()
        count = 0
        while success:
            success, frame = vidcap.read()
            if not success:
                break
            os.chdir('./' + video_name)
            cv2.imwrite("frame%s.jpg" % str(count).zfill(4), frame)
            os.chdir('../')
            count += 1
        vidcap.release()
    cv2.destroyAllWindows()
    return fps


def addWatermark(logo, img_name):
    img = Image.open('./' + video_name + '/' + img_name)
    width, height = img.size
    transparent = Image.new('RGBA', (width, height), (0,0,0,0))
    transparent.paste(img, (0,0))
    transparent.paste(logo, (0,0), mask=logo)
    if not os.path.exists('watermark'):
        os.makedirs('watermark')
    transparent.convert('RGB').save('./watermark/' + img_name)


def createVideo(video, video_name, fps):
    new_video = video_name + "_watermark.MP4"
    image_folder = './watermark'
    images = [img for img in os.listdir(image_folder)]
    frame = cv2.imread(os.path.join(image_folder, images[0]))
    height, width, layers = frame.shape
    fourcc = cv2.VideoWriter_fourcc(*'mp4v')
    video = cv2.VideoWriter(new_video, fourcc, fps, (width, height))
    images.sort()

    for image in images:
        video.write(cv2.imread(os.path.join(image_folder, image)))

    cv2.destroyAllWindows()
    video.release()


if __name__== "__main__" :
    video = 'fatfat.MP4'
    logo = Image.open("Logo.png")
    video_name = video.split(".")[0]
    fps = decomposeFrame(video, video_name)
    size = (500, 100)
    logo.thumbnail(size)
    for img_name in os.listdir('./' + video_name):
        addWatermark(logo, img_name)
    createVideo(video, video_name, fps)