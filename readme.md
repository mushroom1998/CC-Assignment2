前端需求：

1.可以直接上传一个视频和一个水印图片，存储到谷歌云上，触发服务器加水印操作，并返回给用户一个任务identifier

2.可以上传一个存在谷歌云的视频的url和一个存在谷歌云的水印图片的url，通过url读取到视频和水印给服务器加水印，并返回给用户一个任务identifier

3.输入一个任务的identifier，可以返回任务的状态

...


文件说明：

upload文件夹：拿来测试当前php代码上传文件功能，目前实现的是上传视频和图片以后，资源会存进upload里

fatfat.mp4：测试视频

Logo.png：测试水印

upload_file.php：前端上传水印图片后，获取图片并存进upload的代码

upload_video.php：前端上传视频后，获取视频并存进upload的代码

watermark.py：把视频分帧，每一帧加上水印，在合并为完整视频的脚本

web.php：前端页面代码


php运行：
需要先安装，到项目路径下，在命令行输入php -S localhost:8000，访问网页http://localhost:8000/web.php