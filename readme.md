前端需求：\
1.可以直接上传一个视频和一个水印图片，触发加水印操作，并返回给用户一个任务identifier\
2.可以上传一个存在谷歌云的视频的url和一个存在谷歌云的水印图片的url，通过url读取到视频和水印，触发服务器加水印，并返回给用户一个任务identifier\
3.输入一个任务的identifier，可以返回任务的状态\
...

文件说明：\
static: 前端页面代码\
fatfat.mp4：测试视频\
Logo.png：测试水印\
decompose.py, process.py, combine.py: 后端代码，通过Pub/Sub messaging通信\
decompose.py: 读取前端输入并处理，视频分帧
process.py: 给每一帧加水印
combine.py: 合并加水印后的帧并上传google cloud storage

SQL结构：(NoSQL)
"id": "unique-task-id",\
"video_path": "/tmp/video.mp4",\
"image_path": "/tmp/image.png",\
"output_path": "/tmp/watermarked_video.mp4",\
"status": "decomposing/processing 50%/merging/finish",\
"download_url": "NULL/cloud-storage-url",\
"update_time": "last-updated-timestamp"

k8s部署命令：\
创建仓库\
gcloud artifacts repositories create watermark-repo \
--project=thinking-banner-421414 \
--repository-format=docker \
--location=us-central1 \
--description="Docker repository"
\
创建容器镜像\
gcloud builds submit \
--tag us-central1-docker.pkg.dev/thinking-banner-421414/watermark-repo/watermark-gke .
\
删除容器镜像\
gcloud artifacts docker images delete \
us-central1-docker.pkg.dev/thinking-banner-421414/watermark-repo/watermark-gke
\
创建集群\
gcloud container clusters create-auto watermark-gke \
--location us-central1
\
删除集群\
gcloud container clusters delete watermark-gke \
--location us-central1
\
列出集群中的节点\
kubectl get nodes
\
把资源部署到集群\
kubectl apply -f deployment.yaml
\
跟踪 Deployment 的状态\
kubectl get deployments
\
查看pods\
kubectl get pods
\
出问题的时候查看pods log\
kubectl logs _POD_NAME_
\
创建service\
kubectl apply -f service.yaml
\
获取IP\
kubectl get services
\
进入容器命令行\
kubectl exec -it <pod-name> -- /bin/bash