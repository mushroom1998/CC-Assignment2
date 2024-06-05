文件说明：\
static: 前端页面代码\
fatfat.mp4 zhuang.MOV：测试视频，长度分别为3s和12s\
Logo.png：测试水印\
decompose.py, process.py, combine.py: 后端代码，通过Pub/Sub messaging通信\
decompose.py: 读取前端输入，如果输入为文件则上传GCS，传给process.py URL； 读取token返回任务进度
process.py: GKE上创建4个集群副本并行执行，下载视频和水印，处理对应视频片段，并把加好水印的视频片段上传GCS
combine.py: 下载视频片段，合并后上传GCS

SQL结构：(NoSQL)
"id": "unique-task-id",\
"status": "decomposing/processing 50%/merging/finish",\
"update_time": "last-updated-time"

k8s部署命令：\
创建仓库\
gcloud artifacts repositories create watermark-repo --project=thinking-banner-421414 --repository-format=docker --location=us-central1 --description="Docker repository"
\
创建本地身份验证凭据\
gcloud auth application-default login
\
创建容器镜像\
gcloud builds submit --tag us-central1-docker.pkg.dev/thinking-banner-421414/watermark-repo/process .
\
删除容器镜像\
gcloud artifacts docker images delete us-central1-docker.pkg.dev/thinking-banner-421414/watermark-repo/process
\
创建集群\
\
gcloud container clusters create watermark-gke --zone=asia-east1-b --workload-pool=thinking-banner-421414.svc.id.goog --machine-type=e2-small --disk-type=pd-standard --disk-size=50GB
\
删除集群\
gcloud container clusters delete watermark-gke --region asia-east1-b
\
修改节点池\
gcloud container node-pools update default-pool \
--cluster=watermark-gke \
--region=asia-east1-b \
--workload-metadata=GKE_METADATA
\
集群log\
kubectl get events --sort-by=.metadata.creationTimestamp_
\
获取集群凭据\
gcloud container clusters get-credentials watermark-gke --region=asia-east1-b
\
创建service account kubectl create serviceaccount watermark-sa --namespace default
\
增加数据库权限\
gcloud projects add-iam-policy-binding projects/thinking-banner-421414 \
--role=roles/pubsub.editor \
--member=principal://iam.googleapis.com/projects/451938362682/locations/global/workloadIdentityPools/thinking-banner-421414.svc.id.goog/subject/ns/default/sa/watermark-sa \
--condition=None
\
增加pubSub通信权限\
gcloud projects add-iam-policy-binding projects/thinking-banner-421414 \
--role=roles/datastore.user \
--member=principal://iam.googleapis.com/projects/451938362682/locations/global/workloadIdentityPools/thinking-banner-421414.svc.id.goog/subject/ns/default/sa/watermark-sa \
--condition=None
\
增加云存储通信权限\
gcloud projects add-iam-policy-binding projects/thinking-banner-421414 \
--role=roles/storage.objectUser \
--member=principal://iam.googleapis.com/projects/451938362682/locations/global/workloadIdentityPools/thinking-banner-421414.svc.id.goog/subject/ns/default/sa/watermark-sa \
--condition=None
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