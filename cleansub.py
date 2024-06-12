from kubernetes import client, config
from google.cloud import pubsub_v1, datastore, storage

project_id = "thinking-banner-421414"
decompose_topic_id = "decompose-video"
decompose_subscription_paths = []
process_subscription_id = "process-video-sub"

publisher = pubsub_v1.PublisherClient()
subscriber = pubsub_v1.SubscriberClient()
decompose_topic_path = publisher.topic_path(project_id, decompose_topic_id)
process_subscription_path = subscriber.subscription_path(project_id, process_subscription_id)


def callback(message):
    print(f"Dropping message: {message.data}")
    message.ack()


def clearSubscription(subscription_path):
    future = subscriber.subscribe(subscription_path, callback)
    try:
        future.result(timeout=10)
    except Exception as e:
        future.cancel()
        print(f"Subscription cleared: {subscription_path}")


def getPodCount():
    '''Get the total number of pods in GKE'''
    try:
        config.load_kube_config()
    except:
        return 1
    v1 = client.CoreV1Api()
    pods = v1.list_namespaced_pod(namespace='default')
    return len(pods.items)


pod_num = getPodCount()
for i in range(pod_num):
    decompose_subscription_path = subscriber.subscription_path(project_id, "decompose-video-sub-" + str(i))
    clearSubscription(decompose_subscription_path)
clearSubscription(process_subscription_path)