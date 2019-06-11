# pubsub_example.py
from google.cloud import pubsub
from google.cloud import monitoring
from config import data_settings
import time

PROJECT = 'vanaurum'
TOPIC = 'vanaurum-etl-queue'
SUBSCRIPTION = 'vanaurum-etl-queue-sub'


# This is a dirty hack since Pub/Sub doesn't expose a method for determining
# if the queue is empty (to my knowledge). We have to use the metrics API which
# is only updated every minute. Hopefully someone from Google can clarify!
def queue_empty(client):
    result = client.query(
        'pubsub.googleapis.com/subscription/num_undelivered_messages',
        minutes=1).as_dataframe()
    return result['pubsub_subscription'][PROJECT][SUBSCRIPTION][0] == 0


def print_message(message):
    print(message.data)
    message.ack()


def pub_callback(message_future):
    # When timeout is unspecified, the exception method waits indefinitely.
    topic = 'projects/{}/topics/{}'.format(PROJECT, TOPIC)
    if message_future.exception(timeout=30):
        print('Publishing message on {} threw an Exception {}.'.format(
            topic, message_future.exception()))
    else:
        print(message_future.result())

def sub_callback(message):
    print('Received message: {}'.format(message))
    message.ack()


def main():
    client = monitoring.MetricServiceClient()

    # Publishes the message 'Hello World'
    publisher = pubsub.PublisherClient()
    topic = 'projects/{}/topics/{}'.format(PROJECT, TOPIC)
    for asset in data_settings.ASSET_LIST:
        asset = asset.encode('utf-8')
        message_future = publisher.publish(topic, data=asset)
        message_future.add_done_callback(pub_callback)

    # Opens a connection to the message queue asynchronously
    subscriber = pubsub.SubscriberClient()
    subscription_path = 'projects/{}/subscriptions/{}'.format(PROJECT, SUBSCRIPTION)
    future = subscriber.subscribe(subscription_path, callback = sub_callback)
    print(future.result)

    # Waits until the queue is empty to exit. See queue_empty for more
    # explanation.
    time.sleep(5)
    while not queue_empty(client):
        pass
    subscription.close()


if __name__ == '__main__':
    main()