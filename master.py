# master.py
from google.cloud import pubsub
from config import data_settings

PROJECT = 'vanaurum'
TOPIC = 'vanaurum-etl-queue'


def pub_callback(message_future):
    # When timeout is unspecified, the exception method waits indefinitely.
    topic = 'projects/{}/topics/{}'.format(PROJECT, TOPIC)
    if message_future.exception(timeout=30):
        print('Publishing message on {} threw an Exception {}.'.format(
            topic, message_future.exception()))
    else:
        print(message_future.result())

def main():

    # Publishes the message 'Hello World'
    publisher = pubsub.PublisherClient()
    topic = 'projects/{}/topics/{}'.format(PROJECT, TOPIC)
    for asset in data_settings.ASSET_LIST:
        asset = asset.encode('utf-8')
        message_future = publisher.publish(topic, data=asset)
        message_future.add_done_callback(pub_callback)

if __name__ == '__main__':
    main()