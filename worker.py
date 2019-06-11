# worker.py
import logging
from google.cloud import pubsub
from google.cloud import monitoring
import subprocess as sp
import time
from config.data_settings import *
from config import log_settings
from ingest.utils import get_remote_data, map_data
from helpers.gcp_utils import (
    df_to_temp_csv,
    _safe_filename,
    _check_extension,
    _get_storage_client,
    upload_file
    )

# Initialize logger
logging.basicConfig(
    level=log_settings.LOG_LEVEL,
    format=log_settings.LOG_FORMAT,
    datefmt=log_settings.LOG_DATE_FORMAT,
    )
log = logging.getLogger(__name__)


PROJECT = 'vanaurum'
TOPIC = 'vanaurum-etl-queue'
SUBSCRIPTION = 'vanaurum-etl-queue-sub'
BUCKET = 'vanaurum-blob-data'



def update_asset(asset):
    df = get_remote_data(asset)
    df = map_data(df, asset)
    path = df_to_temp_csv(df, asset+'.csv')
    upload_file(path, asset+'.csv')
    return


def queue_empty(client):
    result = client.query(
        'pubsub.googleapis.com/subscription/num_undelivered_messages',
        minutes=1).as_dataframe()
    return result['pubsub_subscription'][PROJECT][SUBSCRIPTION][0] == 0


def handle_message(message):
    asset = message.data
    asset = asset.decode('utf-8')
    log.debug('worker is processing '+asset)
    try:
        update_asset(asset)
        message.ack()
        log.debug('message was acknowledged for '+asset)
    except KeyboardInterrupt:
        message.nack()    


def main():
    client = monitoring.MetricServiceClient()

    # Opens a connection to the message queue asynchronously
    subscriber = pubsub.SubscriberClient()
    subscription_path = 'projects/{}/subscriptions/{}'.format(PROJECT, SUBSCRIPTION)
    future = subscriber.subscribe(subscription_path, callback = handle_message)

    time.sleep(5)


if __name__ == '__main__':
    main()