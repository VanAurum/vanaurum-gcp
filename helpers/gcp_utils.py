# Standard Python Library Imports
import datetime
import os 
import tempfile
import pandas as pd

# Local Imports
from config import keys

#3rd Party Imports
import six
from google.cloud import storage
from werkzeug import secure_filename
from werkzeug.exceptions import BadRequest

def _get_storage_client():
    return storage.Client(
        project=keys.PROJECT_ID)


def _check_extension(filename, allowed_extensions):
    if ('.' not in filename or
            filename.split('.').pop().lower() not in allowed_extensions):
        raise BadRequest(
            "{0} has an invalid name or extension".format(filename))


def _safe_filename(filename):
    """
    Generates a safe filename that is unlikely to collide with existing objects
    in Google Cloud Storage.
    ``filename.ext`` is transformed into ``filename-YYYY-MM-DD-HHMMSS.ext``
    """
    filename = secure_filename(filename)
    date = datetime.datetime.utcnow().strftime("%Y-%m-%d-%H%M%S")
    basename, extension = filename.rsplit('.', 1)
    return "{0}-{1}.{2}".format(basename, date, extension)

def df_to_temp_csv(dataframe, filename):
    '''return temp path to csv
    '''
    directory_name = tempfile.mkdtemp()
    dataframe.to_csv(directory_name+'/'+filename)
    return directory_name+'/'+filename


def upload_file(path_to_file, filename, safe_filename=False):
    """
    Uploads a file to a given Cloud Storage bucket and returns the public url
    to the new object.
    """
    _check_extension(filename, keys.ALLOWED_EXTENSIONS)

    if safe_filename:
        filename = _safe_filename(filename)

    client = _get_storage_client()
    bucket = client.bucket(keys.CLOUD_STORAGE_BUCKET)
    blob = bucket.blob(filename)

    blob.upload_from_filename(
        path_to_file)

    url = blob.public_url

    if isinstance(url, six.binary_type):
        url = url.decode('utf-8')

    return url