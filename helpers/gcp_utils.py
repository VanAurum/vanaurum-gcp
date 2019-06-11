# Standard Python Library Imports
import datetime
import os 
import tempfile
import pandas as pd
import logging

# Local Imports
from config import keys

#3rd Party Imports
import six
from google.cloud import storage
from werkzeug import secure_filename
from werkzeug.exceptions import BadRequest


# Initialize logger
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%m/%d/%Y %I:%M:%S %p')
log = logging.getLogger(__name__)


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


def clean_dataframe(df, tag):

    df.columns = [x.upper() for x in df.columns]
    columns = list(df)

    if('Date' not in columns and 'DATE' not in columns and 'date'  not in columns):
        df.reset_index(level=0, inplace=True)
        df.columns = [x.upper() for x in df.columns]

    columns=list(df)
    if ('MID' in columns):
        #If cryptocurrency
        try:
            #If dataset has VOLUME
            col_list = ['DATE','HIGH','LOW','MID','LAST','VOLUME']
            df = df[col_list]
            df.columns = ['DATE','HIGH','LOW','MID','CLOSE','VOLUME']
        except:
            #If it doesn't have VOLUME
            col_list = ['DATE','HIGH','LOW','MID','LAST']
            df = df[col_list]
            df.columns = ['DATE','HIGH','LOW','MID','CLOSE']   

    elif ('SETTLE' in columns):
        #If data from SCF
        try:
            #If dataset has VOLUME
            col_list = ['DATE','OPEN','HIGH','LOW','SETTLE','VOLUME']
            df = df[col_list]
            df.columns = ['DATE','OPEN','HIGH','LOW','CLOSE','VOLUME']
        except:
            col_list = ['DATE','OPEN','HIGH','LOW','SETTLE']
            df = df[col_list]
            df.columns = ['DATE','OPEN','HIGH','LOW','CLOSE']    

    elif ('CLOSE' in columns):
        #If CLOSE is already specified.
        #Economic data points will be ingested as a dataframe with two columns. By checking if the length of the column list is greater
        #than 2 we screen for that.
        if (len(columns)>2):
            if ('ADJ_CLOSE' in columns):
                try:
                    col_list=['DATE','ADJ_OPEN','ADJ_HIGH','ADJ_LOW','ADJ_CLOSE','ADJ_VOLUME']
                    df=df[col_list]
                    df.columns=['DATE','OPEN','HIGH','LOW','CLOSE','VOLUME']
                except:        
                    pass
        else:
            pass

    else:
        log.error('Could not clean dataframe for '+tag+'.  Columns are misspecified or unhandled.')
        for elem in list(df):
            log.error(elem)
        return None

    df = df.sort_values('DATE')
    log.debug('Sorted the date column for '+tag)
    df = df.reset_index(drop=True)
    log.debug('Reset dataframe index for '+tag)    
    log.debug('Cleaned dataframe column names for '+tag)
    return df            
