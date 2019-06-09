# Standard Python Library Imports
import unittest
import os
import datetime

# Local imports
from config.keys import QUANDL_KEY
from config.data_settings import *
from ingest.utils import get_remote_data
from helpers.gcp_utils import (
    df_to_temp_csv,
    _safe_filename,
    _check_extension,
    _get_storage_client,
    upload_file
    )

class TestHelpers(unittest.TestCase):

    df = get_remote_data('GOLD_D')

    def test_upload_file(self):
        path = df_to_temp_csv(self.df, 'GOLD_D.csv')
        url = upload_file(path, 'GOLD_D.csv')
        self.assertIsNotNone(url)
        print(url)

        
    
    