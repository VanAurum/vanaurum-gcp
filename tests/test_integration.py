# Standard Python Library Imports
import unittest
import os
import datetime

# Local imports
from config.keys import QUANDL_KEY
from config.data_settings import *
from ingest.utils import get_remote_data, map_data
from helpers.gcp_utils import (
    df_to_temp_csv,
    _safe_filename,
    _check_extension,
    _get_storage_client,
    upload_file
    )

class TestHelpers(unittest.TestCase):

    def test_upload_file(self):
        test_asset = 'GOLD_D'
        df = get_remote_data(test_asset)
        df = map_data(df, test_asset)
        path = df_to_temp_csv(df, test_asset+'.csv')
        url = upload_file(path, test_asset+'.csv')
        self.assertIsNotNone(url)
        print(url)

        
    
    