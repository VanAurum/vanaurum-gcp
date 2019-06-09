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

    def test_temporary_csv_directory(self):
        path = df_to_temp_csv(self.df, 'test.csv')
        print(path)
        self.assertTrue(os.path.isfile(path))
        os.remove(path)

    def test_safe_filename_utility(self):
        filename = 'kevin.csv'
        date = datetime.datetime.utcnow().strftime("%Y-%m-%d")
        safe = _safe_filename(filename)
        self.assertIn(date, safe)


    def test_upload_file(self):
        
    
    