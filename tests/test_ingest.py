# Standard Python Library Imports
import unittest

# Local imports
from config.keys import QUANDL_KEY
from config.data_settings import *
from ingest.utils import (
    get_remote_data,
    )

class TestIngest(unittest.TestCase):

    df = get_remote_data('GOLD_D')

    def test_get_remote_data(self):
        self.assertIsNotNone(self.df)

    def test_temporary_csv_directory(self):
        path = 
        self.assertTrue(os.path.isfile(path))    
    
    