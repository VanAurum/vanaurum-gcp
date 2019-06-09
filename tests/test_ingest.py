# Standard Python Library Imports
import unittest

# Local imports
from config.keys import QUANDL_KEY
from config.data_settings import *
from ingest.utils import (get_remote_data)

class TestIngest(unittest.TestCase):

    def test_get_remote_data(self):
        self.assertIsNotNone(get_remote_data('GOLD_D'))
    
    