import unittest
import random, string
from service.insert_update import InsertUpdateDetails
from util.utils import Utils


class TestInsertUpdateDetails(unittest.TestCase):
    """
    Test class for insert annd update
    """


    def test_init(self):
        """
        Test the constructor
        """
        iud = InsertUpdateDetails()
        self.assertIsInstance(iud.utils, Utils)


    def test_insert_update_data(self):
        """
        Test insert update into mongo
        """
        new_data = [{"name": ''.join(random.choice(string.ascii_lowercase) for i in range(10)),
                    "sku": ''.join(random.choice(string.ascii_lowercase) for i in range(15)),
                    "description": "New Art community floor adult your single type. Per\
                     back community former stock thing."}]
        iud = InsertUpdateDetails()
        result = iud.insert_update_data(new_data)
        self.assertEqual(result, {'Total Success': 1, 'Total Failed': 0})
