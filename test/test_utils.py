import unittest
import pymongo
from util.utils import Utils


class TestUtils(unittest.TestCase):
    """
    Test class for Utils
    """


    def test_generate_hash_value(self):
        """
        Testing hash value generated
        """
        ut = Utils()
        hash_value = ut.generate_hash_value('pratik sharma')
        self.assertEqual(hash_value, 'b47f7b8e9d1ac41f703e')


    def test_get_mongo_db_details(self):
        """
        Test instance of mongo db
        """
        ut = Utils()
        db = ut.get_mongo_db_details()
        self.assertIsInstance(db, pymongo.database.Database)

    
    def test_get_chunked_list(self):
        """
        Testing chunking of link into multiple lists
        """
        ut = Utils()
        chunked_list = ut.get_chunked_list([1, 2, 3, 4, 5], 3)
        self.assertListEqual(chunked_list, [[1, 2, 3], [4, 5]])
