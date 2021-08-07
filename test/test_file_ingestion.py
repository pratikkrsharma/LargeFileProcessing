import unittest
from service.file_ingestion import LargeFileIngestion
from util.utils import Utils


class TestLargeFileIngestion(unittest.TestCase):
    """
    Test class for large file ingestion
    """


    def test_init(self):
        """
        Test the constructor
        """
        lfi = LargeFileIngestion(path='data/test_products.csv')
        products_df = lfi.products_df
        self.assertEqual(len(products_df), 500)
        self.assertListEqual(products_df.columns.to_list(), ['name', 'sku', 'description'])
        self.assertIsInstance(lfi.utils, Utils)


    def test_create_collections_data(self):
        """
        Tests creation of collection data
        """
        lfi = LargeFileIngestion(path='data/test_products.csv')
        aggregated_df = lfi.create_collections_data()
        products_df = lfi.products_df
        self.assertEqual(len(products_df), 500)
        self.assertListEqual(products_df.columns.to_list(), ['name', 'sku_id', 'sku', 'description'])
        self.assertListEqual(aggregated_df.columns.to_list(), ['name', 'no_of_products'])
        self.assertEqual(aggregated_df['no_of_products'].sum(), 500)

    
    def test_ingest_all_data(self):
        lfi = LargeFileIngestion(path='data/test_products.csv')
        lfi.products_df = lfi.products_df.sample(n=5)
        status = lfi.ingest_all_data()
        self.assertEqual(status, True)
