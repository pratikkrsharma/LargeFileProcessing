import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from util import utils
from configparser import ConfigParser

config = ConfigParser()
config.read('config.ini')


class LargeFileIngestion():
    """
    Class for large file processing and ingestion into mongodb
    """


    def __init__(self, path='data/products.csv.gz'):
        """ 
        Constructor for class when object is created 
        """
        # Loading product reviews input
        self.products_df = pd.read_csv(path)
        self.utils = utils.Utils()
        
    
    def create_collections_data(self):
        """ 
        Preprocesses and creates 3 collection data to be inserted into mongodb
        
        Returns:
        aggregated_col_df: pd.DataFrame()
            has name and no_of_products data     
        """
        # modifying reviews collection data which will contain: name, sku_id, description
        self.products_df['sku_id'] = self.products_df['sku'].apply(self.utils.generate_hash_value)
        self.products_df = self.products_df[['name', 'sku_id', 'sku', 'description']]
        # creating aggregated collection data which will contain: name, no_of_products
        aggregated_df = self.products_df.groupby(['name', 'sku_id']).size().reset_index(name='no_of_reviews')
        aggregated_df = aggregated_df.groupby(['name']).size().reset_index(name='no_of_products')
        # return
        return aggregated_df
    
    
    def parallel_insert(self, collection, data):
        collection.insert_many(data)
    
    
    def insert_into_mongo(self, collection, data):
        """
        Insert the given collection data into respective colection
        Making use of ThreadPoolExecutor by concurrent.futures for parallel insertion
        Inserted 500,000 rows within 1 min 40 secs with 4 cores/20 threads parallel
        """
        chunked_list = self.utils.get_chunked_list(data, 5000)
        with ThreadPoolExecutor() as executor:
            fn = partial(self.parallel_insert, collection)
            final_list = executor.map(fn, chunked_list)
            executor.shutdown()
        
    
    def ingest_all_data(self):
        """  
        Inserts preprocessed file data to respective mongodb collection
        """
        try:
            # getting all data
            aggregated_df = self.create_collections_data()
            products_df = self.products_df
            # mongo db client object
            db = self.utils.get_mongo_db_details()
            start = datetime.now()
            # Insert 3 collection datas
            self.insert_into_mongo(db[config['mongo-details']['products_col']], products_df.to_dict(orient='records'))
            self.insert_into_mongo(db[config['mongo-details']['aggregated_col']], aggregated_df.to_dict(orient='records'))
            return (True, str(datetime.now() - start))
        except Exception as exp:
            raise str(exp)
