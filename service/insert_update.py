import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from util import utils
from configparser import ConfigParser

config = ConfigParser()
config.read('config.ini')


class InsertUpdateDetails():
    """
    Class for inserting and updating product description
    """
        
        
    def __init__(self):
        """ 
        Constructor for class when object is created 
        """
        self.utils = utils.Utils()
       
    
    def insert_update_data(self, new_data):
        """ Insert or update given data """
        db = self.utils.get_mongo_db_details()
        with ThreadPoolExecutor() as executor:
            fn = partial(self.parallel_insert_update, db)
            final_list = executor.map(fn, new_data)
            executor.shutdown()
        final_list = list(final_list)
        return {'Total Success': final_list.count(True), 'Total Failed': final_list.count(False)}
        
    
    def parallel_insert_update(self, db, new_data):
        """ Called by insert_update_data to run parallely """
        try:
            # generating sku_id for new data
            new_data['sku_id'] = self.utils.generate_hash_value(new_data['sku'])
            new_data = pd.DataFrame(new_data, index=[0])
            new_data = new_data[['name', 'sku_id', 'sku', 'description']]
            new_data = new_data.to_dict(orient='records')[0]
            # insert or update products collection data
            products_collection = db[config['mongo-details']['products_col']]
            check = self.insert_update_products_col(products_collection, new_data)
            # insert or update aggregated collection data
            agg_collection = db[config['mongo-details']['aggregated_col']]
            self.insert_update_aggregated_col(agg_collection, new_data, check)
            return True
        except Exception as exp:
            print(str(exp))
            return False

    
    def insert_update_products_col(self, products_collection, new_data):
        """ Insert or update products collection data """
        # insert or update in products table
        check = False
        previous_df = pd.DataFrame(list(products_collection.find({'name': {'$eq': new_data['name']}}, {'_id': 1, 'name': 1, 'sku_id': 1})))
        if previous_df.shape[1] != 0:
            existing_df = previous_df[previous_df['sku_id'] == new_data['sku_id']]
            if len(existing_df) > 0:
                products_collection.update_one({'_id': existing_df['_id'].iloc[0]}, {"$set": {'description': new_data['description']}})
            else:
                products_collection.insert_one(new_data)
                check = True
        else:
            products_collection.insert_one(new_data)
            check = True
        return check
        
        
    def insert_update_aggregated_col(self, agg_collection, new_data, check):
        """ Insert or update aggregated collection data """
        previous_agg_df = pd.DataFrame(list(agg_collection.find({'name': {'$eq': new_data['name']}}, {'_id': 1, 'no_of_products': 1})))
        if check is True:
            if previous_agg_df.shape[1] != 0:
                agg_collection.update_one({'_id': previous_agg_df['_id'].iloc[0]}, {"$set": {'no_of_products': int(previous_agg_df['no_of_products'].iloc[0]) + 1}})
            else:
                agg_collection.insert_one({'name': new_data['name'], 'no_of_products': 1})
                