import json
import urllib
from pymongo import MongoClient
from pymongo.errors import CollectionInvalid
from collections import OrderedDict
from configparser import ConfigParser

config = ConfigParser()
config.read('config.ini')


def create_collection_with_schema(mongo_uri, collection, index):
    """
    Create given collection with its schema
    """
    if collection == 'products':
        with open('schema/products_schema.json', 'r') as f:
            schema = json.load(f)
    else:
        with open('schema/aggregated_schema.json', 'r') as f:
            schema = json.load(f)
    
    client = MongoClient(mongo_uri, ssl=True, tlsAllowInvalidCertificates=True)
    db = client[config['mongo-details']['db']]

    validator = {'$jsonSchema': {'bsonType': 'object', 'properties': {}}}
    required = []

    for field_key in schema:
        field = schema[field_key]
        properties = {'bsonType': field['type']}
        minimum = field.get('minlength')

        if type(minimum) == int:
            properties['minimum'] = minimum

        if field.get('required') is True:
            required.append(field_key)
            
        validator['$jsonSchema']['properties'][field_key] = properties

    if len(required) > 0:
        validator['$jsonSchema']['required'] = required

    query = [('collMod', collection),
            ('validator', validator)]

    try:
        if collection not in db.list_collection_names():
            db.create_collection(collection)
            db[collection].create_index(index)
            db.command(OrderedDict(query))
    except CollectionInvalid:
        pass


def create_db_with_schemas():
    # get mongo details
    host = config['mongo-details']['host']
    username = config['mongo-details']['username']
    password = config['mongo-details']['password']
    password = urllib.parse.quote(password)
    if config['mongo-details']['mongo_srv'] == 'True':
        mongo_uri = f'mongodb+srv://{username}:{password}@{host}'
    else:
        mongo_uri = f'mongodb://{username}:{password}@{host}'
    # create prodcuts collection
    products_collection = config['mongo-details']['products_col']
    index = 'name'
    create_collection_with_schema(mongo_uri, products_collection, index)
    # create aggregated colection
    aggregated_collection = config['mongo-details']['aggregated_col']
    index = 'name'
    create_collection_with_schema(mongo_uri, aggregated_collection, index)


if __name__ == '__main__':
    create_db_with_schemas()
