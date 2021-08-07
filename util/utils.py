import hashlib
import urllib
from pymongo import MongoClient
from configparser import ConfigParser

config = ConfigParser()
config.read('config.ini')


class Utils():
    """ Utils class - contains helper functions """

    def generate_hash_value(self, row):
        """
        Returns hash value of the given sting of length 20
        """
        return hashlib.shake_256(row.encode()).hexdigest(10)


    def get_mongo_db_details(self):
        """
        Returns the mongodb client object
        """
        try:
            # get mongo details
            host = config['mongo-details']['host']
            username = config['mongo-details']['username']
            password = config['mongo-details']['password']
            password = urllib.parse.quote(password)
            if config['mongo-details']['mongo_srv'] == 'True':
                mongo_uri = f'mongodb+srv://{username}:{password}@{host}'
            else:
                mongo_uri = f'mongodb://{username}:{password}@{host}'
            # Creating a client
            client = MongoClient(mongo_uri, ssl=True, tlsAllowInvalidCertificates=True)
            # Greating a database name GFG
            db = client[config['mongo-details']['db']]
            return db
        except Exception:
            return "Error in connection to mongodb"


    def get_chunked_list(self, initialList, chunkSize):
        """
        Returns list of small lists from a big list  for parallel insertion
        """
        finalList = []
        for i in range(0, len(initialList), chunkSize):
            finalList.append(initialList[i:i+chunkSize])
        return finalList
