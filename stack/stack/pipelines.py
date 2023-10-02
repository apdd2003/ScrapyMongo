from itemadapter import ItemAdapter
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv
import os
from scrapy import settings
from scrapy.exceptions import DropItem
# from scrapy import log

load_dotenv()
uri =os.getenv('MONGO_SERVER')

# from scrapy.conf import settings


class MongoDBPipeline(object):

    def __init__(self):
        connection = MongoClient(uri, server_api=ServerApi('1')        )
        self.db = connection.stackoverflow # 'stackoverflow' is existing DB in Mongo server

    def process_item(self, item, spider):
        valid = True
        for data in item:
            if not data:
                valid = False
                raise DropItem("Missing {0}!".format(data))
        if valid:
            self.db.questions.insert_one(dict(item)) # 'questions' is existing collection in the 'stackoverflow' DB
        return item