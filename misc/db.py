# -*- coding: utf-8 -*-
"""
Created on Thu May 10 15:34:14 2018

@author: Vanja Smailovic
"""

from simple_settings import settings

from pymongo import MongoClient
import logging as log

class MongoDB_helper:
    """
    Implemented using MongoDB 3.6
    """

    def __init__(self, username=None, password=None, uri=None, database_name=None, collection_name=None):
        """

        :param username:
        :param password:
        :param uri:
        :param database_name:
        :param collection_name:
        """
        self.username = username
        self.password = password
        self.mongo_uri = uri
        self.db_name = database_name
        self.collection_name = collection_name

        # TODO: Clean this up with some nice looking validation...

        # If a settings is not provided, fall back to simple-settings. Only allow user and pass to be none
        if self.username is None:
            try:
                self.username = settings.MONGODB_USER
            except:
                print("No User configured")
        if self.password is None:
            try:
                self.password = settings.MONGODB_PASSWORD
            except:
                print("No Password configured")
        if self.mongo_uri is None:
            self.mongo_uri = settings.MONGODB_URI
        if self.db_name is None:
            self.db_name = settings.MONGODB_DB_NAME
        if self.collection_name is None:
            self.collection_name = settings.MONGODB_COLLECTION_NAME

        if self.username is None and self.password is None:
            connection_uri = "mongodb://{0}".format(self.mongo_uri)
        else:
            connection_uri = "mongodb://{0}:{1}@{2}".format(self.username, self.password, self.mongo_uri)

        log.info("Initial connecting to mongodb @ "+connection_uri + " targeting collection: " + self.collection_name)

        self.client = MongoClient(connection_uri)
        self.db = self.client[self.db_name]
        self._col = self.db[self.collection_name]

        
    @classmethod
    def helper_from_file(cls, sett_module, col=None):
        """
        Parse settings from module. 
        """
        username = sett_module.MONGODB_USER
        password = sett_module.MONGODB_PASSWORD
        uri = sett_module.MONGODB_URI
        db_name = sett_module.MONGODB_DB_NAME
        if col:
            collection_name = col
        else:
            collection_name = sett_module.MONGODB_COLLECTION_NAME
        
        return cls(username, password, uri, db_name, collection_name)
    
    @property
    def col(self):
        return self._col
        
    def close(self):
        self.client.close()
        
        
    # MongoDB 3.6
    def query_all(self):
        results = self._col.find()
        return results
    
    # MongoDB 3.6
    def get_owners_urls_db(self) -> list:
        out =  [ ]
        pipeline = [
            {'$unwind': '$urls'},
            {'$match': {
                'urls.active': 'yes',
            }},
            {'$project': {
                'owner': 1,        # string
                'url': '$urls.url' # string  
            }}
        ]
        results = self._col.aggregate(pipeline=pipeline)
        for r in results:
            tupl = (r.get('owner'), r.get('url'))
            out.append(tupl)
        return out
    
    # MongoDB 3.6
    def get_emails_db(self, owner) -> list:
        emails = [ ]
        filt = {
            'owner': owner,
        }
        proj = {
            'owner': 1,
            'email': 1
        }
        results = self._col.find(filter = filt, projection = proj)   
        for r in results:
            for email in r.get('email'):
                emails.append(email) 
        return emails
    
    # MongoDB 3.6
    def get_owners_db(self):
        results = self._col.distinct('owner')
        return results   # list 

    # MongoDB 3.6
    def get_unsent_emails_db(self, owner):
        filt = {
            'owner': owner,
            'ad_url': {'$gt': '' },   # avoid $size as it doesn't use indexes 
            'email_sent': 0
        }
        results = self._col.find(filter = filt)
        return results

    # MongoDB 3.6
    def find_one_db(self, owner, url):
        filt = {
            'owner': owner,
            'ad_url': url,
        }
        results = self._col.find_one(filter = filt)
        return results

    # MongoDB 3.6
    # manipulate in order to prepare for updating
    def find_one_and_up(self, up, email_sent=0):
        ad_url = up.get('ad_url')
        owner = up.get('owner')
        if email_sent:
            # from email stage
            up['email_sent'] = email_sent
            id = up.get('_id')
            rest = {'email_sent': 1}
            filt = {'_id': id}
        else: 
            # from scraper stage
            rest = { k:up[k] for k in up.keys() }
            filt = {'owner': owner, 'ad_url': ad_url, 'email_sent': email_sent}
        
        update = {'$set': rest}
        self._col.find_one_and_update(filt, update, upsert=True) 
    
