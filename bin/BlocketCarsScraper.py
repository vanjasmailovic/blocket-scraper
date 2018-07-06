# -*- coding: utf-8 -*-
"""
Created on Thu May 10 15:34:14 2018

@author: Vanja Smailovic
"""

# import a local module from a different path 
# abysmal code due to lack of modularization support for Notebooks 
import os, sys
from os.path import dirname 
module_path = os.path.abspath(os.path.join('..'))
parent = dirname(module_path)
if module_path not in sys.path:
    sys.path.append(module_path)
if parent not in sys.path:
    sys.path.append(parent)

from time import monotonic as stopwatch
from time import time as now
from datetime import datetime as t
import scrapy, re, logging
from scrapy.crawler import CrawlerProcess
from scrapy.http import Request
from scrapy.utils.project import get_project_settings
from simple_settings import settings 
logger = logging.getLogger(__name__)

# Database helper
from misc.db import MongoDB_helper
db = MongoDB_helper(collection_name=settings.MONGODB_COLLECTION_CARS_NAME)
db_owners = MongoDB_helper(collection_name=settings.MONGODB_COLLECTION_OWNERS_NAME)

# Other helpers 
from misc.utils import *
from misc.utils_scraping import *
brand_allow = settings.CAR_BRAND_ALLOW

# Email 
import smtplib
from email.mime.text import MIMEText


# Scraper
class BlocketCarsSpider(scrapy.Spider):
    name = 'blocket_car_spider'
    custom_settings = {
        'COOKIES_ENABLED': False,
        'ITEM_PIPELINES' : {
            '__main__.MongoPipeline': 100, 
        }
    }
    
    def start_requests(self):
        jobs = self.settings['car_jobs'] 
        for owner, url in jobs: 
            print('start for', owner, '-', url)
            yield Request(url = url,
                          callback = self.parse, 
                          meta = {
                              'owner': owner
                          })
    
    def closed(self, reason):
        # called when the crawler process ends
        send_emails()
            
    def parse(self, response):
        page = parse_page(response.url)
        if not page:
            page = '1'  # first page has no parameter 
        owner = response.meta['owner']
        print('OWNER: ', owner)
        print('PAGE: ', page)
        
        # check if page available
        error = 'Just nu finns inga bostäder som matchar din sökning'
        if error in response.text:
            print('ERROR: Page {}.'.format(str(page)))
            return None
        
        locations = response.css("div header div.pull-left ::text").extract() 
        locations = transform_locations(locations)
        ad_urls = response.css(".ptxs ::attr(href)").extract() 
        ad_urls = [re.split('[?&#]', u)[0]  for u in ad_urls]   # strip each URL of parameters
        for url in ad_urls:
            # skip existing URL in DB 
            r = db.find_one_db(owner, url)
            index = ad_urls.index(url) 
            if r:
                print('- - - Skipping no.', index+1, owner, '-', url)
                continue 
            
            price_str = extract_from_css(response, index, 'p.list_price.font-large ::text')
            price_int = cast_int(price_str)
            
            name = extract_from_css(response, index, 'a.item_link ::text')
            # in case the restriction on brands exists 
            if brand_allow:
                check = [ ]
                for b in brand_allow:
                    if b in name:
                        check.append(b) 
                if not check: 
                    continue  # skip to next car 
                
            # pipelines out to DB
            try:
                out = {
                    'name': name,
                    # TODO: improve selector
                    'location_thumb': locations[index].lstrip().rstrip(),
                    'price': price_str,
                    'owner': owner,
                    'ad_url': url,
                    'query_url': response.url,
                    'page': page,
                    'date': round(now()),
                    'email_sent': 0
                }
            except:
                print('INDEX: ', index)
                print('len(locations): ', len(locations))
                print('LOCATIONS: ', locations)
                print('INPUT: ', ad_urls)
                
            yield out
            
        # extract all pages and fire up scrapers
        all_pages = response.css('a.page_nav ::attr(href)')
        if all_pages.extract():
            for next_page in all_pages:
                yield response.follow(url = next_page,
                                      callback = self.parse,
                                      meta = {
                                          'owner': owner
                                      })
        
class MongoPipeline(object):
    def __init__(self, collection_name):
        self.collection_name = collection_name
        self.db = None

    @classmethod
    def from_crawler(cls, crawler):
        """
        Creates a mongo-pipeline from a crawler object
        :param crawler:
        :return: MongoPipeline
        """
        return cls(
            collection_name=crawler.settings.get('db_collection')
        )

    def open_spider(self, spider):
        spider.logger.info("Opening DB to collection: %s", self.collection_name)
        self.db = MongoDB_helper(collection_name=self.collection_name)

    def close_spider(self, spider):
        self.db.close()

    def process_item(self, item, spider):
        # do not update email-sent documents 
        self.db.find_one_and_up(dict(item), email_sent=0)
        return item


def send(owner, results, body, s):
    for res in results:
        # fill email body
        # q = res['query_url']
        # t_unix = res['date']
        body += res['name'] + '\n'
        body += res['price'] + '\n'
        body += res['location_thumb'] + '\n' 
        body += res['ad_url'] + '\n\n' 

        # update as sent in DB
        db.find_one_and_up(res, email_sent = 1)

    mails = db_owners.get_emails_db(owner)
    if mails:
        # send email
        msg = MIMEText(body, 'plain')
        msg['Subject'] = 'Blocket.se cars'
        msg['From'] = settings.EMAIL_ALIAS   # ignored by Google
        msg['To'] = ', '.join(mails)
        s.send_message(msg)
        print('email sent to: ', msg['To'])
    else:
        print('missing email address for recipient: ', owner)

        
def send_emails():
    ALIAS = settings.EMAIL_ALIAS
    USER = settings.EMAIL_USERNAME
    PWD = settings.EMAIL_PASSWORD
    s = smtplib.SMTP_SSL('smtp.gmail.com', 465)   # TLS 
    s.login(USER, PWD)
    
    # iterate
    owners = db.get_owners_db()
    for owner in owners:
        results = db.get_unsent_emails_db(owner)
        count = results.count()
        if count:
            now = t.now().strftime("%A, %Y-%m-%d, %H:%M") 
            body = ''
            body += now + '\n' 
            body += 'Results: ' + str(count) + '\n\n'
            send(owner, results, body, s)
        else:
            print('no unsent emails for: ', owner)
    s.quit()
    

# Main
start = stopwatch()
def crawl():
    # fetch jobs
    owner_urls = db_owners.get_jobs_cars_db()
    owner_urls = clean_owner_urls(owner_urls)
    scrapy_settings = get_project_settings()
    scrapy_settings.set('car_jobs', owner_urls)
    scrapy_settings.set('db_collection', settings.MONGODB_COLLECTION_CARS_NAME)
    scrapy_settings.set('LOG_LEVEL', 'INFO')
    scrapy_settings.set('LOG_FILE', logname)
    scrapy_settings.set('LOG_STDOUT', 'False')  # redirect stdout to file from runner 
    
    process = CrawlerProcess(scrapy_settings)
    process.crawl(BlocketCarsSpider)
    process.start()   # the script will block here until the crawling is finished

if __name__ == '__main__':
    logname = 'BlocketCarsScraper_log.txt'
    crawl()
    send_emails()
    db.close()
    
    end = stopwatch() 
    print('Total execution time: ', round(end-start, 2), 'seconds', '\n\n')
    with open(logname, 'a') as log:
        log.write('\n\n')

