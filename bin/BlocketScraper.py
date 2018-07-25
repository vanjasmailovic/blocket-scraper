# coding: utf-8

# # Blocket.se Scraper
# 
# Execution flow is as follows:
# 1. Edit filters to filter out unwanted apartments and keep desired ones  
# 2. Start the scraper which will look for new ads every 5 minutes
# 3. Once a new ad shows up, you will be notified through email
# 4. Apply for the apartment quickly effectively being among the first
# 
# Explanation of URL parameters:
# 
#     ss   - min. living area
#     se   - max. living area
#     ros  - min. number of rooms
#     roe  - max. number of rooms 
#     bs   - min. number of bedrooms
#     be   - max. number of bedrooms
#     mre  - max. rental price 
#     o    - page number          
#     sort - default is blank for "newest first"
#  
# *NOTE: number value of the URL parameter represents an ordinal in drop-down list for the selected parameter as displayed on blocket.se.*


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
from furl import furl
import scrapy, re, logging
from scrapy.crawler import CrawlerProcess
from scrapy.http import Request
from scrapy.utils.project import get_project_settings
from simple_settings import settings
logger = logging.getLogger(__name__)

# Database helper
from misc.db import MongoDB_helper
db = MongoDB_helper(collection_name=settings.MONGODB_COLLECTION_NAME)
db_owners = MongoDB_helper(collection_name=settings.MONGODB_COLLECTION_OWNERS_NAME)

# Other helpers 
from misc.utils import *
from misc.utils_scraping import *

# Email 
import smtplib
from email.mime.text import MIMEText


# TODO: replace with jobs
price_min = 9000
price_max = 15000
# Scraper
class BlocketSpider(scrapy.Spider):
    name = 'blocket_apartments_spider'
    custom_settings = {
        'COOKIES_ENABLED': False,
        'ITEM_PIPELINES' : {
            '__main__.MongoPipeline': 100, 
        }
    }
    
    def start_requests(self):
        jobs = self.settings['apartment_jobs'] 
        for owner, url in jobs: 
            print('start for ', owner, '-', url)
            yield Request(url = url,
                          callback = self.parse, 
                          meta = {
                              'owner': owner,
                              'page': '1'
                          })
    
    def closed(self, reason):
        # called when the crawler process ends
        send_emails()
            
    def parse(self, response):
        owner = response.meta['owner']
        page = response.meta['page']
        print('OWNER: ', owner)
        print('PAGE: ', page)
        
        # check if page available
        error = 'Just nu finns inga bostäder som matchar din sökning'
        if error in response.text:
            print('ERROR: Page {}.'.format(str(page)))
            return None
        
        ad_urls = response.css("a.vi-link-overlay.xiti_ad_frame ::attr(href)").extract() 
        ad_urls = [re.split('[?&#]', u)[0]  for u in ad_urls]   # strip each URL of parameters
        for url in ad_urls:
            # skip existing URL in DB 
            r = db.find_one_db(owner, url)
            index = ad_urls.index(url) 
            if r:
                print('- - - Skipping no.', index+1, owner, '-', url)
                continue 
            
            price_str = extract_from_css(response, index, "span.li_detail_params.monthly_rent ::text")
            price_int = cast_int(price_str)
            if price_int < price_min:
                print('- - - Price', price_int, 'is under', price_min, 'for', ' - ', url)
                continue
                
            out = {
                'name': extract_from_css(response, index, 'a.item_link.xiti_ad_heading ::text'),
                'location_thumb': extract_from_css(response, index, "span.subject-param.address.separator ::text").upper(),
                'price': price_str,
                'owner': owner,
                'ad_url': url,
                'query_url': response.url,
                'page': page,
                'date': round(now()),
                'email_sent': 0
            }
            yield out
            
        # if exists, retrieve next page as a string of digits 
        np = response.css("li.next ::attr(href)").extract_first()
        if np:
            page = [p.lstrip('o=') for p in np.split('&') if 'o=' in p][0]
            for next_page in response.css('li.next a'):
                yield response.follow(url = next_page,
                                      callback = self.parse,
                                      meta = {
                                          'owner': owner,
                                          'page': page
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
        # TODO maybe bulk update? 
        # do not update email-sent documents 
        self.db.find_one_and_up(dict(item), email_sent=0)
        return item


def send(owner, results, body, s):
    for res in results:
        # fill email body
        # q = res['query_url']
        # t_unix = res['date']
        body += res['location_thumb'] + ' - ' + res['price'] + '\n' 
        body += res['name'] + '\n' 
        body += res['ad_url'] + '\n\n' 

        # update as sent in DB
        db.find_one_and_up(res, email_sent = 1)

    mails = db_owners.get_emails_db(owner)
    if mails:
        # send email
        msg = MIMEText(body, 'plain')
        msg['Subject'] = 'Blocket.se apartments'
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
    owner_urls = db_owners.get_jobs_apartments_db()
    owner_urls = clean_owner_urls(owner_urls)
    # TODO
    # price_min = db_owners.get_price_min_db()
    # price_max = db_owners.get_price_max_db()
    
    scrapy_settings = get_project_settings()
    scrapy_settings.set('apartment_jobs', owner_urls)
    scrapy_settings.set("db_collection", settings.MONGODB_COLLECTION_NAME)
    # scrapy_settings.set("price_min", price_min)
    # scrapy_settings.set("price_max", price_max)
    scrapy_settings.set("LOG_LEVEL", 'INFO')
    scrapy_settings.set('LOG_FILE', 'BlocketScraper_log.txt')
    scrapy_settings.set('LOG_STDOUT', 'False') # redirect stdout to file from runner 
    
    process = CrawlerProcess(scrapy_settings)
    process.crawl(BlocketSpider)
    process.start()   # the script will block here until the crawling is finished

if __name__ == '__main__':
    logname = 'BlocketScraper_log.txt'
    crawl()
    send_emails()
    db.close()
    
    end = stopwatch() 
    print('Total execution time: ', round(end-start, 2), 'seconds', '\n\n')
    with open(logname, 'a') as log:
        log.write('\n\n')

