import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from urlparse import urlparse
import urllib2
import json
import sys
import re
from xgoogle.search import GoogleSearch, SearchError
import time
from collections import defaultdict
import heapq

#===============================================================================
# PrioritySet
#===============================================================================
class PrioritySet(object):

    #===========================================================================
    # __init__ ()
    #===========================================================================
    def __init__(self):
        self.myheap = []
        self.myset = set()

    #===========================================================================
    # add ()
    #===========================================================================
    def add(self, pri, obj):
        if (not obj in self.myset):
            self.myset.add(obj)
            heapq.heappush(self.myheap, (pri, obj))
            print "==> Enqueue    ", pri, obj
        else:
            print "==> Duplicate    ", obj

    #===========================================================================
    # pop ()
    #===========================================================================
    def pop(self):
        pri, obj = heapq.heappop(self.myheap)
        #self.myset.remove(obj)  # Do not remove from set, so that we won't reinsert objects that were previously popped
        print "==> Pop        ", pri, obj
        return pri, obj

    #===========================================================================
    # empty ()
    #===========================================================================
    def empty(self):
        return (len(self.myheap) == 0)

#===============================================================================
# ProgrammableWebSpider
#===============================================================================
class ProgrammableWebSpider(scrapy.Spider):
    name = 'ProgrammableWebWSDL'
    start_urls = [
        # Directory of WSDL apis
        #'http://www.programmableweb.com/category/all/apis?data_format=21183',
        # Directory of SOAP apis
        'http://www.programmableweb.com/category/all/apis?data_format=21176',
    ]

    # scrapy parameter: seconds between successive page crawls
    download_delay = 2
    domain_max_visits = 100
    domain_visits = defaultdict(lambda: 0)
    links_queue = PrioritySet()

    #===========================================================================
    # parse ()
    #===========================================================================
    def parse(self, response):
        yield scrapy.Request(response.url, self.parse_pw_directory_page)

    #===========================================================================
    # parse_pw_directory_page ()
    #===========================================================================
    def parse_pw_directory_page(self, response):
        # Parse current directory page
        for tr in response.xpath("//tr[(@class='odd' or @class='even')]"):
            url = tr.xpath("td[1]/a/@href").extract()[0]
            fullurl = response.urljoin(url).replace("https://", "http://")
            yield scrapy.Request(fullurl, self.parse_pw_api_page)

        # If there is a "next page" url, recursive call this function for it
        next_page = response.xpath("//a[@class='pw_load_more']/@href")
        if next_page:
            fullurl = response.urljoin(next_page[0].extract())
            scrapy.Request(fullurl, self.parse_pw_directory_page)

    #===========================================================================
    # parse_pw_api_page ()
    #===========================================================================
    def parse_pw_api_page(self, response):
        d = dict()
        for div in response.xpath("//div[@id='tabs-content']/div[2]/div[@class='field']"):
            key = str(div.xpath("label/text()").extract()[0])
            try:
                value = str(div.xpath("span/a/text()").extract()[0])
            except:
                value = str(div.xpath("span/text()").extract()[0])
            d[key] = value

        for key in "API Endpoint", "API Homepage", "API Provider":
            if (key in d.keys()):
                self.add_url_to_queue(d[key])
                yield scrapy.Request(d[key], self.parse_website_for_wsdl)
                break

    #===========================================================================
    # parse_website_for_wsdl ()
    #===========================================================================
    def parse_website_for_wsdl(self, response):

        domain = urlparse(response.url).hostname
        self.domain_visits[domain] += 1
        print response.meta['depth'], self.domain_visits[domain], "PARSE  ", response.url

        if (self.response_is_wsdl(response)):
            print "WSDL_URL", response.url
            return

        if (not self.response_is_html(response)):
            print "NOT_HTML"
            return

        # If we reached the max number of visits for this domain, return
        if (self.domain_visits[domain] >= self.domain_max_visits):
            print "RETURN"
            return

        allowed_domains = [ "https://" + domain, "http://" + domain ]
        page_links = LinkExtractor(allow=(allowed_domains)).extract_links(response)

        for link in page_links:
            self.add_url_to_queue(link.url)

        # Recursively parse the first few pages from queue
        for i in range(0, 3):
            url = self.get_url_from_queue()
            if (url):
                yield scrapy.Request(url, self.parse_website_for_wsdl)

    #===========================================================================
    # add_url_to_queue ()
    #===========================================================================
    def add_url_to_queue(self, url):
        queue = self.links_queue

        # Avoid parsing the same url with different schema: parse only 'http://' urls so that scrapy automatically detects duplicate urls
        url = url.replace("https://", "http://")
        url_lower_case = url.lower()
        if ("wsdl" in url_lower_case or "soap" in url_lower_case):
            queue.add(1, url)
        elif ("webservice" in url_lower_case):
            queue.add(2, url)
        elif ("api" in url_lower_case or "rest" in url_lower_case):
            queue.add(3, url)
        else:
            queue.add(5, url)

    #===========================================================================
    # get_url_from_queue ()
    #===========================================================================
    def get_url_from_queue(self):
        queue = self.links_queue
        if (not queue.empty()):
            priority, url = queue.pop()
            return url

    #===========================================================================
    # response_is_wsdl ()
    #===========================================================================
    def response_is_wsdl(self, response):
        if (("text/xml" in response.headers['Content-Type'] or
            "application/wsdl+xml" in response.headers['Content-Type'])
            and
            ("schemas.xmlsoap.org/wsdl" in response.body or
             "schemas.xmlsoap.org/soap" in response.body)):
            return True
        return False

    #===========================================================================
    # response_is_html ()
    #===========================================================================
    def response_is_html(self, response):
        if ("text/html" in response.headers['Content-Type']):
            return True
        return False
