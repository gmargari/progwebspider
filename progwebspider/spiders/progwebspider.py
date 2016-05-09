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
import tldextract

#===============================================================================
# ProgrammableWebSpider
#===============================================================================
class ProgrammableWebSpider(scrapy.Spider):
    name = 'ProgrammableWeb'
    start_urls = [
        # NOTE: Don't forget to append "&page=0" to the url of the directory page
        # Directory of WSDL apis
        #'http://www.programmableweb.com/category/all/apis?data_format=21183&page=0',
        # Directory of SOAP apis
        'http://www.programmableweb.com/category/all/apis?data_format=21176&page=0',
    ]

    # scrapy parameter: seconds between successive page crawls
    download_delay = 2
    domain_max_visits = 100
    domain_visits = defaultdict(lambda: 0)

    #===========================================================================
    # parse ()
    #===========================================================================
    def parse(self, response):
        yield self.request_with_priority(response.url, self.parse_pw_directory_page, 40)

    #===========================================================================
    # parse_pw_directory_page ()
    #===========================================================================
    def parse_pw_directory_page(self, response):
        # If we reached the last page of results
        for div in response.xpath('//div[@class="view-empty"]/text()'):
            text = div.extract()
            if (text.find("Sorry, your search did not give any results")):
                return

        # Parse current directory page
        for tr in response.xpath("//tr[(@class='odd' or @class='even')]"):
            url = tr.xpath("td[1]/a/@href").extract()[0]
            fullurl = response.urljoin(url).replace("https://", "http://")
            yield self.request_with_priority(fullurl, self.parse_pw_api_page, 30)

        # Recursive call this function for next page
        next_page = self.get_next_page_url(response.url)
        yield self.request_with_priority(next_page, self.parse_pw_directory_page, 40)

    #===========================================================================
    # get_next_page_url ()
    #===========================================================================
    def get_next_page_url(self, url):
        needle = "page="
        num_start = url.rfind(needle) + len(needle)
        num_end = url.rfind("&", num_start)
        if (num_end == -1):
            num_end = len(url) - 1
        else:
            num_end = num_end - 1
        num = int(url[num_start:num_end+1])
        next_num = num + 1
        next_url = url[:num_start] + str(next_num) + url[num_end+1:]
        return next_url

    #===========================================================================
    # parse_pw_api_page ()
    #===========================================================================
    def parse_pw_api_page(self, response):
        for div in response.xpath("//div[@id='tabs-content']/div[2]/div[@class='field']"):
            key = str(div.xpath("label/text()").extract()[0])
            try:
                value = str(div.xpath("span/a/text()").extract()[0])
            except:
                value = str(div.xpath("span/text()").extract()[0])

            if ("API Endpoint" in key):
                yield self.request_with_priority(value, self.parse_website_for_wsdl, 20)
            elif ("API Homepage" in key):
                yield self.request_with_priority(value, self.parse_website_for_wsdl, 18)
            elif ("API Provider" in key):
                yield self.request_with_priority(value, self.parse_website_for_wsdl, 16)

    #===========================================================================
    # parse_website_for_wsdl ()
    #===========================================================================
    def parse_website_for_wsdl(self, response):
        domain = tldextract.extract(response.url).domain
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
            # Avoid parsing the same url with different schema: parse only 'http://' urls so that scrapy automatically detects duplicate urls
            url = link.url.replace("https://", "http://")
            url_lower_case = url.lower()
            if ("wsdl" in url_lower_case or "soap" in url_lower_case):
                yield self.request_with_priority(url, self.parse_website_for_wsdl, 10)
            elif ("webservice" in url_lower_case or "web_service" in url_lower_case or "web-service" in url_lower_case):
                yield self.request_with_priority(url, self.parse_website_for_wsdl, 8)
            elif ("api" in url_lower_case or "rest" in url_lower_case):
                yield self.request_with_priority(url, self.parse_website_for_wsdl, 6)
            else:
                yield self.request_with_priority(url, self.parse_website_for_wsdl, 4)

    #===========================================================================
    # request_with_priority ()
    #===========================================================================
    def request_with_priority(self, req_url, req_callback, req_priority):
        return scrapy.Request(req_url, callback = req_callback, priority = req_priority)

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
