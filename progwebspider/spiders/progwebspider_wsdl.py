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

clean_html_tags_regex = re.compile('<.*?>')

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
    domain_depth_limit = 4
    domain_max_visits = 10
    domain_visits = defaultdict(lambda: 0)

#    rules = (Rule(LinkExtractor(), callback='parse_website_for_wsdl', follow=True),)
#    rules = (Rule(LinkExtractor(), callback='parse_obj', follow=True),)

    #===========================================================================
    # parse ()
    #===========================================================================
    def parse(self, response):
        yield scrapy.Request(response.url, self.parse_api_directory_page)
        #yield scrapy.Request("http://bn.gy/", self.parse_website_for_wsdl)
        #yield scrapy.Request("http://www.konakart.com/", self.parse_website_for_wsdl)

    #===========================================================================
    # parse_api_directory_page ()
    #===========================================================================
    def parse_api_directory_page(self, response):
        # Parse current directory page
        for tr in response.xpath("//tr[(@class='odd' or @class='even')]"):
            url = tr.xpath("td[1]/a/@href").extract()[0]
            fullurl = response.urljoin(url)
            yield scrapy.Request(fullurl, self.parse_api_page)

        # If there is a "next page" url, recursive call this function for it
        next_page = response.xpath("//a[@class='pw_load_more']/@href")
        if next_page:
            fullurl = response.urljoin(next_page[0].extract())
            yield scrapy.Request(fullurl, self.parse_api_directory_page)

    #===========================================================================
    # parse_api_page ()
    #===========================================================================
    def parse_api_page(self, response):
        for div in response.xpath("//div[@id='tabs-content']/div[2]/div[@class='field']"):
            key = str(div.xpath("label/text()").extract()[0])
            try:
                value = str(div.xpath("span/a/text()").extract()[0])
            except:
                value = str(div.xpath("span/text()").extract()[0])
            if (key == "API Provider"):
                time.sleep(4)
                #yield scrapy.Request(self.google_url(value), self.parse_google_results_for_wsdl)
                yield scrapy.Request(value, self.parse_website_for_wsdl)

    #===========================================================================
    # parse_google_results_for_wsdl ()
    #===========================================================================
    def parse_google_results_for_wsdl(self, response):
        print response.meta['depth'], "PARSE:", response.url

        for link in response.xpath("//div[@class='g']/h3[@class='r']/a/@href").extract():
            google_link = "https://www.google.com/" + link
            # From google results' link to actual link after google redirection
            req = urllib2.Request(google_link)
            res = urllib2.urlopen(req)
            finalurl = res.geturl()
            # If url ends in "?wsdl"
            url = re.sub(clean_html_tags_regex, '', finalurl)
            match = re.match(r'(?i).*\?wsdl$', url)
            if (match):
                print " ***", url
            else:
                print "    ", url

        # Recursive call this function for the next page of results
        print "CUR:  ", response.url
        print "NEXT: ", self.get_next_url(response.url)
        #yield scrapy.Request(self.get_next_url(response.url), self.parse_google_results_for_wsdl)

    #===========================================================================
    # parse_website_for_wsdl ()
    #===========================================================================
    def parse_website_for_wsdl(self, response):

        domain = urlparse(response.url).hostname
        self.domain_visits[domain] += 1

        # If we reached either the depth limit or the max number of visits for this domain, return
        # (-2: each domain starts with depth 2 because its url has been extracted from parse() and parse_api_directory_page()
        if (response.meta['depth'] - 2 >= self.domain_depth_limit or
            self.domain_visits[domain] >= self.domain_max_visits):
            return

        print response.meta['depth'], self.domain_visits[domain], "parse_website_for_wsdl(", response.url, ")"

        allowed_domains = [ "https://" + domain, "http://" + domain ]
        for link in LinkExtractor(allow=(allowed_domains)).extract_links(response):
            url = link.url

            # For some reason, a "=" or "+" sign may be added to the end of the extracted url. Remove it.
            url = re.sub("=$", "", url)
            url = re.sub("\+$", "", url)

            #           (?i)                         .*                       \?wsdl                 $
            # case insensitive search - for any number of characters - then the string "?wsdl" - then the end
            match = re.match(r'(?i).*\?wsdl$', url)
            if (match):
                print " ***", url
            else:
                print "    ", url

            # Avoid parsing the same url with different schema: parse only 'http://' urls so that scrapy automatically detects duplicate urls
            url = url.replace("https://", "http://")
            # Recursively parse that page
            yield scrapy.Request(url, self.parse_website_for_wsdl)

    #===========================================================================
    # google_url ()
    #===========================================================================
    def google_url(self, url):
        domain = urlparse(url).hostname
        print "-- ", domain

        # Ask google for urls of the above domain that contain the word "wsdl"
        client = "firefox"
        language = "en"
        url = "http://www.google.gr/search?" + \
            "client=" + client + \
            "&hl=" + language + \
            "&q=site:" + domain + "+inurl:wsdl" + \
            "&filter=0"
        return url

    #===========================================================================
    # get_next_url ()
    #===========================================================================
    def get_next_url(self, url):
        if (url.find("#") != -1):
            params = dict(map(lambda x: x.split("="), url.split("#")[1].split("&")))
        else:
            params = dict(map(lambda x: x.split("="), url.split("?")[1].split("&")))
        #if no "&start=" part in url, append "start=10". Else, replace "start=x" with "start=x+10"
        if ('start' in params and int(params['start']) < 20):
            return url.replace('&start=' + params['start'], '&start=' + str(int(params['start']) + 10))
        else:
            return url + "&start=10"
