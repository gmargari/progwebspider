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
import logging
from collections import defaultdict
import tldextract
import string

#===============================================================================
# ProgrammableWebWSDLExtractorSpider
#===============================================================================
class ProgrammableWebWSDLExtractorSpider(scrapy.Spider):
    name = 'ProgrammableWebWSDLExtractor'
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
    blocked_domains = set()
    wsdl_extracted = 0

    #===========================================================================
    # __init__ ()
    #===========================================================================
    def __init__(self):
        print "["

    #===========================================================================
    # closed ()
    #===========================================================================
    def closed(self, reason):
        print "]"

    #===========================================================================
    # parse ()
    #===========================================================================
    def parse(self, response):
        logging.info("==  PARSE " + response.url)
        yield self.request_with_priority(response.url, self.parse_pw_directory_page, 40)

    #===========================================================================
    # parse_pw_directory_page ()
    #===========================================================================
    def parse_pw_directory_page(self, response):
        logging.info("==  PARSE_DIR " + response.url)
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
        # (Note that this will increase by 1 the depth of the next directory
        # page and all subsequently extracted links)
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
        logging.info("==  PARSE_API " + response.url)

        api = dict()
        api['progweb_url'] = response.url
        keys_xpaths = {
            'title': "//div[@class='node-header']/h1/text()",
            'description': "//div[@class='api_description tabs-header_description']/text()",
            'logo': "//div[@class='field-item even']/img/@src",
        }
        for key in keys_xpaths:
            api[key] = response.xpath(keys_xpaths[key]).extract()[0].strip()
        api['logo'] = "http://www.programmableweb.com" + api['logo']

        api['progweb_specs'] = dict()
        progweb_specs_field_type_link = (
            "API Endpoint",
            "API Forum",
            "API Homepage",
            "API Provider",
        )
        for div in response.xpath("//div[@id='tabs-content']/div[2]/div[@class='field']"):
            key = str(div.xpath("label/text()").extract()[0])
            if (key in progweb_specs_field_type_link):
                value = str(div.xpath("span/a/@href").extract()[0]).strip("\"" + string.whitespace)
            else:
                try:
                    links = div.xpath("span/a/text()").extract()
                    value = ", ".join(links)
                except:
                    value = str(div.xpath("span/text()").extract()[0])
            api['progweb_specs'][key] = value

            if key == "API Endpoint":
                yield self.request_with_priority(value, self.parse_website_for_wsdl, 20, api)
            elif key == "API Homepage":
                yield self.request_with_priority(value, self.parse_website_for_wsdl, 18, api)
            elif key == "API Provider":
                yield self.request_with_priority(value, self.parse_website_for_wsdl, 16, api)

        categories = ""
        d = api['progweb_specs']
        if "Primary Category" in d:
            categories += d["Primary Category"]
        if "Secondary Categories" in d:
            if (len(categories)):
                categories += ", "
            categories += d["Secondary Categories"]
        api['categories'] = categories

    #===========================================================================
    # parse_website_for_wsdl ()
    #===========================================================================
    def parse_website_for_wsdl(self, response):
        api = response.meta['api']
        url_parts = tldextract.extract(response.url)
        subdomain = url_parts.subdomain
        domain = url_parts.domain
        suffix = url_parts.suffix
        self.domain_visits[domain] += 1
        logging.info("==  PARSE_WSDL " + str(response.meta['depth']) + " " + str(self.domain_visits[domain]) + " " + response.url)

        # Found a page containing valid WSDL description
        if (self.response_is_wsdl(response)):
            logging.info("WSDL_URL " + response.url)
            api['wsdl_url'] = response.url
            print "%s%s" % ("," if self.wsdl_extracted > 0 else "", json.dumps(api, sort_keys=True))
            self.wsdl_extracted += 1
            return

        if (not self.response_is_html(response)):
            return

        # If we reached the max number of visits for this domain, return
        if (self.domain_visits[domain] >= self.domain_max_visits):
            logging.info("==   ADD TO BLOCK_DOMAINS " + domain)
            self.blocked_domains.add(domain)
            return

        allowed_domains = [ domain + "." + suffix ]
        page_links = LinkExtractor(allow=(allowed_domains), canonicalize=False).extract_links(response)
        page_links = [ link.url for link in page_links ]

        for link in page_links:
            # Avoid parsing the same url with different schema: parse only 'http://' urls so that scrapy automatically detects duplicate urls
            url = link.replace("https://", "http://")
            url_lower_case = url.lower()

            priority_per_term_in_url = [
                [10, "wsdl", "soap"],
                [8, "webservice", "web_service", "web-service"],
                [7, "sitemap"],
                [6, "api", "rest"],
            ]
            default_priority = 4

            found = False
            for priority_terms in priority_per_term_in_url:
                priority = priority_terms[0]
                terms = priority_terms[1:]
                for term in terms:
                    if (term in url_lower_case):
                        yield self.request_with_priority(url, self.parse_website_for_wsdl, priority, api)
                        found = True
                        break
                if found == True:
                    break
            if found == False:
                yield self.request_with_priority(url, self.parse_website_for_wsdl, default_priority, api)

    #===========================================================================
    # request_with_priority ()
    #===========================================================================
    def request_with_priority(self, req_url, req_callback, req_priority, api = {}):
        logging.info("==      REQ " + str(req_priority) + " " + req_url + "  [ " + req_callback.func_name.upper() + " ]")
        request = scrapy.Request(req_url, callback = req_callback, priority = req_priority)
        if (api):
            request.meta['api'] = api
        return request

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
        return ("text/html" in response.headers['Content-Type'])
