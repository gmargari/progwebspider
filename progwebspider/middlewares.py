from scrapy.exceptions import IgnoreRequest
from urlparse import urlparse
import tldextract
import logging

# Based on: http://stackoverflow.com/a/30619887

#===============================================================================
# BlockedDomainMiddleware ()
#===============================================================================
class BlockDomainMiddleware(object):

    #===========================================================================
    # process_request ()
    #===========================================================================
    def process_request(self, request, spider):
        domain = tldextract.extract(request.url).domain
        if (domain in spider.blocked_domains):
            logging.info("Blocked domain: %s (url: %s)" % (domain, request.url))
            raise IgnoreRequest("URL blocked: %s" % request.url)

#===============================================================================
# BlockDomainOnTooManyErrosMiddleware ()
#===============================================================================
class BlockDomainOnTooManyErrosMiddleware(object):

    def process_response(self, request, response, spider):
        if (response.status < 200 or response.status > 300):
            domain = tldextract.extract(request.url).domain
            spider.errors_per_domain[domain] += 1
            if (spider.errors_per_domain[domain] > spider.max_domain_errors):
                logging.info("==   ADD TO BLOCK_DOMAINS (TOO MANY ERRORS) " + domain)
                spider.blocked_domains.add(domain)
        return response
