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
