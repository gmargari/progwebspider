import scrapy
import json
import sys

#===============================================================================
# ProgrammableWebSpider
#===============================================================================
class ProgrammableWebSpider(scrapy.Spider):
    name = 'ProgrammableWeb'
    start_urls = [
        'http://www.programmableweb.com/category/travel/api',
    ]

    # scrapy parameter: seconds between successive page crawls
    download_delay = 0.25

    #===========================================================================
    # parse ()
    #===========================================================================
    def parse(self, response):
        yield scrapy.Request(response.url, self.parse_api_directory_page)

    #===========================================================================
    # parse_api_directory_page ()
    #===========================================================================
    def parse_api_directory_page(self, response):
        # Parse current directory page
        for tr in response.xpath("//tr[(@class='odd' or @class='even')]"):
            url = tr.xpath("td[1]/a/@href").extract()[0]
            #title tr.xpath("td[1]/a/@title").extract()[0],
            #short_description = tr.xpath("td[2]/text()").extract()[0].strip(),
            #category = tr.xpath("td[3]/a/text()").extract()[0],
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
        d = dict()
        for div in response.xpath("//div[@id='tabs-content']/div[2]/div[@class='field']"):
            key = str(div.xpath("label/text()").extract()[0])
            try:
                value = str(div.xpath("span/a/text()").extract()[0])
            except:
                value = str(div.xpath("span/text()").extract()[0])
            d[key] = value
        print json.dumps(d, sort_keys=True)
