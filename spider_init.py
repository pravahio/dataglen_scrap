from twisted.internet import reactor
from scrapy.crawler import CrawlerRunner
from scrapy.utils.log import configure_logging
from dataglen import DataglenSpider
from growatt import GrowattSpider

def main():
    configure_logging()
    runner = CrawlerRunner()

    d = runner.crawl(DataglenSpider)
    d.addBoth(lambda _: reactor.stop())

    g = runner.crawl(GrowattSpider)
    g.addBoth(lambda _: reactor.stop())
    
    reactor.run()
    

if '__main__' == __name__:
    main()