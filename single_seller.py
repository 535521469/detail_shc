# encoding=UTF-8
'''
Created on 2013-5-1
@author: Administrator
'''
from bot.config import configdata
from bot.dbutil import get_unfetched_seller
from const import ScrapyConst, DetailSpiderConst
from crawler.shc.fe.const import FEConstant as const
from scrapy.cmdline import execute
from scrapy.settings import CrawlerSettings
import os

class SpiderProcess(object):
    
    def __init__(self, configdata, sis):
        self.configdata = dict(configdata)
        self.sis = sis
    
    def run(self):

        values = configdata.get(DetailSpiderConst.SellerSettings, {})
        values[const.SELLER_LIST] = self.sis
        values.update(**{
                  const.CONFIG_DATA:self.configdata,
                  })
        
        if ScrapyConst.Console in values:
            if values[ScrapyConst.Console] == u'1':# out to console
                values[ScrapyConst.LOG_FILE] = None
            else:
                log_dir = values.get(ScrapyConst.LOG_DIR, os.getcwd())
                if ScrapyConst.LOG_FILE in values:
                    log_file = values[ScrapyConst.LOG_FILE]
                    values[ScrapyConst.LOG_FILE] = os.sep.join([log_dir , log_file])
                    
        settings_path = u'crawler.shc.fe.sellersettings'
        module_import = __import__(settings_path, {}, {}, [''])
        settings = CrawlerSettings(module_import, values=values)
        execute(argv=["scrapy", "crawl", 'CustomerShopSpider' ], settings=settings)
        
if __name__ == '__main__':
    
    sis = []
    sellerinfos = get_unfetched_seller()
    while sellerinfos:
        si = sellerinfos.pop()
        sis.append(si)
        if len(sis) == 300:
            sp = SpiderProcess(configdata, sis)
            sp.run()
            cis = []
    else:
        if cis:
            sp = SpiderProcess(configdata, cis).run()
