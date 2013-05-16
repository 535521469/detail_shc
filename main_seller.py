# encoding=utf8
'''
Created on 2013-3-20
@author: corleone
'''
from bot.config import configdata
from bot.dbutil import  get_unfetched_seller
from const import ScrapyConst, DetailSpiderConst
from crawler.shc.fe.const import FEConstant as const
from multiprocessing import Process
from scrapy.cmdline import execute
from scrapy.settings import CrawlerSettings
import datetime
import os
import time

class SpiderProcess(Process):
    
    def __init__(self, configdata, sis):
        super(SpiderProcess, self).__init__()
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
                if not os.path.exists(log_dir):
                    os.makedirs(log_dir,)
                if ScrapyConst.LOG_FILE in values:
                    log_file = values[ScrapyConst.LOG_FILE]
                    values[ScrapyConst.LOG_FILE] = os.sep.join([log_dir , log_file])
                    
        settings_path = u'crawler.shc.fe.settings'
        module_import = __import__(settings_path, {}, {}, [''])
        settings = CrawlerSettings(module_import, values=values)
        execute(argv=["scrapy", "crawl", 'CustomerShopSpider' ], settings=settings)
        

spider_process_mapping = {}


if __name__ == '__main__':
    
    sis = []
    ps = []
    while 1:
        sellerinfos = get_unfetched_seller()
        while sellerinfos:
            
            while sellerinfos:
                si = sellerinfos.pop()
                sis.append(si)
                if len(sis) == 500:
                    sp = SpiderProcess(configdata, sis)
                    ps.append(sp)
                    sp.start()
                    sis = []
            else:
                if sis:
                    sp = SpiderProcess(configdata, sis)
                    ps.append(sp)
                    sp.start()
                    sis = []
    
                print (u'%s [seller] sleep 600s wait process '
                       'stop') % datetime.datetime.now()
                
                time.sleep(600)
                for p in ps:
                    try:
                        p.terminate()
                    except :pass
            
        else:
            sis = []
            ps = []
            sellerinfos = get_unfetched_seller()
            
        if not sellerinfos:
            print (u'%s [seller] sleep 120s and get unfetched detail '
                   'again') % datetime.datetime.now()
            time.sleep(120)
            
        
        
        
