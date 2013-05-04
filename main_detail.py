# encoding=utf8
'''
Created on 2013-3-20
@author: corleone
'''
from bot.config import configdata
from bot.dbutil import get_unfetched_carinfo
from const import ScrapyConst, DetailSpiderConst
from crawler.shc.fe.const import FEConstant as const
from multiprocessing import Process
from scrapy.cmdline import execute
from scrapy.settings import CrawlerSettings
import datetime
import os
import time

class SpiderProcess(Process):
    
    def __init__(self, configdata, cis):
        super(SpiderProcess, self).__init__()
        self.configdata = dict(configdata)
        self.cis = cis
    
    def run(self):

        values = configdata.get(DetailSpiderConst.DetailSettings, {})
        values[const.DETAIL_LIST] = self.cis
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
                    
        settings_path = u'crawler.shc.fe.settings'
        module_import = __import__(settings_path, {}, {}, [''])
        settings = CrawlerSettings(module_import, values=values)
        execute(argv=["scrapy", "crawl", 'CarDetailSpider' ], settings=settings)
        

spider_process_mapping = {}


if __name__ == '__main__':
    
    cis = []
    ps = []
    while 1:
        carinfos = get_unfetched_carinfo()
        while carinfos:
            
            while carinfos:
                ci = carinfos.pop()
                cis.append(ci)
                if len(cis) == 50:
                    sp = SpiderProcess(configdata, cis)
                    ps.append(sp)
                    sp.start()
                    cis = []
            else:
                if cis:
                    sp = SpiderProcess(configdata, cis)
                    ps.append(sp)
                    sp.start()
                    cis = []
    
                print (u'%s sleep 180s wait process stop' % datetime.datetime.now())
                
                time.sleep(180)
                for p in ps:
                    try:
                        p.terminate()
                    except :pass
            
        else:
            cis = []
            ps = []
            carinfos = get_unfetched_carinfo()
            
        print (u'%s sleep 120s and get unfetched detail again' % datetime.datetime.now())
        time.sleep(120)
            
        
        
        
#    else:
#        if cis:
#            sp = SpiderProcess(configdata, cis).run()
        
            
        
