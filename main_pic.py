# encoding=utf8
'''
Created on 2013-3-20
@author: corleone
'''
from bot.config import configdata
from bot.dbutil import get_unfetched_pic
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

        values = configdata.get(DetailSpiderConst.PicSettings, {})
        values[const.DETAIL_LIST] = self.cis
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
        execute(argv=["scrapy", "crawl", 'PersonPhoneSpider' ], settings=settings)
        
spider_process_mapping = {}

if __name__ == '__main__':
    
    cis = []
    ps = []
    while 1:
        try:
            carinfos = get_unfetched_pic()
            while carinfos:
                
                while carinfos:
                    ci = carinfos.pop()
                    cis.append(ci)
                    if len(cis) == 100:
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
        
                    print (u'%s [pic] sleep 60s wait process stop' % datetime.datetime.now())
                    
                    time.sleep(60)
                    for p in ps:
                        try:
                            p.terminate()
                        except :pass
                
            else:
                cis = []
                ps = []
                carinfos = get_unfetched_pic()
            
            if not carinfos:
                print (u'%s [pic] sleep 120s and get unfetched detail again' % datetime.datetime.now())
                time.sleep(120)
        except Exception as e:
            print str(e)
            raise e
