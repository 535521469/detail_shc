# encoding=UTF-8
'''
Created on 2013-5-1
@author: Administrator
'''
from bot.config import configdata
from const import ScrapyConst, DetailSpiderConst
from crawler.shc.fe.const import FEConstant as const
from scrapy.cmdline import execute
from scrapy.settings import CrawlerSettings
import os
from bot.dbutil import get_unfetched_pic

class SpiderProcess(object):
    
    def __init__(self, configdata, cis):
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
                if ScrapyConst.LOG_FILE in values:
                    log_file = values[ScrapyConst.LOG_FILE]
                    values[ScrapyConst.LOG_FILE] = os.sep.join([log_dir , log_file])
                    
        settings_path = u'crawler.shc.fe.picsettings'
        module_import = __import__(settings_path, {}, {}, [''])
        settings = CrawlerSettings(module_import, values=values)
        execute(argv=["scrapy", "crawl", 'PersonPhoneSpider' ], settings=settings)
        
if __name__ == '__main__':
    
    cis = []
    carinfos = get_unfetched_pic()
    while carinfos:
        ci = carinfos.pop()
        cis.append(ci)
        if len(cis) == 300:
            sp = SpiderProcess(configdata, cis)
            sp.run()
            cis = []
    else:
        if cis:
            sp = SpiderProcess(configdata, cis).run()
        
        
        
