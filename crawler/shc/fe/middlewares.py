# encoding=utf8
'''
Created on 2013-4-15
@author: corleone
'''
from bot.dbutil import FetchSession
from bot.item import CarInfo
from bot.proxyutil import get_valid_proxy
from crawler.shc.const import FetchConstant
from scrapy import log
from scrapy.contrib.downloadermiddleware.redirect import RedirectMiddleware
from scrapy.contrib.downloadermiddleware.retry import RetryMiddleware
from scrapy.exceptions import IgnoreRequest
from twisted.internet.error import TCPTimedOutError

class ProxyRetryMiddleWare(RetryMiddleware):

    def __init__(self, settings):
        RetryMiddleware.__init__(self, settings)

    def _retry(self, request, reason, spider):
        
        if isinstance(reason, TCPTimedOutError):
            reason.args = (u'...',)
        
        retries = request.meta.get('retry_times', 0)
        if retries <= self.max_retry_times - 1:
            next_proxy = get_valid_proxy.next()
            rs = request.copy()
            if next_proxy:
                proxy_str = next_proxy.build_literal()
                rs = rs.replace(dont_filter=True)
                rs.meta['proxy'] = proxy_str
                msg = (u'use to %s access %s ') % (proxy_str, rs.url)
                spider.log(msg, log.DEBUG)
            else:
                try:
                    del rs.meta[u'proxy']
                    msg = (u'use self ip asscess %s') % (rs.url)
                    spider.log(msg, log.DEBUG)
                except :pass
            
        return RetryMiddleware._retry(self, rs, reason, spider)


class PopularRedirectMiddleWare(RedirectMiddleware):
    '''
    用来判断推广信息的原始url是否已经抓取过。如果抓取的话，就删除此次抓取任务
    但是为了保存时间序列的信息，此去重被弃用
    '''
    def _redirect(self, redirected, request, spider, reason):
        
        redirect_url = redirected.url
        try:
            ci = request.cookies[FetchConstant.CarInfo]
            seqid = ci.seqid
        except:pass
        
        try:
            fs = FetchSession()
            ci_exist = fs.query(CarInfo).filter(CarInfo.sourceurl == redirect_url)\
                        .filter(CarInfo.seqid != seqid).first()
            if ci_exist:
                msg = (u'car with popular exist %s' % seqid)
                spider.log(msg, log.INFO)
#                ci.statustype = u'3'
                try:
#                    fs.merge(ci)
                    fs.delete(ci)
                except:
                    fs.rollback()
                else:
                    msg = (u'delete car with popular exist %s ' % seqid)
                    spider.log(msg, log.INFO)
                    fs.commit()
                finally:
                    fs.close()
                
                raise IgnoreRequest
            else:
                return RedirectMiddleware._redirect(self, redirected, request, spider, reason)
        except Exception as e:
            raise e
        
            
