# encoding=utf8
'''
Created on 2013-3-31
@author: Administrator
'''
from md5 import md5
from bot.const import CarInfoValueConst
from bot.dbutil import FetchSession
from bot.item import CarInfo, SellerInfo
from bot.proxyutil import get_valid_proxy
from crawler.shc.const import FetchConstant
from crawler.shc.fe.item import SHCFEShopInfoConstant
from crawler.tools import gen_uuid
from functools import wraps
from scrapy import log
from scrapy.http.request import Request
from scrapy.selector import HtmlXPathSelector
from sqlalchemy.sql.expression import or_
import datetime
import os



def ignore_notice(parse):
    
    @wraps(parse)
    def parse_simulate(self, response):
        '''
        the page is not exist
        '''
        hxs = HtmlXPathSelector(response)
        notice_div = hxs.select('//div[@id="Notice"]')
        url = response.url
        if notice_div:
            
            ci = response.request.cookies[FetchConstant.CarInfo]
            ci.statustype = CarInfoValueConst.offline
            ci.offlinedatetime = datetime.datetime.now()
            fs = FetchSession()
            try:
                fs.merge(ci)
            except Exception:
                fs.rollback()
            else:
                fs.commit()
                self.log((u'ignore for not exist detail page, with Notice div '
                          '%s') % (url,), log.INFO)
            finally:
                fs.close()
            
        else:
            rss = parse(self, response)
            if rss:
                for rs in rss:
                    if isinstance(rs, Request):
                        rs = rs.replace(dont_filter=True)
                        yield rs
    return parse_simulate

def check_award(parse):
    '''
    see situation_pic.award.jpg
    '''
    @wraps(parse)
    def parse_simulate(self, response):
        '''
        the page is not exist
        '''
        hxs = HtmlXPathSelector(response)
        award_div = hxs.select('//div[@class="awardPic"]')
        url = response.url
        if award_div:
            
            while 1:
                proxy = get_valid_proxy.next()
                if proxy:
                    break
            try:
                precede_url = response.meta['redirect_urls'][0]
                req = response.request.copy()
                req = req.replace(url=precede_url)
                proxy_str = proxy.build_literal()
                req.meta[u'proxy'] = proxy_str
                self.log((u'with award div %s , use '
                          '%s ') % (precede_url, proxy_str), log.INFO)
                yield req
                
            except Exception as e:
#                precede_url = response.request.headers['Referer']
                pass
            
        else:
            rss = parse(self, response)
            if rss:
                for rs in rss:
                    if isinstance(rs, Request):
                        rs = rs.replace(dont_filter=True)
                        yield rs
    return parse_simulate

def check_verification_code(parse):
    @wraps(parse)
    def parse_simulate(self, response):
        '''
        need you to input verification code
        '''
        hxs = HtmlXPathSelector(response)
        verification_div = hxs.select('//div[@class="w_990"]')
        url = response.url
        if verification_div or url.find('support') > -1:
            precede_url = url[url.index(u'url=') + 4:]
            
            while 1:
                proxy = get_valid_proxy.next()
                if proxy:
                    break
            try:
                req = response.request.copy()
                req = req.replace(url=precede_url)
                proxy_str = proxy.build_literal()
                req.meta[u'proxy'] = proxy_str
                self.log((u'with verification div %s , use '
                          '%s ') % (precede_url, proxy_str), log.INFO)
                yield req
            except Exception as e:
#                precede_url = response.request.headers['Referer']
                pass
            
            
        else:
            rss = parse(self, response)
            if rss:
                for rs in rss:
                    if isinstance(rs, Request):
                        rs = rs.replace(dont_filter=True)
                        yield rs
                
    return parse_simulate

def check_blank_page(parse):
    @wraps(parse)
    def parse_simulate(self, response):
        '''
        the whole page is blank 
        '''
        if not len(response.body.strip()):
            url = response.url
            self.log(u'the page is blank %s' % url,
                     log.INFO)
            
            while 1:
                proxy = get_valid_proxy.next()
                if proxy:
                    break
            
            proxy_str = proxy.build_literal()
            req = response.request.copy()
            req.meta[u'proxy'] = proxy_str
            self.log((u'with blank body %s , use %s ') % (req.url, proxy_str),
                     log.INFO)
            
            yield req
            
        else:
            rss = parse(self, response)
            if rss:
                for rs in rss:
                    if isinstance(rs, Request):
                        rs = rs.replace(dont_filter=True)
                        yield rs
                
    return parse_simulate

def check_method_not_allowed(parse):
    @wraps(parse)
    def parse_simulate(self, response):
        '''
        see method_not_allowed.jpg
        '''
        p_val = u""
        try:
            p_val = HtmlXPathSelector(response).select('//p/text()')[0].extract()
        except:
            pass
        
        if p_val.strip() == u"Method Not Allowed":
            
            fs = FetchSession()
            try:
                ci = response.request.cookies[FetchConstant.CarInfo]
                ci.statustype = CarInfoValueConst.offline
                ci.offlinedatetime = datetime.datetime.now()
                fs.merge(ci)
            except Exception as e:
                fs.rollback()
                self.log(u'mna, something wrong %s' % str(e),
                                                     log.CRITICAL)
                raise e
            else:
                fs.commit()
                self.log((u'method not allowed '
                          '%s') % (response.url,), log.INFO)
            finally:
                fs.close()
        else:
            rss = parse(self, response)
            if rss:
                for rs in rss:
                    if isinstance(rs, Request):
                        rs = rs.replace(dont_filter=True)
                        yield rs
                
    return parse_simulate

def check_verification_code_gif(parse):
    @wraps(parse)
    def parse_simulate(self, response):
        '''
        need you to input verification code
        '''
        try:
            cookies = response.request.cookies
            hxs = HtmlXPathSelector(response)
            verification_div = hxs.select('//div[@class="w_990"]')
            url = response.url
            if verification_div:
                self.log(u'need input verification code crawl %s' % url,
                         log.CRITICAL)
                
                precede_url = url[url.index(u'url=') + 4:]
                shield_file_name = self.get_random_id()
                
                self.log((u'use ip proxy to request %s again , '
                          'shield %s') % (precede_url, shield_file_name),
                         log.INFO)
                proxy = self.get_next_proxy(cookies)
                
                if self.is_develop_debug(cookies):
                    self.save_body(self.build_shield_file_dir(cookies),
                                    shield_file_name + u'.html', response)
                
                yield Request(precede_url, self.parse, meta={'proxy':proxy},
                              cookies=cookies, dont_filter=True)
                    
        except Exception:
            rss = parse(self, response)
            if rss:
                for rs in rss:
                    if isinstance(rs, Request):
                        rs = rs.replace(dont_filter=True)
                        yield rs
                
    return parse_simulate

def with_ip_proxy(parse):
    @wraps(parse)
    def parse_simulate(self, response):
        
        rss = parse(self, response)
        if rss:
            for rs in rss:
                if isinstance(rs, Request) :
                    proxy = get_valid_proxy.next()
                    if proxy:
                        proxy_str = proxy.build_literal()
                        rs = rs.replace(dont_filter=True)
                        rs.meta['proxy'] = proxy_str
                        msg = (u'use proxy %s access '
                               '%s ') % (proxy_str, rs.url)
                        self.log(msg, log.DEBUG)
                    yield rs
                else:
                    yield rs
        
    return parse_simulate

def with_ip_proxy_start_requests(start_requests):
    @wraps(start_requests)
    def parse_simulate(self):
        
        rss = start_requests(self)
        if rss:
            for rs in rss:
                if isinstance(rs, Request) :
                    proxy = get_valid_proxy.next()
                    if proxy:
                        proxy_str = proxy.build_literal()
                        rs = rs.replace(dont_filter=True)
                        rs.meta['proxy'] = proxy_str
                        msg = (u'use proxy %s access '
                               '%s ') % (proxy_str, rs.url)
                        self.log(msg, log.DEBUG)
                    yield rs
                else:
                    yield rs
        
    return parse_simulate

def list_page_parse_4_remove_duplicate_detail_page_request(parse):
    
    @wraps(parse)
    def parse_simulate(self, response):
        rss = parse(self, response)
        from crawler.shc.fe.spiders import CarDetailSpider, CarListSpider
        if rss:
            rs_len = 0
            for rs in rss:
                if isinstance(rs, Request):
                    # detail spider
                    if rs.callback.im_class == CarDetailSpider:
                        rs_len = rs_len + 1
                        fs = FetchSession()
                        try:
                            or_express = or_(CarInfo.sourceurl == rs.url,
                                          CarInfo.popularizeurl == rs.url)
                            ci = fs.query(CarInfo).filter(or_express).first() 
                            if ci:
                                self.log(u'give up fetched detail page %s %s '
                                         '%s' % (rs.url, ci.cityname, ci.seqid),
                                         log.INFO)
                            else:
                                self.log(u'add detail page %s' % (rs.url,),
                                         log.INFO)
                                
                                ci = CarInfo()
                                if rs.url.find(u'jump.zhineng') <> -1:
                                    ci.popularizeurl = rs.url
                                else:
                                    ci.sourceurl = rs.url
                                
                                ci.sourcetype = '58'
                                
                                fs.add(ci)
                                
                                #===============================================
                                # not crawl the detail page this service 
                                #===============================================
#                                yield rs
                        except Exception as e:
                            fs.rollback()
                            self.log(u'something wrong %s' % str(e),
                                     log.CRITICAL)
                            raise e
                        else:
                            fs.commit()
                        finally:
                            fs.close()
                    elif rs.callback.im_class == CarListSpider:
                        # next page spider
                        if rs_len:
                            yield rs
                        else:
                            self.log(u'%s has no detail page , '
                                     'give up to crawl next '
                                     'page' % response.request.url, log.INFO)
                    
    return parse_simulate

# 本方法保留，配合 redirect 中间件 在推广部分 实现 去重短路
#def detail_page_parse_4_save_2_db(parse):
#    
#    def stuff_ci(ci, sellerinfo, current_city):
#        contacter_url = sellerinfo.get(SHCFEShopInfoConstant.contacter_url)
#        contacterphonepicurl = sellerinfo.get(SHCFEShopInfoConstant.contacter_phone_url)
#        ci.statustype = CarInfoValueConst.offline \
#                if contacterphonepicurl is None else CarInfoValueConst.online 
#        ci.title = sellerinfo.get(SHCFEShopInfoConstant.title)
#        ci.declaredate = sellerinfo.get(SHCFEShopInfoConstant.declaretime)
#        ci.fetchdatetime = datetime.datetime.now()
#        ci.lastactivedatetime = datetime.datetime.now()
#        ci.price = sellerinfo.get(SHCFEShopInfoConstant.price)
#        ci.cartype = sellerinfo.get(SHCFEShopInfoConstant.cartype)
#        ci.contacter = sellerinfo.get(SHCFEShopInfoConstant.contacter)
#        ci.contacterurl = contacter_url
#        ci.contacterphonepicname = sellerinfo.get(SHCFEShopInfoConstant.contacter_phone_picture_name)
#        ci.carcolor = sellerinfo.get(SHCFEShopInfoConstant.car_color)
#        ci.roadhaul = sellerinfo.get(SHCFEShopInfoConstant.road_haul)
#        ci.displacement = sellerinfo.get(SHCFEShopInfoConstant.displacement)
#        ci.gearbox = sellerinfo.get(SHCFEShopInfoConstant.gearbox)
#        ci.licenseddate = sellerinfo.get(SHCFEShopInfoConstant.license_date)
#        ci.sourceurl = sellerinfo.get(SHCFEShopInfoConstant.info_url)
#        ci.cityname = current_city
#        carsourcetype = sellerinfo.get(SHCFEShopInfoConstant.custom_flag)
#        ci.carsourcetype = carsourcetype
#        ci.contacterphonepicurl = contacterphonepicurl
#        ci.sourcetype = u'58'
#        
#    @wraps(parse)
#    def parse_simulate(self, response):
#        rss = parse(self, response)
#        if rss:
#            for rs in rss:
#                fs = FetchSession()
#                ci_seqid = response.request.cookies[FetchConstant.CarInfo].seqid
#                try:
#                    if not isinstance(rs, Request):
#
#                        contacter_url = rs.get(SHCFEShopInfoConstant.contacter_url)
#                        
#                        ci = fs.query(CarInfo).filter(
#                                          CarInfo.seqid == ci_seqid).first()
#                                          
#                        si = fs.query(SellerInfo).filter(
#                                 SellerInfo.sellerurl == contacter_url).first()
#                        
#                        if ci.statustype is None:
#                            stuff_ci(ci, rs, rs.get(SHCFEShopInfoConstant.cityname))
#                            self.log(u'stuff carinfo %s' % ci.seqid, log.INFO)
#                        else:
#                            self.log(u"give up to crawl exist car info "
#                                     "%s %s" % (ci.cityname, ci.seqid),
#                                     log.INFO)
#                        
#                        if si is not None:
#                            
#                            ci.sellerid = si.seqid
#                            self.log(u'stuff carinfo %s' % ci.seqid, log.INFO)
#                            fs.add(ci)
#                                
#                        else:
#
#                            ci.sellerid = gen_uuid()
#                            fs.add(ci)
#                            
#                            si = SellerInfo()
#                            si.seqid = ci.sellerid
#                            si.selleraddress = rs.get(SHCFEShopInfoConstant.shop_address)
#                            si.sellerurl = contacter_url
#                            
#                            fs.add(si)
#                        
#                except Exception as e:
#                    self.log(u'something wrong %s' % str(e), log.CRITICAL)
#                    fs.rollback()
#                else:
#                    fs.commit()
#                finally:
#                    fs.close()
#    return parse_simulate


def detail_page_parse_4_save_2_db(parse):
    
    def stuff_ci(ci, sellerinfo, current_city):
        contacter_url = sellerinfo.get(SHCFEShopInfoConstant.contacter_url)
        contacterphonepicurl = sellerinfo.get(SHCFEShopInfoConstant.contacter_phone_url)
        ci.statustype = CarInfoValueConst.offline \
                if contacterphonepicurl is None else CarInfoValueConst.online 
        ci.title = sellerinfo.get(SHCFEShopInfoConstant.title)
        ci.declaredate = sellerinfo.get(SHCFEShopInfoConstant.declaretime)
        ci.fetchdatetime = datetime.datetime.now()
        ci.lastactivedatetime = datetime.datetime.now()
        ci.price = sellerinfo.get(SHCFEShopInfoConstant.price)
        ci.cartype = sellerinfo.get(SHCFEShopInfoConstant.cartype)
        ci.contacter = sellerinfo.get(SHCFEShopInfoConstant.contacter)
        ci.contacterurl = contacter_url
        ci.contacterphonepicname = sellerinfo.get(SHCFEShopInfoConstant.contacter_phone_picture_name)
        ci.carcolor = sellerinfo.get(SHCFEShopInfoConstant.car_color)
        ci.roadhaul = sellerinfo.get(SHCFEShopInfoConstant.road_haul)
        ci.displacement = sellerinfo.get(SHCFEShopInfoConstant.displacement)
        ci.gearbox = sellerinfo.get(SHCFEShopInfoConstant.gearbox)
        ci.licenseddate = sellerinfo.get(SHCFEShopInfoConstant.license_date)
        ci.sourceurl = sellerinfo.get(SHCFEShopInfoConstant.info_url)
        ci.cityname = current_city
        carsourcetype = sellerinfo.get(SHCFEShopInfoConstant.custom_flag)
        ci.carsourcetype = carsourcetype
        ci.contacterphonepicurl = contacterphonepicurl
        if contacterphonepicurl :
            ci.lastactivedatetime = datetime.datetime.now()
        else:
            ci.offlinedatetime = datetime.datetime.now()
        ci.sourcetype = u'58'
        
    @wraps(parse)
    def parse_simulate(self, response):
        rss = parse(self, response)
        if rss:
            for rs in rss:
                fs = FetchSession()
                ci_seqid = response.request.cookies[FetchConstant.CarInfo].seqid
                
                try:
                    if not isinstance(rs, Request):

                        contacter_url = rs.get(SHCFEShopInfoConstant.contacter_url)
                        
                        ci = fs.query(CarInfo).filter(
                                          CarInfo.seqid == ci_seqid).first()
                                          
                        si = fs.query(SellerInfo).filter(
                                 SellerInfo.sellerurl == contacter_url).first()

                        exist_ci = fs.query(CarInfo).filter(CarInfo.sourceurl == rs.get(SHCFEShopInfoConstant.info_url))\
                                            .filter(CarInfo.declaredate == rs.get(SHCFEShopInfoConstant.declaretime))\
                                            .filter(CarInfo.seqid != ci_seqid).first()
                        
                        stuff_ci(ci, rs, rs.get(SHCFEShopInfoConstant.cityname))
                        
                        if exist_ci:
                            self.log(u"give up and delete exist car info "
                                     "%s %s" % (ci.cityname, ci.seqid),
                                     log.INFO)
                            fs.delete(ci)
                        else:
                            
                            if si is not None:
                                ci.sellerid = si.seqid
                                self.log(u'stuff carinfo %s' % ci.seqid, log.INFO)
                                fs.add(ci)
                            else:
                                self.log(u'stuff carinfo %s' % ci.seqid, log.INFO)
                                ci.sellerid = gen_uuid()
                                fs.add(ci)
                                si = SellerInfo()
                                si.seqid = ci.sellerid
                                si.selleraddress = rs.get(SHCFEShopInfoConstant.shop_address)
                                si.sellerurl = contacter_url
                                fs.add(si)
                                
                except Exception as e:
                    self.log(u'something wrong %s' % str(e), log.CRITICAL)
                    fs.rollback()
                else:
                    fs.commit()
                finally:
                    fs.close()
    return parse_simulate

def detail_page_parse_4_change_status(parse):
    
    @wraps(parse)
    def parse_simulate(self, response):
        rss = parse(self, response)
        if rss:
            for rs in rss:
                fs = FetchSession()
                ci = response.request.cookies[FetchConstant.CarInfo]
                contacterphonepicurl = rs.get(SHCFEShopInfoConstant.contacter_phone_url)
                try:
                    if not contacterphonepicurl:
                        ci.statustype = CarInfoValueConst.offline
                        ci.offlinedatetime = datetime.datetime.now()
                    else:
                        ci.lastactivedatetime = datetime.datetime.now()
                    
                    fs.merge(ci)
                    
                except Exception as e:
                    self.log(u'something wrong %s' % str(e), log.CRITICAL)
                    fs.rollback()
                else:
                    if contacterphonepicurl:
                        msg = (u'change last active time %s '
                               '%s') % (ci.seqid, ci.sourceurl)
                    else:
                        msg = (u'offline %s '
                               '%s') % (ci.seqid, ci.sourceurl)
                    self.log(msg, log.INFO)
                    fs.commit()
                finally:
                    fs.close()
    return parse_simulate

def modify_carinfo(parse):
    
    @wraps(parse)
    def parse_simulate(self, response):
        fs = FetchSession()
        ci = response.request.cookies[FetchConstant.CarInfo]
        pic_dir = response.request.cookies[FetchConstant.pic_dir]
        
        if not os.path.exists(pic_dir):
            os.makedirs(pic_dir)
        
        pic_path = os.sep.join([pic_dir, ci.contacterphonepicname])
        if response.headers['Content-Type'] == u'image/gif':
            
            pic_content = response.body
            
            m = md5()
            m.update(pic_content)
            md5code = m.hexdigest()
            
            with open(pic_path, u'wb') as f:
                f.write(pic_content)
                
            try:
                ci.contacterphonepicmd5 = md5code
                fs.merge(ci)
            except Exception as e:
                fs.rollback()
                self.log(u'sth wrong pick phone pic %s %s' % (ci.seqid, str(e)), log.CRITICAL)
                raise e
            else:
                fs.commit()
                self.log(u'pick phone pic %s ' % ci.seqid, log.INFO)
            finally:
                fs.close()
            
        else:
            self.log(u'get nonbinary response body')
        
    return parse_simulate

def seller_page_parse_4_save_2_db(parse):
    
    def stuff_si(si, rs):
        si.selleraddress = rs.get(SHCFEShopInfoConstant.shop_address)
        si.sellername = rs.get(SHCFEShopInfoConstant.shop_name)
        si.sellerphone = rs.get(SHCFEShopInfoConstant.shop_phone)
        si.enterdate = rs.get(SHCFEShopInfoConstant.enter_time)
        
    @wraps(parse)
    def parse_simulate(self, response):
        rss = parse(self, response)
        if rss:
            for rs in rss:
                fs = FetchSession()
                try:
                    si = response.request.cookies[FetchConstant.SellerInfo]
                    if si is not None:
                        stuff_si(si, rs)
                    fs.merge(si)
                except Exception as e:
                    self.log(u'something wrong %s' % str(e), log.CRITICAL)
                    fs.rollback()
                    raise e
                else:
                    self.log(u'fetch seller info %s %s' % (si.seqid, si.sellerurl),
                             log.INFO)
                    fs.commit()
                finally:
                    fs.close()
    return parse_simulate

