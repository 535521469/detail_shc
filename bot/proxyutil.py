# encoding=UTF-8
'''
Created on 2013-4-29
@author: Administrator
'''
from bot.const import HTTPProxyValueConst
from bot.dbutil import FetchSession
from bot.item import HTTPProxy
import datetime
from sqlalchemy import desc

def get_valid_proxy_fun():

    fs = FetchSession()
    try:
        proxies = fs.query(HTTPProxy).filter(HTTPProxy.validflag == HTTPProxyValueConst.validflag_yes)\
        .filter(HTTPProxy.fetchdate == datetime.date.today()).order_by(desc(HTTPProxy.validdatetime)).all()
    except Exception as e:
        raise e
    finally:fs.close()
    
    if not proxies:
        proxies = []

    def search_proxies(proxies=[]):
        while 1:
            for proxy in proxies:
                yield proxy
            else:
                yield None
    
                fs = FetchSession()
                try:
                    proxies = fs.query(HTTPProxy).filter(HTTPProxy.validflag == HTTPProxyValueConst.validflag_yes)\
                    .filter(HTTPProxy.fetchdate == datetime.date.today()).order_by(desc(HTTPProxy.validdatetime)).all()
                except Exception as e:
                    raise e
                finally:fs.close()
                
                if not proxies:
                    proxies = []
            
    return search_proxies(proxies=proxies)

get_valid_proxy = get_valid_proxy_fun()
