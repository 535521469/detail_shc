BOT_NAME = 'SHCSpider'
SPIDER_MODULES = ['crawler.shc.fe.spiders', ]
LOG_ENCODING = u'UTF-8'

RETRY_TIMES = 500
DOWNLOADER_MIDDLEWARES = {
                          'crawler.shc.fe.middlewares.ProxyRetryMiddleWare':450,
                          'scrapy.contrib.downloadermiddleware.retry.RetryMiddleware':None,
                          'crawler.shc.fe.middlewares.PopularRedirectMiddleWare':600,
                          'scrapy.contrib.downloadermiddleware.redirect.RedirectMiddleware':None,
                           }

DOWNLOAD_TIMEOUT=2
DOWNLOAD_DELAY=0

USER_AGENT='Mozilla/5.0 (Windows NT 6.1; WOW64; rv:20.0) Gecko/20100101 Firefox/20.0'