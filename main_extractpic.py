# encoding=UTF-8
'''
Created on 2013-9-21
@author: Administrator
'''

from bot.config import configdata
from const import DetailSpiderConst
from crawler.shc.const import FetchConstant
import shutil


if __name__ == '__main__':
    pic_source_dir = configdata[DetailSpiderConst.PicSettings].get(FetchConstant.pic_dir)
    pic_extract_dir = configdata[DetailSpiderConst.PicSettings].get(FetchConstant.pic_extract_dir)
    
    size = 0 
    
    with open(r'picnames.txt', 'r') as f:
        for line in f.readlines():
            line = line.strip()
            shutil.copy(pic_source_dir + line, pic_extract_dir + line)
            size = size + 1
            print 'copy ' + line
            
    print 'total copy ' + unicode(size)
    
