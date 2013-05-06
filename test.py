# encoding=UTF-8
'''
Created on 2013-5-5
@author: Administrator
'''
import md5

#with open(u'', u'rb') as f:
#    lines = f.readlines()


m = md5.md5()
m.update(u'4312')
#m.update(lines)
print m.hexdigest()
    
