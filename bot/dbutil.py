# encoding=UTF-8
'''
Created on 2013-4-22
@author: Administrator
'''
from bot.config import configdata
from bot.item import CarInfo, SellerInfo
from const import FetchConst
from sqlalchemy import create_engine
from sqlalchemy.orm.session import sessionmaker
from bot.const import CarInfoValueConst

dbconfig = configdata[FetchConst.DBConfig]
# mysql://root:@localhost:3306/test
db_connect_str = (u'mysql+mysqldb://%s:%s@%s:%s/%s?charset='
                  u'%s') % (
                            dbconfig[FetchConst.DBConfig_user],
                            dbconfig[FetchConst.DBConfig_passwd],
                            dbconfig[FetchConst.DBConfig_host],
                            dbconfig[FetchConst.DBConfig_port],
                            dbconfig[FetchConst.DBConfig_dbname],
                            dbconfig[FetchConst.DBConfig_charactset],
                            )

engine = create_engine(db_connect_str, echo=False,
                       pool_size=dbconfig.get(FetchConst.DBConfig_poolsize, 2))
FetchSession = sessionmaker(bind=engine)

def get_unfetched_carinfo():

    fs = FetchSession()
    try:
        cis = fs.query(CarInfo).filter(CarInfo.statustype == None)\
        .limit(1000).all()
    except Exception as e:
        raise e
    finally:fs.close()
    
    return cis

def get_fetched_carinfo():

    fs = FetchSession()
    try:
        cis = fs.query(CarInfo).filter(CarInfo.statustype == CarInfoValueConst.online)\
        .order_by(CarInfo.declaredate).order_by(CarInfo.lastactivedatetime).limit(600).all()
    except Exception as e:
        raise e
    finally:fs.close()
    
    return cis

def get_unfetched_seller():

    fs = FetchSession()
    try:
        cis = fs.query(SellerInfo).filter(SellerInfo.enterdate == None)\
        .order_by(SellerInfo.ctime).limit(500).all()
    except Exception as e:
        raise e
    finally:fs.close()
    
    return cis

def get_unfetched_pic():

    fs = FetchSession()
    try:
        cis = fs.query(CarInfo).filter(CarInfo.contacterphonepicmd5 == None)\
            .filter(CarInfo.contacterphonepicurl != None)\
            .order_by(CarInfo.declaredate).limit(500).all()
    except Exception as e:
        raise e
    finally:fs.close()
    return cis
