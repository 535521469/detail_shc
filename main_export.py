# encoding=UTF-8
'''
Created on 2013-5-8
@author: Administrator
'''
from bot.config import configdata
from bot.const import CarInfoValueConst
from bot.dbutil import FetchSession
from bot.item import CarInfo, SellerInfo
from const import ExportConst, DetailSpiderConst
from crawler.shc.const import FetchConstant
from crawler.shc.fe.item import SHCFEShopInfoConstant as voconst
import csv
import datetime
import os
import shutil
from zipfile import ZipFile
import zipfile

def get_customer_fields():
    return [
          voconst.cityname,
          voconst.title,
          voconst.declaretime,
          voconst.price,
          voconst.cartype,
          voconst.contacter,
          voconst.contacter_url,
          voconst.contacter_phone_picture_name,
          voconst.car_color,
          voconst.road_haul,
          voconst.displacement,
          voconst.gearbox,
          voconst.license_date,
          voconst.shop_name,
          voconst.shop_address,
          voconst.shop_phone,
          voconst.enter_time,
          voconst.info_url,
          ]
    
def get_personal_fields():
    return [
          voconst.cityname,
          voconst.title,
          voconst.declaretime,
          voconst.price,
          voconst.cartype,
          voconst.contacter,
          voconst.contacter_url,
          voconst.contacter_phone_picture_name,
          voconst.car_color,
          voconst.road_haul,
          voconst.displacement,
          voconst.gearbox,
          voconst.license_date,
#          voconst.shop_name,
#          voconst.shop_address,
#          voconst.shop_phone,
          voconst.enter_time,
          voconst.info_url,
          ]
    
def get_customer_headers():
    return {
           voconst.cityname:u'城市名称',
           voconst.title:u'标题信息',
           voconst.declaretime:u'信息发布时间',
           voconst.price:u'价格',
           voconst.cartype:u'车型名称',
           voconst.contacter:u'联系人',
           voconst.contacter_url:u'联系人链接地址',
           voconst.contacter_phone_picture_name:u'联系方式图片文件名',
           voconst.car_color:u'车辆颜色',
           voconst.road_haul:u'行驶里程',
           voconst.displacement:u'车辆排量',
           voconst.gearbox:u'变速箱',
           voconst.license_date:u'上牌时间',
           voconst.shop_name:u'商户名称',
           voconst.shop_phone:u'商户电话',
           voconst.shop_address:u'商户地址',
           voconst.enter_time:u'入驻时间',
           voconst.info_url:u'信息原始链接地址',
           }
    
def get_personal_headers():
    return {
           voconst.cityname:u'城市名称',
           voconst.title:u'标题信息',
           voconst.declaretime:u'信息发布时间',
           voconst.price:u'价格',
           voconst.cartype:u'车型名称',
           voconst.contacter:u'联系人',
           voconst.contacter_url:u'联系人链接地址',
           voconst.contacter_phone_picture_name:u'联系方式图片文件名',
           voconst.car_color:u'车辆颜色',
           voconst.road_haul:u'行驶里程',
           voconst.displacement:u'车辆排量',
           voconst.gearbox:u'变速箱',
           voconst.license_date:u'上牌时间',
#           voconst.shop_name:u'商户名称',
#           voconst.shop_phone:u'商户电话',
#           voconst.shop_address:u'商户地址',
           voconst.enter_time:u'注册时间',
           voconst.info_url:u'信息原始链接地址',
           }

def get_export_date():
    export_date = configdata[ExportConst.export_config].get(ExportConst.export_config_date)
    
    export_date = export_date  if export_date else datetime.date.today() - datetime.timedelta(days=1)
    try:
        export_date = datetime.datetime.strptime(export_date, u'%Y-%m-%d').date()
    except :
        export_date = datetime.date.today() - datetime.timedelta(days=1)
    return export_date

export_date = get_export_date()

customer_fields = get_customer_fields()
personal_fields = get_personal_fields()

export_dir = configdata[ExportConst.export_config].get(ExportConst.export_config_dir)

pic_source_dir = configdata[DetailSpiderConst.PicSettings].get(FetchConstant.pic_dir)

def write_header(file_path, car_source_type=CarInfoValueConst.car_source_shop):
    with open(file_path, u'w') as f:
        if car_source_type == CarInfoValueConst.car_source_shop:
            dw = csv.DictWriter(f, customer_fields)
            dw.writerow(get_customer_headers())
        else :
            dw = csv.DictWriter(f, personal_fields)
            dw.writerow(get_personal_headers())

def get_shop_cis():
    fs = FetchSession()
    try:
        shop_cis = fs.query(CarInfo, SellerInfo).filter(CarInfo.sellerid == SellerInfo.seqid)\
        .filter(CarInfo.carsourcetype.declaredate == export_date)\
        .filter(CarInfo.carsourcetype == CarInfoValueConst.car_source_shop)\
        .filter(CarInfo.title != None).filter(CarInfo.statustype != None).all()
        return shop_cis
    finally:
        fs.close()
    return []

def get_cis(car_source_type=CarInfoValueConst.car_source_shop):
    fs = FetchSession()
    try:
        shop_cis = fs.query(CarInfo, SellerInfo).filter(CarInfo.sellerid == SellerInfo.seqid)\
        .filter(CarInfo.declaredate == export_date)\
        .filter(CarInfo.carsourcetype == car_source_type)\
        .filter(CarInfo.title != None).filter(CarInfo.statustype != None).all()
        return shop_cis
    finally:
        fs.close()
    return []

def write_csv(cis, filedir, shopflag=CarInfoValueConst.car_source_shop):
    dcis = rebuild_ci_dicts(cis, shopflag)
    dcis = sorted(dcis, key=lambda x:x[voconst.cityname])
    if shopflag == CarInfoValueConst.car_source_shop:
        fields = customer_fields
    else:
        fields = personal_fields
        
    with open(filedir, u'a') as f:
        dw = csv.DictWriter(f, fields)
        for dci in dcis:
            dw.writerow(dci)

def get_subfolder_suffix(shopflag=CarInfoValueConst.car_source_shop):
    if shopflag == CarInfoValueConst.car_source_shop:
        subfolder_suffix = u'customer'
    else:
        subfolder_suffix = u'personal'
    return subfolder_suffix

def get_export_date_dir(export_date):
    date_str = export_date.strftime(u'%Y%m%d')
    export_date_dir = os.sep.join([export_dir, date_str])
    if not os.path.exists(export_date_dir):
        os.makedirs(export_date_dir,)
    return export_date_dir

def create_dirs(export_date, shopflag=CarInfoValueConst.car_source_shop):
    
    export_date_dir = get_export_date_dir(export_date)
    
    date_str = export_date.strftime(u'%Y%m%d')
    
    subfolder_suffix = get_subfolder_suffix(shopflag)
        
    export_csv_folder_name = u'total%s%s' % (date_str, subfolder_suffix)
    export_csv_folder = os.sep.join([export_date_dir, export_csv_folder_name])
    if not os.path.exists(export_csv_folder):
        os.mkdir(export_csv_folder)
        
    export_pic_folder_name = u'total%s%sPIC' % (date_str, subfolder_suffix)
    export_pic_folder = os.sep.join([export_date_dir, export_pic_folder_name])
    if not os.path.exists(export_pic_folder):
        os.mkdir(export_pic_folder)
    
    return export_csv_folder, export_pic_folder
    
def get_csv_path_and_pic_dir(export_date, shopflag=CarInfoValueConst.car_source_shop):
    export_csv_folder, export_pic_folder = create_dirs(export_date, shopflag)
    date_str = export_date.strftime(u'%Y%m%d')
    subfolder_suffix = get_subfolder_suffix(shopflag)
    csv_file_name = u'total%s%s.csv' % (date_str, subfolder_suffix)
    export_csv_path = os.sep.join([export_csv_folder, csv_file_name])
    return export_csv_path, export_pic_folder

def copy_pics(cis, export_pic_folder):
    for ci, si in cis:
        pic_name = ci.contacterphonepicname
        src = os.sep.join([pic_source_dir , pic_name])
        dst = os.sep.join([export_pic_folder, pic_name])
        try:
            shutil.copy(src, dst)
        except :
            pass
    
#===============================================================================
# --20130507
# ----total20130507customer
# ------total20130507customer.csv
# ----total20130507customerPIC
# ------***.gif
# ------***.gif
#===============================================================================

def rebuild_ci_dicts(cis, shopflag=CarInfoValueConst.car_source_shop):
    dcis = []
    for ci, si in cis:
        dci = {
           voconst.cityname:ci.cityname,
           voconst.title:ci.title,
           voconst.declaretime:ci.declaredate,
           voconst.price:ci.price,
           voconst.cartype:ci.cartype,
           voconst.contacter:ci.contacter,
           voconst.contacter_url:ci.contacterurl,
           voconst.contacter_phone_picture_name:ci.contacterphonepicname,
           voconst.car_color:ci.carcolor,
           voconst.road_haul:ci.roadhaul,
           voconst.displacement:ci.displacement,
           voconst.gearbox:ci.gearbox,
           voconst.license_date:ci.licenseddate,
           voconst.shop_name:si.sellername,
           voconst.shop_phone:si.sellerphone,
           voconst.shop_address:si.selleraddress,
           voconst.enter_time:si.enterdate,
           voconst.info_url:ci.sourceurl,
           }
        if shopflag != CarInfoValueConst.car_source_shop:
            del dci[voconst.shop_phone]
            del dci[voconst.shop_name]
            del dci[voconst.shop_address]
        dcis.append(dci)
    return dcis

def export_shop_cis():
    shop_cis = get_cis()
    export_csv_path, export_pic_folder = get_csv_path_and_pic_dir(export_date,)
    copy_pics(shop_cis, export_pic_folder)
    write_header(export_csv_path)
    write_csv(shop_cis, export_csv_path)

def export_individual_cis():
    shop_cis = get_cis(car_source_type=CarInfoValueConst.car_source_individual)
    export_csv_path, export_pic_folder = get_csv_path_and_pic_dir(export_date,
                                      CarInfoValueConst.car_source_individual)
    copy_pics(shop_cis, export_pic_folder)
    write_header(export_csv_path, CarInfoValueConst.car_source_individual)
    write_csv(shop_cis, export_csv_path, CarInfoValueConst.car_source_individual)

def send_email():
    pass
        
if __name__ == '__main__':
    
    export_date_dir = get_export_date_dir(export_date)
    export_shop_cis()
    export_individual_cis()
    date_str = export_date.strftime(u'%Y%m%d')
    
    filelist=[]
    for root, dirs, files in os.walk(export_date_dir):
        for name in files:
            filelist.append(os.path.join(root, name))
    with ZipFile(export_date_dir + u'.zip', u'a', compression=zipfile.ZIP_DEFLATED) as f:
        for tar in filelist:
            arcname = tar[len(export_date_dir):]
            f.write(tar,arcname)
