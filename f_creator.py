#!/usr/bin/python3
import json
import rds_config_db
import pymysql
import datetime
import hashlib
import logging
import boto3
import os
import lxml
from lxml import etree
import progressbar

mydb = pymysql.connect(rds_config_db.host,rds_config_db.user,rds_config_db.passwd,rds_config_db.database)
bucket_name = rds_config_db.bucket_name

s3_bucket_location = rds_config_db.s3_bucket_location
s3_access_key = rds_config_db.s3_access_key
s3_secret_key = rds_config_db.s3_secret_key

s3_r = boto3.resource('s3')
s3_c = boto3.client('s3')

s3_img_location = "https://"+bucket_name+".s3."+s3_bucket_location+".amazonaws.com/img/"

def db_get_countries():
    mycursor = mydb.cursor()
    sql = "select distinct stream_status_country from stream_checks"
    mycursor.execute(sql)
    rows = mycursor.fetchall()
    return (rows)

def db_get_channels():
    mycursor = mydb.cursor()
    sql = "select stream_title from streams"
    mycursor.execute(sql)
    rows = mycursor.fetchall()
    return (rows)

def db_get_cats():
    mycursor = mydb.cursor()
    sql = "select cat_name,cat_img_sd,cat_img_hd from stream_cat_img"
    mycursor.execute(sql)
    rows = mycursor.fetchall()
    return (rows)

def upload_to_s3(temp_path,fname,the_type):
    fpath_orig = temp_path+"/"+fname
    filename = fname

    statinfo = os.stat(fpath_orig)
    up_progress = progressbar.progressbar.ProgressBar(maxval=statinfo.st_size)
    up_progress.start()
    def upload_progress(chunk):
        up_progress.update(up_progress.currval + chunk)

    if the_type == 'json':
        s3_c.upload_file(fpath_orig, bucket_name, filename,ExtraArgs={'ContentType': "application/json"},Callback=upload_progress)
    else:
        s3_c.upload_file(fpath_orig, bucket_name, filename,Callback=upload_progress)
    up_progress.finish()
    object_acl = s3_r.ObjectAcl(bucket_name,filename)
    response = object_acl.put(ACL='public-read')

def db_get_channels_by_country_and_cat(country,cat):

    try:
        curs = mydb.cursor()
        sql = 'select streams.stream_title, stream_cats.cat_name, stream_images.image_logo, streams.stream_path, streams.stream_type, sum(stream_scores.score) AS status, stream_scores.stream_status_country AS country from streams left join stream_scores on stream_scores.stream_path = streams.stream_path left join stream_images on stream_images.stream_name = streams.stream_title left join stream_cats on stream_cats.stream_name = streams.stream_title where stream_scores.stream_status_country = %s and stream_cats.cat_name = %s group by streams.stream_title,stream_scores.stream_status_country'
        curs.execute(sql,(country,cat))
        rows = curs.fetchall()
        return (rows)
    except Exception as e:
        print(e)


def db_get_channels_by_country(country):

    try:
        curs = mydb.cursor()
        sql = 'select v_streams.stream_title,stream_cats.cat_name,stream_images.image_logo,stream_log.stream_path,stream_log.stream_type,sum(stream_log.stream_status) AS status,stream_log.stream_status_country AS country from v_streams left join stream_log on stream_log.stream_title = v_streams.stream_title left join stream_images on stream_images.stream_name = v_streams.stream_title left join stream_cats on stream_cats.stream_name = v_streams.stream_title where stream_log.stream_status_timestamp > NOW()-INTERVAL 72 HOUR and stream_log.stream_status_country = %s group by stream_log.stream_title,stream_log.stream_status_country'
        curs.execute(sql,(country))
        rows = curs.fetchall()
        return (rows)
    except Exception as e:
        print(e)


def start_creation(t1,t2):
    countries = db_get_countries()
    cats = db_get_cats()
    for country in countries:
        fname_basic_xml = "greektv-active-"+country[0]+".xml"
        fname_roku_category_leaf_xml = "roku_category_leaf_"+country[0]+".xml"
        fname_roku_dp = "roku_dp_active_"+country[0]+".json"

#       basic xml feed

        data_basic_xml = {}
        data_basic_xml["providerName"] = "Greek TV xml feed"
#        channels = db_get_channels_by_country(country[0])

#       basic xml feed

#       roku category leaf xml
        data_el = etree.Element('categories')
        category_el = etree.SubElement(data_el, "category", title = 'Greek Broadcasting', sd_img = "test", hd_img = "test")
        for cat in cats:
            category_leaf_el = etree.SubElement(category_el, "categoryLeaf", title = cat[0].strip(), description='test', feed="test")
        data_roku_category_leaf = etree.tostring(data_el, encoding='utf-8', xml_declaration = False, pretty_print = True)
#       roku category leaf xml

#       roku direct publisher feed
        data_roku_dp = {}
        data_roku_dp["providerName"] = "Greek TV"
        data_roku_dp["language"] = "en-US"
        data_roku_dp["lastUpdated"] = datetime.datetime.now().replace(microsecond=0).isoformat()
        data_roku_dp["tvSpecials"] = []

        for cat in cats:
            channels = db_get_channels_by_country_and_cat(country[0],cat[0])
            for channel in channels:
                channel_title = str(channel[0])
                channel_cat = channel[1]
                channel_img = s3_img_location+channel[2]
                channel_path = channel[3]
                channel_type = channel[4]
                channel_active = int(channel[5])
                channel_country = channel[6]
                channel_cat = cat[0]

                if channel_type == "hls" and channel_active > 0.65:
                    data_roku_dp_content = []
                    data_roku_dp_videos = []
                    data_roku_dp_videos.append({
                    'url':channel_path,
                    'quality': "SD",
                    'videotype': channel_type
                })

                    data_roku_dp_content.append({
                    'dateAdded':datetime.datetime.now().replace(microsecond=0).isoformat(),
                    'captions': [],
                    'duration': "999",
                    'videos' : data_roku_dp_videos
                })

                    data_roku_dp["tvSpecials"].append({
                    'id': hashlib.md5((channel_title+channel_path).encode('utf-8')).hexdigest(),
                    'title': channel_title,
                    'shortDescription': "Enjoy "+channel_title+" from the "+channel_cat+" category. You may also view it on your computer using VLC or any other hls compatible audio/video player from : "+channel_path,
                    'thumbnail': channel_img,
                    'genres': ["special"],
                    'tags': [channel_cat],
                    'releaseDate': datetime.datetime.today().strftime('%Y-%m-%d'),
                    'content': data_roku_dp_content[0]
                })
#       roku direct publisher feed

#       basic xml feed



        with open("output/"+fname_basic_xml, "w") as outfile:
            json.dump(data_basic_xml, outfile)
            upload_to_s3('output',fname_basic_xml,'xml')
            os.remove('output'+"/"+fname_basic_xml)

        with open("output/"+fname_roku_category_leaf_xml, "w") as outfile:
            outfile.write(str(data_roku_category_leaf.decode('utf-8')))
#            print (data_roku_category_leaf.decode('utf-8'))
            upload_to_s3('output',fname_roku_category_leaf_xml,'xml')
            os.remove('output'+"/"+fname_roku_category_leaf_xml)


        with open("output/"+fname_roku_dp, "w") as outfile:
            json.dump(data_roku_dp, outfile)
            upload_to_s3('output/',fname_roku_dp,'json')
            os.remove("output/"+"/"+fname_roku_dp)
