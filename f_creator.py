#!/usr/bin/python3
import json
import rds_config_db
import pymysql
import datetime
import hashlib
import logging
import boto3
import os

mydb = pymysql.connect(rds_config_db.host,rds_config_db.user,rds_config_db.passwd,rds_config_db.database)
bucket_name = rds_config_db.bucket_name

aws_endpoint = rds_config_db.aws_endpoint
s3_access_key = rds_config_db.s3_access_key
s3_secret_key = rds_config_db.s3_secret_key

s3_r = boto3.resource('s3')
s3_c = boto3.client('s3')


#s3 = boto3.client('s3')

#conn = tinys3.Connection(s3_access_key,s3_secret_key,tls=True,endpoint=aws_endpoint)

def db_get_countries():
    mycursor = mydb.cursor()
    sql = "select country from v_countries"
    mycursor.execute(sql)
    rows = mycursor.fetchall()
    return (rows)

def db_get_channels():
    mycursor = mydb.cursor()
    sql = "select stream_title from v_streams"
    mycursor.execute(sql)
    rows = mycursor.fetchall()
    return (rows)

def db_get_cats():
    mycursor = mydb.cursor()
    sql = "select cat_name from v_cats"
    mycursor.execute(sql)
    rows = mycursor.fetchall()
    return (rows)

def upload_to_s3(temp_path,fname):
    fpath_orig = temp_path+"/"+fname
    filename = fname
    s3_c.upload_file(fpath_orig, bucket_name, filename)
    object_acl = s3_r.ObjectAcl(bucket_name,filename)
    response = object_acl.put(ACL='public-read')

def db_get_channels_by_country(country,cat):

    try:
        curs = mydb.cursor()
        sql = 'select v_streams.stream_title,stream_cats.cat_name,stream_images.image_logo,stream_log.stream_path,stream_log.stream_type,sum(stream_log.stream_status) AS status,stream_log.stream_status_country AS country from v_streams left join stream_log on stream_log.stream_title = v_streams.stream_title left join stream_images on stream_images.stream_name = v_streams.stream_title left join stream_cats on stream_cats.stream_name = v_streams.stream_title where stream_log.stream_status_timestamp > NOW()-INTERVAL 5 HOUR and stream_log.stream_status_country = %s and stream_cats.cat_name = %s group by stream_log.stream_title,stream_log.stream_status_country'
        curs.execute(sql,(country,cat))
        rows = curs.fetchall()
        return (rows)
    except Exception as e:
        print(e)


def start_creation(t1,t2):
    countries = db_get_countries()
    cats = db_get_cats()
    for country in countries:
        fname_basic_xml = "greektv-active-"+country[0]+".xml"
        fname_roku_dp = "roku_dp_active_"+country[0]+".json"



#       basic xml feed
        data_basic_xml = {}
        data_basic_xml["providerName"] = "Greek TV xml feed"


#       roku direct publisher feed
        data_roku_dp = {}
        data_roku_dp["providerName"] = "Greek TV"
        data_roku_dp["language"] = "en-US"
        data_roku_dp["lastUpdated"] = datetime.datetime.now().replace(microsecond=0).isoformat()
        data_roku_dp["tvSpecials"] = []

        for cat in cats:
            channels = db_get_channels_by_country(country[0],cat[0])
            for channel in channels:
                channel_title = str(channel[0])
                channel_cat = channel[1]
                channel_img = channel[2]
                channel_path = channel[3]
                channel_type = channel[4]
                channel_active = int(channel[5])
                channel_country = channel[6]
                channel_cat = cat[0]

#       roku direct publisher feed
                if channel_type == "hls" and channel_active > 0:
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


        with open("output/"+fname_basic_xml, "w") as outfile:
            json.dump(data_basic_xml, outfile)
            upload_to_s3('output',fname_basic_xml)
            os.remove('output'+"/"+fname_basic_xml)

        with open("output/"+fname_roku_dp, "w") as outfile:
            json.dump(data_roku_dp, outfile)
            upload_to_s3('output/',fname_roku_dp)
            os.remove("output/"+"/"+fname_roku_dp)
