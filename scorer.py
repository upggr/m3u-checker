#!/usr/bin/python3
import sys
import urllib
import urllib.request
from urllib.parse import urlparse
import codecs
from contextlib import closing
#from botocore.vendored import requests
import requests
import datetime
import time
import json
import rds_config_db
import target_addr
import pymysql
import re
from urllib.parse import urlparse

class Track:
    def __init__(self, length, title, path):
        self.length = length
        self.title = title
        self.path = path

def_addr = target_addr.def_addr
def_youtube_api_key = rds_config_db.youtube_api

mydb = pymysql.connect(rds_config_db.host,rds_config_db.user,rds_config_db.passwd,rds_config_db.database)

def check_country():
    url = "https://geoip-db.com/json"
    response = urllib.request.urlopen(url)
    data = json.loads(response.read())
    return data['country_code']

country = check_country()

def enterdb_log(path,status,status_code,country):
    mycursor = mydb.cursor()
    sql = "INSERT INTO stream_checks (stream_path,stream_status,stream_status_code,stream_status_country) VALUES (%s,%s,%s,%s)"
    val = (path,status,status_code,country)
    mycursor.execute(sql, val)
    mydb.commit()

def get_paths():
    mycursor = mydb.cursor()
    sql = "SELECT distinct stream_path,stream_type,stream_title from streams"
    mycursor.execute(sql)
    return mycursor.fetchall()

returned_paths = get_paths()

def cleanup_logs():
    mycursor = mydb.cursor()
    sql = "delete from stream_checks where stream_status_timestamp < DATE_ADD(CURDATE(), INTERVAL -5 DAY)"
    mycursor.execute(sql)
    mydb.commit()

cleanup_logs()

def video_id(url):
    o = urlparse(url)
    if o.netloc == 'youtu.be':
        return o.path[1:]
    elif o.netloc in ('www.youtube.com', 'youtube.com'):
        if o.path == '/watch':
            id_index = o.query.index('v=')
            return o.query[id_index+2:id_index+13]
        elif o.path[:7] == '/embed/':
            return o.path.split('/')[2]
        elif o.path[:3] == '/v/':
            return o.path.split('/')[2]
    return None  # fail?


def check_status_code(path):
    try:
        r1 = requests.head(path, timeout=5)
        r2 = requests.request('GET', path, timeout=5)
        if  (r1.status_code == 500) :
            return r2.status_code
        else :
            return r1.status_code
    except Exception as ex:
#            raise Exception("timeout", ex)
#            print("timeout")
        return 0
        pass

def check_status(path,media_type):
    if media_type == 'hls':
        st = check_status_code(path)
#        print(path,st)
        if  (st == 200) or (st == 405) or (st == 301) or (st == 302):
            return 1
        else :
            return 0
    elif media_type == 'youtube':
        vid_id = video_id(path)
        api_constr = "https://www.googleapis.com/youtube/v3/videos?part=contentDetails&id="+vid_id+"&key="+def_youtube_api_key
        restr = get_youtube_restriction(api_constr)
        if restr == 'nores':
            return 1
        elif restr == 'dead':
            return 0
        else:
            if country in restr:
                return 0
            else:
                return 1
    else:
        return 1


def get_youtube_restriction(api_url):
#    print (api_url)
    with urllib.request.urlopen(api_url) as url:
        data = json.loads(url.read())
        if len(data['items']) > 0:
            if 'regionRestriction' not in data['items'][0]['contentDetails']:
                return 'nores'
            else:
                if 'blocked' not in data['items'][0]['contentDetails']['regionRestriction']:
                    return 'nores'
                else:
                    return data['items'][0]['contentDetails']['regionRestriction']['blocked']
        else:
            return 'dead'

def is_url(url):
  try:
    result = urlparse(url)
    return all([result.scheme, result.netloc])
  except ValueError:
    return False


def parse(uri):
    with closing(urllib.request.urlopen(uri)
                 if is_url(uri)
                else open(uri, 'r', encoding='utf8')) as inf:

        playlist = []
        song = Track(None, None, None)
        for line_no, line in enumerate(inf):
            try:
                line = line.decode("utf-8")
                if line.startswith('#EXTINF:'):
                    length, title = line.split('#EXTINF:')[1].split(',', 1)
                    song = Track(length, title, None)
                elif line.startswith('#'):
                    pass
                elif len(line) != 0:
                    song.path = line
                    playlist.append(song)
                    song = Track(None, None, None)
            except Exception as ex:
                raise Exception("Can't parse line %d: %s" % (line_no, line), ex)
    return playlist


def start_scoring(t1,t2):
    index=1
    for item in returned_paths:
#        print(item[2].strip())
        path = item[0].strip()
        stream_type = item[1].strip()
        uri_status = check_status(path,stream_type)
        if stream_type == 'hls' :
            uri_status_code = check_status_code(path)
        else :
            uri_status_code = 'NA'
        if path:
            enterdb_log(path,uri_status,uri_status_code,country)
#        print ("path : "+path)
#        print ("type : "+stream_type)
#        print (uri_status)
#        print ('----------')
        index += 1
