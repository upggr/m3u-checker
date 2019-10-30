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

def enterdb_log(title,path,type,status,country):
    mycursor = mydb.cursor()
    sql = "INSERT INTO stream_log (stream_title,stream_path,stream_type,stream_status,stream_status_country) VALUES (%s,%s,%s,%s,%s)"
    val = (title,path,type,status,country)
    mycursor.execute(sql, val)
    mydb.commit()

def cleanup_logs():
    mycursor = mydb.cursor()
    sql = "delete from stream_log where stream_status_timestamp < DATE_ADD(CURDATE(), INTERVAL -3 DAY)"
    mycursor.execute(sql)
    mydb.commit()

cleanup_logs()

def update_img(title,image_logo):
    mycursor = mydb.cursor()
    sql = "INSERT INTO stream_images (stream_name,image_logo) VALUES (%s,%s) ON DUPLICATE KEY UPDATE image_logo = IF(override = 1,VALUES(image_logo),image_logo)"
    val = (title,image_logo)
    mycursor.execute(sql, val)
    mydb.commit()

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

def check_status(path,media_type):
    if media_type == 'hls':
        try:
            r = requests.head(path, timeout=5)
            if  (r.status_code == 200) or (r.status_code == 405):
                return 1
            else:
                return 0
        except Exception as ex:
#            raise Exception("timeout", ex)
#            print("timeout")
            return 0
            pass
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

def find_media_type(path):
    the_uri = path.strip()
    first_let = the_uri[:3]
    last_let = the_uri[-2:]
#    print (first_let,last_let)
    if first_let == "htt":
        if "youtu" in path:
            return 'youtube'
        else:
            if last_let == 'u8':
                return 'hls'
            elif last_let == "ts":
                return 'ts'
            else:
                return 'unknown http'
    elif  first_let == "rtm":
        return 'rtmp'
    elif  first_let == "rts":
        return 'rtsp'
    elif  first_let == "ws:":
        return 'softvelum'
    else:
        return 'unknown protocol'

def fileExt( url ):
    reQuery = re.compile( r'\?.*$', re.IGNORECASE )
    rePort = re.compile( r':[0-9]+', re.IGNORECASE )
    reExt = re.compile( r'(\.[A-Za-z0-9]+$)', re.IGNORECASE )
    url = reQuery.sub( "", url )
    url = rePort.sub( "", url )
    matches = reExt.search( url )
    if None != matches:
        return matches.group( 1 )
    return None

def get_youtube_restriction(api_url):
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


#if __name__ == '__main__':
def start_checks(t1,t2):
    m3ufile = def_addr
    index=1
    playlist = parse(m3ufile)

    for item in playlist:
        path = item.path
        title = item.title.strip()
        p = re.compile("tvg-logo=\"(.*)\"")
        if item.length:
            result = p.search(item.length)
            img = result.group(1)
            if (img != ''):
                img_ext = fileExt(img)
                img_fname = title.replace(' ','').lower()
                img_file = urllib.request.urlopen(img)
            else:
                img_ext = none
                img_fname = none
                img_to_store = img_fname+img_ext
                update_img(title,img_to_store)
        media_type = find_media_type(path)
#        if media_type == 'youtube':
        uri_status = check_status(path,media_type)
        if title:
            enterdb_log(title,path,media_type,uri_status,country)

        print ("path : "+item.path)
        print ("title : "+item.title)
#        print (uri_status)
#        print ("media type : "+media_type)
#        print ("img ext : "+img_ext)
#        print ("img : "+img)
#        print ("img filename : "+img_fname)
#        print ("img to store : "+img_to_store)
#        print ("country : "+country)
#        print ('----------')
        index += 1
