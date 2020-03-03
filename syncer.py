#!/usr/bin/python3
import sys
import urllib
import urllib.request
from urllib.parse import urlparse
from contextlib import closing
from botocore.vendored import requests
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
def_addr2 = target_addr.def_addr_2
mydb = pymysql.connect(rds_config_db.host,rds_config_db.user,rds_config_db.passwd,rds_config_db.database)


def enterdb_log(title,img,path,type):
    mycursor = mydb.cursor()
    sql = "INSERT INTO streams (stream_title,logo,stream_path,stream_type) VALUES (%s,%s,%s,%s) ON DUPLICATE KEY UPDATE stream_path = %s"
    val = (title,img,path,type,path)
    mycursor.execute(sql, val)
    mydb.commit()


def find_media_type(path):
    the_uri = path.strip()
    first_let = the_uri[:3]
    last_let = the_uri[-2:]
#    print (first_let,last_let)
    if first_let == "htt":
        if "youtube.com" in path:
            return 'youtube'
        else:
            if last_let == 'u8':
                return 'hls'
            elif last_let == "ts":
                return 'ts'
            elif last_let == "ls":
                return 'pls'
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


def parse2(uri):
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
def start_sync(t1,t2):
    m3ufile = def_addr
#    m3ufile2 = def_addr2
    index=1
#    index2=1
    playlist = parse(m3ufile)
#    playlist2 = parse2(m3ufile2)

    for item in playlist:
        if(item.title is not None):
            path = item.path.strip()
            title = item.title.strip()
            p = re.compile("tvg-logo=\"(.*)\"")
            if item.length:
                result = p.search(item.length)
                img = result.group(1)
                if (img != ''):
                    img_ext = fileExt(img)
                    img_fname = title.replace(' ','').lower()
                else:
                    img_ext = none
                    img_fname = none
                img_to_store = img_fname+img_ext
                media_type = find_media_type(path)
                if title:
                    enterdb_log(title,img,path,media_type)
                index += 1


#    for item in playlist2:
#        if(item.title is not None):
#            path = item.path.strip()
#            title = item.title.strip()
#            if item.length:
#                media_type = find_media_type(path)
#                if title:
#                    enterdb_log(title,img,path,media_type)
#                index += 1
