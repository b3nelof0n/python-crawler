# -*- coding: utf-8 -*-
import binascii
import sys
from urlparse import urlparse

reload(sys)
sys.setdefaultencoding('utf8')
__author__ = 'patmue'
import beanstalkc
import pycurl
from BeautifulSoup import BeautifulSoup
import MySQLdb
from StringIO import StringIO
import json

# Connection
beanstalk = beanstalkc.Connection(host='localhost', port=11300)
beanstalk.watch('url_list')

def info(curl):
        "Return a dictionary with all info on the last response."
        m = {}
        m['effective_url'] = curl.getinfo(pycurl.EFFECTIVE_URL)
        m['http_code'] = curl.getinfo(pycurl.HTTP_CODE)
        m['total_time'] = curl.getinfo(pycurl.TOTAL_TIME)
        m['namelookup_time'] = curl.getinfo(pycurl.NAMELOOKUP_TIME)
        m['connect_time'] = curl.getinfo(pycurl.CONNECT_TIME)
        m['pretransfer_time'] = curl.getinfo(pycurl.PRETRANSFER_TIME)
        m['redirect_time'] = curl.getinfo(pycurl.REDIRECT_TIME)
        m['redirect_count'] = curl.getinfo(pycurl.REDIRECT_COUNT)
        m['size_upload'] = curl.getinfo(pycurl.SIZE_UPLOAD)
        m['size_download'] = curl.getinfo(pycurl.SIZE_DOWNLOAD)
        m['speed_upload'] = curl.getinfo(pycurl.SPEED_UPLOAD)
        m['header_size'] = curl.getinfo(pycurl.HEADER_SIZE)
        m['request_size'] = curl.getinfo(pycurl.REQUEST_SIZE)
        m['content_length_download'] = curl.getinfo(pycurl.CONTENT_LENGTH_DOWNLOAD)
        m['content_length_upload'] = curl.getinfo(pycurl.CONTENT_LENGTH_UPLOAD)
        m['content_type'] = curl.getinfo(pycurl.CONTENT_TYPE)
        m['response_code'] = curl.getinfo(pycurl.RESPONSE_CODE)
        m['speed_download'] = curl.getinfo(pycurl.SPEED_DOWNLOAD)
        m['ssl_verifyresult'] = curl.getinfo(pycurl.SSL_VERIFYRESULT)
        m['filetime'] = curl.getinfo(pycurl.INFO_FILETIME)
        m['starttransfer_time'] = curl.getinfo(pycurl.STARTTRANSFER_TIME)
        m['redirect_time'] = curl.getinfo(pycurl.REDIRECT_TIME)
        m['redirect_count'] = curl.getinfo(pycurl.REDIRECT_COUNT)
        m['http_connectcode'] = curl.getinfo(pycurl.HTTP_CONNECTCODE)
        m['httpauth_avail'] = curl.getinfo(pycurl.HTTPAUTH_AVAIL)
        m['proxyauth_avail'] = curl.getinfo(pycurl.PROXYAUTH_AVAIL)
        m['os_errno'] = curl.getinfo(pycurl.OS_ERRNO)
        m['num-connects'] = curl.getinfo(pycurl.NUM_CONNECTS)
        m['ssl_engines'] = curl.getinfo(pycurl.SSL_ENGINES)
        m['cookielist'] = curl.getinfo(pycurl.INFO_COOKIELIST)
        m['lastsocket'] = curl.getinfo(pycurl.LASTSOCKET)
        m['ftp_entry_path'] = curl.getinfo(pycurl.FTP_ENTRY_PATH)
        return m

while True:
    job = beanstalk.reserve(timeout=0)
    # print (job.body)
    #print(job.delete())
    skip = 0
    if job:
        # print (job.body)
        content = json.loads(job.body)
        url = content["domain"].encode("utf8")
        parent_domain = content["parent_domain"].encode("utf8")
        if(-1 != job.body.find("#")):
            continue


        print "catch job" + url
        db = MySQLdb.connect(host="localhost", user="root", passwd="xxxxx", init_command='SET NAMES utf8mb4',
                     use_unicode=True)

        cursor = db.cursor()
        cursor.execute(""" SELECT id FROM spider.urllist WHERE crc_url = %s AND url = %s
            LIMIT 1
             """,(
             str(binascii.crc32(url))
            ,url

            ))
        for (id) in cursor:
            if id > 0:
                skip = 1
                print "skip " + url

        if skip == 0:
            buffer = StringIO()
            c = pycurl.Curl()
            c.setopt(c.URL, url)
            c.setopt(c.WRITEDATA, buffer)
            c.setopt(c.CONNECTTIMEOUT, 500)
            c.setopt(c.AUTOREFERER, 1)
            c.setopt(c.FOLLOWLOCATION, 1)
            c.setopt(c.COOKIEFILE, '')
            c.setopt(c.MAXREDIRS, 10)
            CURLOPT_ENCODING = getattr(pycurl, 'ACCEPT_ENCODING', pycurl.ENCODING)
            c.setopt(CURLOPT_ENCODING, "utf-8")

            c.setopt(c.TIMEOUT, 500)
            c.setopt(c.USERAGENT, 'Crawler: Patmue v1.0 (http://www.x303.de) ')
            c.perform()

            curinfo = info(c)

            # HTTP response code, e.g. 200.

            c.close()
            text = buffer.getvalue()
            soup = BeautifulSoup(text)
            text = text.decode(soup.originalEncoding)

            cursor.execute("""INSERT INTO spider.urllist (
                  `url`
                , `crc_url`
                , `content`
                , `domain`
                , `http_code`
                , `total_time`
                , `namelookup_time`
                , `connect_time`
                , `pretransfer_time`
                , `redirect_time`
                , `redirect_count`
                , `size_download`
                , `header_size`
                , `content_length_download`
                , `content_type`
                , `response_code`
                , `speed_download`
                , `filetime`
                , `http_connectcode`
            )
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (    url
                   , binascii.crc32(url)
                   , text.decode('Latin-1').decode('utf8')
                   , str(urlparse(url).hostname)
                   , curinfo['http_code']
                   , curinfo['total_time']
                   , curinfo['namelookup_time']
                   , curinfo['connect_time']
                   , curinfo['pretransfer_time']
                   , curinfo['redirect_time']
                   , curinfo['redirect_count']
                   , curinfo['size_download']
                   , curinfo['header_size']
                   , curinfo['content_length_download']
                   , curinfo['content_type']
                   , curinfo['response_code']
                   , curinfo['speed_download']
                   , curinfo['filetime']
                   , curinfo['http_connectcode']
                   )
            )
            db.commit()
            #parent_domain
            cursor.execute("""INSERT INTO spider.link_list (
                  `crc_root_domain`
                , `root_domain`
                , `crc_parent_domain`
                , `parent_domain`
            )
                    VALUES (%s,%s,%s,%s)""",
                (    binascii.crc32(url)
                   , url
                   , binascii.crc32(parent_domain)
                   , parent_domain
                   )
            )
            db.commit()

            beanstalkSecond = beanstalkc.Connection(host='localhost', port=11300)
            beanstalkSecond.use("analyser_url")
            beanstalkSecond.put(str(cursor.lastrowid))
        skip = 0
        job.delete()
print "no job"
