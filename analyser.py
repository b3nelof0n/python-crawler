# -*- coding: utf-8 -*-
from urlparse import urlparse
import lxml

__author__ = 'patmue'
import beanstalkc
import MySQLdb
from pyquery import PyQuery
import sys
import json
import nltk


reload(sys)
sys.setdefaultencoding('utf8')

# Connection
beanstalk = beanstalkc.Connection(host='localhost', port=11300)
beanstalk.watch('analyser_url')

def buildKeywordList(text, lenght):
        keylist = []
        list = []
        words = text.split()
        elements = len(words)
        iterations = elements / lenght
        for x in range(0, iterations, lenght):
            word = " ".join(words[x:x+lenght])
            if len(word) <= 3:
                continue
            if list.count(word):
                continue
            list.append(word)
            worddata = {
                  "word": word, "count": text.count(word)
                , "frequency": "{0:.4f}".format((float(text.count(word)) / float(elements)) * float(100))
            }
            keylist.append(worddata)
        return keylist

def getmetadata(pyq, token):
        elements = []
        for item in pyq.find('meta[ name="'+token+'" ]').items():
            try:
                if(None != item and None != item.attr('content')):
                    elements.append(item.attr('content'))
            except AttributeError:
                continue
            except KeyError:
                continue
        return elements

def linkelemt(pyq, token):
        elements = []
        for item in pyq.find('link[ rel="'+token+'" ]').items():
            try:
                if(None != item and None != item.attr('href')):
                    elements.append(item.attr('href'))
            except AttributeError:
                continue
            except KeyError:
                continue
        return elements

while True:
    job = beanstalk.reserve(timeout=0)
    if job:
        print "catch job" + str(job.body)
        db = MySQLdb.connect(host="localhost",user="root",passwd="xxxx",init_command='SET NAMES utf8mb4',use_unicode=True)
        cursor = db.cursor()
        sql = "SELECT id,content, domain FROM  spider.urllist WHERE id = "+job.body+" LIMIT 1"
        cursor.execute(sql)

        followLinks     = 0
        nofollowLinks   = 0
        externLinks     = 0
        internalLinks   = 0
        nofollow = 0

        for (id, content, domain) in cursor:
            print "found " +  str(id)
            try:
                xml = PyQuery(content)
            except AttributeError:
                continue
            except KeyError:
                continue
            except lxml.etree.ParserError:
                continue


            try:
                robots = getmetadata(xml, "robots")
                robots = robots[0].encode('latin1').decode('utf8')
                robots = str(robots)
            except AttributeError:
                robots = ""
            except KeyError:
                robots = ""

            if -1 == robots.lower().find("nofollow"):
                nofollow = 1

            for item in xml.find("a").items():

                try:
                    if(item.attr['rel'] == 'nofollow'):
                        #print "link no follow"
                        nofollowLinks += 1
                        continue
                except AttributeError:
                    continue
                except KeyError:
                    continue
                followLinks += 1
                callUrl = item.attr['href'].encode('latin1').decode('utf8')
                if callUrl[0] == "/":
                    callUrl = callUrl.replace("http://"+domain, "")
                    rightDomain =  "http://" + domain + callUrl
                else:
                    rightDomain = callUrl

                if -1 == rightDomain.find(domain):
                    externLinks += 1
                else:
                    internalLinks += 1

                if -1 == rightDomain.find("renego"):
                    continue
                #print "next " + rightDomain

                if nofollow == 0:
                    nofollowLinks = internalLinks + externLinks + nofollowLinks
                    continue
                jsonobject = json.dumps({"domain": rightDomain})
                beanstalkSecond = beanstalkc.Connection(host='localhost', port=11300)
                beanstalkSecond.use("url_list")
                beanstalkSecond.put(jsonobject)

            try:
                title = xml.find("title").text().encode('latin1').decode('utf8')
                title_lenght = str(len(title))
            except AttributeError:
                title_lenght = ""
            except KeyError:
                title_lenght = ""
            try:
                descr = getmetadata(xml, "description")
                descr = descr[0].encode('latin1').decode('utf8')
                descr_lenght = str(len(descr))
            except AttributeError:
                descr_lenght = ""
            except KeyError:
                descr_lenght = ""
            try:
                canoncical = linkelemt(xml, "canonical")
                canoncical = canoncical[0].encode('latin1').decode('utf8')
                canoncical = str(canoncical)
            except AttributeError:
                canoncical = ""
            except KeyError:
                canoncical = ""


            ads_onpage = 0
            try:
                for item in xml.find(".posts .post").items():
                    ads_onpage += 1
            except AttributeError:
                ads_onpage = 0
            except KeyError:
                ads_onpage = 0


            sposored_ads = 0
            try:
                for item in xml.find(".posts .post.sponsored").items():
                    sposored_ads += 1
            except AttributeError:
                sposored_ads = 0
            except KeyError:
                sposored_ads = 0


            text = nltk.util.clean_html(content)
            text = ' '.join(text.lower().split())
            kwlist = buildKeywordList(text, 1)
            kwlist = kwlist + buildKeywordList(text, 2)
            kwlist = kwlist + buildKeywordList(text, 3)
            sortedlist = sorted(kwlist, reverse=True, key=lambda k: float(k['frequency']))
            sortedlist = json.dumps(sortedlist[0:10])


            cursor.execute(""" UPDATE spider.urllist SET
            title_lenght = %s
            ,description_lenght = %s
            ,code_text_ratio = %s
            ,canonical = %s
            ,nofollow_links = %s
            ,extern_links = %s
            ,links_count = %s
            ,robot_tag = %s
            ,keyword_density = %s
            ,intern_links = %s

             WHERE id = %s LIMIT 1

             """,(
             title_lenght
            ,descr_lenght
            ,str(len(content)) + "/" + str(len(content) - len(text))
            ,canoncical
            ,str(nofollowLinks)
            ,str(externLinks)
            ,str(internalLinks + externLinks + nofollowLinks)
            ,robots
            ,sortedlist
            ,str(internalLinks)
            ,job.body
            ))

        db.commit()
        job.delete()