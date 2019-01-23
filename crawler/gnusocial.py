#!/usr/bin/python

import json
import urllib2
import MySQLdb
import re
from elasticsearch import Elasticsearch
from datetime import datetime
from dateutil.parser import parse
import pytz
import threading
import os
import time

fonts_path="/home/osint"

#################################################################

def convert(obj):

  if isinstance(obj, bool):
    return str(obj).lower()
  if isinstance(obj, (list, tuple)):
    return [convert(item) for item in obj]
  if isinstance(obj, dict):
    return {convert(key):convert(value) for key, value in obj.items()}

  return obj

#################################################################

def indexa(host,idgrup,data):

  global es

  timestamp=data["created_at"]
  timestamp=parse(timestamp)
  timestamp=timestamp.astimezone(pytz.utc)

  indexname="gnusocial-%s" % (timestamp.strftime("%Y-%m-%d"))
  timestamp=timestamp.strftime('%Y-%m-%dT%H:%M:%S.%f')

  user_name=data["user"]["name"]
  user_screen_name=data["user"]["screen_name"]
  text=data["text"]
  message_id=data["id"]

  print "Indexa %d %s" % (idgrup,indexname)

  data=json.dumps(data)

  es.index(
    index=indexname,
    doc_type="gnusocial",
    body={
      "host": host,
      "message_id": message_id,
      "grup_id": idgrup,
      "timestamp": timestamp,
      "user_name": user_name,
      "user_screen_name": user_screen_name,
      "text": text,
      "retweeted": "false",  ### REVISAR
      "json": data
      }
    )

#################################################################

def llegeixfonts():

  global fonts_path

  path="%s/gnusocial.json" % (fonts_path)

  with open(path) as data_file:
    return json.load(data_file)

  return []

#################################################################

def escriufonts(f):

 global fonts_path

 with open(fonts_path, 'w') as outfile:
   json.dump(f, outfile)

#################################################################

def getlast(urlserver):

  try:

    url="%s/api/statuses/public_and_external_timeline.json"%(urlserver)
    #print "Retrieving " + url
    response = urllib2.urlopen(url, timeout=5)
    data = response.read()
    a=json.loads(data)
    id=a[0]['id']
    return id

  except urllib2.HTTPError as e:

    print("ERROR code=%d" % e.code)
    return 0

  except urllib2.URLError, e:

    print("%s %s" % (urlserver,e))
    return 0

  except:

    return 0

  return 0

#################################################################

def processajson(host,a):

  indexa(host,0,a)

  db = MySQLdb.connect(host="localhost",
                       user="osint",
                       passwd="password",
                       db="osint")

  cur = db.cursor()

  sql="SELECT idgrup,name,llistaparaules,llistausuaris"
  sql=sql + " FROM grup"
  sql=sql + " ORDER BY idgrup"

  cur.execute(sql)

  grups=cur.fetchall()

  db.close()

  for grup in grups:

    idgrup=grup[0]
    paraules=grup[2].split('\n')
    usuaris=grup[3].split('\n')

    m=False
    for paraula in paraules:

      paraula=paraula.strip()
      if paraula!='':
        m=m or re.match(r'.*%s.*'%(paraula), a["text"], re.M|re.I)

    if m:
      indexa(host,idgrup,a)

#################################################################

def processaitem(host,url):

  try:

    print "Retrieving " + url
    response = urllib2.urlopen(url, timeout=5)
    data=response.read()
    a=json.loads(data)

    #created_at=a["created_at"].encode('utf-8')
    #text=a["text"].encode('utf-8')
    #userid=a["user"]["id"]
    #username=a["user"]["name"]

    #print("%s %s %s" % (created_at, userid, username))

  except urllib2.HTTPError as e:

     if   e.code==403:
       print "403 RESTRICTED"
       return 0
     elif e.code==404:
       print "404 NO SUCH NOTICE"
       return 0
     elif e.code==410:
       print "410 DELETED"
       return 0

     print("ERROR codi=%d" % e.code)
     return 0

  except urllib2.URLError, e: # timeout

    print("%s %s" % (urlserver,e))
    return 1

  except:

    print "ERROR %s" % (url)
    return 1

  processajson(host,a)

  return 0

#################################################################

def getlastread(name):

  global fonts_path

  path="%s/%s.last" % (fonts_path, name)
  #print "Reading %s" % path

  if not os.path.isfile(path):
    return 0

  with open(path) as f:
    return int(f.read())

#################################################################

def savelastread(name,iid):

  global fonts_path

  path="%s/%s.last" % (fonts_path, name)
  #print "Saving %s" % path

  with open(path, "w") as f:
    f.write(str(iid))

#################################################################

def processafont(font):

  while True:

    url=font["url"]
    name=font["name"]

    last=getlast(url)
    if last==0:
      return

    lastread=getlastread(name)
    if lastread==0:
      lastread=last-100

    if last-lastread>0:

      print("name=%s url=%s lastread=%i last=%i pendents=%i" % (name,url,lastread,last,last-lastread))

      for iid in range(lastread+1, last+1):

        urljson="%s/api/statuses/show/%i.json"%(url,iid)

        if processaitem(url,urljson)==1:
          print "BREAK"
          break

        #print "urljson=%s urlcaptura=%s" % (urljson, urlcaptura)
        savelastread(name,iid)

    time.sleep(5)

#################################################################

def processafonts():

  threads=[]

  for i in range(0,len(fonts)):

    t = threading.Thread(target=processafont, args=(fonts[i],))
    threads.append(t)
    t.start()

#################################################################

es = Elasticsearch(["127.0.0.1"],max_retries=10,retry_on_timeout=True)

fonts=llegeixfonts()
processafonts()

#################################################################
