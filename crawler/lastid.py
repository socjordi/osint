#!/usr/bin/python

import urllib2,json

#######################################################

def getlast(urlserver):

  try:

    url="%s/api/statuses/public_and_external_timeline.json"%(urlserver)
    print "Retrieving " + url
    response = urllib2.urlopen(url, timeout=5)
    data = response.read()
    a=json.loads(data)
    id=a[0]['id']
    return id

  except urllib2.HTTPError as e:

     print("ERROR HTTP code=%d" % e.code)
     return 1

  except urllib2.URLError, e:

     print(e)
     #print("ERROR URL code=%d" % e.code)
     return 1

  except:

    return 1

  return 0

#######################################################

def llegeixfonts():

  global fonts_path

  with open(fonts_path) as data_file:
    return json.load(data_file)

  return []

#######################################################

fonts_path="/home/osint/gnusocial.json"
fonts=llegeixfonts()

for i in range(0,len(fonts)):

  url=fonts[i]["url"]
  name=fonts[i]["name"]

  if "last" in fonts[i]:
    lastread=fonts[i]["last"]
  else:
    lastread=0

  last=getlast(url)

  print url,lastread,last,last-lastread

#######################################################
