#!/usr/bin/python

import os
import json
import time
import sys
from datetime import datetime
#from dateutil.parser import parse
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy import API
from elasticsearch import Elasticsearch
import hmac
import hashlib
import base64

hmac_secret=base64.b64decode("NR6xaQoKjL4=")

################################################################################

def esborra(message_id, user_id, timestamp):

  global es

  try:

    res=es.search(index="osint-*", body={"query": {"match": {'message_id': message_id}}})

    index=res['hits']['hits'][0]["_index"]
    ident=res['hits']['hits'][0]["_id"]
    source=res['hits']['hits'][0]["_source"]

    source["deleted_user_id"]=user_id
    source["deleted_timestamp"]=timestamp

    es.update(
      index=index,
      doc_type="twitter",
      id=ident,
      body={"doc": source}
    )

  except:

    return

################################################################################

def ocupacio(path):

  statvfs = os.statvfs(path)
  espai_total=statvfs.f_frsize * statvfs.f_blocks
  espai_lliure=statvfs.f_frsize * statvfs.f_bavail
  oo=100.0-float(espai_lliure)/float(espai_total)*100.0

  return oo

################################################################################

def error(idgrup, msg):

  global es

  now = datetime.utcnow()
  indexname="errors-%s" % (now.strftime("%Y-%m"))
  logfilename="/home/osint/errors-%d.log" % (idgrup)

  es.index(
    index=indexname,
    doc_type="error",
    body={
         "grup_id": idgrup,
         "message": msg,
         "timestamp": now
         }
  )

  with open(logfilename, "a") as errfile:
    errfile.write(msg+"\n")

  if msg=="[Errno 28] No space left on device":
    o=ocupacio("/media/img")
    with open(logfilename, "a") as errfile:
      errfile.write("Ocupacio /media/img: %f\n" % o)
    o=ocupacio("/media/es")
    with open(logfilename, "a") as errfile:
      errfile.write("Ocupacio /media/es: %f\n" % o)
    o=ocupacio("/tmp")
    with open(logfilename, "a") as errfile:
      errfile.write("Ocupacio /tmp: %f\n" % o)

################################################################################

def indexa(data):

  global es, idgrup, hmac_secret, captura

  #print(data)
  #print

  digest=hmac.new(hmac_secret, msg=data, digestmod=hashlib.sha256).digest()
  signature=base64.b64encode(digest).decode()

  now = datetime.now()
  indexname="osint-%s" % (now.strftime("%Y-%m-%d"))
  deleteindexname="osint-delete-%s" % (now.strftime("%Y-%m-%d"))

  j=json.loads(data)

  if 'delete' in j:

    timestamp=int(j["delete"]["timestamp_ms"])
    timestamp=datetime.utcfromtimestamp(timestamp/1000.0)
    timestamp=timestamp.strftime('%Y-%m-%dT%H:%M:%S.%f')

    message_id=str(j["delete"]["status"]["id"])
    user_id=str(j["delete"]["status"]["user_id"])

    es.index(
      index=indexname,
      doc_type="twitter",
      body={
           "grup_id": idgrup,
           "message_id": message_id,
           "user_id": user_id,
           "timestamp": timestamp,
           "json": data,
           "signature": signature
           }
    )

    esborra(message_id, user_id, j["delete"]["timestamp_ms"])

  else:

    if 'retweeted_status' in j:
      retweeted=True
    else:
      retweeted=False

    #print(j)
    #print("text=%s retweeted=%d" % (j['text'], retweeted))

    timestamp=int(j["timestamp_ms"])
    timestamp=datetime.utcfromtimestamp(timestamp/1000.0)
    timestamp=timestamp.strftime('%Y-%m-%dT%H:%M:%S.%f')

    message_id=str(j["id"])
    user_id=str(j["user"]["id"])
    pathimg="%04d/%02d/%02d/%s.jpg" % (now.year,now.month,now.day,message_id)

    es.index(
      index=indexname,
      doc_type="twitter",
      body={
           "grup_id": idgrup,
           "message_id": message_id,
           "user_screen_name": j["user"]["screen_name"],
           "user_name": j["user"]["name"],
           "user_id": user_id,
           "timestamp": timestamp,
           "text": j["text"],
           "json": data,
           "retweeted": retweeted,
           "signature": signature,
           "deleted_user_id": "",
           "deleted_timestamp": "",
           "pathimg": pathimg
           }
    )

    if captura==1:

      path="/var/www/html/osintimg/%04d" % (now.year)
      if not os.path.exists(path):
        os.makedirs(path)

      path="%s/%02d" % (path,now.month)
      if not os.path.exists(path):
        os.makedirs(path)

      path="%s/%02d" % (path,now.day)
      if not os.path.exists(path):
        os.makedirs(path)

      path="%s/%s.jpg" % (path,message_id)

      if not os.path.isfile(path):

        url="https://twitter.com/%s/status/%s" % (j["user"]["screen_name"],message_id)
        cmd="xvfb-run -a --server-args=\"-screen 0 1280x1200x24\" cutycapt --min-width=1024 --min-height=2048 --url=%s --out=%s --print-backgrounds=on --delay=3000 --max-wait=10000 --http-proxy=\"http://192.168.47.162:8080\" >/dev/null 2>&1 &" % (url,path)

        os.system(cmd)

################################################################################

class FileDumperListener(StreamListener):

	def __init__(self):

		super(FileDumperListener,self).__init__(self)

		self.tweetCount=0
		self.errorCount=0
		self.limitCount=0
		self.last=datetime.now()
	
	#This function gets called every time a new tweet is received on the stream
	def on_data(self, data):

                print(data)

                indexa(data)

		self.tweetCount+=1

		self.status()

		return True
		
	def close(self):

		print "close"
	
	def on_error(self, statusCode):

                if statusCode==401:
                  msg="ERROR 401 - No autoritzat (credencials incorrectes o inexistents)"
                elif statusCode==406:
                  msg="ERROR 406 - No acceptable (peticio amb format no valid)"
                elif statusCode==429:
                  msg="ERROR 429 - Massa peticions"
                else:
		  msg="ERROR %s (API Twitter)" % (statusCode)

		print(msg)
		error(idgrup, msg)

                #with open(logFileName, "a") as logfile:
		#  logfile.write(msg)

		self.errorCount+=1
	
	def on_timeout(self):

		raise TimeoutException()
	
	def on_limit(self, track):

		msg="LIMIT missatge rebut %s " % (track)

		print(msg)
		error(idgrup, msg)

                #with open(logFileName, "a") as logfile:
		#  logfile.write(msg)

		self.limitCount+=1
	
	def status(self):

		now=datetime.now()
		if (now-self.last).total_seconds()>300:

		  msg="%s - %i tweets, %i limits, %i errors in previous five minutes\n" % (now,self.tweetCount,self.limitCount,self.errorCount)

		  print(msg)

                  #with open(logFileName, "a") as logfile:
		  #  logfile.write(msg)

		  self.tweetCount=0
		  self.limitCount=0
		  self.errorCount=0
		  self.last=now

################################################################################

class TimeoutException(Exception):

  msg="%s TIMEOUT\n" % (datetime.now())

  print(msg)

  #with open(logFileName, "a") as logfile:
  #  logfile.write(msg)

  pass

################################################################################

def process_users_old(api,users):

  u=[]
  n=[]
  for user in users:
    if user=="":
      continue
    user=user.encode("ascii","ignore")
    print "user=%s" % (user)
    #if user[0]==u'\u200f':
    if user[0]=='@':
      n.append(user[1:])
    else:
      u.append(user)

  twinfo=api.lookup_users(user_ids=u, screen_names=n)

  u=[]
  for t in twinfo:
    u.append(str(t.id))

  return u

################################################################################

def process_users(api,users):

  nbatch=50

  u2=[]

  for i in range(0,len(users), nbatch):

    u=[]
    n=[]

    if i+nbatch>len(users):
      final=len(users)
    else:
      final=i+nbatch

    for j in range(i, final):

      user=users[j].encode("ascii","ignore")
      if user[0]=='@':
        n.append(user[1:])
      else:
        u.append(user)

      #print("j=%d %s" % (j, users[j]))

    twinfo=api.lookup_users(user_ids=u, screen_names=n)
    for t in twinfo:
      #print(str(t.id))
      u2.append(str(t.id))

  return u2

################################################################################

if __name__ == '__main__':

  if len(sys.argv)!=2:
    print "Cal passar com argument el path del fitxer de parametres"
    exit()

  settings=sys.argv[1]
  fh = open(settings,"r")
  json_data=fh.read()
  fh.close()
  data=json.loads(json_data)

  #print(data)

  twitter_consumer_key=data["twitter_consumer_key"]
  twitter_consumer_secret=data["twitter_consumer_secret"]
  twitter_access_token=data["twitter_access_token"]
  twitter_access_token_secret=data["twitter_access_token_secret"]

  if twitter_consumer_key=='':
    time.sleep(60)
    exit()

  if twitter_consumer_secret=='':
    time.sleep(60)
    exit()

  if twitter_access_token=='':
    time.sleep(60)
    exit()

  if twitter_access_token_secret=='':
    time.sleep(60)
    exit()

  keywords=data["llistaparaules"]
  users=data["llistausuaris"]

  idgrup=data["idgrup"]

  captura=data["captura"]

  es = Elasticsearch(["127.0.0.1"],max_retries=10,retry_on_timeout=True)

  while True:

	try:
		listener = FileDumperListener()
		auth = OAuthHandler(twitter_consumer_key, twitter_consumer_secret)
		auth.set_access_token(twitter_access_token, twitter_access_token_secret)
                api = API(auth)
		stream = Stream(auth, listener)

                users=process_users(api,users)

                print users
		print keywords

                if (not users) and (not keywords):
                  time.sleep(60)
                  exit()

		stream.filter(follow=users, track=keywords)

	except KeyboardInterrupt:

		print("KeyboardInterrupt caught. Closing stream and exiting.")
		listener.close()
		stream.disconnect()
		break

	except TimeoutException:

		msg="Timeout exception caught. Closing stream and reopening."
		print(msg)
		error(idgrup, msg)
		try:
			listener.close()
			stream.disconnect()
		except:
			pass
		continue

	except Exception as e:
		try:
			exc_type, exc_obj, exc_tb = sys.exc_info()
			info = str(e)
			#msg="%s - Unexpected exception. %s\n" % (datetime.now(),info)
    			#print(exc_type, fname, exc_tb.tb_lineno)
			#msg=msg+" "+exc_type+" "+fname+" "+exc_tb.tb_lineno
			msg=info
			sys.stderr.write(msg)
		        error(idgrup, msg)
		except:
			print "ERROR ERROR\n"
			pass
                print "sleep"
		time.sleep(60)
		exit()

###############################################################################
