#!/usr/bin/python

import MySQLdb
import json
import time
import subprocess
import psutil
import os

#################################################################

def isrunning(idgrup):

  par="/home/osint/%d_twitter.json" % idgrup

  for p in psutil.process_iter():
    cmdline=p.cmdline()
    if cmdline[0]=='/usr/bin/python' and cmdline[1]=='/home/osint/twitter.py' and cmdline[2]==par:
      return p.pid

  return 0

#################################################################

def llegeix_grups():

  db = MySQLdb.connect(host="localhost",
                       user="osint",
                       passwd="mlbUCPNU0zu7iaP9",
                       db="osint")

  cur = db.cursor()

  sql="SELECT idgrup,name,llistaparaules,llistausuaris,twitter_consumer_key,twitter_consumer_secret,twitter_access_token,twitter_access_token_secret,captura"
  sql=sql + " FROM grup"
  sql=sql + " WHERE twitter_consumer_key<>''"
  sql=sql + " ORDER BY idgrup"

  cur.execute(sql)

  return cur.fetchall()

  db.close()

#################################################################

def checkgroups():

  grups=llegeix_grups()

  for grup in grups:

    idgrup=grup[0]

    jdb={}
    jdb["idgrup"]=idgrup
    jdb["name"]=grup[1]
    jdb["twitter_consumer_key"]=grup[4]
    jdb["twitter_consumer_secret"]=grup[5]
    jdb["twitter_access_token"]=grup[6]
    jdb["twitter_access_token_secret"]=grup[7]
    jdb["captura"]=grup[8]
    jdb["llistaparaules"]=grup[2].split("\n")
    jdb["llistausuaris"]=grup[3].split("\n")

    jdb["llistausuaris"]=[x.strip() for x in jdb["llistausuaris"] if x.strip()]
    jdb["llistaparaules"]=[x.strip() for x in jdb["llistaparaules"] if x.strip()]

    jdbstr = json.dumps(jdb, sort_keys=True)

    filename="/home/osint/%d_twitter.json" % idgrup

    try:
      with open(filename) as data_file:    
        jf=json.load(data_file)
      jfstr = json.dumps(jf, sort_keys=True)
    except:
      jfstr=""

    pid=isrunning(idgrup)

    # Si esta corrent i no ha canviat la conf, no fer res
    if pid>0 and jdbstr==jfstr:
      continue

    # Si esta corrent i ha canviat la conf, matar el proces
    if pid>0:
      try:
        os.kill(pid, 9)
        print("Atura grup %d (pid=%d)" % (idgrup,pid))
      except OSError:
        print "ERROR kill pid=%d" % pid
    
    #print "Escriu %s" % filename
    with open(filename, 'w') as outfile:
      json.dump(jdb, outfile)

    script="/home/osint/twitter.py"
    conf="/home/osint/%d_twitter.json" % idgrup
    log="/home/osint/%d_twitter.log" % idgrup
    with open(log, "w") as lf:
      pid=subprocess.Popen([script, conf], stdout=lf).pid
      print("Inicia grup %d (pid=%d)" % (idgrup,pid))

#################################################################

while True:

  checkgroups()
  time.sleep(1)

#################################################################
