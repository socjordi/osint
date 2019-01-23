#!/usr/bin/python

import os
import time
import shutil
import re
import time
from elasticsearch import Elasticsearch

#########################################################################################################

def ocupacio(path):

  statvfs = os.statvfs(path)
  espai_total=statvfs.f_frsize * statvfs.f_blocks
  espai_lliure=statvfs.f_frsize * statvfs.f_bavail
  oo=100.0-float(espai_lliure)/float(espai_total)*100.0

  return oo

#########################################################################################################

def purgetmp():

  files = [f for f in os.listdir('/tmp') if re.match(r'xvfb-run.*', f)]

  now=int(time.time())

  for f in files:
    try:
      path = "/tmp/"+f
      stat = os.stat(path)
      mtime=stat.st_mtime
      if now-mtime>3600:
        print("Esborra %s" % path)
        shutil.rmtree(path)
    except:
      print "ERROR purgetmp %s" % f

#########################################################################################################

def purgeimg():

  while True:

    oo=ocupacio("/media/img")

    if oo<80:
      return

    hora=time.strftime("%d/%m/%Y %H:%M:%S", time.localtime(time.time()))
    with open("/home/osint/purge.log", "a") as log:
      log.write("%s %.2f\n" % (hora,oo))

    pathimg="/media/img/img"
    anys = [f for f in os.listdir(pathimg) if os.path.isdir(os.path.join(pathimg, f))]
    anys=sorted(anys)

    for aany in anys:

      pathany="%s/%s" % (pathimg,aany)
      mesos = [f for f in os.listdir(pathany) if os.path.isdir(os.path.join(pathany, f))]
      mesos=sorted(mesos)

      for mes in mesos:

        pathmes="%s/%s" % (pathany,mes)
        dies = [f for f in os.listdir(pathmes) if os.path.isdir(os.path.join(pathmes, f))]
        dies=sorted(dies)

        for dia in dies:

          pathdia="%s/%s" % (pathmes,dia)

          print "Esborra %s" % pathdia
          shutil.rmtree(pathdia)

          if ocupacio("/media/img")<80:
            return

#########################################################################################################

def purgees(ind):

  es = Elasticsearch(["127.0.0.1"],max_retries=10,retry_on_timeout=True)

  while True:

    oo=ocupacio("/media/es")

    if oo<80:
      return

    indexos=es.indices.get_alias(ind)
    indexos=sorted(indexos.keys())

    for index in indexos:

      print "Delete %s" % index
      es.indices.delete(index=index)

      oo=ocupacio("/media/es")
      if oo<80:
        return

#########################################################################################################

purgeimg()
purgees("osint-*")
purgetmp()

#########################################################################################################
