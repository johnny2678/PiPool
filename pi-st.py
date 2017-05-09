#!/usr/bin/python3

import os
import sys
import glob
import re
from io import BytesIO
import json
import pprint
import requests
import time
import subprocess
import datetime
import re
import logging

#================================
# Script Setup
#
# Should the script check for the nodejs_poolcontroller found here and only run when the pump is running?
# https://github.com/tagyoureit/nodejs-poolController
# Set to False if you don't have Intellitouch and the above script reading RS485 commands from the Intellitouch COM port
nodejs_poolcontroller = True

# Documentation TBD 
sendto_smartthings = True

# Send data to influxDB
sendto_influxdb = True

if sendto_influxdb:
   influx_host = '192.168.5.133'
   influx_port = 8086
   influx_user = 'root'
   influx_password = 'root'
   influx_db = 'PiPool'
   influx_db_retention_policy_name = 'PiPool retention'
   influx_db_retention_duration = '720d'
   influx_db_retention_replication = 1

   from influxdb import InfluxDBClient
   client = InfluxDBClient(influx_host, influx_port, influx_user, influx_password, influx_db)

   try:
      logging.info("Creating (if not exists) INFLUX DB %s" % (influx_db))
      client.create_database(influx_db)
   except IOError as e:
      logging.error("Unable to create/connect to INFLUX DB %s" % (influx_db))
      exit()

   try:
      logging.info("Creating (if not exists) retention policy on INFLUX DB %s" % (influx_db))
      client.create_retention_policy(influx_db_retention_policy_name, influx_db_retention_duration, influx_db_retention_replication)
   except IOError as e:
      logging.error("Unable to create INFLUX DB retention policy on DB %s" % (influx_db))
      exit()
   

#================================
# Logging
from logging.handlers import RotatingFileHandler
logging.getLogger("urllib3").setLevel(logging.WARNING)

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')


fh = RotatingFileHandler('/var/log/PiPool/PiPool.log', maxBytes=100000, backupCount=10)
fh.setLevel(logging.INFO)
#fh.setLevel(logging.DEBUG)
fh.setFormatter(formatter)

sh = logging.StreamHandler(stream=sys.stdout)
#sh.setLevel(logging.DEBUG)
sh.setLevel(logging.INFO)
sh.setFormatter(formatter)

logger.addHandler(fh)
logger.addHandler(sh)

#=================================

base_dir = '/sys/bus/w1/devices/'

#device name array
dsname = ['Pool - Solar Lead Temp'
,       'Pool - Solar Return Temp'
,       'Pool - Return Temp'
#,	'Test mini sensor'	
]

#device id array
dsid = [ '28-800000262ce2'
,       '28-80000026331f'
,       '28-0114b80e0bff'
#,	'28-800000272f0f'
]

#device APP client ID (get from IDE -> App Settings)
client_id = ['b0fcacd9-d55a-4b87-a64a-80eb61a7a948'
,      	     '4c31867c-304b-4aa9-8df1-3fc2f0d99e8f'
,            '0f6a05f7-06fb-452e-bf8d-f3006b5e8991'
#,            '973bfa64-269d-4ce4-8a86-1fff2499b1fd'
]

#device access_token (get from local PHP oath2 authorization code)
ds_token= ['8c98af31-267e-4704-aa10-fe696f34c041'
,          'bb983ef4-c644-49a8-b560-43f86801627b'
,          'baa5b0e2-7cc0-493e-bc6a-db928bfa2780'
#,          '3219883b-66e1-4b0d-b1bf-3028b77173de'
]

### no modifications should be needed below this line

def read_temp_raw(cntc):
    logging.debug ("\t\tread_temp_raw(%i): %s" % (cntc,dsfile[cntc]))
    catdata = subprocess.Popen(['cat',dsfile[cntc]], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out,err = catdata.communicate()
    out_decode = out.decode('utf-8')
    lines = out_decode.split('\n')
    return lines

def read_temp(cntb):
    lines = read_temp_raw(cntb)
    logging.debug ("\tread_temp(%i): %s" % (cntb,lines))
    while lines[0].strip()[-3:] != 'YES':
       	time.sleep(0.2)
        lines = read_temp_raw(cntb)
    equals_pos = lines[1].find('t=')

    if equals_pos != -1:
       	temp_string = lines[1][equals_pos+2:]
        temp_c = float(temp_string) / 1000.0
       	temp_f = temp_c * 9.0 / 5.0 + 32.0
        return temp_c, temp_f

def influxdb(counter, temp_f, sub_dsname,sub_last_temp_influx):
  logging.info ("Sending to InfluxDB (Cycle %s): dsname: %s\ttemp_f: %s (oldtemp: %s)" % (counter, sub_dsname, temp_f, sub_last_temp_influx))
  pump_mode = get_pump_mode(nodejs_poolcontroller)
  pump_rpm, pump_watts = get_pump_rpm(nodejs_poolcontroller)

  influx_json = [
  {
    "measurement": "Pool",
    "tags": {
       "sensor": sub_dsname,
       "mode": pump_mode
    },
    "fields": {
       "temp": temp_f,
       "pump_rpm": pump_rpm,
       "pump_watts": pump_watts
    }
  }
]
        
  client.write_points(influx_json)

def get_pump_onoff(nodejs_poolcontroller):
  if nodejs_poolcontroller:
    url = 'http://192.168.5.31:3000/pump'
    r = requests.get(url)
    pumps = json.loads(r.text)
    pump_onoff = pumps[1]['power']

    if pump_onoff == 0:
      logging.warning("Pump is not running. Exiting.")
      exit()
  else:
    logging.info("Node.js Pool Controller is not installed - skipping getting pump on/off status")

def get_pump_mode(nodejs_poolcontroller):
  if nodejs_poolcontroller:
    url = 'http://192.168.5.31:3000/circuit/1'
    r = requests.get(url)
    resp = json.loads(r.text)
    pumpmode = resp['status']

    if pumpmode == 0:
      logging.debug("Circuit 1 status = POOL (OFF)")
      return 'pool'
    elif pumpmode == 1:
      logging.debug("Circuit 1 status = SPA (ON)")
      return 'spa'
    else:
      logging.error("Circuit 1 status (SPA) can't be read. Exiting")
      exit()
  else:
    logging.info("Node.js Pool Controller is not installed - skipping getting pump mode(pool/spa)")

def get_pump_rpm(nodejs_poolcontroller):
  if nodejs_poolcontroller:
    url = 'http://192.168.5.31:3000/pump'
    r = requests.get(url)
    resp = json.loads(r.text)
    pumprpm = resp[1]['rpm']
    pumpwatts = resp[1]['watts']

    return pumprpm, pumpwatts
  else:
    logging.info("Node.js Pool Controller is not installed - skipping getting pump rpm/watts")


def main():
   last_temp_st = {}
   last_temp_influx = {}
   temp_url = []
   i=0
   upper_submit_limit = 125
   lower_submit_limit = 45

   for tmpdsid in dsid:
       last_temp_st[tmpdsid]=9999999.99
       last_temp_influx[tmpdsid]=9999999.99

       get_endpoints = ( "https://graph.api.smartthings.com/api/smartapps/endpoints/" + client_id[i] + "?access_token=" + ds_token[i] )
       if sendto_smartthings:
          req =  requests.get(get_endpoints)
          if (req.status_code != 200):
             logging.error ("Error: " + str(r.status_code) + "exiting.")
       	     exit()
          else:
             logging.info ("Endpoint URL for %s found!..." % (dsname[i]))
             endpoints = json.loads( req.text )
         
             for endp in endpoints:
               uri = endp['uri']
               temp_url.append( uri + ("/update/"))
             
       i += 1
   
   for counter in range(niters, 0, -1):
       cnta=0

       get_pump_onoff(nodejs_poolcontroller)

       for tmpds in dsid:
       	  (temp_c, temp_f) = read_temp(cnta)
          endp_url = temp_url[cnta] + ("%.2f/F" % temp_f)     
       	  headers = { 'Authorization' : 'Bearer ' + ds_token[cnta] } 
          
          if ( sendto_smartthings and abs(round(temp_f, 2) - round(last_temp_st[tmpds], 2)) > 0.18 and lower_submit_limit <= temp_f <= upper_submit_limit):
             logging.debug ("Endpoint submit URL: %s" % (endp_url))
             logging.info ("Sending to Smartthings (Cycle %s): dsname: %s\ttemp_f: %s (oldtemp: %s)" % (counter, dsname[cnta], temp_f, last_temp_st[tmpds]))
             r = requests.put(endp_url, headers=headers)
             last_temp_st[tmpds] = temp_f

          if ( sendto_influxdb and round(temp_f, 2) != round(last_temp_influx[tmpds], 2)):
             influxdb(counter, temp_f, dsname[cnta], last_temp_influx[tmpds])
             last_temp_influx[tmpds] = temp_f

          cnta += 1

       time.sleep(interval)



get_pump_onoff(nodejs_poolcontroller)

#device files array
dsfile = []
for tmpdsid in dsid:
   tmpfile = (base_dir + tmpdsid + '/w1_slave')
   try:
      with open(tmpfile) as file:
       	 logging.info("Found DS18B20 sensor %s" % (tmpdsid))
         pass
   except IOError as e:
      logging.error("Could not find DS18B20 sensor %s. Exiting." % (tmpfile))
      exit();
   dsfile.append(base_dir + tmpdsid + '/w1_slave')

niters = 120
interval = 15



main()

