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

# True = sleep timers will not fire / False = default settings - sleep timers to normalize temps
debug_mode = True

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
   except:
      logging.error("Unable to create/connect to INFLUX DB %s" % (influx_db))
      raise
      exit()

   try:
      logging.info("Creating (if not exists) retention policy on INFLUX DB %s" % (influx_db))
      client.create_retention_policy(influx_db_retention_policy_name, influx_db_retention_duration, influx_db_retention_replication)
   except:
      logging.error("Unable to create INFLUX DB retention policy on DB %s" % (influx_db))
      raise
      exit()
   

#================================
# Logging
from logging.handlers import RotatingFileHandler
logging.getLogger("urllib3").setLevel(logging.WARNING)

logger = logging.getLogger()
if not debug_mode:
  logger.setLevel(logging.INFO)
else:
  logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')


fh = RotatingFileHandler('/var/log/PiPool/PiPool.log', maxBytes=100000, backupCount=10)

if not debug_mode:
  fh.setLevel(logging.INFO)
else:
  fh.setLevel(logging.DEBUG)
fh.setFormatter(formatter)

#sh = logging.StreamHandler(stream=sys.stdout)
#if not debug_mode:
#  sh.setLevel(logging.INFO)
#else:
#  sh.setLevel(logging.DEBUG)
#sh.setFormatter(formatter)

logger.addHandler(fh)
#logger.addHandler(sh)

#=================================

#base_dir = '/sys/bus/w1/devices/'

dsref = ['solarlead', 'solarreturn', 'poolreturn']

dsname = {}
dsname["solarlead"]   = "Pool - Solar Lead Temp"
dsname["solarreturn"] = "Pool - Solar Return Temp"
dsname["poolreturn"]  = "Pool - Return Temp"

dsid = {}
dsid["solarlead"]     = "800000262ce2"
dsid["solarreturn"]   = "80000026331f"
dsid["poolreturn"]    = "0114b80e0bff"

#device Smartthings APP client ID (get from IDE -> App Settings)
client_id = {}
client_id["solarlead"]   = "b0fcacd9-d55a-4b87-a64a-80eb61a7a948"
client_id["solarreturn"] = "4c31867c-304b-4aa9-8df1-3fc2f0d99e8f"
client_id["poolreturn"]  = "0f6a05f7-06fb-452e-bf8d-f3006b5e8991"

#device access_token (get from local PHP oath2 authorization code)
####need to document
ds_token = {}
ds_token["solarlead"]   = "8c98af31-267e-4704-aa10-fe696f34c041"
ds_token["solarreturn"] = "bb983ef4-c644-49a8-b560-43f86801627b"
ds_token["poolreturn"]  = "baa5b0e2-7cc0-493e-bc6a-db928bfa2780"

### no modifications should be needed below this line

#get smartthings endpoint URLs
###need to document
temp_st_url={}
last_temp_st={}
last_temp_influx={}
last_temp_change_ts={}
temp_change_per_sec={}
temp_change_per_min={}
temp_change_per_hour={}
cur_temp = {}
cur_temp_timestamp = {}
temp_at_12pm={}
temp_at_2pm={}
temp_at_4pm={}
temp_at_6pm={}
old_pump_mode=None

curtime = int(time.mktime(time.localtime()))
pumpstarttime = curtime

ctime_struct = time.localtime()
time12pm_struct = (ctime_struct[0], ctime_struct[1], ctime_struct[2], 12, 0, 0, ctime_struct[6], ctime_struct[7], ctime_struct[8])
time2pm_struct  = (ctime_struct[0], ctime_struct[1], ctime_struct[2], 14, 0, 0, ctime_struct[6], ctime_struct[7], ctime_struct[8])
time4pm_struct  = (ctime_struct[0], ctime_struct[1], ctime_struct[2], 16, 0, 0, ctime_struct[6], ctime_struct[7], ctime_struct[8])
time6pm_struct  = (ctime_struct[0], ctime_struct[1], ctime_struct[2], 18, 0, 0, ctime_struct[6], ctime_struct[7], ctime_struct[8])
time12pm = time.mktime(time12pm_struct)
time2pm = time.mktime(time2pm_struct)
time4pm = time.mktime(time4pm_struct)
time6pm = time.mktime(time6pm_struct)

logging.debug("Today unixtime 12pm: %d" % time12pm)
logging.debug("Today unixtime 2pm: %d" % time2pm)
logging.debug("Today unixtime 4pm: %d" % time4pm)
logging.debug("Today unixtime 6pm: %d" % time6pm)

def get_est_temp_at_hr(tmphr, tmpF, tmpRate, ctime):
  if (tmphr == 12):
    esttime = time12pm
  elif (tmphr == 14):
    esttime = time2pm
  elif (tmphr == 16):
    esttime = time4pm
  elif (tmphr == 18):
    esttime = time6pm
  else:
    logging.error("Incorrect request for unixtime at %d. Valid values are 12, 14, 16, 18. Exiting..." % (tmphr))
    exit()

  estTimeDiff = esttime - ctime
  if (esttime > ctime):
    estTempF = tmpF + tmpRate * estTimeDiff
    logging.debug("There are %d seconds between now and %d" % (estTimeDiff, tmphr))
#    if not (0 < estTempF < estTempF *2):
#      logging.debug(" Estimated %d pm value of %.3f out of Range. Skipping" % (tmphr, estTempF))
#      estTempF = None
  else:
    estTempF = None
    logging.debug("There are %d seconds between now and %d. Setting return value to NONE." % (estTimeDiff, tmphr))

  return estTempF

def generate_holt_winters(tmpMeasurement):
#DROP MEASUREMENT temp_4h
#SELECT holt_winters(mean("temp"),8,0) INTO "temp_4h" FROM "Pool" WHERE "mode" = 'pool' AND "sensor" = 'Pool - Solar Lead Temp' AND time>now() - 2h GROUP BY time(30m,30m)
#SELECT holt_winters(mean("temp"),8,0) FROM "Pool" WHERE "mode" = 'pool' AND "sensor" = 'Pool - Solar Lead Temp' AND time>now() - 2h GROUP BY time(30m,30m)
#SELECT mean("temp") FROM "Pool" WHERE "mode" = 'pool' AND "sensor" = 'Pool - Solar Lead Temp' AND time>now() - 1h GROUP BY time(30m)

  query = 'DELETE FROM temp_' + tmpMeasurement
  try:
    logging.debug(" INFLUX: deleting from measurement %s" % tmpMeasurement)
    result = client.query(query)
    logging.debug(" INFLUX: SUCCESS running query: %s" % query)
  except:
    logging.error(" INFLUX: FAIL Unable to execute INFLUX query: %s" % query) 
    raise
    exit()

  if tmpMeasurement == "4h" or tmpMeasurement == "4h_tracking":
    tmpQueryTime = "2h"
    tmpGroupBy = "30m"
    tmpNumPeriods = "8"
  elif tmpMeasurement == "2h" or tmpMeasurement == "2h_tracking":
    tmpQueryTime = "90m"
    tmpGroupBy = "10m"
    tmpNumPeriods = "12"
  else:
    tmpQueryTime = "1h"
    tmpGroupBy = "5m"
    tmpNumPeriods = "12"

  query = 'SELECT holt_winters(mean("temp"),' + tmpNumPeriods + ',0) INTO "temp_' + tmpMeasurement + '" FROM "Pool" WHERE "mode" = \'pool\' AND "sensor" = \'Pool - Solar Lead Temp\' AND time>now() - ' + tmpQueryTime + ' GROUP BY time(' + tmpGroupBy + ',' + tmpGroupBy + ')'
  try:
    logging.debug(" INFLUX: Generating HOLT WINTERS %s temperature projections" % tmpMeasurement)
    result = client.query(query)
    logging.debug(" INFLUX: SUCCESS running query: %s" % query)
    logging.debug("   INFLUX:{0}".format(result))
  except:
    logging.error(" INFLUX: FAIL Unable to execute INFLUX query: %s" % query) 
    raise
    exit()

def get_st_endpoints():
  for tmpds in dsref:
    last_temp_st[tmpds]=999999.99
    last_temp_influx[tmpds]=999999.99
    last_temp_change_ts[tmpds]=999999.99
    temp_change_per_hour[tmpds]=999999.99

    if sendto_smartthings:
      get_endpoints = ( "https://graph.api.smartthings.com/api/smartapps/endpoints/" + client_id[tmpds] + "?access_token=" + ds_token[tmpds] )
      req =  requests.get(get_endpoints)
      if (req.status_code != 200):
        logging.error ("Error: " + str(r.status_code) + "exiting.")
        exit()
      else:
        logging.info ("Endpoint URL for %s found!..." % (dsname[tmpds]))
        endpoints = json.loads( req.text )

        for endp in endpoints:
          uri = endp['uri']
          temp_st_url[tmpds] = uri + ("/update/")

def influxdb(counter, temp_f, sub_dsname, sub_last_temp_influx, sub_solar_temp_diff, sub_temp_change_per_hour, tmp12, tmp14, tmp16, tmp18):
  logging.info ("Sending to InfluxDB (Cycle %04d): dsname: %s\ttemp_f: %.4f oldtemp: %.4f solar diff: %s temp_change_per_hour(F/hour): %s" % (counter, sub_dsname, temp_f, sub_last_temp_influx, sub_solar_temp_diff, sub_temp_change_per_hour))
  if not (tmp18 is None):
    logging.info ("  Est values: 12pm: %s 2pm: %s 4pm: %s 6pm: %s" % (tmp12, tmp14, tmp16, tmp18))
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
       "temp_solar_diff": sub_solar_temp_diff,
       "temp_change_per_hour": sub_temp_change_per_hour,
       "est_temp_12pm": tmp12,
       "est_temp_2pm": tmp14,
       "est_temp_4pm": tmp16,
       "est_temp_6pm": tmp18,
       "pump_rpm": pump_rpm,
       "pump_watts": pump_watts
    }
  }
]
        
  client.write_points(influx_json)

def get_pump_onoff(nodejs_poolcontroller):
  if nodejs_poolcontroller:
    url = 'http://192.168.5.31:3000/pump'
    try:
      r = requests.get(url)
    except:
      raise
      logging.error("Cannot connect to the Pool Controller (Node.js)... exiting.")
      exit()

    pumps = json.loads(r.text)
    pump_onoff = pumps[1]['power']

    return pump_onoff

  else:
    logging.info("Node.js Pool Controller is not installed - skipping getting pump on/off status")

def pump_mode_change(nodejs_poolcontroller, old_pump_mode):
  pump_mode = get_pump_mode(nodejs_poolcontroller)

  return pump_mode

def get_pump_mode(nodejs_poolcontroller):
  if nodejs_poolcontroller:
    url = 'http://192.168.5.31:3000/circuit/1'
    try:
      r = requests.get(url)
    except:
      raise
      logging.error("Cannot connect to the Pool Controller (Node.js)... exiting.")
      exit()

    resp = json.loads(r.text)
    pumpmode = resp['status']

    if pumpmode == 0:
      logging.debug("Circuit 1 status = POOL (ON)")
      pumpmode = 'pool'
    elif pumpmode == 1:
      logging.debug("Circuit 1 status = SPA (ON)")
      pumpmode = 'spa'
    else:
      logging.error("Circuit 1 status (SPA) can't be read. Exiting")
      exit()

    logging.debug("*** current_pump_mode = %s|old_pump_mode = %s" % (pumpmode, old_pump_mode))
    if pumpmode != old_pump_mode:
      if not debug_mode:
        time.sleep(30)
      logging.info("Pump mode has changed from %s to %s. Sleeping 30 seconds..." % (old_pump_mode, pumpmode))
      global old_pump_mode
      old_pump_mode = pumpmode

    return pumpmode

  else:
    logging.info("Node.js Pool Controller is not enabled/installed - skipping getting pump mode(pool/spa)")

def get_pump_rpm(nodejs_poolcontroller):
  if nodejs_poolcontroller:
    url = 'http://192.168.5.31:3000/pump'
    try:
      r = requests.get(url)
    except:
      raise
      logging.error("Cannot connect to the Pool Controller (Node.js)... exiting.")
      exit()

    resp = json.loads(r.text)
    pumprpm = resp[1]['rpm']
    pumpwatts = resp[1]['watts']

    return pumprpm, pumpwatts
  else:
    logging.info("Node.js Pool Controller is not enabled/installed - skipping getting pump rpm/watts")


def read_temp(tmpds):

  from w1thermsensor import W1ThermSensor
  sensor = W1ThermSensor(W1ThermSensor.THERM_SENSOR_DS18B20, tmpds)
  temp_f = sensor.get_temperature(W1ThermSensor.DEGREES_F)

  return temp_f

def pump_exit_if_off():

  pump_onoff = None
  pump_onoff = get_pump_onoff(nodejs_poolcontroller)

  if pumpstarttime and curtime:
    runtime = curtime - pumpstarttime
    logging.debug("Pump has been active since %d (%d runtime)" % (pumpstarttime, runtime))
  if pump_onoff == 0:
    logging.warning("Pump is not running. Exiting.")
    exit()

def main():
  pumphour = 0 
  logging.info ("Pump start time recorded [%.0d]" % (pumpstarttime))
  logging.info ("  Sleeping while temps normalize")
  if not debug_mode:
    time.sleep(300)

  upper_submit_limit = 125
  lower_submit_limit = 45
  solar_temp_diff = None

  get_st_endpoints()

  cnt=0
  while (1):
    curtime = int(time.mktime(time.localtime()))

    for tmpds in dsref:
      temp_f = read_temp(dsid[tmpds])
      cur_temp[tmpds] = temp_f

      endp_url = temp_st_url[tmpds] + ("%.2f/F" % temp_f)
      headers = { 'Authorization' : 'Bearer ' + ds_token[tmpds] }

      if ( lower_submit_limit <= temp_f <= upper_submit_limit ):
        if ( sendto_smartthings and abs(round(temp_f, 2) - round(last_temp_st[tmpds], 2)) > 0.18 ):
          logging.debug ("Endpoint submit URL: %s" % (endp_url))
          logging.info ("Sending to Smartthings (Cycle %04d): dsname: %s\ttemp_f: %s (oldtemp: %s)" % (cnt, dsname[tmpds], temp_f, last_temp_st[tmpds]))
          r = requests.put(endp_url, headers=headers)
          last_temp_st[tmpds] = temp_f

        if ( sendto_influxdb and round(cur_temp[tmpds], 2) != round(last_temp_influx[tmpds], 2)):
          logging.debug("\n\ncur_temp DICT:")
          logging.debug(cur_temp)
          if ('solarreturn' in cur_temp and 'solarlead' in cur_temp):
            logging.debug("\t\t\tSolar Return temp: %s" % cur_temp['solarreturn'])
            logging.debug("\t\t\tSolar Lead temp:  %s " % cur_temp['solarlead'])
            logging.debug("\n\n")

            solar_temp_diff = cur_temp['solarreturn'] - cur_temp['solarlead']
            if (solar_temp_diff > 15 or solar_temp_diff < -5):
              solar_temp_diff = None

#          if (curtime - last_temp_change_ts[tmpds] < 60 * 60 * 2):
          if (tmpds in last_temp_influx and tmpds in last_temp_change_ts):
            # time change in degrees F per second
            temp_change_per_sec[tmpds] = (cur_temp[tmpds] - last_temp_influx[tmpds]) / ((curtime - last_temp_change_ts[tmpds]))
            # time change in degrees F per minute
            temp_change_per_min[tmpds] = (cur_temp[tmpds] - last_temp_influx[tmpds]) / ((curtime - last_temp_change_ts[tmpds]) / 60)
            # time change in degrees F per hour
            temp_change_per_hour[tmpds] = (cur_temp[tmpds] - last_temp_influx[tmpds]) / ((curtime - last_temp_change_ts[tmpds]) / 60 / 60)
          if (temp_change_per_hour[tmpds] > 4 or temp_change_per_hour[tmpds] < -4):
            temp_change_per_hour[tmpds] = None


          if (tmpds == "solarlead" and tmpds in last_temp_influx and tmpds in temp_change_per_sec and last_temp_influx[tmpds]<200):
#            logging.debug("Begin setting est temps")
            temp_at_12pm[tmpds] = get_est_temp_at_hr(12, last_temp_influx[tmpds], temp_change_per_sec[tmpds], curtime)
            temp_at_2pm[tmpds] = get_est_temp_at_hr(14, last_temp_influx[tmpds], temp_change_per_sec[tmpds], curtime)
            temp_at_4pm[tmpds] = get_est_temp_at_hr(16, last_temp_influx[tmpds], temp_change_per_sec[tmpds], curtime)
            temp_at_6pm[tmpds] = get_est_temp_at_hr(18, last_temp_influx[tmpds], temp_change_per_sec[tmpds], curtime)
            logging.debug("Est temps for Solar Lead %s %s %s %s" % (temp_at_12pm[tmpds], temp_at_2pm[tmpds], temp_at_4pm[tmpds], temp_at_6pm[tmpds]))
          else:
            logging.debug("Setting est temps to NONE")
            temp_at_12pm[tmpds] = None
            temp_at_2pm[tmpds]  = None
            temp_at_4pm[tmpds]  = None
            temp_at_6pm[tmpds]  = None


          influxdb(cnt, cur_temp[tmpds], dsname[tmpds], last_temp_influx[tmpds], solar_temp_diff, temp_change_per_hour[tmpds], temp_at_12pm[tmpds], temp_at_2pm[tmpds], temp_at_4pm[tmpds], temp_at_6pm[tmpds])
          last_temp_influx[tmpds] = cur_temp[tmpds]
          last_temp_change_ts[tmpds] = curtime

          if tmpds == "solarlead":
            logging.debug("Generating Holt Winters predictions (outer)")
            generate_holt_winters("1h")
            generate_holt_winters("2h")
            generate_holt_winters("4h")
            if (curtime - pumpstarttime) / 60 / 60 > pumphour:
              logging.debug("Generating Holt Winters tracking prediction (for forumla tuning)")
              generate_holt_winters("1h_tracking")
              generate_holt_winters("2h_tracking")
              generate_holt_winters("4h_tracking")
              pumphour += 1

    cnt += 1
    
    time.sleep(15)

    if cnt % 4 == 0:
      pump_exit_if_off()


pump_exit_if_off()
#get_st_endpoints()

main()

