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

# increased logging
debug_mode = True

# increased increased logging
verbose_mode = False

#RPM baseline test: Run Pump for 3 minutes at every 10 RPMs between the Pump_min and Pump_max
rpm_baseline = False

#if nodejs_poolcontroller:
#  from socketIO_client import SocketIO, BaseNamespace
#  socketIO = SocketIO('192.168.5.31', 3000, BaseNamespace)
# socketIO.emit('setPumpCommand', 'run', 1, rpm)

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

logging.VERBOSE = 5
logging.addLevelName(logging.VERBOSE, "VERBOSE")
logging.Logger.verbose = lambda inst, msg, *args, **kwargs:inst.log(logging.VERBOSE, msg, *args, **kwargs)
logging.verbose = lambda msg, *args, **kwargs: logging.log(logging.VERBOSE, msg, *args, **kwargs)

logger = logging.getLogger()
if debug_mode:
  logger.setLevel(logging.DEBUG)
elif verbose_mode:
  logger.setLevel(logging.VERBOSE)
else:
  logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')


fh = RotatingFileHandler('/var/log/PiPool/PiPool.log', maxBytes=100000, backupCount=10)

if debug_mode:
  fh.setLevel(logging.DEBUG)
elif verbose_mode:
  fh.setLevel(logging.VERBOSE)
else:
  fh.setLevel(logging.INFO)
fh.setFormatter(formatter)

logger.addHandler(fh)

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
#dsid["poolreturn"]    = "0114b80e0bff"   -- Replaced with 3M version to reduce wire tension
dsid["poolreturn"]    = "02161de351ee"

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

#st_deviceid = 'jhws1400int'
st_endpoint = 'https://graph.api.smartthings.com/api/smartapps/installations/b83a222b-628b-4eaa-84c5-38ba013bf5e4'
st_api_token = 'baa5b0e2-7cc0-493e-bc6a-db928bfa2780'
st_headers = { 'Authorization' : 'Bearer ' + st_api_token }
#st_baseurl = st_endpoint + '/update/' + st_deviceid + '/'


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

curtime = int(time.mktime(time.localtime()))
pumpstarttime = curtime

ctime_struct = time.localtime()

def generate_holt_winters(tmpMeasurement):

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

  query = 'SELECT holt_winters(mean("temp"),' + tmpNumPeriods + ',0) INTO "temp_' + tmpMeasurement + '" FROM "PoolStats" WHERE "mode" = \'pool\' AND "sensor" = \'Pool - Solar Lead Temp\' AND time>now() - ' + tmpQueryTime + ' GROUP BY time(' + tmpGroupBy + ',' + tmpGroupBy + ')'
  try:
    logging.debug(" INFLUX: Generating HOLT WINTERS %s temperature projections" % tmpMeasurement)
    result = client.query(query)
    logging.debug(" INFLUX: SUCCESS running query: %s" % query)
  except:
    logging.error(" INFLUX: FAIL Unable to execute INFLUX query: %s" % query) 
    raise
    exit()

  query = 'select holt_winters from temp_' + tmpMeasurement + ' order by time desc limit 1'
  try:
#    logging.debug(" INFLUX: Generating HOLT WINTERS %s temperature projections" % tmpMeasurement)
    result = client.query(query)
    logging.debug(" INFLUX: SUCCESS running query: %s" % query)
    logging.info("   INFLUX:{0}".format(result))
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

def influxdb(counter, temp_f, sub_dsname, sub_last_temp_influx, sub_solar_temp_diff, sub_temp_change_per_hour, tmpDataStatus, tmpPumpMode, tmptesttag):
  logging.info ("Sending to InfluxDB (%s Cycle %04d( %s mode ): dsname: %s\ttemp_f: %.4f oldtemp: %.4f solar diff: %s temp_change_per_hour(F/hour): %s" % (tmpDataStatus, counter, tmpPumpMode, sub_dsname, temp_f, sub_last_temp_influx, sub_solar_temp_diff, sub_temp_change_per_hour))
  pump_rpm, pump_watts = get_pump_rpm(nodejs_poolcontroller)

  influx_json = [
  {
    "measurement": "PoolStats",
    "tags": {
       "sensor": sub_dsname,
       "mode": tmpPumpMode,
       "data_status": tmpDataStatus,
       "test_tag": tmptesttag
    },
    "fields": {
       "temp": temp_f,
       "temp_solar_diff": sub_solar_temp_diff,
       "temp_change_per_hour": sub_temp_change_per_hour,
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
    logging.verbose(pumps)
    logging.verbose(pumps['1'])
    pump_onoff = pumps['1']['power']

    return pump_onoff

  else:
    logging.info("Node.js Pool Controller is not installed - skipping getting pump on/off status")

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
    pumprpm = resp['1']['rpm']
    pumpwatts = resp['1']['watts']

    return pumprpm, pumpwatts
  else:
    logging.info("Node.js Pool Controller is not enabled/installed - skipping getting pump rpm/watts")


def change_pump_rpm(nodejs_poolcontroller, pump_rpm_speed):
  if nodejs_poolcontroller:
    url = 'http://192.168.5.31:3000/pumpCommand/run/pump/1/rpm/'+str(pump_rpm_speed)

#    try:
#      socketIO.emit('setPumpCommand', 'run', 1, pump_rpm_speed)
#      logging.info("  ** CHANGE RPM ** : changing RPM to %s" % (pump_rpm_speed))
    try:
      r = requests.get(url)
      logging.info("  ** CHANGE RPM ** : changing RPM to %s using URL %s" % (pump_rpm_speed, url))
    except:
      raise
      logging.error("Cannot connect to the Pool Controller (Node.js - change_pump_rpm)... exiting.")
      exit()

  else:
    logging.info("Node.js Pool Controller is not enabled/installed - skipping getting pump rpm/watts")


def read_temp(tmpds):

  from w1thermsensor import W1ThermSensor
  sensor = W1ThermSensor(W1ThermSensor.THERM_SENSOR_DS18B20, tmpds)
  temp_f = sensor.get_temperature(W1ThermSensor.DEGREES_F)

  return temp_f

def pump_exit_if_off(ctime):

  pump_onoff = None
  pump_onoff = get_pump_onoff(nodejs_poolcontroller)

  if pump_onoff == 0:
    logging.warning("Pump is not running. Exiting.")
    exit()
  if pumpstarttime and ctime:
    runtime = ctime - pumpstarttime
    logging.debug("  %d: Pump has been active since %d (%d runtime)" % (ctime, pumpstarttime, runtime))

def main():
  pumphour = 1 
  dataStatus = 'Transitional'
  old_pump_mode = None
  pump_mode_time = None

  pump_rpm_min = 2300
  pump_rpm_max = 3250
  pump_rpm_period = 300

  logging.info ("Pump start time recorded [%.0d]" % (pumpstarttime))

  upper_submit_limit = 125
  lower_submit_limit = 45
  solar_temp_diff = None
  pump_rpm_change_time = int(time.mktime(time.localtime()))
  pump_rpm_speed = pump_rpm_min

  get_st_endpoints()

  cnt=0
  while (1):
    curtime = int(time.mktime(time.localtime()))
    pump_mode = get_pump_mode(nodejs_poolcontroller)

    logging.debug("*** current_pump_mode = %s|old_pump_mode = %s" % (pump_mode, old_pump_mode))

    if pump_mode != old_pump_mode:
      pump_mode_time = curtime
      old_pump_mode = pump_mode
      logging.info("Pump mode has changed from %s to %s. Data is Transitional for 60 seconds..." % (old_pump_mode, pump_mode))
      dataStatus = 'Transitional'


    if rpm_baseline and curtime - pump_rpm_change_time > 180 and dataStatus == 'Active' and pump_mode == 'pool':
      pump_rpm_speed += 10
      change_pump_rpm(nodejs_poolcontroller, pump_rpm_speed)
      pump_rpm_change_time = curtime
      test_tag = 'RPM Baseline'
    else:
      test_tag = None
      logging.debug("  ** Change RPM: baseline not running. |rpm_baseline: %s|curtime: %d|pump_rpm_change_time: %d|dataStatus: %s|pump_mode: %s" % (rpm_baseline, curtime, pump_rpm_change_time, dataStatus, pump_mode))

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

          if curtime - pumpstarttime > 600 and pump_mode_time + 60 < curtime:
            logging.debug("Changing/Confirming data status to %s" % dataStatus)
            dataStatus = 'Active'
          elif debug_mode:
            logging.debug("  DEBUG MODE: Changing/Confirming data status to %s" % dataStatus)
            dataStatus = 'Active'
          else:
            logging.debug("Data status remains %s" % dataStatus)

          influxdb(cnt, cur_temp[tmpds], dsname[tmpds], last_temp_influx[tmpds], solar_temp_diff, temp_change_per_hour[tmpds], dataStatus, pump_mode, test_tag)
          last_temp_influx[tmpds] = cur_temp[tmpds]
          last_temp_change_ts[tmpds] = curtime

          if tmpds == "solarlead" and pump_mode == "pool":
            if curtime - pumpstarttime > 600:
              logging.debug("Generating Holt Winters 1h predictions (outer)")
              generate_holt_winters("1h")
            if curtime - pumpstarttime > 900:
              logging.debug("Generating Holt Winters 2h predictions (outer)")
              generate_holt_winters("2h")
            if curtime - pumpstarttime > 1800:
              logging.debug("Generating Holt Winters 4h predictions (outer)")
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
      pump_exit_if_off(curtime)


pump_exit_if_off(curtime)

main()

