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
from st_endpoint import *

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

# Pump RPM automation
auto_rpm = True

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

#=====================================

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
if verbose_mode:
  logger.setLevel(logging.VERBOSE)
elif debug_mode:
  logger.setLevel(logging.DEBUG)
else:
  logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')


fh = RotatingFileHandler('/var/log/PiPool/PiPool.log', maxBytes=100000, backupCount=10)

if verbose_mode:
  fh.setLevel(logging.VERBOSE)
elif debug_mode:
  fh.setLevel(logging.DEBUG)
else:
  fh.setLevel(logging.INFO)
fh.setFormatter(formatter)

#fh.setLevel(logging.DEBUG)

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
dsid["poolreturn"]    = "02161de351ee"

pumprpms = ['2500','2700','2850','3050','3250','3450']
pumprpm_id = [13,12,14,15,16,4]

rpm_change_mode = 'aggressive'

temp_diff_ma = {
  'aggressive'  : 20,
  'balanced'    : 60,
  'conservative': 120
}

temp_diff_lb = {
  'aggressive'  : 0.7,
  'balanced'    : 0.8,
  'conservative': 1.0
}

temp_diff_ub = {
  'aggressive'  : 1.5,
  'balanced'    : 1.7,
  'conservative': 2.0
}

outside_temp_diff_offset = {
  'aggressive'  : 3,
  'balanced'    : 1.5,
  'conservative': 0
}

outside_temp_schedule_start_offset = {
  'aggressive'  : 8,
  'balanced'    : 4,
  'conservative': 1
}

top_rpm_offset = {
  'aggressive'  : 0,
  'balanced'    : 1,
  'conservative': 2
}


### no modifications should be needed below this line
# Read from external file
logging.verbose ("reading the following variables from st_endpoint.py:")
logging.verbose ("  st_endpoint: %s" % st_endpoint)
logging.verbose ("  st_api_token: %s" % st_api_token)
 
st_headers = { 'Authorization' : 'Bearer ' + st_api_token }

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

if debug_mode or verbose_mode:
  cnt_offset = 5
else:
  cnt_offset = 25

def generate_holt_winters(tmpMeasurement):

  query = 'DELETE FROM temp_' + tmpMeasurement
  try:
    logging.verbose(" INFLUX: deleting from measurement %s" % tmpMeasurement)
    result = client.query(query)
    logging.verbose(" INFLUX: SUCCESS running query: %s" % query)
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
    logging.verbose(" INFLUX: Generating HOLT WINTERS %s temperature projections" % tmpMeasurement)
    result = client.query(query)
    logging.verbose(" INFLUX: SUCCESS running query: %s" % query)
  except:
    logging.error(" INFLUX: FAIL Unable to execute INFLUX query: %s" % query) 
    raise
    exit()

  query = 'select holt_winters from temp_' + tmpMeasurement + ' order by time desc limit 1'
  try:
#    logging.debug(" INFLUX: Generating HOLT WINTERS %s temperature projections" % tmpMeasurement)
    result = client.query(query)
    logging.verbose(" INFLUX: SUCCESS running query: %s" % query)
    logging.verbose("   INFLUX:{0}".format(result))
  except:
    logging.error(" INFLUX: FAIL Unable to execute INFLUX query: %s" % query) 
    raise
    exit()

def initialize_vars():
  for tmpds in dsref:
    last_temp_st[tmpds]=999999.99
    last_temp_influx[tmpds]=999999.99
    last_temp_change_ts[tmpds]=999999.99
    temp_change_per_hour[tmpds]=999999.99

def influxdb(counter, temp_f, sub_dsname, sub_last_temp_influx, sub_solar_temp_diff, sub_temp_change_per_hour, tmpDataStatus, tmpPumpMode, tmptesttag):
## horrible coding - couldn't figure out how to shorten the logging string for variables below when they are [None]
## had to create separate strings for logging so floats/None could still be passed to Influx
  if sub_solar_temp_diff is not None and sub_solar_temp_diff != 0:
    if (sub_solar_temp_diff > 15 or sub_solar_temp_diff < -5):
      sub_solar_temp_diff = None
      sub_sub_solar_temp_diff = None
    else:
      sub_sub_solar_temp_diff = "{:.4f}".format(sub_solar_temp_diff)
  else:
    sub_sub_solar_temp_diff = None
    sub_solar_temp_diff = None
  if sub_temp_change_per_hour is not None and sub_temp_change_per_hour != 0:
    if (sub_temp_change_per_hour > 4 or sub_temp_change_per_hour < -4):
      sub_temp_change_per_hour = None
      sub_sub_temp_change_per_hour = None
    else:
      sub_sub_temp_change_per_hour = "{:.4f}".format(sub_temp_change_per_hour)
  else:
    sub_sub_temp_change_per_hour = None
    sub_temp_change_per_hour = None
## end horrible coding

  logging.info ("Sending to InfluxDB (%s Cycle %04d( %s mode ): dsname: %s\ttemp_f: %.4f oldtemp: %.4f solar diff: %s temp_change_per_hour(F/hour): %s" % (tmpDataStatus, counter, tmpPumpMode, sub_dsname, temp_f, sub_last_temp_influx, sub_sub_solar_temp_diff, sub_sub_temp_change_per_hour))
#  logging.info ("Sending to InfluxDB (%s Cycle %04d( %s mode ): dsname: %s\ttemp_f: %.4f oldtemp: %.4f solar diff: %s temp_change_per_hour(F/hour): %s" % (tmpDataStatus, counter, tmpPumpMode, sub_dsname, temp_f, sub_last_temp_influx, sub_solar_temp_diff, sub_temp_change_per_hour))
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

def get_temp_diff_ma(tmpcnt):
  if tmpcnt > temp_diff_ma[rpm_change_mode]:
    tmpcnt = temp_diff_ma[rpm_change_mode]

  query = 'SELECT moving_average(mean("temp_solar_diff"), ' + str(tmpcnt) + ') FROM PiPool.autogen.PoolStats WHERE "mode" = \'pool\' AND "data_status" = \'Active\' AND time >= now() - 1h GROUP BY time(5s) fill(previous) ORDER BY time desc LIMIT 1'
  try:
    result = client.query(query)
    logging.verbose(" INFLUX: SUCCESS running query: %s" % query)
    logging.verbose("   INFLUX:{0}".format(result))

    testma = result.raw
    testma = json.dumps(testma)
    testma = json.loads(testma)
    testma = testma["series"][0]["values"][0][1]

    logging.verbose("   INFLUX: latest MA: %s" % testma)
  except:
    logging.error(" INFLUX: FAIL Unable to execute INFLUX query: %s" % query) 
    raise
    exit()

  return testma

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
    logging.warning("Node.js Pool Controller is not installed - skipping getting pump on/off status")

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
      logging.verbose("Circuit 1 status = POOL (ON)")
      pumpmode = 'pool'
    elif pumpmode == 1:
      logging.verbose("Circuit 1 status = SPA (ON)")
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

def change_pump_rpm(nodejs_poolcontroller, pump_rpm_speed_step, pump_rpm_action):
  if nodejs_poolcontroller:
    tmpcnt=0
    active_speed_step = -1
    for tmpcircuit in pumprpm_id:
      url = 'http://192.168.5.31:3000/circuit/'+str(tmpcircuit)
      try:
        r = requests.get(url)
        logging.debug("  ** GET CIRCUIT STATUS ** : checking status of circuit %s(%s) using URL %s" % (pumprpms[tmpcnt], tmpcircuit, url))

        resp = json.loads(r.text)
        circuitstatus = resp['status']

        if circuitstatus == 0:
          logging.debug("Circuit %s(%s) status = OFF - no action needed" % (pumprpms[tmpcnt],tmpcircuit))
        elif circuitstatus == 1:
          active_speed_step = tmpcnt
          if ((pump_rpm_action == "increase" and tmpcnt != len(pumprpms)-1) or (pump_rpm_action == "decrease" and tmpcnt != 0 )):
            logging.info("Circuit %s(%s) status = ON - toggling to OFF" % (pumprpms[tmpcnt],tmpcircuit))
            circuittoggle = toggle_circuit(tmpcnt)

            if circuittoggle == 1:
              logging.error("LOGIC FAILURE - Circuit %s(%s) reported ON, was toggled OFF, but still reporting ON" % (pumprpms[tmpcnt],tmpcircuit))
              logging.error("EXITING")
              exit()
      except:
        raise
        logging.error("Cannot connect to the Pool Controller (Node.js - change_pump_rpm)... exiting.")
        exit()

      tmpcnt+=1

    if pump_rpm_speed_step:
      circuittoggle = toggle_circuit(pump_rpm_speed_step)
    elif pump_rpm_action:
      prev_speed_step = active_speed_step
      if pump_rpm_action == "increase":
        if active_speed_step < 5 - top_rpm_offset[rpm_change_mode] or active_speed_step is -1:
          active_speed_step += 1
        else:
          logging.debug("  Max speed step reached for %s mode.  Can't increase RPMs any higher" % rpm_change_mode)
      elif pump_rpm_action == "decrease":
        if active_speed_step >= 0:
          active_speed_step -= 1
        else:
          logging.debug("  Minimum speed step reached.  Can't decrease RPMs any lower")
      else:
        logging.error("Invalid pump rpm action: (%s). Exiting..." % pump_rpm_action)
        exit()

      if active_speed_step != prev_speed_step and active_speed_step != -1:
        logging.debug("    RPM CHANGE: %s requested. Setting RPM to %s(%s)" % (pump_rpm_action, pumprpms[active_speed_step], pumprpm_id[active_speed_step]))
        circuittoggle = toggle_circuit(active_speed_step)

    return active_speed_step

  else:
    logging.info("Node.js Pool Controller is not enabled/installed - skipping getting pump rpm/watts")

def toggle_circuit(pump_rpm_speed_step):
  try:
    url = 'http://192.168.5.31:3000/circuit/' + str(pumprpm_id[pump_rpm_speed_step]) + '/toggle'

    r = requests.get(url)
    resp = json.loads(r.text)
    circuittoggle = resp['value']

    if circuittoggle == 0:
      logging.info("  Circuit %s(%s) confirmed OFF" % (pumprpms[pump_rpm_speed_step],pumprpm_id[pump_rpm_speed_step]))
    elif circuittoggle == 1:
      logging.info("  Circuit %s(%s) confirmed ON" % (pumprpms[pump_rpm_speed_step],pumprpm_id[pump_rpm_speed_step]))
    else:
      logging.error("Error toggling circuit %s(%s) - expected an output of 0 or 1 - got %s" % (pumprpms[pump_rpm_speed_step],pumprpm_id[pump_rpm_speed_step],circuittoggle))
      logging.error("EXITING")
      exit()
  except:
    raise
    logging.error("Cannot connect to the Pool Controller (Node.js - change_pump_rpm)... exiting.")
    exit()

  return circuittoggle

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
    try:
      cnt 
    except:
      logging.debug("Pump cycle count is not set so exiting without checking circuit status")
    else:
      active_speed_step = change_pump_rpm(nodejs_poolcontroller, None, None)
  
    exit()

  if pumpstarttime and ctime:
    runtime = ctime - pumpstarttime
    logging.debug("  %d: Pump has been active since %d (%d runtime)" % (ctime, pumpstarttime, runtime))

def main():
  pumphour = 1 
  dataStatus = 'Transitional'
  old_pump_mode = None
  pump_mode_time = None
  solar_prime_active = "false"

  logging.info ("Pump start time recorded [%.0d]" % (pumpstarttime))

  upper_submit_limit = 125
  lower_submit_limit = 45
  solar_temp_diff = None
  last_solar_temp_diff = None
  pump_rpm_period = 300
  pump_rpm_change_time = int(time.mktime(time.localtime()))
  active_speed_step = 0
  solar_temp_diff_ma = 0


  initialize_vars()

  cnt=0
  while (1):
    curtime = int(time.mktime(time.localtime()))
    pump_mode = get_pump_mode(nodejs_poolcontroller)

    if ('solarreturn' in cur_temp and 'solarlead' in cur_temp):
      logging.verbose("\t\t\tSolar Return temp: %.4f" % cur_temp['solarreturn'])
      logging.verbose("\t\t\tSolar Lead temp:  %.4f " % cur_temp['solarlead'])
      logging.verbose("\n\n")
      solar_temp_diff = cur_temp['solarreturn'] - cur_temp['solarlead']

    if pump_mode != old_pump_mode:
      pump_mode_time = curtime
      old_pump_mode = pump_mode
      logging.info("Pump mode has changed from %s to %s. Data is Transitional for 60 seconds..." % (old_pump_mode, pump_mode))
      dataStatus = 'Transitional'
    else:
      logging.verbose("*** current_pump_mode = %s|old_pump_mode = %s" % (pump_mode, old_pump_mode))

    if rpm_baseline and curtime - pump_rpm_change_time > 180 and dataStatus == 'Active' and pump_mode == 'pool':
      pump_rpm_speed += 10
      pump_rpm_change_time = curtime
      test_tag = 'RPM Baseline'
      logging.debug("  ** Change RPM: baseline running. |rpm_baseline: %s|curtime: %d|pump_rpm_change_time: %d|dataStatus: %s|pump_mode: %s" % (rpm_baseline, curtime, pump_rpm_change_time, dataStatus, pump_mode))
    else:
      test_tag = None
      logging.verbose("  ** Change RPM: baseline not running. |rpm_baseline: %s|curtime: %d|pump_rpm_change_time: %d|dataStatus: %s|pump_mode: %s" % (rpm_baseline, curtime, pump_rpm_change_time, dataStatus, pump_mode))

    for tmpds in dsref:
      temp_f = read_temp(dsid[tmpds])
      cur_temp[tmpds] = temp_f

      st_baseurl = st_endpoint + '/update/' + dsid[tmpds] + '/'
      endp_url = st_baseurl + ("%.2f/F" % temp_f)

      if ( lower_submit_limit <= temp_f <= upper_submit_limit ):
        if ( sendto_smartthings and abs(round(temp_f, 2) - round(last_temp_st[tmpds], 2)) > 0.18 ):
          logging.debug ("Endpoint submit URL: %s" % (endp_url))
          logging.info ("Sending to Smartthings (Cycle %04d): dsname: %s\ttemp_f: %.4f (oldtemp: %.4f)" % (cnt, dsname[tmpds], temp_f, last_temp_st[tmpds]))
          r = requests.put(endp_url, headers=st_headers)
          last_temp_st[tmpds] = temp_f

        if ( sendto_influxdb and round(cur_temp[tmpds], 2) != round(last_temp_influx[tmpds], 2)):
          logging.verbose("\n\ncur_temp DICT:")
          logging.verbose(cur_temp)

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
            logging.verbose("Changing/Confirming data status to %s" % dataStatus)
            dataStatus = 'Active'
          elif debug_mode or verbose_mode:
            dataStatus = 'Active'
            logging.verbose("  Changing/Confirming data status to %s" % dataStatus)
          else:
            logging.verbose("Data status remains %s" % dataStatus)

          influxdb(cnt, cur_temp[tmpds], dsname[tmpds], last_temp_influx[tmpds], solar_temp_diff, temp_change_per_hour[tmpds], dataStatus, pump_mode, test_tag)
          last_temp_influx[tmpds] = cur_temp[tmpds]
          last_temp_change_ts[tmpds] = curtime

# Generate Holt-Winters temp predictions
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

    # Change RPM speed based on temperature differences
    pump_rpm_speed_step = None
    if auto_rpm and solar_prime_active == "false" and dataStatus == "Active" and pump_mode == "pool" and active_speed_step <= len(pumprpms)-1 and cnt > cnt_offset:
      if cur_temp['solarlead'] < 87 and active_speed_step <= len(pumprpms)-1:
          rpm_change_elapsed_sec = curtime - pump_rpm_change_time
          if rpm_change_elapsed_sec > pump_rpm_period:
            logging.debug("  Criteria for rpm change met. Checking solar_temp_diff: %s" % (solar_temp_diff_ma))
            if solar_temp_diff_ma >= temp_diff_ub[rpm_change_mode]:
              logging.info("    Temp diff above threshold of %s; increasing RPM" % temp_diff_ub[rpm_change_mode])
              active_speed_step = change_pump_rpm(nodejs_poolcontroller, pump_rpm_speed_step, "increase")
              pump_rpm_change_time = curtime
            elif solar_temp_diff_ma < temp_diff_lb[rpm_change_mode]:
              logging.info("    Temp diff below threshold of %s; decreasing RPM" % temp_diff_lb[rpm_change_mode])
              active_speed_step = change_pump_rpm(nodejs_poolcontroller, pump_rpm_speed_step, "decrease")
              pump_rpm_change_time = curtime
            else:
              logging.debug("    NO RPM CHANGE: Temp diff between %s range of %s and %s." % (rpm_change_mode, temp_diff_lb[rpm_change_mode], temp_diff_ub[rpm_change_mode]))
          else:
            logging.debug ("    NO RPM CHANGE: Current RPM will remain active for %d seconds (%d seconds elapsed)" % (pump_rpm_period, rpm_change_elapsed_sec))
      elif cur_temp['solarlead'] >= 87 and active_speed_step != 0:
        logging.debug ("    RPM CHANGE - reducing RPMs - pool is at or above comfort level: %s (expected <87): " % cur_temp['solarlead'])
        active_speed_step = change_pump_rpm(nodejs_poolcontroller, pump_rpm_speed_step, "decrease")
      else:
        logging.debug ("    NO RPM CHANGE: Pool is above comfort level: %s (expected <87), and Pump RPMs are at minimum." % cur_temp['solarlead'])
    else:
      logging.debug ("  Criteria for RPM change not met:")
      logging.verbose ("    Solar Prime Active: %s" % solar_prime_active)
      logging.verbose ("    Data Status: %s" % dataStatus)
      logging.verbose ("    Pump Mode: %s" % pump_mode)
      logging.verbose ("    Cycle Cnt: %s" % cnt)
      logging.verbose ("    Auto RPM value: %s (expected True)" % auto_rpm)

    # Prime system when temp change indicates solar has been turned on
    if cnt > 4 and last_solar_temp_diff != solar_temp_diff and solar_prime_active == "false":
      solar_diff_diff = solar_temp_diff - last_solar_temp_diff
      solar_temp_diff_ma = get_temp_diff_ma(cnt)

      if solar_diff_diff > 2:
        logging.info ("Solar priming ACTIVE:")
        logging.debug ("    current temp diff: %.4f" % solar_temp_diff)
        logging.debug ("    last temp diff   : %.4f" % last_solar_temp_diff)
        logging.debug ("    temp diff diff   : %.4f" % solar_diff_diff)
        solar_prime_activated_ts = curtime 
        solar_prime_active = "true"
        active_speed_step = change_pump_rpm(nodejs_poolcontroller, 2, None)
      else:
        logging.debug ("Solar priming NOT active:")
        logging.debug ("    current temp diff: %.4f" % solar_temp_diff)
        logging.debug ("    last temp diff   : %.4f" % last_solar_temp_diff)
        logging.debug ("    temp diff diff   : %.4f" % solar_diff_diff)
        solar_prime_active = "false"
    elif solar_prime_active == "true":
      solar_prime_elapsed_sec = curtime - solar_prime_activated_ts
      if curtime - solar_prime_activated_ts > 300:
        solar_prime_active = "false"
        logging.info ("Solar priming has been running for the last %d seconds.  Deactivating" % solar_prime_elapsed_sec)
        pump_rpm_speed_step = None
        active_speed_step = change_pump_rpm(nodejs_poolcontroller, pump_rpm_speed_step, None)
      else:
        logging.debug ("Solar Priming STILL ACTIVE - will remain active for 5 minutes (%d seconds elapsed)" % solar_prime_elapsed_sec)
    last_solar_temp_diff = solar_temp_diff

    cnt += 1
    
    time.sleep(15)

    if cnt % 4 == 0:
      pump_exit_if_off(curtime)


pump_exit_if_off(curtime)

main()

