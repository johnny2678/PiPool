#!/usr/bin/python3

import os
import sys
import glob
import re
from io import BytesIO
import requests
import time
import subprocess
import datetime
import re
import logging
import pprint
import json
import RPi.GPIO as GPIO
#from st_endpoint import *

#os.environ['TZ']='UTC'
os.environ['TZ'] = 'America/New_York'

sess = requests.Session()
adapter = requests.adapters.HTTPAdapter(max_retries=10)
sess.mount('http://', adapter)

def read_config():
  with open('/home/pi/PiPool/config.json') as data_file:
    config = json.loads(data_file.read())
  logging.debug("reading config from json file")
  nodejs_poolcontroller = config["integrations"][0]["poolController"][1]["enabled"]
  sendto_smartthings = config["integrations"][0]["smartthings"][1]["enabled"]
  auto_rpm = config["pump_options"][0]["auto_change_pump_rpm"][1]["auto_rpm"]
  
  rpm_change_mode = config["pump_options"][0]["auto_change_pump_rpm_mode"][1]["rpm_change_mode"]
  temp_diff_ma = config["pump_options"][0]["temp_diff_ma"][1]
  temp_diff_lb = config["pump_options"][0]["temp_diff_lb"][1]
  temp_diff_ub = config["pump_options"][0]["temp_diff_ub"][1]

  if config["script_options"][1]["logging_mode"] == "Debug":
    debug_mode = True
    verbose_mode = False
  elif config["script_options"][1]["logging_mode"] == "Verbose":
    verbose_mode = True
    debug_mode = False
  else:
    verbose_mode = False
    debug_mode = False
  sendto_influxdb = config["integrations"][0]["influxDB"][1]["enabled"]
  influx_host = config["integrations"][0]["influxDB"][1]["host"]
  influx_port = config["integrations"][0]["influxDB"][1]["port"]
  influx_user = config["integrations"][0]["influxDB"][1]["user"]
  influx_password = config["integrations"][0]["influxDB"][1]["password"]
  influx_db = config["integrations"][0]["influxDB"][1]["dbname"]
  influx_db_retention_policy_name = config["integrations"][0]["influxDB"][1]["retention_policy"]
  influx_db_retention_duration = config["integrations"][0]["influxDB"][1]["retention_duration"]
  influx_db_retention_replication = config["integrations"][0]["influxDB"][1]["retention_replication"]
  st_endpoint = config["integrations"][0]["smartthings"][1]["endpoint"]
  st_api_token = config["integrations"][0]["smartthings"][1]["api_token"]
  pump_base_rpm = config["pump_options"][0]["energy_baseline"][1]["pump_base_rpm"]
  pump_base_watts = config["pump_options"][0]["energy_baseline"][1]["pump_base_watts"]
  cents_per_kWh = config["pump_options"][0]["energy_baseline"][1]["cents_per_kWh"]
#  pad_energy_usage = config["pump_options"][0]["energy_baseline"][1]["pad_energy_usage"]
#  pad_energy_start_hour = config["pump_options"][0]["energy_baseline"][1]["pad_energy_start_hour"]
#  pad_energy_stop_hour = config["pump_options"][0]["energy_baseline"][1]["pad_energy_stop_hour"]

#  return rpm_change_mode, temp_diff_ma, temp_diff_ub, temp_diff_lb, pad_energy_usage, pad_energy_start_hour, pad_energy_stop_hour, pump_base_rpm, pump_base_watts, cents_per_kWh, st_endpoint, st_api_token, sendto_smartthings, sendto_influxdb, nodejs_poolcontroller, auto_rpm, debug_mode, verbose_mode, influx_host, influx_port, influx_user, influx_password, influx_db, influx_db_retention_policy_name, influx_db_retention_duration, influx_db_retention_replication
  return rpm_change_mode, temp_diff_ma, temp_diff_ub, temp_diff_lb, pump_base_rpm, pump_base_watts, cents_per_kWh, st_endpoint, st_api_token, sendto_smartthings, sendto_influxdb, nodejs_poolcontroller, auto_rpm, debug_mode, verbose_mode, influx_host, influx_port, influx_user, influx_password, influx_db, influx_db_retention_policy_name, influx_db_retention_duration, influx_db_retention_replication

#(rpm_change_mode, temp_diff_ma, temp_diff_ub, temp_diff_lb,pad_energy_usage, pad_energy_start_hour, pad_energy_stop_hour, pump_base_rpm, pump_base_watts, cents_per_kWh, st_endpoint, st_api_token, sendto_smartthings, sendto_influxdb, nodejs_poolcontroller, auto_rpm, debug_mode, verbose_mode, influx_host, influx_port, influx_user, influx_password, influx_db, influx_db_retention_policy_name, influx_db_retention_duration, influx_db_retention_replication) = read_config()
(rpm_change_mode, temp_diff_ma, temp_diff_ub, temp_diff_lb,pump_base_rpm, pump_base_watts, cents_per_kWh, st_endpoint, st_api_token, sendto_smartthings, sendto_influxdb, nodejs_poolcontroller, auto_rpm, debug_mode, verbose_mode, influx_host, influx_port, influx_user, influx_password, influx_db, influx_db_retention_policy_name, influx_db_retention_duration, influx_db_retention_replication) = read_config()

#=====================================
if sendto_influxdb:

   from influxdb import InfluxDBClient
   client = InfluxDBClient(influx_host, influx_port, influx_user, influx_password, influx_db,retries=0)

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
#logger.setLevel(logging.VERBOSE)
if verbose_mode:
  logger.setLevel(logging.VERBOSE)
elif debug_mode:
  logger.setLevel(logging.DEBUG)
else:
  logger.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

fh_info = RotatingFileHandler('/var/log/PiPool/PiPool.info.log', maxBytes=100000, backupCount=10)
fh_debug = RotatingFileHandler('/var/log/PiPool/PiPool.debug.log', maxBytes=100000, backupCount=10)
fh_verbose = RotatingFileHandler('/var/log/PiPool/PiPool.verbose.log', maxBytes=100000, backupCount=10)

fh_info.setLevel(logging.INFO)
fh_debug.setLevel(logging.DEBUG)
fh_verbose.setLevel(logging.VERBOSE)

fh_info.setFormatter(formatter)
fh_debug.setFormatter(formatter)
fh_verbose.setFormatter(formatter)
logger.addHandler(fh_info)
logger.addHandler(fh_debug)
logger.addHandler(fh_verbose)

#=================================

#base_dir = '/sys/bus/w1/devices/'

dsref = ['solarlead', 'solarreturn', 'poolreturn']

dsname = {}
dsname["solarlead"]   = "Pool - Solar Lead Temp"
dsname["solarreturn"] = "Pool - Solar Return Temp"
dsname["poolreturn"]  = "Pool - Return Temp"

dsid = {}
#dsid["solarlead"]     = "800000262ce2"
dsid["solarlead"]     = "011621375bee"
dsid["solarreturn"]   = "80000026331f"
dsid["poolreturn"]    = "02161de351ee"

pumprpms = ['Pool Lowest','Pool Lower','Pool Low','Pool High','Pool Higher','Pool Highest']
pumprpm_id = [13,12,14,15,16,4]

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

bottom_rpm_offset = {
  'aggressive'  : 2,
  'balanced'    : 1,
  'conservative': 0
}


### no modifications should be needed below this line
logging.verbose ("  st_endpoint: %s" % st_endpoint)
logging.verbose ("  st_api_token: %s" % st_api_token)
 
st_headers = { 'Authorization' : 'Bearer ' + st_api_token }

last_temp_st={}
temp_offset={}
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
cnt = 0

if debug_mode or verbose_mode:
  cnt_offset = 4
else:
  cnt_offset = 20

if auto_rpm == "False":
  auto_rpm = None

baseline_day_start = " 09:00:00"
baseline_day_end   = " 17:00:00"
tsnow = datetime.datetime.now()
epnow = datetime.datetime.now().timestamp()
ts9 = str(tsnow.year) + '-' + str(tsnow.month) + '-' + str(tsnow.day) + baseline_day_start
ep9 = int(time.mktime(time.strptime(ts9,'%Y-%m-%d %H:%M:%S')))
ts5 = str(tsnow.year) + '-' + str(tsnow.month) + '-' + str(tsnow.day) + baseline_day_end
ep5 = int(time.mktime(time.strptime(ts5,'%Y-%m-%d %H:%M:%S')))

def initialize_vars():
  for tmpds in dsref:
    last_temp_st[tmpds]=999999.99
    last_temp_influx[tmpds]=999999.99
    last_temp_change_ts[tmpds]=999999.99
    temp_change_per_hour[tmpds]=999999.99

  temp_offset['solarlead'] = 0
  temp_offset['solarreturn'] = -0.788
  temp_offset['poolreturn'] = 0.338

def influxdb(counter, temp_f, sub_dsname, sub_last_temp_influx, sub_solar_temp_diff, sub_temp_change_per_hour, tmpDataStatus, tmpPumpMode, tmpPumpBaseRPM, tmpPumpBaseWatts,last_temp_of_prev_day,first_temp_of_day,overnight_temp_loss,daily_temp_gain, net_temp_gain, running_temp_change_per_hour, tmptimesincelastmeasure):
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

  logging.verbose("current ds is: %s:" % sub_dsname)
  if sub_dsname == 'Pool - Solar Lead Temp':
    tsnow = datetime.datetime.now()
    epnow = datetime.datetime.now().timestamp()
    
    pump_rpm, pump_watts = get_pump_rpm(nodejs_poolcontroller)
    pump_incremental_cost = cents_per_kWh / 1000 / 60 / 60 * pump_watts * tmptimesincelastmeasure


    logging.verbose("    checking that %s[ts5: %s] > %s[tsnow: %s] (diff %d) > %s[ts9: %s] (diff %d)" % (ep5,ts5,epnow,tsnow, ep5-epnow,ep9, ts9,epnow-ep9))
    if (ep5 > epnow > ep9):
      logging.verbose("      calculating pump_base_incremental_cost with %d (cents_per_kWh), %d (tmpPumpBaseWatts), and %d (tmptimesincelastmeasure)" % (cents_per_kWh, tmpPumpBaseWatts, tmptimesincelastmeasure))
      pump_base_incremental_cost = cents_per_kWh / 1000 / 60 / 60 * tmpPumpBaseWatts * tmptimesincelastmeasure
    else:
      pump_base_watts = None
      pump_base_rpm = None
      pump_base_incremental_cost = None

    logging.verbose ("Sending Pool Usage data to InfluxDB(%s Cycle %04d( %s mode ): pump_rpm: %d, pump_watts: %d\n\ttime since last measurement: %d\n\tpump_incremental_cost: %.04f\tpump_base_incremental_cost: %s" % (tmpDataStatus, counter, tmpPumpMode, pump_rpm, pump_watts, tmptimesincelastmeasure, pump_incremental_cost, pump_base_incremental_cost))
    logging.info ("\n\ttime since last measurement: %d\n" % (tmptimesincelastmeasure))
  else:
    logging.verbose("resetting usage data to avoid dupes\n\n")
    pump_rpm = None
    pump_watts = None
    pump_incremental_cost = None
    pump_base_incremental_cost = None

  if sub_dsname and temp_f and sub_last_temp_influx:
    logging.verbose ("Sending Pool Temp data to InfluxDB (%s Cycle %04d( %s mode ): dsname: %s\ttemp_f: %.4f oldtemp: %.4f solar diff: %s temp_change_per_hour(F/hour): %s" % (tmpDataStatus, counter, tmpPumpMode, sub_dsname, temp_f, sub_last_temp_influx, sub_sub_solar_temp_diff, sub_sub_temp_change_per_hour))
    logging.info ("New Temp recorded for %s:\t%.4f. Last Temp was:\t%.4f." % (sub_dsname, temp_f, sub_last_temp_influx))
  influx_json = [
  {
    "measurement": "PoolStats",
    "tags": {
       "sensor": sub_dsname,
       "mode": tmpPumpMode,
       "data_status": tmpDataStatus
    },
    "fields": {
       "temp": temp_f,
       "temp_solar_diff": sub_solar_temp_diff,
       "temp_change_per_hour": sub_temp_change_per_hour,
       "pump_rpm": pump_rpm,
       "pump_watts": pump_watts,
       "pump_incremental_cost": pump_incremental_cost,
       "pump_base_rpm": tmpPumpBaseRPM,
       "pump_base_watts": tmpPumpBaseWatts,
       "pump_base_incremental_cost": pump_base_incremental_cost,
       "last_temp_of_prev_day1": last_temp_of_prev_day,
       "first_temp_of_day": first_temp_of_day,
       "overnight_temp_loss": overnight_temp_loss,
       "daily_temp_gain": daily_temp_gain,
       "net_temp_gain": net_temp_gain,
       "running_temp_change_per_hour": running_temp_change_per_hour
    }
  }
]
        
  client.write_points(influx_json)

def get_temp_diff_ma():

  query = 'SELECT moving_average(mean("temp_solar_diff"), ' + str(temp_diff_ma[rpm_change_mode]) + ') FROM PiPool.autogen.PoolStats WHERE "mode" = \'pool\' AND "data_status" = \'Active\' AND time >= now() - 1h GROUP BY time(5s) fill(previous) ORDER BY time desc LIMIT 1'

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

def get_first_temp():
  from datetime import datetime
  from pytz import timezone

  query = 'SELECT mean("temp") FROM "PoolStats" WHERE ("sensor" = \'Pool - Solar Lead Temp\' AND "mode" = \'pool\') AND  time > now() - 10h  GROUP BY time(3m) fill(none) ORDER BY time asc LIMIT 4;'
  try:
    result = client.query(query)
    logging.verbose(" INFLUX: SUCCESS running query: %s" % query)
    logging.verbose("   INFLUX:{0}".format(result))
   
  except:
    logging.error(" INFLUX: FAIL Unable to execute INFLUX query: %s" % query) 
    raise
    exit()

  testma = result.raw
  testma = json.dumps(testma)
  testma = json.loads(testma)

  try:
    time_of_first_temp = None
    first_temp_of_day = None
    

    try:
      first_temp_of_day = testma["series"][0]["values"][3][1]
    except:
      logging.warning("Not enough data points to get average Solar Lead Temp in the last of the first 4 measurements. Will try again next time.")
#      logging.warning("Not enough data points to get average Solar Lead Temp in the last of the first 4 measurements. Attempting to use the last of the first 3 measurements.")
#      try:
#        first_temp_of_day = testma["series"][0]["values"][2][1]
#      except:
#        logging.warning("  2nd attempt failed.  Will retry to get first temp of day next cycle.")
##        first_temp_of_day = None
##        first_temp_of_day = 80
    try:  
      time_of_first_temp = testma["series"][0]["values"][0][0]
    except:
      logging.warning("No data points today.")
      time_of_first_temp = '2019-07-20T19:22:00Z'
    time_pattern = '%Y-%m-%dT%H:%M:%SZ'
    os.environ['TZ'] = 'UTC'
    time_of_first_temp = int(time.mktime(time.strptime(time_of_first_temp,time_pattern)))
    os.environ['TZ'] = 'America/New_York'

    logging.verbose("\n\n   INFLUX: first temp of day: %s\nraw json: %s" % (first_temp_of_day,testma))

    try:
      if first_temp_of_day > 40:
        logging.info(" First temp of day result is valid.")
        first_temp_of_day += 0.0000000001
    except:
      first_temp_of_day = None
  except IndexError:
    logging.warning("  Error getting first temp of day: %s" % testma)
    first_temp_of_day = None
#    first_temp_of_day = 80


  if first_temp_of_day:
    return first_temp_of_day, time_of_first_temp
  else:
    if time_of_first_temp:
      return None, time_of_first_temp
    else:
      return None, None

def get_last_temp():
  query = 'SELECT mean("temp") as test FROM "PoolStats" WHERE ("sensor" = \'Pool - Solar Lead Temp\' AND "mode" = \'pool\')  AND  time > now() - 25h AND time < now() - 10h GROUP BY time(5m) fill(none) ORDER BY time desc LIMIT 1;'
  
  try:
    result = client.query(query)
    logging.verbose(" INFLUX: SUCCESS running query: %s" % query)
    logging.verbose("   INFLUX:{0}".format(result))

    testma = result.raw
    testma = json.dumps(testma)
    testma = json.loads(testma)
    try:
      last_temp_of_prev_day = testma["series"][0]["values"][0][1]
    except:
      last_temp_of_prev_day = 85.000000001

    logging.verbose("   INFLUX: temp at the end of yesterday: %s" % last_temp_of_prev_day)

    if last_temp_of_prev_day > 40:
      logging.verbose(" Last temp of day result is valid.")
      last_temp_of_prev_day += 0.0000001
    else:
      last_temp_of_prev_day = None
  except:
    logging.error(" INFLUX: FAIL Unable to execute INFLUX query: %s" % query) 
    raise
    exit()
#    last_temp_of_prev_day = 82

  return last_temp_of_prev_day

def add_baseline_rpm():
  tsnow = datetime.datetime.now()
  epnow = datetime.datetime.now().timestamp()
  ts9 = str(tsnow.year) + '-' + str(tsnow.month) + '-' + str(tsnow.day) + baseline_day_start
  ep9 = int(time.mktime(time.strptime(ts9,'%Y-%m-%d %H:%M:%S')))
  ts5 = str(tsnow.year) + '-' + str(tsnow.month) + '-' + str(tsnow.day) + baseline_day_end
  ep5 = int(time.mktime(time.strptime(ts5,'%Y-%m-%d %H:%M:%S')))

  logging.verbose("checking that %s[ts5: %s] > %s[tsnow: %s] (diff %d) > %s[ts9: %s] (diff %d)" % (ep5,ts5,epnow,tsnow, ep5-epnow,ep9, ts9,epnow-ep9))
  if (ep5 > epnow > ep9):
    logging.debug("Adding basline RPM values before shutting down.\n\n")

    pump_base_incremental_cost = cents_per_kWh / 1000 / 60 * pump_base_watts
    influx_json = [
        {
          "measurement": "PoolStats",
          "tags": {
            "mode": 'OFF'
          },
          "fields": {
            "pump_base_rpm": pump_base_rpm,
            "pump_base_watts": pump_base_watts,
            "pump_base_incremental_cost": pump_base_incremental_cost
          }
        }
      ]
    client.write_points(influx_json)
  else:
    logging.info("Pump is off and time is not between 9AM and 5PM. Exiting.\n\n")        

def get_pump_mode(nodejs_poolcontroller):
  if nodejs_poolcontroller:
    url = 'http://192.168.5.31:3000/circuit/6'
    try:
      r = sess.get(url)
    except:
      raise
      logging.error("Cannot connect to the Pool Controller (Node.js)... exiting.")
      exit()

    resp = json.loads(r.text)
    pumpmode = resp['status']

    if pumpmode == 1:
      logging.verbose("Circuit 1 status = POOL (ON)")
      pumpmode = 'pool'
    else:
      url = 'http://192.168.5.31:3000/circuit/1'
      try:
        r = sess.get(url)
      except:
        raise
        logging.error("Cannot connect to the Pool Controller (Node.js)... exiting.")
        exit()

      resp = json.loads(r.text)
      pumpmode = resp['status']

      if pumpmode == 1:
        logging.verbose("Circuit 1 status = SPA (ON)")
        pumpmode = 'spa'

    if pumpmode != 'pool' and pumpmode != 'spa':
      pumpmode = 'off'

    return pumpmode

  else:
    logging.error("Node.js Pool Controller is not enabled/installed - skipping getting pump mode(pool/spa)")

def get_pump_rpm(nodejs_poolcontroller):
  if nodejs_poolcontroller:
    url = 'http://192.168.5.31:3000/pump'
    try:
      r = sess.get(url)
    except:
      raise
      logging.error("Cannot connect to the Pool Controller (Node.js)... exiting.")
      exit()

    resp = json.loads(r.text)
    pumprpm = resp['pump']['1']['rpm']
    pumpwatts = resp['pump']['1']['watts']

    return pumprpm, pumpwatts
  else:
    logging.info("Node.js Pool Controller is not enabled/installed - skipping getting pump rpm/watts")

def get_swg_status(nodejs_poolcontroller):
  if nodejs_poolcontroller:
    url = 'http://192.168.5.31:3000/chlorinator'
    try:
      r = sess.get(url)
    except:
      raise
      logging.error("Cannot connect to the Pool Controller (Node.js)... exiting.")
      exit()

    resp = json.loads(r.text)
    swg_status = resp['chlorinator']['status']

    return swg_status
  else:
    logging.info("Node.js Pool Controller is not enabled/installed - skipping getting chlorinator status")

def change_pump_rpm(nodejs_poolcontroller, pump_rpm_speed_step, pump_rpm_action):
  if nodejs_poolcontroller:
    tmpcnt=0
    active_speed_step = -1
    for tmpcircuit in pumprpm_id:
      url = 'http://192.168.5.31:3000/circuit/'+str(tmpcircuit)
      try:
        r = sess.get(url)
        logging.verbose("  ** GET CIRCUIT STATUS ** : checking status of circuit %s(%s) using URL %s" % (pumprpms[tmpcnt], tmpcircuit, url))

        resp = json.loads(r.text)
        circuitstatus = resp['status']

        if circuitstatus == 0:
          logging.verbose("Circuit %s(%s) status = OFF - no action needed" % (pumprpms[tmpcnt],tmpcircuit))
        elif circuitstatus == 1:
          active_speed_step = tmpcnt
#          if (pump_rpm_action == None or (pump_rpm_action == "increase" and tmpcnt != len(pumprpms) - top_rpm_offset[rpm_change_mode] - 1) or (pump_rpm_action == "decrease" and tmpcnt != bottom_rpm_offset[rpm_change_mode] -1 )):
          if (pump_rpm_action == None or (pump_rpm_action and 0 <= tmpcnt < len(pumprpms) - top_rpm_offset[rpm_change_mode] - 1)):
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
#      if active_speed_step != prev_speed_step:
        logging.debug("    RPM CHANGE: The current speed step (%s) doesn't equal the previous speed step (%s)" % (active_speed_step, prev_speed_step))
        logging.debug("    RPM CHANGE: %s requested. Setting RPM to %s(%s)" % (pump_rpm_action, pumprpms[active_speed_step], pumprpm_id[active_speed_step]))
        circuittoggle = toggle_circuit(active_speed_step)

    return active_speed_step

  else:
    logging.info("Node.js Pool Controller is not enabled/installed - skipping getting pump rpm/watts")

def toggle_circuit(pump_rpm_speed_step):
  try:
    url = 'http://192.168.5.31:3000/circuit/' + str(pumprpm_id[pump_rpm_speed_step]) + '/toggle'

    r = sess.get(url)
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

def read_temp(tmpds, lasttemp):

  temp_f = None

  with open('/home/pi/PiPool/temps.json') as json_file:
    temps = json.loads(json_file.read())
  
  for sensor in temps["sensor"]:
    logging.verbose("checking json entry %s matches script value %s", sensor['dsid'], tmpds)
    if (str(sensor['dsid'])) == tmpds:
      try:
        temp_f = sensor['temp']
      except:
        logging.verbose("Unable to read temp for sensor %s" % sensor[dsid])

  if not temp_f:
    temp_f = lasttemp

  return temp_f

def pump_exit_if_off(ctime, tmpcnt):
  pumpmode = get_pump_mode(nodejs_poolcontroller)

  if pumpmode == 'off':
    logging.warning("Pump is not running. Exiting.")
    try:
      tmpcnt 
    except:
      logging.debug("Pump cycle count is not set so exiting without checking circuit status")
    else:
      active_speed_step = change_pump_rpm(nodejs_poolcontroller, None, None)

    try:
      logging.verbose("Adding Baseline RPM values before exiting. (Outside Sub)")
      add_baseline_rpm()
    except Exception as e:
      logging.exception("\n\n\nJH SCRIPT ERROR:\n\n")
      logging.error(str(e)+"\n\n")
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

  global pumpstarttime
  logging.info ("Pump start time recorded [%.0d]" % pumpstarttime)

  upper_submit_limit = 125
  lower_submit_limit = 45
  solar_temp_diff = None
  last_solar_temp_diff = None
  pump_rpm_intro_period = 600
  pump_rpm_change_period = 600
  pump_rpm_transitional_period = 90
  pump_rpm_change_time = int(time.mktime(time.localtime()))
  active_speed_step = 0
  solar_temp_diff_ma = 0
  pump_rpm = 0
  pump_watts = 0
  prev_pump_rpm = 0
  prev_pump_watts = 0

  first_temp_of_day = None
  time_of_first_temp = None
  last_temp_of_prev_day = None
  overnight_temp_loss = None
  daily_temp_gain = None
  net_temp_gain = None
  running_temp_change_per_hour = None

  initialize_vars()

  cnt=0
  while (1):
    curtime = int(time.mktime(time.localtime()))
    if cnt < 3:
      prevcurtime = curtime

    pump_mode = get_pump_mode(nodejs_poolcontroller)

    logging.info("\n\n\n---------------------- %s %s Cycle %d (%d) --------------------------" % (pump_mode,dataStatus,cnt, curtime))

    if ('solarreturn' in cur_temp and 'solarlead' in cur_temp):
      logging.verbose("\t\t\tSolar Return temp: %.4f" % cur_temp['solarreturn'])
      logging.verbose("\t\t\tSolar Lead temp:  %.4f " % cur_temp['solarlead'])
      logging.verbose("\n\n")
      solar_temp_diff = cur_temp['solarreturn'] - cur_temp['solarlead']

    if pump_mode != old_pump_mode:
      logging.info("\nPump mode has changed from %s to %s. Data is Transitional for %d seconds..." % (old_pump_mode, pump_mode,pump_rpm_transitional_period))
      pump_mode_time = curtime
      old_pump_mode = pump_mode
      dataStatus = 'Transitional'

      if cnt > 4:
        try:
          active_speed_step = change_pump_rpm(nodejs_poolcontroller, None, None)
        except Exception as e:
          logging.exception("\n\n\nJH SCRIPT ERROR:\n\n")
          logging.error(str(e)+"\n\n")
    else:
      logging.verbose("*** current_pump_mode = %s|old_pump_mode = %s" % (pump_mode, old_pump_mode))

    for tmpds in dsref:
      temp_f = read_temp(dsid[tmpds], last_temp_influx[tmpds]) + temp_offset[tmpds]
      cur_temp[tmpds] = temp_f 

      st_baseurl = st_endpoint + '/update/' + dsid[tmpds] + '/'
      endp_url = st_baseurl + ("%.2f/F" % temp_f)

      if ( lower_submit_limit <= temp_f <= upper_submit_limit ):
        if ( sendto_smartthings and abs(round(temp_f, 2) - round(last_temp_st[tmpds], 2)) > 0.18 ):
          logging.verbose ("Endpoint submit URL: %s" % (endp_url))
          logging.verbose ("Sending to Smartthings (Cycle %04d): dsname: %s\ttemp_f: %.4f (oldtemp: %.4f)" % (cnt, dsname[tmpds], temp_f, last_temp_st[tmpds]))
          logging.info ("New Smartthings Temp recorded for %s:\t%.4f. Last Temp was:\t%.4f.)" % (dsname[tmpds], temp_f, last_temp_st[tmpds]))
          try:
            r = requests.put(endp_url, headers=st_headers)
          except Exception as e:
            logging.exception("\n\n\nJH SCRIPT ERROR:\n\n")
            logging.error(str(e)+"\n\n")
          last_temp_st[tmpds] = temp_f

        if ( sendto_influxdb and (round(cur_temp[tmpds], 2) != round(last_temp_influx[tmpds], 2) or (cnt < 20 and first_temp_of_day == None))):
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

          logging.debug("Checking that curtime(%s) - pumpstarttime(%s) > pump_rpm_intro_period (%s)\n   AND that pump_mode_time (%s) + pump_rpm_transitional_period (%s)< curtime (%s)" % (curtime,pumpstarttime,pump_rpm_intro_period,pump_mode_time,pump_rpm_transitional_period,curtime))
          if curtime - pumpstarttime > pump_rpm_intro_period and pump_mode_time + pump_rpm_transitional_period < curtime:
            logging.verbose("Changing/Confirming data status to %s" % dataStatus)
            dataStatus = 'Active'
          else:
            logging.verbose("Data status remains %s" % dataStatus)

          try:
            logging.verbose("\n\n**** curtime:\t%d\n**** prevcurtime:\t%d\n\n" % (curtime, prevcurtime))
            influxdb(cnt, cur_temp[tmpds], dsname[tmpds], last_temp_influx[tmpds], solar_temp_diff, temp_change_per_hour[tmpds], dataStatus, pump_mode, pump_base_rpm, pump_base_watts, last_temp_of_prev_day,first_temp_of_day,overnight_temp_loss,daily_temp_gain, net_temp_gain, running_temp_change_per_hour, curtime - prevcurtime)
          except Exception as e:
            logging.exception("\n\n\nJH SCRIPT ERROR:\n\n")
            logging.error(str(e)+"\n\n")

          last_temp_influx[tmpds] = cur_temp[tmpds]
          last_temp_change_ts[tmpds] = curtime
        else:
          if tmpds == 'solarlead':
            logging.verbose("\n\n *** Sending usage data even though temp didn't change ***\n\n")
            try:
              influxdb(cnt,None,dsname[tmpds],None,None,None,dataStatus,pump_mode, pump_base_rpm, pump_base_watts,None,None,None,None,None,None, curtime - prevcurtime)
            except Exception as e:
              logging.exception("\n\n\nJH SCRIPT ERROR:\n\n")
              logging.error(str(e)+"\n\n")

    # Prime system/solar panels when rpm change indicates solar has been turned on OR when Chlorinator detects low flow
    if cnt > 4 and dataStatus == 'Active' and solar_prime_active == "false":
      pump_rpm, pump_watts = get_pump_rpm(nodejs_poolcontroller)     
      swg_status = get_swg_status(nodejs_poolcontroller)
      solar_diff_diff = solar_temp_diff - last_solar_temp_diff
      solar_temp_diff_ma = get_temp_diff_ma()

      if (pump_watts < prev_pump_watts * 0.93 and pump_rpm == prev_pump_rpm) or swg_status.startswith("Low Flow"):
        tsnow = datetime.datetime.now()
        epnow = datetime.datetime.now().timestamp()
        ts5 = str(tsnow.year) + '-' + str(tsnow.month) + '-' + str(tsnow.day) + baseline_day_end
        ep5 = int(time.mktime(time.strptime(ts5,'%Y-%m-%d %H:%M:%S')))
        ts9 = str(tsnow.year) + '-' + str(tsnow.month) + '-' + str(tsnow.day) + baseline_day_start
        ep9 = int(time.mktime(time.strptime(ts9,'%Y-%m-%d %H:%M:%S')))       
        
        if (epnow > ep5 or epnow < ep9):
          logging.info ("Low Flow or Pump watt reduction detected but time is after threshold.  Skipping Solar Priming.")
        else:
          logging.info ("Solar priming ACTIVE:")
          logging.debug ("    current temp diff: %.4f - last temp diff: %.4f =  temp diff diff: %.4f" % (solar_temp_diff, last_solar_temp_diff, solar_diff_diff))
          logging.debug ("    prev pump rpm    : %s |    current pump rpm   : %s" % (prev_pump_rpm,pump_rpm))
          logging.debug ("    prev pump watts  : %s |    current pump watts : %s (if current < %s then Solar Priming will activate)" % (prev_pump_watts,pump_watts,prev_pump_watts*.93))
          logging.debug ("    SWG status       : %s" % swg_status)
          solar_prime_activated_ts = curtime 
          solar_prime_active = "true"
          active_speed_step = change_pump_rpm(nodejs_poolcontroller, 3, None)
      else:
        logging.debug ("Solar priming NOT active:")
        logging.verbose ("    current temp diff: %.4f - last temp diff: %.4f =  temp diff diff: %.4f" % (solar_temp_diff, last_solar_temp_diff, solar_diff_diff))
        logging.verbose ("    prev pump rpm    : %s |    current pump rpm   : %s" % (prev_pump_rpm,pump_rpm))
        logging.verbose ("    prev pump watts  : %s |    current pump watts : %s (if current < %s then Solar Priming will activate)" % (prev_pump_watts,pump_watts,prev_pump_watts*.93))       
        logging.verbose ("    SWG status       : %s" % swg_status)
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
    
    prev_pump_rpm = pump_rpm
    prev_pump_watts = pump_watts  

    # Change RPM speed based on temperature differences
    pump_rpm_speed_step = None
    if auto_rpm and solar_prime_active == "false" and dataStatus == "Active" and pump_mode == "pool" and active_speed_step <= len(pumprpms)-1 and cnt > cnt_offset:
      logging.verbose("  **** current temp: %s current speed step: %s len(pumprpms): %s top_rpm_offset[%s]: %s bottom_rpm_offset[%s]: %s" % (cur_temp['solarlead'], active_speed_step, len(pumprpms), rpm_change_mode, top_rpm_offset[rpm_change_mode], rpm_change_mode, bottom_rpm_offset[rpm_change_mode]))
      if cur_temp['solarlead'] < 87 and active_speed_step <= len(pumprpms) - top_rpm_offset[rpm_change_mode]:
          rpm_change_elapsed_sec = curtime - pump_rpm_change_time
          if rpm_change_elapsed_sec > pump_rpm_change_period:
            logging.debug("  Criteria for rpm change met. Checking solar_temp_diff: %s" % (solar_temp_diff_ma))
            if solar_temp_diff_ma >= temp_diff_ub[rpm_change_mode]:
              logging.info("    Temp diff (%.4f) above threshold of %s; increasing RPM" % (solar_temp_diff_ma, temp_diff_ub[rpm_change_mode]))
              active_speed_step = change_pump_rpm(nodejs_poolcontroller, None, "increase")
              pump_rpm_change_time = curtime
            elif solar_temp_diff_ma < temp_diff_lb[rpm_change_mode]:
              logging.info("    Temp diff (%.4f) below threshold of %s; decreasing RPM" % (solar_temp_diff_ma, temp_diff_lb[rpm_change_mode]))
              active_speed_step = change_pump_rpm(nodejs_poolcontroller, None, "decrease")
              pump_rpm_change_time = curtime
            else:
              logging.debug("    NO RPM CHANGE: Temp diff between %s range of %s and %s." % (rpm_change_mode, temp_diff_lb[rpm_change_mode], temp_diff_ub[rpm_change_mode]))
          else:
            logging.debug ("    NO RPM CHANGE: Current RPM will remain active for %d seconds (%d seconds elapsed)" % (pump_rpm_change_period, rpm_change_elapsed_sec))
      elif cur_temp['solarlead'] >= 87 and active_speed_step >= bottom_rpm_offset[rpm_change_mode]:
        logging.debug ("    RPM CHANGE - reducing RPMs - pool is at or above comfort level: %s (expected <87): " % cur_temp['solarlead'])
        logging.verbose ("    RPM CHANGE - Active Speed Step (%s) is greater than equal to %s threshold of %s" % (active_speed_step, rpm_change_mode, bottom_rpm_offset[rpm_change_mode]))
        active_speed_step = change_pump_rpm(nodejs_poolcontroller, None, None)
      else:
        logging.debug ("    NO RPM CHANGE: Pool is above comfort level: %s (expected <87), and Pump RPMs are at minimum." % cur_temp['solarlead'])
    else:
      logging.debug ("  Criteria for RPM change not met:")
      logging.verbose ("    Solar Prime Active: %s" % solar_prime_active)
      logging.verbose ("    Data Status: %s" % dataStatus)
      logging.verbose ("    Pump Mode: %s" % pump_mode)
      logging.verbose ("    Cycle Cnt: %s" % cnt)
      logging.verbose ("    Auto RPM value: %s (expected True)" % auto_rpm)    
    
    if((cnt > 25 and first_temp_of_day is None) or cnt == 0):
      logging.debug("\n\n** Getting First Temp of Day **")
      prevpumpstarttime = pumpstarttime
      (first_temp_of_day, pumpstarttime) = get_first_temp()
      if first_temp_of_day:
        logging.debug("First temp of day: %.4f at %s\n\n" % (first_temp_of_day, pumpstarttime))
      else:
        pumpstarttime = prevpumpstarttime
        if cnt > 10:
          first_temp_of_day = cur_temp['solarlead']

      if cnt == 0:
        if curtime - pumpstarttime > 300:
          logging.info('** Midday startup detected - setting data mode to Active')
          dataStatus = 'Active'

    if(last_temp_of_prev_day is None):
      logging.debug("\n\n** Getting Last Temp of Yesterday **")
      last_temp_of_prev_day = get_last_temp()
      logging.debug("Last temp of yesterday: %.4f\n\n" % last_temp_of_prev_day)

    if(first_temp_of_day):
      if last_temp_of_prev_day and overnight_temp_loss is None:
        logging.debug("\n\n** Getting Overnight Temp Loss **")
        overnight_temp_loss = last_temp_of_prev_day - first_temp_of_day
        logging.debug("Temperature lost overnight was %s\n\n" % overnight_temp_loss)

      daily_temp_gain = cur_temp['solarlead'] - first_temp_of_day
      net_temp_gain = daily_temp_gain - overnight_temp_loss
      logging.debug("\n  Temp gained today: %.4f\n  Net Temp gained (including overnight loss): %.4f\n" % (daily_temp_gain,net_temp_gain))
      
      running_temp_change_per_hour = daily_temp_gain / ((curtime - pumpstarttime) / 60 / 60)
      logging.debug("  Running Temp Change per Hour: %.2f\n" % (running_temp_change_per_hour))
      if (cnt < cnt_offset or running_temp_change_per_hour > 5):
          running_temp_change_per_hour = None

    last_solar_temp_diff = solar_temp_diff

    cnt += 1
    
    time.sleep(15)

    prevcurtime = curtime

#    if cnt > 40:
    pump_exit_if_off(curtime, cnt)

pump_exit_if_off(curtime, cnt)
try:
  main()
except SystemExit as e:
  logging.exception("\n\n\nJH SCRIPT ERROR:\n\n")
  logging.error(str(e)+"\n\n")
#  raise

