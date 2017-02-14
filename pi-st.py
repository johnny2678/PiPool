#!/usr/bin/python3

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
#import logging.config

#logging.config.fileConfig('/home/pi/logging.conf', disable_existing_loggers=False)
#logging.fileConfig(loginpath, defaults={'logfilename': '/home/pi/temp.log'})
#logging.getLogger("urllib3").setLevel(logging.WARNING)

#================================
# Logging
from logging.handlers import RotatingFileHandler
logging.getLogger("urllib3").setLevel(logging.WARNING)

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')


fh = RotatingFileHandler('temp.log', maxBytes=10000, backupCount=10)
fh.setLevel(logging.INFO)
fh.setFormatter(formatter)

sh = logging.StreamHandler(stream=sys.stdout)
sh.setLevel(logging.DEBUG)
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
,            '4c31867c-304b-4aa9-8df1-3fc2f0d99e8f'
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
# ===================================================

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


niters = 60
interval = 30
 
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

def main():
   old_temp = []
   temp_url = []
   i=0
   for tmpdsid in dsid:
       old_temp.append(-99999999.99)
       get_endpoints = ( "https://graph.api.smartthings.com/api/smartapps/endpoints/" + client_id[i] + "?access_token=" + ds_token[i] )
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
       for tmpds in dsid:
          (temp_c, temp_f) = read_temp(cnta)
          endp_url = temp_url[cnta] + ("%.2f/F" % temp_f)     
          headers = { 'Authorization' : 'Bearer ' + ds_token[cnta] } 
          
#          if ( round(temp_f, 2) != round(old_temp[cnta], 2) ):
          if ( abs(round(temp_f, 2) - round(old_temp[cnta], 2)) > 0.18 ):
             logging.debug ("Endpoint submit URL: %s" % (endp_url))
             logging.info ("Niter: %s\tdsname: %s\ttemp_f: %s" % (counter, dsname[cnta], temp_f))

             r = requests.put(endp_url, headers=headers)

             old_temp[cnta] = temp_f

          cnta += 1

       time.sleep(interval)

main()

