import os
import RPi.GPIO as GPIO
import time
import json
import datetime

dsid = {}

dsid["solarlead"]     = "011621375bee"
dsid["solarreturn"]   = "80000026331f"
dsid["poolreturn"]    = "02161de351ee"

# drok probe #1 - "00000b88396e"
# drok probe #2 - "00000b885c23"
# drok probe #3 - "00000b88b7d2"
# drok probe #4 - "00000b88e5c7"

while(1):
  data = {}
  data['sensor'] = []
  
  for tmpds in dsid:
    tmpdir = ('/sys/bus/w1/devices/28-' + dsid[tmpds])
    tsnow = datetime.datetime.now()

    if (os.path.isdir(tmpdir) == False):
      print("\n\n************\nW1 dir for %s not found.  Attempting to reset GPIO 17 power\n******************\n\n" % tmpdir)
      GPIO.setmode(GPIO.BCM)
      GPIO.setup(17, GPIO.OUT)
      GPIO.output(17, GPIO.LOW)
      time.sleep(3)
      GPIO.output(17, GPIO.HIGH)
      time.sleep(5)

    try:
      from w1thermsensor import W1ThermSensor
      sensor = W1ThermSensor(W1ThermSensor.THERM_SENSOR_DS18B20, dsid[tmpds])
      temp_f = sensor.get_temperature(W1ThermSensor.DEGREES_F)
      print(dsid[tmpds] + "\t" + str(temp_f) + "\n")
    except:
      print("\n\nFailed to read temperature from %s" % dsid[tmpds], exc_info=True)

    data['sensor'].append({
      'dsid': dsid[tmpds],
      'temp': temp_f,
      'ts': str(tsnow.year) + '-' + str(tsnow.month) + '-' + str(tsnow.day) + ' ' + str(tsnow.hour) + ':' + str(tsnow.minute) + ':' + str(tsnow.second)
    })

  with open('/home/pi/PiPool/temps.json', 'w') as outfile:
    json.dump(data, outfile)

  time.sleep(13)

