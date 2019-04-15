import os
import RPi.GPIO as GPIO
import time

dsid = {}
#dsid["solarlead"]     = "800000262ce2"
dsid["solarlead"]     = "011621375bee"
dsid["solarreturn"]   = "80000026331f"
dsid["poolreturn"]    = "02161de351ee"

for tmpds in dsid:
	tmpdir = ('/sys/bus/w1/devices/28-' + dsid[tmpds])
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
	except (SystemExit, KeyboardInterrupt):
		raise
	except Exception:
		print("\n\nFailed to read temperature from %s" % dsid[tmpds], exc_info=True)
