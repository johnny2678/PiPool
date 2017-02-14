import os
import glob
import time

os.system('modprobe w1-gpio')
os.system('modprobe w1-therm')

base_dir = '/sys/bus/w1/devices/'
device_folder = glob.glob(base_dir + '28*')[0]
device_file = device_folder + '/w1_slave'

oldtemp_f = 0
temp_f = 0

def read_temp_raw():
    f = open(device_file, 'r')
    lines = f.readlines()
    f.close()
    return lines

def read_temp():
    global oldtemp_f
    global temp_f

    lines = read_temp_raw()
    while lines[0].strip()[-3:] != 'YES':
        time.sleep(0.2)
        lines = read_temp_raw()
    equals_pos = lines[1].find('t=')
    if equals_pos != -1:
	oldtemp_f = temp_f
        temp_string = lines[1][equals_pos+2:]
        temp_c = float(temp_string) / 1000.0
	temp_f = temp_c * 9.0 / 5.0 + 32.0
	from time import gmtime, strftime
	return strftime("%Y-%m-%d %H:%M:%S", gmtime()), temp_c, temp_f
while True:
# need to find a way to do this without calling the function twice
# and is it necessary to us global variables?
	read_temp()
	if temp_f != oldtemp_f:
		print(read_temp())

	time.sleep(1)

