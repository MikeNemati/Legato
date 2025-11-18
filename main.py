import datetime
import logging
import platform
import sys
import time
import ssl
import pathlib

import yaml
#MN To interact with Cumulocity IoT devices. Versioan 1.02
from c8y.c8y_device import c8yDevice
#MN To read "holding registers" in a Modbus device.
from modbus.modbus_client import read_hr

#MN Logging to the console is enabled
log_console = True
if log_console:
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)
    root.addHandler(handler)


if __name__ == "__main__":
    logging.basicConfig(
        format="%(asctime)-15s %(levelname)s %(name)-18s %(message)s",
        level=logging.DEBUG,
    )

    if "Ubuntu" in platform.version():
        CONFIG_FILE = "ubuntu/config.yaml"
        CERTS_PATH = "ubuntu/certificates"
    else:
        CONFIG_FILE = "/home/root/myapp/modbus2cumulocity/config.yaml"
        CERTS_PATH = "/home/root/myapp/modbus2cumulocity/certificates"
#MN Opens and reads a YAML configuration file. The file's contents are loaded into the dictionary s using yaml.load, which safely parses the YAML file.
    with open(CONFIG_FILE) as f:
        s = yaml.load(f, Loader=yaml.FullLoader)

    server_cert_required = s["cumulocity"]["server_cert_required"]

    certfile = f"{CERTS_PATH}/{s['cumulocity']['device_id']}_deviceCertChain.pem"
    keyfile = f"{CERTS_PATH}/{s['cumulocity']['device_id']}_deviceKey.pem"

    cert_file_list = [certfile, keyfile]

    if server_cert_required:
        ca_certs = f"{CERTS_PATH}/{s['cumulocity']['url']}_serverCertChain.pem"
        cert_file_list += [ca_certs]
        cert_reqs = ssl.CERT_REQUIRED
    else:
        ca_certs = None
        cert_reqs = ssl.CERT_NONE

    for cert_file in cert_file_list:
        pathlib_path = pathlib.Path(cert_file)
        assert (
            pathlib_path.is_file()
        ), f"The file {pathlib_path} is inaccessible or not there!"

    client = c8yDevice(
        url=s["cumulocity"]["url"],
        tenant=s["cumulocity"]["tenant"],
        device_id=str(s["cumulocity"]["device_id"]),
        device_type=s["cumulocity"]["device_type"],
        measurement_qos=s["cumulocity"]["measurement_qos"],
        ca_certs=ca_certs,
        certfile=certfile,
        keyfile=keyfile,
        cert_reqs=cert_reqs,
    )
    client.start()

    # remove this for production
    # while True:
    #     time.sleep(10)
	
	#1	102CV MBFR	'Turbidity_RAW~NTU	MBF 32bits float
	#2	104CV MBFR	'Temp_Turb_RAW~NTU	MBF 32bits float
	#3	106CV MBFR	'pH_RAW	MBF 32bits float
	#4	108CV MBFR	'Temp_pH_RAW~degC	MBF 32bits float
	#5	110CV MBFR	'Conductivity_RAW~mS/cm	MBF 32bits float
	#6	112CV MBFR	'TDS_RAW~mg/L	MBF 32bits float
	#7	114CV MBFR	'Dis_Oxy_RAW~mg/L	MBF 32bits float
	#8	116CV MBFR	'TSS_RAW~mg/L	MBF 32bits float
	#9	118CV MBFR	'Velocity_RAW~m/s	MBF 32bits float
	#10	120CV MBFR	'Level_RAW~m	MBF 32bits float
	
	#1	124CV MBI	'Float switch state Level (Low = 0, High = 1) MBI 16bits integer
	#2	128CV MBI	'Door switch state Level (Low = 0, High = 1) MBI 16bits integer
	#0	202CV MBFR	'Turbidity~NTU
	#1	204CV MBFR	'Temp-Turb~degC
	#2	206CV MBFR	'pH_RAW
	#3	208CV MBFR	'Temp-pH~degC
	#4	210CV MBFR	'Conductivity~mS/cm
	#5	212CV MBFR	'TDS~mg/L
	#6	214CV MBFR	'Dis_Oxy~mg/L
	#7	216CV MBFR	'TSS~mg/L
	#8	218CV MBFR	'Velocity~m/s
	#9	220CV MBFR	'Level~m
	#10	226CV MBFR	'Stores average Battery Voltage ~V	MBF 32bits float
	

	

    #1 200CV MBF	'Temp~DEGC	MBF 32bits float
	#2 202CV MBF	'Turbidity~NTU	MBF 32bits float
	#3 204CV MBF	'ph~ph 	MBF 32bits float
	#4 206CV MBF	'Depth~m 	MBF 32bits float
	#5 208CV MBF	'cond~uS/cm	MBF 32bits float
	#6 210CV MBF	'nlfcond~uS/cm	MBF 32bits float
	#7 212CV MBF	'DO SAT~%sat	MBF 32bits float
	#8 214CV MBF	'DO CB ~%cb	MBF 32bits float
	#9 216CV MBF	'DO MGL~mg/L	MBF 32bits float
	#10 218CV MBF	'ORP~mV	MBF 32bits float
	#11 220CV MBF	'PSI ~psia	MBF 32bits float
	#12 222CV MBF	'SAL ~psu  	MBF 32bits float
	#13 224CV MBF	'SP cond~uS/cm	MBF 32bits float
	#14 226CV MBF	'TDS~mg/L	MBF 32bits float
	#15 228CV MBF	'TSS~mg/L	MBF 32bits float
	#16 230CV MBF	'PH MV~mV	MBF 32bits float
	#17 232CV MBF	'CABLE POWER~volt	MBF 32bits float
	#18 234CV MBF	'Stores average Battery Voltage ~V	MBF 32bits float
	#19 236CV MBI 1	 Level (Low = 0, High = 1) MBI 16bits integer


    # Totally 13 registers will be polled
	# 11 x 2 bytes floats needed-> size
    # Temperature, Turbidity, Battery Voltage + rest of above are polled by FX30 every 6 minutes
    # pushed to Cumulocity around the same interval.
    # Level switch is polled by FX30 every 10 seconds, and pushed to
    # Cumulocity as changed or the first reading
    last_read = None
    last_read_time = None

    SEND_INTERVAL = 30

    try:
        while True:
            server_ip = s["modbus"]["slave_ip"]
            values1 = read_hr(
                holding_register=201,
                size=20,
                server_ip=server_ip,
                format="32bit_float",
                word_order="reverse",
            )
            logging.info(f"values output: {values1}")
            
            values1 = [round(v, 4) for v in values1]
            logging.info(f"rounded values: {values1}")
			
			# Read register 226 (1 float value for Battery)
            values2 = read_hr(
                holding_register=225,  # 226 - 1
                size=2,  # 1 float * 2 registers
                server_ip=server_ip,
                format="32bit_float",
                word_order="reverse",
            )
            logging.info(f"values2 output: {values2}")
            values2 = [round(v, 4) for v in values2]
            logging.info(f"rounded values2: {values2}")
			
            values3 = read_hr(
                holding_register=123,
                size=1,
                server_ip=server_ip,
                format="16bit_integer",
            )
            logging.info(f"values output: {values3}")
			
            values4 = read_hr(
                holding_register=127,
                size=1,
                server_ip=server_ip,
                format="16bit_integer",
            )
            logging.info(f"values output: {values4}")
			
            values = values1 + values2 + values3 + values4
			
            if len(values) == 13:
                if last_read is None:
                    logging.info("first push, all 13 measurements")
                    client.send(
                        "datalogger",
                        "Turbidity(202)",
                        values[0],
                        "NTU",
                        datetime.datetime.utcnow(),
                    )
                    client.send(
                        "datalogger",
                        "Temp_Turb(204)",
                        values[1],
                        "degC",
                        datetime.datetime.utcnow(),
                    )
                    client.send(
                        "datalogger",
                        "pH(206)",
                        values[2],
                        "ph",
                        datetime.datetime.utcnow(),
                    )
                    client.send(
                        "datalogger",
                        "Temp_pH(208)",
                        values[3],
                        "degC",
                        datetime.datetime.utcnow(),
                    )
                    client.send(
                        "datalogger",
                        "Conductivity(210)",
                        values[4],
                        "mS/cm",
                        datetime.datetime.utcnow(),
                    )
                    client.send(
                        "datalogger",
                        "TDS(212)",
                        values[5],
                        "mg/L",
                        datetime.datetime.utcnow(),
                    )
                    client.send(
                        "datalogger",
                        "Dis_Oxy(214)",
                        values[6],
                        "mg/L",
                        datetime.datetime.utcnow(),
                    )
                    client.send(
                        "datalogger",
                        "TSS(216)",
                        values[7],
                        "mg/L",
                        datetime.datetime.utcnow(),
                    )
                    client.send(
                        "datalogger",
                        "Velocity(218)",
                        values[8],
                        "m/s",
                        datetime.datetime.utcnow(),
                    )
                    client.send(
                        "datalogger",
                        "Level(220)",
                        values[9],
                        "m",
                        datetime.datetime.utcnow(),
                    )
                    client.send(
                        "datalogger",
                        "Bat(226)",
                        values[10],
                        "V",
                        datetime.datetime.utcnow(),
                    )
                    client.send(
                        "datalogger",
                        "FloatSwitchState(124)",
                        values[11],
                        "",
                        datetime.datetime.utcnow(),
                    )
                    client.send(
                        "datalogger",
                        "DoorSwitchState(128)",
                        values[12],
                        "",
                        datetime.datetime.utcnow(),
                    )
					
                 
                    last_read = values
                    last_read_time = time.time()
                else:
                    if time.time() - last_read_time > 60 * SEND_INTERVAL:
                        logging.info(
                            f"{SEND_INTERVAL} mins reached, push 13 measurements"
                        )
                    client.send(
                        "datalogger",
                        "Turbidity(202)",
                        values[0],
                        "NTU",
                        datetime.datetime.utcnow(),
                    )
                    client.send(
                        "datalogger",
                        "Temp_Turb(204)",
                        values[1],
                        "degC",
                        datetime.datetime.utcnow(),
                    )
                    client.send(
                        "datalogger",
                        "pH(206)",
                        values[2],
                        "ph",
                        datetime.datetime.utcnow(),
                    )
                    client.send(
                        "datalogger",
                        "Temp_pH(208)",
                        values[3],
                        "degC",
                        datetime.datetime.utcnow(),
                    )
                    client.send(
                        "datalogger",
                        "Conductivity(210)",
                        values[4],
                        "mS/cm",
                        datetime.datetime.utcnow(),
                    )
                    client.send(
                        "datalogger",
                        "TDS(212)",
                        values[5],
                        "mg/L",
                        datetime.datetime.utcnow(),
                    )
                    client.send(
                        "datalogger",
                        "Dis_Oxy(214)",
                        values[6],
                        "mg/L",
                        datetime.datetime.utcnow(),
                    )
                    client.send(
                        "datalogger",
                        "TSS(216)",
                        values[7],
                        "mg/L",
                        datetime.datetime.utcnow(),
                    )
                    client.send(
                        "datalogger",
                        "Velocity(218)",
                        values[8],
                        "m/s",
                        datetime.datetime.utcnow(),
                    )
                    client.send(
                        "datalogger",
                        "Level(220)",
                        values[9],
                        "m",
                        datetime.datetime.utcnow(),
                    )
                    client.send(
                        "datalogger",
                        "Bat(226)",
                        values[10],
                        "V",
                        datetime.datetime.utcnow(),
                    )
                    client.send(
                        "datalogger",
                        "FloatSwitchState(124)",
                        values[11],
                        "",
                        datetime.datetime.utcnow(),
                    )
                    client.send(
                        "datalogger",
                        "DoorSwitchState(128)",
                        values[12],
                        "",
                        datetime.datetime.utcnow(),
                    )
                    last_read_time = time.time()
                if values[11] != last_read[11]:
                    logging.info("Float Switch changed, push")
                    client.send(
                        "datalogger",
                        "FloatSwitchState(124)",
                        values[11],
                        "",
                        datetime.datetime.utcnow(),
                    )
                    last_read = values
                
                if values[12] != last_read[12]:
                    logging.info("Door Switch changed, push")
                    client.send(
                        "datalogger",
                        "DoorSwitchState(128)",
                        values[12],
                        "",
                        datetime.datetime.utcnow(),
                    )
                    last_read = values

            time.sleep(10)
    except (KeyboardInterrupt, SystemExit):
        logging.info("Received keyboard interrupt, quitting ...")
        client.on = False
        exit(0)


