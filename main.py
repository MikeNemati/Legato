import datetime
import json
import logging
import platform
import sys
import time
import ssl
import pathlib

import yaml
import paho.mqtt.client as mqtt

#MN To interact with AWS IoT devices. Versioan 2.03 with Device ID sending dummy data
# MN To read "holding registers" in a Modbus device.
from modbus.modbus_client import read_hr

# MN Logging to the console is enabled
log_console = True
if log_console:
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    root.addHandler(handler)


def aws_connect(cfg, certs_path):
    endpoint = cfg["aws"]["endpoint"]
    port = int(cfg["aws"].get("port", 8883))

    # Use aws.device_id as the MQTT client_id (fallback to old cumulocity.device_id if present)
    client_id = cfg["aws"].get("device_id", cfg.get("cumulocity", {}).get("device_id", "fx30-test"))

    ca = f"{certs_path}/{cfg['aws']['ca_cert']}"
    cert = f"{certs_path}/{cfg['aws']['cert']}"
    key = f"{certs_path}/{cfg['aws']['key']}"

    for f in [ca, cert, key]:
        p = pathlib.Path(f)
        assert p.is_file(), f"Missing TLS file: {p}"

    c = mqtt.Client(client_id=str(client_id), clean_session=True)
    c.enable_logger(logging.getLogger("aws_mqtt"))

    c.tls_set(
        ca_certs=ca,
        certfile=cert,
        keyfile=key,
        cert_reqs=ssl.CERT_REQUIRED,
        tls_version=ssl.PROTOCOL_TLS_CLIENT,
    )

    c.connect(endpoint, port, keepalive=60)
    c.loop_start()
    return c


def aws_publish_json(client, topic, payload):
    client.publish(topic, json.dumps(payload), qos=1)
    logging.info(f"MQTT publish -> {payload}")


def publish_measurement(client, topic, device_id, series, value, unit):
    aws_publish_json(
        client,
        topic,
        {
            "type": "measurement",
            "deviceId": str(device_id),  # <-- added
            "fragment": "datalogger",
            "series": series,
            "value": value,
            "unit": unit,
            "timestamp": datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        },
    )


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

    with open(CONFIG_FILE) as f:
        s = yaml.load(f, Loader=yaml.FullLoader)

    aws_topic = s["aws"]["topic"]
    device_id = s["aws"].get("device_id", s.get("cumulocity", {}).get("device_id", "fx30-test"))  # <-- added

    aws_client = aws_connect(s, CERTS_PATH)
    logging.info("Connected to AWS IoT Core MQTT")

    # Hello World cadence
    last_hello_time = 0
    HELLO_INTERVAL_SEC = 30

    # Sensor send cadence
    last_read = None
    last_read_time = None
    SEND_INTERVAL = 6  # minutes

    try:
        while True:
            now_epoch = time.time()

            # ---- HELLO WORLD (always, even without sensors) ----
            if now_epoch - last_hello_time >= HELLO_INTERVAL_SEC:
                aws_publish_json(
                    aws_client,
                    aws_topic,
                    {
                        "type": "hello",
                        "deviceId": str(device_id),  # <-- added
                        "message": "Hello World from FX30",
                        "timestamp": datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                    },
                )
                last_hello_time = now_epoch

            # ---- MODBUS READS ----
            try:
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
                logging.info(f"values3 output: {values3}")

                values4 = read_hr(
                    holding_register=127,
                    size=1,
                    server_ip=server_ip,
                    format="16bit_integer",
                )
                logging.info(f"values4 output: {values4}")

                values = values1 + values2 + values3 + values4

                if len(values) == 13:

                    def push_all():
                        publish_measurement(aws_client, aws_topic, device_id, "Turbidity(202)", values[0], "NTU")
                        publish_measurement(aws_client, aws_topic, device_id, "Temp_Turb(204)", values[1], "degC")
                        publish_measurement(aws_client, aws_topic, device_id, "pH(206)", values[2], "ph")
                        publish_measurement(aws_client, aws_topic, device_id, "Temp_pH(208)", values[3], "degC")
                        publish_measurement(aws_client, aws_topic, device_id, "Conductivity(210)", values[4], "mS/cm")
                        publish_measurement(aws_client, aws_topic, device_id, "TDS(212)", values[5], "mg/L")
                        publish_measurement(aws_client, aws_topic, device_id, "Dis_Oxy(214)", values[6], "mg/L")
                        publish_measurement(aws_client, aws_topic, device_id, "TSS(216)", values[7], "mg/L")
                        publish_measurement(aws_client, aws_topic, device_id, "Velocity(218)", values[8], "m/s")
                        publish_measurement(aws_client, aws_topic, device_id, "Level(220)", values[9], "m")
                        publish_measurement(aws_client, aws_topic, device_id, "Bat(226)", values[10], "V")
                        publish_measurement(aws_client, aws_topic, device_id, "FloatSwitchState(124)", values[11], "")
                        publish_measurement(aws_client, aws_topic, device_id, "DoorSwitchState(128)", values[12], "")

                    if last_read is None:
                        logging.info("first push, all 13 measurements")
                        push_all()
                        last_read = values
                        last_read_time = time.time()
                    else:
                        if time.time() - last_read_time > 60 * SEND_INTERVAL:
                            logging.info(f"{SEND_INTERVAL} mins reached, push 13 measurements")
                            push_all()
                            last_read_time = time.time()

                    if last_read is not None and values[11] != last_read[11]:
                        logging.info("Float Switch changed, push")
                        publish_measurement(aws_client, aws_topic, device_id, "FloatSwitchState(124)", values[11], "")
                        last_read = values

                    if last_read is not None and values[12] != last_read[12]:
                        logging.info("Door Switch changed, push")
                        publish_measurement(aws_client, aws_topic, device_id, "DoorSwitchState(128)", values[12], "")
                        last_read = values

                else:
                    if last_read_time is None or (time.time() - last_read_time > 60 * SEND_INTERVAL):
                        logging.warning("No sensors detected, sending dummy values (9999)")
                        dummy = [5555] * 13
                        publish_measurement(aws_client, aws_topic, device_id, "Turbidity(202)", dummy[0], "NTU")
                        publish_measurement(aws_client, aws_topic, device_id, "Temp_Turb(204)", dummy[1], "degC")
                        publish_measurement(aws_client, aws_topic, device_id, "pH(206)", dummy[2], "ph")
                        publish_measurement(aws_client, aws_topic, device_id, "Temp_pH(208)", dummy[3], "degC")
                        publish_measurement(aws_client, aws_topic, device_id, "Conductivity(210)", dummy[4], "mS/cm")
                        publish_measurement(aws_client, aws_topic, device_id, "TDS(212)", dummy[5], "mg/L")
                        publish_measurement(aws_client, aws_topic, device_id, "Dis_Oxy(214)", dummy[6], "mg/L")
                        publish_measurement(aws_client, aws_topic, device_id, "TSS(216)", dummy[7], "mg/L")
                        publish_measurement(aws_client, aws_topic, device_id, "Velocity(218)", dummy[8], "m/s")
                        publish_measurement(aws_client, aws_topic, device_id, "Level(220)", dummy[9], "m")
                        publish_measurement(aws_client, aws_topic, device_id, "Bat(226)", dummy[10], "V")
                        publish_measurement(aws_client, aws_topic, device_id, "FloatSwitchState(124)", dummy[11], "")
                        publish_measurement(aws_client, aws_topic, device_id, "DoorSwitchState(128)", dummy[12], "")
                        last_read_time = time.time()

            except Exception as e:
                logging.warning(f"Modbus skipped (no sensors / unreachable): {e}")

            time.sleep(10)

    except (KeyboardInterrupt, SystemExit):
        logging.info("Received keyboard interrupt, quitting ...")
        try:
            aws_client.loop_stop()
            aws_client.disconnect()
        except Exception:
            pass
        exit(0)
