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
from modbus.modbus_client import read_hr


#Script code version 2.05

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

    client_id = cfg["aws"].get("device_id", "fx30-test")

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
            "deviceId": str(device_id),
            "fragment": "datalogger",
            "series": series,
            "value": value,
            "unit": unit,
            "timestamp": datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        },
    )


if __name__ == "__main__":

    if "Ubuntu" in platform.version():
        CONFIG_FILE = "ubuntu/config.yaml"
        CERTS_PATH = "ubuntu/certificates"
    else:
        CONFIG_FILE = "/home/root/myapp/modbus2cumulocity/config.yaml"
        CERTS_PATH = "/home/root/myapp/modbus2cumulocity/certificates"

    with open(CONFIG_FILE) as f:
        s = yaml.load(f, Loader=yaml.FullLoader)

    aws_topic = s["aws"]["topic"]
    device_id = s["aws"].get("device_id", "fx30-test")

    aws_client = aws_connect(s, CERTS_PATH)
    logging.info("Connected to AWS IoT Core MQTT")

    last_hello_time = 0
    HELLO_INTERVAL_SEC = 30

    last_read = None
    last_read_time = None
    SEND_INTERVAL = 6

    try:
        while True:

            now_epoch = time.time()

            if now_epoch - last_hello_time >= HELLO_INTERVAL_SEC:
                aws_publish_json(
                    aws_client,
                    aws_topic,
                    {
                        "type": "hello",
                        "deviceId": str(device_id),
                        "message": "Hello World from FX30",
                        "timestamp": datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                    },
                )
                last_hello_time = now_epoch

            try:
                server_ip = s["modbus"]["slave_ip"]

                values1 = read_hr(
                    holding_register=201,
                    size=20,
                    server_ip=server_ip,
                    format="32bit_float",
                    word_order="reverse",
                )
                logging.info(f"values1: {values1}")

                values1 = [round(v, 4) for v in values1]

                values2 = read_hr(
                    holding_register=225,
                    size=2,
                    server_ip=server_ip,
                    format="32bit_float",
                    word_order="reverse",
                )
                logging.info(f"values2 (Battery float): {values2}")

                values2 = [round(v, 4) for v in values2]

                # DEBUG RAW BATTERY (important part)
                raw_bat = read_hr(
                    holding_register=226,  # try 225 if this prints zeros
                    size=2,
                    server_ip=server_ip,
                    format="16bit_integer",
                )
                logging.info(f"RAW Battery words: {raw_bat}")

                values3 = read_hr(
                    holding_register=123,
                    size=1,
                    server_ip=server_ip,
                    format="16bit_integer",
                )

                values4 = read_hr(
                    holding_register=127,
                    size=1,
                    server_ip=server_ip,
                    format="16bit_integer",
                )

                values = values1 + values2 + values3 + values4

                if len(values) == 13:

                    publish_measurement(aws_client, aws_topic, device_id, "Bat(226)", values[10], "V")

            except Exception as e:
                logging.warning(f"Modbus skipped: {e}")

            time.sleep(10)

    except (KeyboardInterrupt, SystemExit):
        logging.info("Stopping ...")
        aws_client.loop_stop()
        aws_client.disconnect()
        exit(0)
