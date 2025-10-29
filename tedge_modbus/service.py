import argparse
import asyncio
import logging
import os
import signal
import sys
import time
import tomllib
from asyncio import Event
from datetime import datetime, timezone
from itertools import chain

from pymodbus.client import AsyncModbusTcpClient
from setuptools.logging import configure

from tedge_modbus.core import assemble_groups, tedge_compile, collect_data
from tedge_modbus.model import Configuration
from tedge_modbus.mqtt import MqttClient
from tedge_modbus.parser import RegisterLoader
from tedge_modbus.util import next_timestamp

# Configuration
CONFIG_DIR = '/etc/tedge/plugins/modbus/'
LOG_FORMAT = '%(asctime)s [%(levelname)s] %(name)s:%(lineno)d — %(message)s'
DATE_FORMAT = "%Y-%m-%dT%H:%M:%S%z"

MQTT_HOST = 'localhost'
MQTT_PORT = 1883


log = logging.getLogger()
logging.basicConfig(level=logging.DEBUG)


async def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--configdir", required=False)
    args = parser.parse_args()
    config_dir = os.path.abspath(args.configdir or CONFIG_DIR)

    configuration = None
    with open(os.path.join(config_dir, "service.toml"), "rb") as config_file:
        configuration = Configuration(tomllib.load(config_file))

    logging.basicConfig(
        level=configuration.log_level,
        format=LOG_FORMAT,
        datefmt=DATE_FORMAT,
        handlers=[logging.StreamHandler(sys.stdout)],
        force=True,
    )

    loader = RegisterLoader(os.path.join(config_dir, 'registers.csv'))
    loader.set_columns(
        number='Register',
        size='Words',
        description='English',
    )
    registers = loader.load_registers()
    log.info(f"Found {len(registers)} register definitions.")

    groups = assemble_groups(registers)
    log.info(f"Found {len(groups)} logical register groups.")
    for group in groups:
        log.info(f'Group "{group.name}": Interval {group.interval} seconds')
        for i, sequence in enumerate(group.sequences):
            log.info(f'  - Sequence {i+1}:')
            for register in sequence:
                log.info(f'     - Register {register.number}')

    modbus_client = AsyncModbusTcpClient(configuration.modbus_host, port=configuration.modbus_port)
    await modbus_client.connect()

    mqtt_client = MqttClient(MQTT_HOST, MQTT_PORT)
    await mqtt_client.start()

    # === main loop ====

    stop_event = Event()

    def stop():
        log.info("Stop signal received. Shutting down ...")
        stop_event.set()

    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGINT, stop)

    try:
        log.info("Service started. Press CTRL-C to exit.")

        # set initial due times to be in the past
        next_timestamps = {
            group: next_timestamp(group.interval)-group.interval
            for group in groups
        }

        while not stop_event.is_set():

            now = time.time()

            for group in groups:
                due_ts = next_timestamps[group]
                if now > due_ts:
                    log.info(f"Collecting data for measurement group: {group.name}")
                    tag_values = []
                    for sequence in group.sequences:
                        tag_values.extend(await collect_data(modbus_client, sequence))
                    topic, payload = tedge_compile(due_ts, group.name, tag_values)
                    mqtt_client.publish(topic, payload)
                    next_ts = next_timestamp(group.interval)
                    next_timestamps[group] = next_ts
                    log.info(f"Next sample: {datetime.fromtimestamp(next_ts).isoformat()}")

            await asyncio.wait_for(stop_event.wait(), timeout=10)

    finally:
        modbus_client.close()
        mqtt_client.stop()

