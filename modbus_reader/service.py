import argparse
import asyncio
import logging
import os
import signal
import sys
import time
import tomllib
from asyncio import Event
from contextlib import suppress
from datetime import datetime

from pymodbus.client import AsyncModbusTcpClient

from modbus_reader.config import Configuration
from modbus_reader.core import assemble_groups, tedge_compile, collect_data
from modbus_reader.mqtt import MqttClient
from modbus_reader.parser import RegisterLoader, CsvParser
from modbus_reader.util import next_timestamp

# Configuration
CONFIG_DIR = '/etc/tedge/plugins/modbus/'
LOG_FORMAT = '%(asctime)s [%(levelname)s] %(name)s:%(lineno)d — %(message)s'
DATE_FORMAT = "%Y-%m-%dT%H:%M:%S%z"

MQTT_HOST = 'localhost'
MQTT_PORT = 1883


log = logging.getLogger()
logging.basicConfig(level=logging.DEBUG)


async def main():

    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("-c", "--configdir", required=False)
    args = arg_parser.parse_args()

    # init configuration
    config_dir = os.path.abspath(args.configdir or CONFIG_DIR)
    configuration = Configuration(config_dir)

    # init logging
    logging.basicConfig(
        datefmt=DATE_FORMAT,
        format=LOG_FORMAT,
        level=configuration.logging.level,
        handlers=[logging.StreamHandler(sys.stdout)],
        force=True,
    )

    # read registers
    file_parser = CsvParser(
        delimiter=configuration.csv.delimiter,
        quote_char=configuration.csv.quote,
        skip_lines=0
    )
    loader = RegisterLoader()
    loader.set_columns(
        number=configuration.mapping.registers.number,
        size=configuration.mapping.registers.size,
        type=configuration.mapping.registers.type,
        uom=configuration.mapping.registers.uom,
        value=configuration.mapping.registers.value,
        min=configuration.mapping.registers.min,
        max=configuration.mapping.registers.max,
        tag=configuration.mapping.registers.tag,
        description=configuration.mapping.registers.description,
        device=configuration.mapping.registers.device,
        group=configuration.mapping.registers.group,
    )

    with open(os.path.join(config_dir, 'registers.csv')) as csv_file:
        registers = loader.load_from_lines(file_parser.read_lines(csv_file))
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

            with suppress(TimeoutError):
                await asyncio.wait_for(stop_event.wait(), timeout=10)

    finally:
        modbus_client.close()
        mqtt_client.stop()

