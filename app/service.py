import asyncio
import logging
import signal
from asyncio import Event
from datetime import datetime, timezone

from pymodbus.client import AsyncModbusTcpClient

from app.main import assemble_groups, tedge_compile, collect_data
from app.mqtt import MqttClient
from app.parser import RegisterLoader


# Configuration
MODBUS_HOST = '192.168.178.176'  # Change to your Modbus server IP
MODBUS_PORT = 502
MQTT_HOST = 'localhost'
MQTT_PORT = 1883


log = logging.getLogger()
logging.basicConfig(level=logging.INFO)


async def main():

    stop_event = Event()

    def stop():
        log.info("Stop signal received. Shutting down ...")
        stop_event.set()

    loader = RegisterLoader('registers.csv')
    loader.set_columns(
        number='Register',
        size='Words',
        description='English',
    )
    registers = loader.load_registers()
    log.info(f"Found {len(registers)} register definitions.")

    register_groups = assemble_groups(registers)
    log.info(f"Found {len(register_groups)} logical register groups.")
    for group in register_groups:
        log.info(f'Group "{group.name}": Interval {group.interval} seconds')

    modbus_client = AsyncModbusTcpClient(MODBUS_HOST, port=MODBUS_PORT)
    await modbus_client.connect()

    mqtt_client = MqttClient(MQTT_HOST, MQTT_PORT)
    await mqtt_client.start()

    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGINT, stop)

    try:
        log.info("Service started. Press CTRL-C to exit.")

        next_timestamps = {group: datetime.now(timezone.utc) for group in register_groups}

        while not stop_event.is_set():

            now = datetime.now(timezone.utc)

            for group in register_groups:
                due_time = next_timestamps[group]
                if now > due_time:
                    log.info(f"Collecting data for register group: {group.name}")
                    tag_values = await collect_data(modbus_client, group)
                    topic, payload = tedge_compile(due_time, tag_values)
                    mqtt_client.publish(topic, payload)

            await asyncio.sleep(10)  # todo: MAIN_INTERVAL

    finally:
        modbus_client.close()
        mqtt_client.stop()



asyncio.run(main())
