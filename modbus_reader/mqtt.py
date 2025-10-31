import logging

import paho.mqtt.client as mqtt


log = logging.getLogger(__name__)


class MqttClient:

    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.client = mqtt.Client(
            clean_session=True,
            reconnect_on_failure=True,
        )

    async def start(self):
        self.client.connect(self.host, self.port)
        self.client.loop_start()

    def stop(self):
        self.client.loop_stop()
        self.client.disconnect()

    def publish(self, topic, payload):
        self.client.publish(topic, payload)
        log.debug(f"Published to {topic}: {payload}")
