import asyncio

from modbus_reader.service import main


CONFIG_DIR = '/etc/tedge/plugins/modbus/'
LOG_FORMAT = '%(asctime)s [%(levelname)s] %(name)s — %(message)s'
DATE_FORMAT = "%Y-%m-%dT%H:%M:%S%z"


if __name__ == "__main__":
    asyncio.run(main())

