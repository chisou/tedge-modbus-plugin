import asyncio
import csv
import logging
import os
import re

from pymodbus.client import AsyncModbusTcpClient
from pymodbus.payload import BinaryPayloadDecoder
from pymodbus.constants import Endian
import struct

from app.parser import RegisterLoader
from app.registers import TagValue, SimpleRegister, DecimalRegister, IntRegister, BitRegister

# Configuration
MODBUS_HOST = '192.168.178.176'  # Change to your Modbus server IP
MODBUS_PORT = 502
REGISTER_START = 0         # Starting register


FORMAT_INT = r'INT'
FORMAT_DECIMAL = r'DECIMAL ([1-9]+)'

log = logging.getLogger()
logging.basicConfig(level=logging.INFO)

async def main():

    # read registers
    loader = RegisterLoader(csv_file='boiler.csv')
    loader.set_columns(
        number='Register',
        size='Words',
        description='English',
    )

    registers = loader.load_registers()
    log.info(f"Found {len(registers)} register definitions.")

    # split registers into sequences
    # we have to read sequences of a similar kind - sequential, same raw type, same size, same interval

    sequences = []
    chunk = [registers[0]]
    previous = registers[0]
    stop = False
    for register in registers[1:]:

        if len(chunk) == 1:
            log.info(f"Starting sequence: {chunk[0].number}")

        # the chunk list now contains all valid registers, we only have to
        # check whether the current register would also be fitting
        if previous.size != register.size:
            log.info(f"Stopping sequence: Size differs ({register.size}).")
            stop = True
        elif previous.number + previous.size != register.number:
            log.info(f"Stopping sequence: Not sequential ({register.number}).")
            stop = True

        if not stop:
            log.info(f"Adding to sequence: {register.number}, Size {register.size}")
            chunk.append(register)
        else:
            log.info(f"Adding sequence: {chunk[0].number} - {chunk[-1].number}")
            sequences.append(chunk)
            chunk = [register]
            stop = False

        previous = register

    log.info(f"Final sequence: {chunk[0].number} - {chunk[-1].number}")
    sequences.append(chunk)


    # Connect to Modbus server
    client = AsyncModbusTcpClient(MODBUS_HOST, port=MODBUS_PORT)
    await client.connect()

    for sequence in sequences:

        start_number = sequence[0].number
        start_offset = start_number if start_number < 40000 else start_number - 40000
        num_words = len(sequence) * sequence[0].size
        log.info(f"Reading {len(sequence)} registers ({num_words} words) starting at {start_number} ({start_offset}) ...")

        response = await client.read_holding_registers(start_offset, count=num_words)
        if response.isError():
            log.error("Error reading registers:", response)
            continue

        words = response.registers
        decoded = client.convert_from_registers(words, data_type=client.DATATYPE.INT32, word_order='big')  # todo: other data types
        if not isinstance(decoded, list):
            decoded = [decoded]

        for register, raw_value in zip(sequence, decoded):
            tag_values = register.parse(raw_value)
            for tag_value in tag_values:
                log.info(f"  - {register.number}  ->  {tag_value.tag} = <{tag_value.value}>")

    # Close client
    client.close()


asyncio.run(main())