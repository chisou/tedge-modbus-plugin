import asyncio

from pymodbus.client import AsyncModbusTcpClient
from pymodbus.payload import BinaryPayloadDecoder
from pymodbus.constants import Endian
import struct

# Configuration
MODBUS_HOST = '192.168.178.176'  # Change to your Modbus server IP
MODBUS_PORT = 502
REGISTER_START = 0         # Starting register
REGISTER_COUNT = 32         # Read 4 registers (enough for 64-bit float or long string)

async def main():

    # Connect to Modbus server
    client = AsyncModbusTcpClient(MODBUS_HOST, port=MODBUS_PORT)
    await client.connect()

    # Read holding registers
    response = await client.read_holding_registers(REGISTER_START, count=REGISTER_COUNT)

    if response.isError():
        print("Error reading registers:", response)
    else:
        registers = response.registers
        print(f"Raw registers: {registers}")

        decoded = client.convert_from_registers(registers, data_type=client.DATATYPE.INT32, word_order='big')
        print(decoded)


        # Try different decoding schemes
        decoder = BinaryPayloadDecoder.fromRegisters(registers, byteorder=Endian.BIG, wordorder=Endian.BIG)

        # 1. Decode as 16-bit integers
        print("\n16-bit Integers:")
        for reg in registers:
            print(f"  {reg}")

        # 2. Decode as 32-bit integer
        decoder.reset()
        int32 = decoder.decode_32bit_int()
        print(f"\n32-bit Integer: {int32}")

        # 3. Decode as 32-bit float
        decoder.reset()
        float32 = decoder.decode_32bit_float()
        print(f"32-bit Float: {float32}")

        # 4. Decode as 64-bit float (double)
        decoder.reset()
        float64 = decoder.decode_64bit_float()
        print(f"64-bit Float: {float64}")

        # 5. Decode as ASCII string
        decoder.reset()
        ascii_str = decoder.decode_string(REGISTER_COUNT * 2)  # 2 bytes per register
        print(f"ASCII String: {ascii_str.decode('ascii', errors='ignore')}")

    # Close client
    client.close()


asyncio.run(main())