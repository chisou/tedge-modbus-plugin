import json
import logging
from datetime import datetime, timezone

from modbus_reader.model import MeasurementGroup

log = logging.getLogger(__name__)


def assemble_groups(registers):
    sequences = []
    chunk = [registers[0]]
    previous = registers[0]
    stop = ""
    for register in registers[1:]:

        if len(chunk) == 1:
            log.info(f"Starting sequence: {previous.number}, Group: {previous.group}")

        # the chunk list now contains all valid registers, we only have to
        # check whether the current register would also be fitting
        if previous.size != register.size:
            log.info(f"Stopping sequence: Size differs ({register.size}).")
            stop = True
        elif previous.number + previous.size != register.number:
            log.info(f"Stopping sequence: Not sequential ({register.number}).")
            stop = True
        elif previous.group != register.group:
            log.info(f"Stopping sequence: Group differs ({register.group}).")
            stop = True

        if not stop:
            log.debug(f"Adding to sequence: {register.number}")
            chunk.append(register)
        else:
            log.info(f"Found register sequence: {chunk[0].number} - {chunk[-1].number}, Group {chunk[0].group}")
            sequences.append(chunk)
            chunk = [register]
            stop = False

        previous = register

    log.info(f"Found final sequence: {chunk[0].number} - {chunk[-1].number}, Group {chunk[0].group}")
    sequences.append(chunk)

    # order sequences by their group
    groups = {}
    for sequence in sequences:
        name = sequence[0].group
        if not name in groups:
            groups[name] = [sequence]
        else:
            groups[name].append(sequence)

    return [MeasurementGroup(name, sequences) for name, sequences in groups.items()]


async def collect_data(client, sequence):

    start_number = sequence[0].number
    start_offset = start_number if start_number < 40000 else start_number - 40000
    num_words = len(sequence) * sequence[0].size
    log.info(f"Reading {len(sequence)} registers ({num_words} words) starting at {start_number} ({start_offset}) ...")

    response = await client.read_holding_registers(start_offset, count=num_words)
    if response.isError():
        log.error("Error reading registers:", response)
        return None

    words = response.registers
    decoded = client.convert_from_registers(words, data_type=client.DATATYPE.INT32, word_order='big')  # todo: other data types
    if not isinstance(decoded, list):
        decoded = [decoded]

    result = []
    for register, raw_value in zip(sequence, decoded):
        tag_values = register.parse(raw_value)
        for tag_value in tag_values:
            log.info(f"  - {register.number}  ->  {tag_value.tag} = <{tag_value.value}>")
            result.append(tag_value)

    return result


def format_message(ts, device, group, tag_values):
    data = {'time': datetime.fromtimestamp(ts, timezone.utc).isoformat()}
    for tag_value in tag_values:
        l0, l1 = tag_value.tag.split('.')
        if l0 not in data:
            data[l0] = {}
        data[l0][l1] = tag_value.value

    return f'te/device/{device}///m//', json.dumps(data)
