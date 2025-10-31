import csv
import logging
import os
import re
from itertools import count
from typing import Self

from modbus_reader.model import Register, IntRegister, DecimalRegister, BitRegister, TagValue, MapRegister, SimpleRegister

log = logging.getLogger(__name__)

NUMBER, NUMBER_DESC = 'number', "Register Number"
SIZE, SIZE_DESC = 'size', "Size/Length"
TYPE, TYPE_DESC = 'type', "Type/Format"
UOM, UOM_DESC = 'uom', "Unit of Measurement"
VALUE, VALUE_DESC = 'value', "Value"
VALUE_MIN, VALUE_MIN_DESC = 'min', "Min Value"
VALUE_MAX, VALUE_MAX_DESC = 'max', "Max Value"
TAG, TAG_DESC = 'tag', "Tag"
DESCRIPTION, DESCRIPTION_DESC = 'description', "Description"
GROUP, GROUP_DESC = 'group', "Tag Group"
DEVICE, DEVICE_DESC = 'device', "Device"


class RegisterParser:
    """A parser for a holding register row."""

    def __init__(self):
        self.number_cell = -1
        self.description_cell = -1
        self.size_cell = 0
        self.type_cell = 0
        self.value_cell = -1
        self.value_min_cell = 0
        self.value_max_cell = 0
        self.group_cell = -1
        self.tag_cell = 0
        self.device_cell = -1

    def set_cells(self, cells):
        self.number_cell = cells[NUMBER]
        self.size_cell = cells[SIZE]
        self.type_cell = cells[TYPE]
        self.value_cell = cells[VALUE]
        self.value_min_cell = cells[VALUE_MIN]
        self.value_max_cell = cells[VALUE_MAX]
        self.tag_cell = cells[TAG]
        self.group_cell = cells[GROUP]

    def parse(self, lines) -> Register:
        """Parse a register from specification rows."""
        pass


class IntRegisterParser(RegisterParser):

    def parse(self, lines):
        line = lines[0]
        return IntRegister(
            number=line[self.number_cell],
            size=line[self.size_cell],
            group=line[self.group_cell],
            tag=line[self.tag_cell],
            description=line[self.description_cell],
        )

class DecimalRegisterParser(RegisterParser):

    def parse(self, lines):
        spec = lines[0]
        decimal_places = int(spec[self.type_cell].split()[1])
        return DecimalRegister(
            number=spec[self.number_cell],
            size=spec[self.size_cell],
            group=spec[self.group_cell],
            tag=spec[self.tag_cell],
            description=spec[self.description_cell],
            decimal_places=decimal_places,
        )


class MapRegisterParser(RegisterParser):

    def parse(self, lines):
        spec = lines[0]
        value_parser = int  # TODO: in a map, only integers seem sensible?
        value_map = {}
        for i in count(1):
            line = lines[i]
            if line[self.number_cell]:  # assume that bitmap lines don't have number
                log.debug(f"End of mapping detected. (Row: {i})")
                break
            value = line[self.value_cell] or line[self.value_min_cell] or line[self.value_max_cell]
            if not value:
                log.warning(f"No mapping value found in any value cell. (Row: {i})")
                continue
            mapped_value = value_parser(line[self.tag_cell] or value)
            description = line[self.description_cell]
            description = f'"{description}"' if description else 'no description'
            log.debug(f"Found mapping: {value} -> <{mapped_value}>  ({description})")
            value_map[int(value)] = mapped_value  # the map value is always an int
        return MapRegister(
            number=spec[self.number_cell],
            size=spec[self.size_cell],
            group=spec[self.group_cell],
            tag=spec[self.tag_cell],
            description=spec[self.description_cell],
            value_parser=value_parser,
            value_map=value_map,
        )


class BitRegisterParser(RegisterParser):

    def parse(self, lines):
        spec = lines[0]
        bit_map = {}
        for i in count(1):
            line = lines[i]
            if line[self.number_cell]:  # assume that bitmap lines don't have number
                log.debug(f"End of bit mapping detected. (Row: {i})")
                break
            if not line[self.tag_cell]:  # only use tagged rows
                log.debug(f"Skipping bit mapping row (no tag). (Row: {i})")
                continue
            tag = line[self.tag_cell]
            value = line[self.value_min_cell] or line[self.value_max_cell]
            if not value:
                log.warning(f"No bit mapping value found in any value cell. Skipping tag '{tag}'.")
                continue
            value = int(value)
            log.debug(f"Found bit mapping: {value} -> {tag}")
            bit_map[value] = TagValue(
                tag=line[self.tag_cell],
                description=line[self.description_cell],
                value=value
            )
        return BitRegister(
            number=spec[self.number_cell],
            size=spec[self.size_cell],
            group=spec[self.group_cell],
            bit_map=bit_map,
        )

class FileParser:
    def read_lines(self, file) -> list:
        pass

class CsvParser(FileParser):

    def __init__(self, delimiter=",", quote_char='"', skip_lines=0):
        self.delimiter = delimiter
        self.quote_char = quote_char
        self.skip_lines = skip_lines

    def read_lines(self, file) -> list:
        reader = csv.reader(
            file,
            delimiter=self.delimiter,
            quotechar=self.quote_char,
        )
        lines = list(reader)
        return lines[self.skip_lines:]  # TODO: proper error when this fails


class RegisterLoader:

    def __init__(self):
        # self.configuration = configuration
        self.parsers = {}
        self.columns = {}
        # add default parsers
        self.add_parser(r'INT.*', IntRegisterParser())
        self.add_parser(r'DEC.*', DecimalRegisterParser())
        self.add_parser(r'BIT.*', BitRegisterParser())
        self.add_parser(f'MAP.*', MapRegisterParser())

    def add_parser(self, pattern, instance) -> Self:
        self.parsers[re.compile(pattern)] = instance
        return self

    def set_columns(self, **kwargs):
        self.columns = {**kwargs}

    def load_from_lines(self, lines):

        headers = list(enumerate(lines[0]))

        options = {
            NUMBER: ("Register Number", self.columns[NUMBER]),
            SIZE: ("Register Size/Length", self.columns[SIZE]),
            TYPE: ("Type/Format", self.columns[TYPE]),
            VALUE: ("Value", self.columns[VALUE]),
            VALUE_MIN: ("Min Value", self.columns[VALUE_MIN]),
            VALUE_MAX: ("Max Value", self.columns[VALUE_MAX]),
            TAG: ("Max Value", self.columns[TAG]),
            DESCRIPTION: ("Description", self.columns[DESCRIPTION]),
            GROUP: ("Tag Group", self.columns[GROUP]),
            DEVICE: ("Device", self.columns[DEVICE]),
        }

        cells = { key: -1 for key in options.keys() }

        for o in range(5): #max(len(x[1]) for x in options)):  # walk through all format options
            for c in cells:  # try to find a match for each cell
                if cells[c] != -1:
                    continue  # next cell
                log.debug(f"Checking {c}")
                for key, option in options.items():
                    if cells[key] != -1:
                        continue
                    name = option[0]
                    patterns = option[1]
                    if o < len(patterns):
                        for i, header in headers:
                            if re.match(patterns[o], header):
                                log.debug(f"Found matching {name} column: {header} (#{i}), Pattern: {patterns[o]}")
                                cells[key] = i  # record matched cell number

        # verify that all cells have been found
        for key in cells.keys():
            if cells[key] == -1:
                log.warning(f"Unable to identify {options[key][0]} column. Available patterns: {', '.join(options[key][1])}")

        # all of these are required
        required = [NUMBER, SIZE, TYPE, TAG]
        if any(cells[x] == -1 for x in required):
            names = [options[key][0] for key in required]
            raise ValueError(f"At least one of the following required columns is missing: {', '.join(names)}")

        # at least one of these
        required = [VALUE, VALUE_MIN, VALUE_MAX]
        if all(cells[x] == -1 for x in required):
            names = [options[key][0] for key in required]
            raise ValueError(f"At least one value column is required: {', '.join(names)}")

        # use these as they are easier to work with
        number_cell = cells[NUMBER]
        format_cell = cells[TYPE]
        tag_cell = cells[TAG]
        description_cell = cells[DESCRIPTION]

        for p in self.parsers.values():
            p.set_cells(cells)

        registers = []
        skipped = 1
        for i, line in enumerate(lines[skipped:]):
            pos = i+skipped
            number = line[number_cell]
            # skip all lines that don't have a register number
            if not number:
                log.debug(f"Empty line {pos+1} skipped.")
                continue

            # skip lines that don't have a tag
            if not line[tag_cell]:
                log.info(f'Register {number} ("{line[description_cell]}") skipped (no tag).')
                continue

            # find fitting parser
            register = None
            for pattern, parser in self.parsers.items():
                if pattern.match(line[format_cell]):
                    register = parser.parse(lines[pos:])
                    if isinstance(register, SimpleRegister):
                        log.info(f'Register {number} ("{register.description}") -> Tag {register.tag} ({type(register).__qualname__}).')
                    elif isinstance(register, BitRegister):
                        log.info(f'Register {number} ({type(register).__qualname__})')
                        for bit_value, tag_value in register.bit_map.items():
                            log.info(
                                f"Register {number} & {bit_value} -> "
                                f'Tag {tag_value.tag} ("{tag_value.description}").')
                    break

            if not register:
                log.warning(f'Register {number} ("{line[description_cell]}") skipped (unknown format: "{line[format_cell]}").')
                continue

            registers.append(register)
        return registers

