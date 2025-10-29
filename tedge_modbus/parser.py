import csv
import logging
import re
from enum import Enum
from itertools import count
from typing import Self

from tedge_modbus.model import Register, IntRegister, DecimalRegister, BitRegister, TagValue, MapRegister, SimpleRegister

log = logging.getLogger(__name__)


class RegisterParser:
    """A parser for a holding register row."""

    def __init__(self):
        self.number_cell = 0
        self.description_cell = 0
        self.size_cell = 0
        self.format_cell = 0
        self.value_min_cell = 0
        self.value_max_cell = 0
        self.interval_cell = 0
        self.group_cell = -1
        self.tag_cell = 0

    def set_cells(
            self,
            number=None,
            size=None,
            format=None,
            value_min=None,
            value_max=None,
            interval=None,
            group=None,
            tag=None,
            description=None,
        ) -> Self:
        """Set named cell numbers."""
        self.number_cell = number
        self.size_cell = size
        self.format_cell = format
        self.value_min_cell = value_min
        self.value_max_cell = value_max
        self.interval_cell = interval
        self.group_cell = group
        self.tag_cell = tag
        self.description_cell = description
        return self

    def parse(self, lines) -> Register:
        """Parse a register from specification rows."""
        pass


class IntRegisterParser(RegisterParser):

    def parse(self, lines):
        line = lines[0]
        return IntRegister(
            number=line[self.number_cell],
            size=line[self.size_cell],
            interval=line[self.interval_cell],
            group=line[self.group_cell],
            tag=line[self.tag_cell],
            description=line[self.description_cell],
        )

class DecimalRegisterParser(RegisterParser):

    def parse(self, lines):
        spec = lines[0]
        decimal_places = int(spec[self.format_cell].split()[1])
        return DecimalRegister(
            number=spec[self.number_cell],
            size=spec[self.size_cell],
            interval=spec[self.interval_cell],
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
            value = value_parser(line[self.value_min_cell] or line[self.value_max_cell])
            if not value:
                log.warning(f"No mapping value found in any value cell. (Row: {i})")
                continue
            new_value = value_parser(line[self.tag_cell] or value)
            description = line[self.description_cell]
            description = f'"{description}"' if description else 'no description'
            log.info(f"Found mapping: {value} -> <{new_value}>  ({description})")
            value_map[value] = new_value
        return MapRegister(
            number=spec[self.number_cell],
            size=spec[self.size_cell],
            interval=spec[self.interval_cell],
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
            interval=spec[self.interval_cell],
            group=spec[self.group_cell],
            bit_map=bit_map,
        )


class RegisterLoader:

    def __init__(self, csv_file, ignore_case, delimiter, quote_char, header):
        self.csv_file = csv_file
        self.header_row = header or 0
        self.delimiter = delimiter
        self.quote_char = quote_char
        self.ignore_case = ignore_case if ignore_case is not None else True
        self.parsers = {}

        # default column names
        self.number_column = 'Number'
        self.size_column = 'Size'
        self.format_column = 'Format'
        self.value_min_column = 'Min'
        self.value_max_column = 'Max'
        self.uom_column = 'UOM'
        self.group_column = 'Group'
        self.interval_column = 'Interval'
        self.tag_column = 'Tag'
        self.description_column = 'Description'

        # add default parsers
        self.add_parser(r'INT.*', IntRegisterParser())
        self.add_parser(r'DEC.*', DecimalRegisterParser())
        self.add_parser(r'BIT.*', BitRegisterParser())
        self.add_parser(f'MAP.*', MapRegisterParser())


    def add_parser(self, pattern, instance) -> Self:
        self.parsers[re.compile(pattern)] = instance
        return self

    def set_columns(
            self,
            number=None,
            size=None,
            format=None,
            value_min=None,
            value_max=None,
            uom=None,
            group=None,
            interval=None,
            tag=None,
            description=None,

    ) -> Self:
        self.number_column = number or self.number_column
        self.size_column = size or self.size_column
        self.format_column = format or self.format_column
        self.value_min_column = value_min or self.value_min_column
        self.value_max_column = value_max or self.value_max_column
        self.uom_column = uom or self.uom_column
        self.group_column = group or self.group_column
        self.interval_column = interval or self.interval_column
        self.tag_column = tag or self.tag_column
        self.description_column = description or self.description_column
        return self

    def load_registers(self):

        with open(self.csv_file) as csv_file:
            lines = list(csv.reader(csv_file, delimiter=))

            # get column names and cell positions
            headers = {value: i for i, value in enumerate(lines[0])}
            # verify that all required columns are defined
            for column_name, expected_header in [
                ("Register Number", self.number_column),
                ("Register Size/Length", self.size_column),
                ("Format", self.format_column),
                ("Min Value", self.value_min_column),
                ("Max Value", self.value_max_column),
                ("Sampling Interval", self.interval_column),
                ("Tag Group", self.group_column),
                ("Tag", self.tag_column),
                ("Description", self.description_column),
            ]:
                if not expected_header in headers:
                    raise ValueError(f"Unable to identify required {column_name} column (expected: '{expected_header}').")

            # find cell positions by name
            number_cell = headers[self.number_column]
            format_cell = headers[self.format_column]
            interval_cell = headers[self.interval_column]
            group_cell = headers[self.group_column]
            tag_cell = headers[self.tag_column]
            description_cell = headers[self.description_column]

            # define `set_cell` parameter kwargs
            cells = {
                'number': number_cell,
                'size': headers[self.size_column],
                'format': format_cell,
                'value_min': headers[self.value_min_column],
                'value_max': headers[self.value_max_column],
                'interval': interval_cell,
                'group': group_cell,
                'tag': tag_cell,
                'description': description_cell,
            }

            # inject cell positions into parser instances
            for p in self.parsers.values():
                p.set_cells(**cells)

            registers = []
            n_skipped = 1
            for i, line in enumerate(lines[1:]):
                pos = i + n_skipped
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

