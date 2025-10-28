import os
import tomllib


class Configuration:
    def __init__(self, config_dict):
        self.modbus_host = config_dict["modbus"]["host"]
        self.modbus_port = config_dict["modbus"]["port"]


class TagValue:
    def __init__(self, tag, description, value):
        self.tag = tag
        self.description = description
        self.value = value


class Register:
    """A holding register specification & parsing utility"""

    def __init__(self, number, size, interval, group):
        self.number = int(number)
        self.size = int(size)
        self.interval = int(interval)
        self.group = group

    def parse(self, value) -> tuple[TagValue]:
        """Parse the register value.

        The result is a sequence (tuple) as a single value might be evaluated
        to multiple tag values being defined.
        """
        pass


class SimpleRegister(Register):
    def __init__(self, number, size, interval, group, tag, description):
        super().__init__(number, size, interval, group)
        self.description = description
        self.tag = tag

    def parse(self, value):
        return (TagValue(self.tag, self.description, self._parse(value)),)

    def _parse(self, value):
        """Just parse the register's actual value."""
        pass


class IntRegister(SimpleRegister):
    def _parse(self, value):
        return int(value)

class DecimalRegister(SimpleRegister):
    def __init__(self, number, size, interval, group, tag, description, decimal_places):
        super().__init__(number, size, interval, group, tag, description)
        self.decimal_places = decimal_places

    def _parse(self, value):
        return int(value) / 10 ** self.decimal_places

class MapRegister(SimpleRegister):

    def __init__(self, number, size, interval, group, tag, description, value_parser, value_map):
        super().__init__(number, size, interval, group, tag, description)
        self.value_parser = value_parser
        self.value_map = value_map

    def _parse(self, value):
        return self.value_map.get(self.value_parser(value), None)


class BitRegister(Register):

    def __init__(self, number, size, interval, group, bit_map):
        super().__init__(number, size, interval, group)
        self.bit_map = bit_map

    def parse(self, value):
        return (
            TagValue(
                tag_value.tag,
                tag_value.description,
                value=int(value) & bit_value > 0,
            )
            for bit_value, tag_value in self.bit_map.items()
        )


class RegisterSequence:
    """A sequence of registers that can be read in one pass."""

    def __init__(self, registers):
        self.registers = registers
        self.name = registers[0].group
        self.interval = registers[0].interval


class MeasurementGroup:
    """A logical grouping of registers that are part of one sample."""

    def __init__(self, name, register_sequences):
        self.name = name
        self.sequences = register_sequences
        self.interval = register_sequences[0][0].interval