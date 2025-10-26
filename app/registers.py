
class TagValue:
    def __init__(self, tag, description, value):
        self.tag = tag
        self.description = description
        self.value = value


class Register:
    """A holding register specification & parsing utility"""

    def __init__(self, number, size):
        self.number = int(number)
        self.size = int(size)

    def parse(self, value) -> tuple[TagValue]:
        """Parse the register value.

        The result is a sequence (tuple) as a single value might be evaluated
        to multiple tag values being defined.
        """
        pass


class SimpleRegister(Register):
    def __init__(self, number, size, tag, description):
        super().__init__(number, size)
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
    def __init__(self, number, size, tag, description, decimal_places):
        super().__init__(number, size, tag, description)
        self.decimal_places = decimal_places

    def _parse(self, value):
        return int(value) / 10 ** self.decimal_places

#
# class MapRegister(SimpleRegister):
#     def __init__(self, number, description, category, tag, entries):
#         super().__init__(number, description, category, tag)
#         self.value_map = {x.value: x for x in entries}
#
#     def parse(self, value):
#         return self.value_map.get(value, None)


class BitRegister(Register):

    def __init__(self, number, size, bit_map):
        super().__init__(number, size)
        self.bit_map = bit_map

    def parse(self, value):
        return (
            TagValue(
                tag_value.tag,
                tag_value.description,
                value=value & bit_value > 0,
            )
            for bit_value, tag_value in self.bit_map.items()
        )
