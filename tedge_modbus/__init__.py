import sys, os

pymodbus_path = os.path.join(os.path.dirname(__file__), "pymodbus")
if pymodbus_path not in sys.path:
    sys.path.insert(0, pymodbus_path)
