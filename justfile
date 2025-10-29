build *ARGS:
    pip3 install pymodbus==3.11.3 --target tedge_modbus/pymodbus
    ./build/build.sh {{ARGS}}
