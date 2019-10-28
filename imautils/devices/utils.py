# -*- coding: utf-8 -*-
"""Device communication interfaces."""

import logging as _logging
import visa as _visa
import serial as _serial
import minimalmodbus as _minimalmodbus


class GPIBInterface():
    """Class for communication with GPIB devices."""

    def __init__(self, logfile=None):
        """Initiaze all variables and prepare log file.

        Args:
        ----
            logfile (str): log file path.

        """
        self.interface = 'gpib'
        self.inst = None
        self.logger = None
        self.logfile = logfile
        self.log_events()

    @property
    def connected(self):
        """Return True if the device is connected, False otherwise."""
        _status = None
        if self.inst is None:
            _status = False
        else:
            try:
                self.inst.resource_name
                _status = True
            except Exception:
                _status = False
        return _status

    def log_events(self):
        """Prepare log file to save info, warning and error status."""
        if self.logfile is not None:
            formatter = _logging.Formatter(
                fmt='%(asctime)s\t%(levelname)s\t%(message)s',
                datefmt='%m/%d/%Y %H:%M:%S')
            file_handler = _logging.FileHandler(self.logfile, mode='w')
            file_handler.setFormatter(formatter)
            logname = self.logfile.replace('.log', '')
            self.logger = _logging.getLogger(logname)
            self.logger.addHandler(file_handler)
            self.logger.setLevel(_logging.ERROR)

    def connect(self, address, board=0, timeout=1000):
        """Connect to a GPIB device with the given address.

        Args:
        ----
            address (int): device address,
            board (int): gpib board (default 0),
            timeout (int): timeout in milliseconds (default 1000).

        Return:
        ------
            True if successful, False otherwise.

        """
        try:
            resource_manager = _visa.ResourceManager()
            name = 'GPIB' + str(board) + '::' + str(address) + '::INSTR'
            inst = resource_manager.open_resource(name.encode('utf-8'))

            if inst.resource_name == (name):
                try:
                    self.inst = inst
                    self.inst.timeout = timeout
                    return True
                except Exception:
                    self.inst.close()
                    if self.logger is not None:
                        self.logger.error('exception', exc_info=True)
                    return False
            else:
                return False
        except Exception:
            if self.logger is not None:
                self.logger.error('exception', exc_info=True)
            return None

    def disconnect(self):
        """Disconnect the GPIB device."""
        try:
            if self.inst is not None:
                self.inst.close()
            return True
        except Exception:
            if self.logger is not None:
                self.logger.error('exception', exc_info=True)
            return None

    def send_command(self, command):
        """Write string message to the device and check size of the answer.

        Args:
        ----
            command (str): command to be executed by the device.

        Return:
        ------
            True if successful, False otherwise.

        """
        try:
            if self.inst is None:
                return None

            return self.inst.write(command)[0] == (len(command))
        except Exception:
            if self.logger is not None:
                self.logger.error('exception', exc_info=True)
            return None

    def read_from_device(self):
        """Read a string from the device.

        Stop reading when termination is detected.
        Tries to read from device, if timeout occurs, returns empty string.

        Return:
        ------
            the string read from the device.

        """
        try:
            if self.inst is None:
                return ''

            reading = self.inst.read()
            return reading
        except Exception:
            if self.logger is not None:
                self.logger.error('exception', exc_info=True)
            return ''

    def read_raw_from_device(self):
        """Read a string from the device.

        Stop reading when termination is detected.
        Tries to read from device, if timeout occurs, returns empty string.

        Return:
        ------
            the string read from the device.

        """
        try:
            if self.inst is None:
                return ''

            reading = self.inst.read_raw()
            return reading
        except Exception:
            if self.logger is not None:
                self.logger.error('exception', exc_info=True)
            return ''

    def status(self):
        """Read status byte."""
        _status = 0
        if self.inst is not None:
            _status = self.inst.stb & 128
        return _status


class SerialInterface():
    """Class for communication with Serial device."""

    def __init__(self, logfile=None):
        """Initiaze all variables and prepare log file.

        Args:
        ----
            logfile (str): log file path.

        """
        self.interface = 'serial'
        self.inst = None
        self.logger = None
        self.logfile = logfile
        self.log_events()

    @property
    def connected(self):
        """Return True if the port is open, False otherwise."""
        _status = False
        if self.inst is not None:
            _status = self.inst.is_open
        return _status

    def log_events(self):
        """Prepare log file to save info, warning and error status."""
        if self.logfile is not None:
            formatter = _logging.Formatter(
                fmt='%(asctime)s\t%(levelname)s\t%(message)s',
                datefmt='%m/%d/%Y %H:%M:%S')
            file_handler = _logging.FileHandler(self.logfile, mode='w')
            file_handler.setFormatter(formatter)
            logname = self.logfile.replace('.log', '')
            self.logger = _logging.getLogger(logname)
            self.logger.addHandler(file_handler)
            self.logger.setLevel(_logging.ERROR)

    def connect(
            self, port, baudrate, bytesize=_serial.EIGHTBITS,
            stopbits=_serial.STOPBITS_ONE, parity=_serial.PARITY_NONE,
            timeout=1000):
        """Connect to a serial port.

        Args:
        ----
            port (str): device port,
            baudrate (int): device baudrate,
            bytesize (int): bytesize (default 8),
            stopbits (int): stopbits (default 1),
            parity (str): parity (default 'N'),
            timeout (int): timeout in seconds (default 1).

        Return:
        ------
            True if successful.

        """
        try:
            self.inst = _serial.Serial(port=port, baudrate=baudrate)
            self.inst.bytesize = bytesize
            self.inst.stopbits = stopbits
            self.inst.parity = parity
            self.inst.timeout = timeout
            if not self.inst.is_open:
                self.inst.open()
            return True
        except Exception:
            if self.logfile is not None:
                self.logger.error('exception', exc_info=True)
            return None

    def disconnect(self):
        """Disconnect the device."""
        try:
            if self.inst is not None:
                self.inst.close()
            return True
        except Exception:
            if self.logger is not None:
                self.logger.error('exception', exc_info=True)
            return None

    def send_command(self, command):
        """Write string message to the device and check size of the answer.

        Args:
        ----
            command (str): command to be executed by the device.

        Return:
        ------
            True if successful, False otherwise.

        """
        try:
            self.inst.reset_input_buffer()
            self.inst.reset_output_buffer()
            command = command + '\r\n'
            return self.inst.write(command.encode('utf-8')) == len(command)
        except Exception:
            if self.logfile is not None:
                self.logger.error('exception', exc_info=True)
            return None

    def read_from_device(self):
        """Read a string from the device.

        Stop reading when termination is detected.
        Tries to read from device, if timeout occurs, returns empty string.

        Return:
        ------
            the string read from the device.

        """
        try:
            reading = self.inst.read_all().decode('utf-8')
            return reading
        except Exception:
            if self.logfile is not None:
                self.logger.error('exception', exc_info=True)
            return ''


class ModBusInterface():
    """ModBus communication class."""

    def __init__(self, logfile=None):
        """Initiaze all variables and prepare log file.

        Args:
        ----
            logfile (str): log file path.

        """
        self.interface = 'modbus'
        self.inst = None
        self.logger = None
        self.logfile = logfile
        self.log_events()

    @property
    def connected(self):
        """Return True if the port is open, False otherwise."""
        _status = False
        if self.inst is not None:
            _status = self.inst.serial.is_open
        return _status

    def log_events(self):
        """Prepare log file to save info, warning and error status."""
        if self.logfile is not None:
            formatter = _logging.Formatter(
                fmt='%(asctime)s\t%(levelname)s\t%(message)s',
                datefmt='%m/%d/%Y %H:%M:%S')
            file_handler = _logging.FileHandler(self.logfile, mode='w')
            file_handler.setFormatter(formatter)
            logname = self.logfile.replace('.log', '')
            self.logger = _logging.getLogger(logname)
            self.logger.addHandler(file_handler)
            self.logger.setLevel(_logging.ERROR)

    def connect(
            self, port, baudrate, slave_address, bytesize=_serial.EIGHTBITS,
            stopbits=_serial.STOPBITS_ONE, parity=_serial.PARITY_NONE,
            timeout=1):
        """Connect to a serial port.

        Args:
        ----
            port (str): device port,
            baudrate (int): device baudrate,
            slave_address (int): slave address in the range 1 to 247,
            bytesize (int): bytesize (default 8),
            stopbits (int): stopbits (default 1),
            parity (str): parity (default 'N'),
            timeout (int): timeout in seconds (default 1).

        Return:
        ------
            True if successful.

        """
        try:
            self.inst = _minimalmodbus.Instrument(port, slave_address)
            self.inst.serial.baudrate = baudrate
            self.inst.serial.bytesize = bytesize
            self.inst.serial.stopbits = stopbits
            self.inst.serial.parity = parity
            self.inst.serial.timeout = timeout
            if not self.inst.serial.is_open:
                self.inst.serial.open()
            return True
        except Exception:
            if self.logfile is not None:
                self.logger.error('exception', exc_info=True)
            return None

    def disconnect(self):
        """Disconnect the device."""
        try:
            if self.inst is not None:
                self.inst.serial.close()
            return True
        except Exception:
            if self.logger is not None:
                self.logger.error('exception', exc_info=True)
            return None

    def read_from_device(self, registeraddress):
        """Read a string from the device.

        Stop reading when termination is detected.
        Tries to read from device, if timeout occurs, returns empty string.

        Return:
        ------
            the string read from the device.

        """
        try:
            return self.inst.read_float(registeraddress)
        except Exception:
            if self.logfile is not None:
                self.logger.error('exception', exc_info=True)
            return ''