# -*- coding: utf-8 -*-
"""Agilent34401A communication.

Created on 10/02/2015
@author: James Citadini
"""

import time as _time

from . import utils as _utils


class Agilent34401ACommands():
    """Commands of Agilent 34401A Digital Multimeter."""

    def __init__(self):
        """Load commands."""
        self._reset()
        self._query()
        self._config()
        self._clear()
        self._read()
        self._remote()

    def _reset(self):
        """Reset and preset functions."""
        self.reset = '*RST'
        self.preset = ':STAT:PRES'

    def _query(self):
        """Query commands."""
        self.qid = '*IDN?'
        self.qbeep = ':SYST:BEEP:STAT?'
        self.qerror = ':SYST:ERR?'

    def _config(self):
        """Configure measure."""
        self.config_volt = ':CONF:VOLT:DC DEF, DEF'
        self.config_temp = ':CONF:RES 100,0.0001'
        self.trig = ':TRIG:SOUR EXT'

    def _clear(self):
        self.clear = '*CLS'

    def _read(self):
        self.read = ':READ?'

    def _remote(self):
        self.remote = ':SYST:REM'


def Agilent34401A_factory(baseclass):
    """Create Agilent34401A class."""
    class Agilent34401A(baseclass):
        """Agilent 34401A digital multimeter."""

        def __init__(self, log=False):
            """Initiaze variables and prepare log.

            Args:
            ----
                log (bool): True to use event logging, False otherwise.

            """
            self.commands = Agilent34401ACommands()
            super().__init__(log=log)

        def connect(self, *args, **kwargs):
            """Connect with the device."""
            if super().connect(*args, **kwargs):
                try:
                    if self.interface == 'serial':
                        self.send_command(self.commands.remote)
                    return True
                except Exception:
                    return False
            else:
                return False

        def config(self):
            """Configure device for voltage measurements."""
            self.config_voltage()

        def config_voltage(self):
            """Configure device for voltage measurements."""
            self.reset()
            self.send_command(self.commands.config_volt)
            self.send_command(self.commands.clear)

        def config_temperature(self):
            """Configure device for voltage measurements."""
            self.reset()
            self.send_command(self.commands.config_volt)
            self.send_command(self.commands.clear)

        def read(self, wait=0.5):
            """Read from the device."""
            self.send_command(self.commands.clear)
            self.send_command(self.commands.read)
            _time.sleep(wait)
            try:
                val = float(self.read_from_device()[:-1])
                return val
            except Exception:
                return None

        def reset(self):
            """Reset device."""
            self.send_command(self.commands.reset)

    return Agilent34401A


Agilent34401AGPIB = Agilent34401A_factory(_utils.GPIBInterface)
Agilent34401ASerial = Agilent34401A_factory(_utils.SerialInterface)
