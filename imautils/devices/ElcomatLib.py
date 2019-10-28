# -*- coding: utf-8 -*-
"""Elcomat communication.

Created on 07/12/2012
@author: James Citadini
"""

import time as _time

from . import utils as _utils


class ElcomatCommands():
    """Elcomat Commands."""

    def __init__(self):
        """Load commands."""
        self.read_relative = 'r'
        self.read_absolute = 'a'


def Elcomat_factory(baseclass):
    """Create Elcomat class."""
    class Elcomat(baseclass):
        """Class for communication with Elcomat device."""

        def __init__(self, logfile=None):
            """Initiaze all variables and prepare log file.

            Args:
            ----
                logfile (str): log file path.

            """
            self.delay = 0.01
            self.commands = ElcomatCommands()
            super().__init__(logfile)

        def connect(self, *args, **kwargs):
            """Connect with the device."""
            if super().connect(*args, **kwargs):
                try:
                    self.send_command(self.commands.remote)
                    return True
                except Exception:
                    return False
            else:
                return False

        def get_absolute_measurement(self, unit='arcsec'):
            """Read X-axis and Y-axis absolute values from the device."""
            try:
                _time.sleep(self.delay)
                self.send_command(self.commands.read_absolute)
                _time.sleep(self.delay)
                _readings = self.read_from_device()
                _rlist = _readings.split('\r')[:-1]
                _x = None
                _y = None
                if len(_rlist) == 1:
                    values = _rlist[0].split(' ')
                    if values[0] == '4' and values[1] == '003':
                        _x = float(values[2])
                        _y = float(values[3])
                        if unit == 'deg':
                            _x = _x/3600
                            _y = _y/3600
                        elif unit == 'rad':
                            _x = _x/206264.8062471
                            _y = _y/206264.8062471
                return (_x, _y)

            except Exception:
                return (None, None)

        def get_relative_measurement(self, unit='arcsec'):
            """Read X-axis and Y-axis relative values from the device."""
            try:
                _time.sleep(self.delay)
                self.send_command(self.commands.read_relative)
                _time.sleep(self.delay)
                _readings = self.read_from_device()
                _rlist = _readings.split('\r')[:-1]
                _x = None
                _y = None
                if len(_rlist) == 1:
                    values = _rlist[0].split(' ')
                    if values[0] == '2' and values[1] == '103':
                        _x = float(values[2])
                        _y = float(values[3])
                        if unit == 'deg':
                            _x = _x/3600
                            _y = _y/3600
                        elif unit == 'rad':
                            _x = _x/206264.8062471
                            _y = _y/206264.8062471
                return (_x, _y)

            except Exception:
                return (None, None)

    return Elcomat


ElcomatSerial = Elcomat_factory(_utils.SerialInterface)
