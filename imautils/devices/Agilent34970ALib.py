# -*- coding: utf-8 -*-
"""Agilent34970A communication.

Created on 10/02/2015
@author: James Citadini
"""

import time as _time

from . import utils as _utils


class Agilent34970ACommands():
    """Commands of Agilent 34970 Data Acquisition/Switch Unit."""

    def __init__(self):
        """Load commands."""
        self.reset = '*RST'
        self.clean = '*CLS'
        self.lock = ':SYST:LOC'
        self.scan = ':ROUT:SCAN'
        self.conf_temp = ':CONF:TEMP FRTD,85,'
        self.conf_volt = ':CONF:VOLT:DC'
        self.qid = '*IDN?'
        self.qread = ':READ?'
        self.qscan = ':ROUT:SCAN?'
        self.qscan_size = ':ROUT:SCAN:SIZE?'


def Agilent34970A_factory(baseclass):
    """Create Agilent34970A class."""
    class Agilent34970A(baseclass):
        """Agilent 34970A multichannel for temperatures readings."""

        def __init__(self, log=False):
            """Initiaze variables and prepare logging.

            Args:
            ----
                log (bool): True to use event logging, False otherwise.

            """
            self._config_channels = []
            self.temperature_channels = ['101', '105']
            self.voltage_channels = ['102', '103', '107']
            self.commands = Agilent34970ACommands()
            super().__init__(log=log)

        @property
        def config_channels(self):
            """Return current channel configuration list."""
            return self._config_channels

        def configure(self, channel_list='all', wait=0.5):
            """Configure channels."""
            if channel_list == 'all':
                volt_channel_list = self.voltage_channels
                temp_channel_list = self.temperature_channels

            else:
                volt_channel_list = []
                temp_channel_list = []
                channel_list = [str(ch) for ch in channel_list]
                for _ch in channel_list:
                    if _ch in self.voltage_channels:
                        volt_channel_list.append(_ch)
                    else:
                        temp_channel_list.append(_ch)

            all_channels = sorted(volt_channel_list + temp_channel_list)
            if not all_channels:
                return False
            elif all_channels == self._config_channels:
                return True

            try:
                self.send_command(self.commands.clean)
                self.send_command(self.commands.reset)

                _cmd = ''
                if volt_channel_list:
                    volt_scanlist = '(@' + ','.join(volt_channel_list) + ')'
                    _cmd = _cmd + self.commands.conf_volt + ' ' + volt_scanlist

                if temp_channel_list:
                    if _cmd:
                        _cmd = _cmd + '; '
                    temp_scanlist = '(@' + ','.join(temp_channel_list) + ')'
                    _cmd = _cmd + self.commands.conf_temp + ' ' + temp_scanlist

                print(_cmd)
                self.send_command(_cmd)
                _time.sleep(wait)
                scanlist = '(@' + ','.join(all_channels) + ')'
                self.send_command(self.commands.scan + ' ' + scanlist)
                _time.sleep(wait)
                self._config_channels = all_channels.copy()
                return True

            except Exception:
                return False

        def get_readings(self, wait=0.5):
            """Get reading list."""
            try:
                self.send_command(self.commands.qread)
                _time.sleep(wait)
                rstr = self.read_from_device()
                rlist = []
                if rstr:
                    rlist = [float(r) for r in rstr.split(',')]
                return rlist
            except Exception:
                return []

        def get_scan_channels(self, wait=0.1):
            """Return the scan channel list read from the device."""
            try:
                self.send_command(self.commands.qscan)
                _time.sleep(wait)
                rstr = self.read_from_device()
                cstr = rstr.split('(@')[1].replace(')', '').replace('\n', '')
                channel_list = []
                if cstr:
                    channel_list = cstr.split(',')
                return channel_list
            except Exception:
                return []

    return Agilent34970A


Agilent34970AGPIB = Agilent34970A_factory(_utils.GPIBInterface)
Agilent34970ASerial = Agilent34970A_factory(_utils.SerialInterface)
