# -*- coding: utf-8 -*-
"""NMR Library.

Created on 09/10/2018
@author: Lucas Balthazar and Vitor Soares
"""

import time as _time
import threading as _threading

from . import utils as _utils


class NMRCommands(object):
    """PT 2025 NMR Teslameter Commands."""

    def __init__(self):
        """Load commands."""
        self._operation_mode()
        self._aquisition_mode()
        self._frequency_selection()
        self._field_sense_selection()
        self._channel_selection()
        self._search_mode()
        self._reset_time_base()
        self._display()
        self._status()
        self._read()

    def _operation_mode(self):
        self.remote = 'R'  # Remote mode (disables the front panel)
        self.local = 'L'  # Local mode (enables the front panel)

    def _aquisition_mode(self):
        self.aquisition = 'A'  # Manual(0) or Auto(1)

    def _frequency_selection(self):
        self.frequency = 'C'  # value is expressed in decimal

    def _field_sense_selection(self):
        self.field_sense = 'F'  # Negative(0) or Positive(1) fields

    def _channel_selection(self):
        self.channel = 'P'  # A, B, C, D, E, F, G or H channels

    def _search_mode(self):
        self.search = 'H'  # activates the automatic field-searching algorithm
        self.quit_search = 'Q'  # inactivates the search mode
        self.nr_channels = 'X'  # number of channels to be used in search
        self.search_time = 'O'  # search time (n=1 -> 9s per probe)

    def _reset_time_base(self):
        self.reset_time = 'T'  # Reset NMR time-base

    def _display(self):
        self.display_unit = 'D'  # displayed value given in MHz(0) or Tesla(1)
        self.display_vel = 'V'  # Normal(0) or Fast(1)

    def _status(self):
        self.status = 'S'  # returns the status (1 Byte)

    def _read(self):
        self.read = '\x05'  # Leitura. Formato: vdd.ddddddF/T


def NMR_factory(baseclass):
    """Create NMR class."""
    class NMR(baseclass):
        """Class for communication with NMR device."""

        def __init__(self, log=False):
            """Initiaze all variables and prepare log.

            Args:
            ----
                log (bool): True to use event logging, False otherwise.

            """
            self.commands = NMRCommands()
            self.rlock = _threading.RLock()
            super().__init__(log=log)

        @property
        def locked(self):
            """Return lock status."""
            _ans = self.read_status(1)
            _locked = None
            if _ans is not None:
                _locked = int(_ans[-6])
            return _locked

        def connect(self, *args, **kwargs):
            """Connect device."""
            if super().connect(*args, **kwargs):
                try:
                    self.send_command(self.commands.remote)
                    return True
                except Exception:
                    return True
            else:
                return False

        def disconnect(self, wait=0.05):
            """Disconnect device."""
            try:
                self.send_command(self.commands.quit_search)
                _time.sleep(wait)
                self.send_command(self.commands.local)
                _time.sleep(wait)
                return super().disconnect()
            except Exception:
                return None

        def configure(
                self, mode, frequency, field_sense, display_unit,
                display_vel, search_time, channel, nr_channels, wait=0.05):
            """Configure NMR.

            Args:
            ----
                mode (int or str): aquisition mode
                    [Manual(0) or Auto(1)],
                frequency (int or str): initial search frequency,
                field_sense (int or str): field sense
                    [Negative(0) or Positive(1)],
                display_unit (int or str): display unit
                    [MHz(0) or Tesla(1)],
                display_vel (int or str): display velocity
                    [Normal(0) or Fast(1)],
                search_time (int or str): search time [n=1 -> 9s per probe],
                channel (str): initial search channel,
                nr_channels (int or str): number of channels.

            Return:
            ------
                True if successful, False otherwise.

            """
            try:
                with self.rlock:
                    self.send_command(self.commands.remote)
                    _time.sleep(wait)
                    
                    self.send_command(self.commands.quit_search)
                    _time.sleep(wait)

                    self.send_command(self.commands.channel + str(channel))
                    _time.sleep(wait)

                    self.send_command(
                        self.commands.nr_channels + str(nr_channels))
                    _time.sleep(wait)

                    if mode == 0:
                        self.send_command(
                            self.commands.aquisition + "0")  
                        _time.sleep(wait)

                    elif mode == 1:
                        self.send_command(
                            self.commands.aquisition + "1") 
                        _time.sleep(wait)

                        self.send_command(
                            self.commands.frequency+str(frequency))
                        _time.sleep(wait)               

                    else:
                        self.send_command(
                            self.commands.search + str(frequency))            
                        _time.sleep(wait)

                    self.send_command(
                        self.commands.field_sense + str(field_sense))
                    _time.sleep(wait)

                    self.send_command(
                        self.commands.display_unit + str(display_unit))
                    _time.sleep(wait)

                    self.send_command(
                        self.commands.display_vel + str(display_vel))
                    _time.sleep(wait)

                    self.send_command(
                        self.commands.search_time + str(search_time))
                    _time.sleep(wait)

                return True

            except Exception:
                self.rlock.release()
                return False

        def read_b_value(self, wait=0.05):
            """Read magnetic field value from the device.

            Return:
            ------
                B value (str): string read from the device. First character
            indicates lock state ('L' for locked, 'N' for not locked and 'S' to
            NMR signal seen. The last character indicates the unit ('T'
            for Tesla and 'F' for MHz). Returns empty string if timeout occurs.

            """
            try:
                self.send_command(self.commands.read)
                _time.sleep(wait)
                _reading = self.read_from_device().strip('\r\n')
                return _reading
            except Exception:
                return ''

        def read_status(self, register, wait=0.05):
            """Read a status register from PT2025 NMR.

            Args:
            ----
                register (int): register number (from 1 to 4)

            Return:
            ------
                string of register bits

            """
            try:
                _status = None
                if 0 < register < 5:
                    self.send_command(self.commands.status + str(register))
                    _time.sleep(wait)
                    _ans = self.read_from_device().strip('S\r\n')
                    if _ans == '':
                        return None
                    _ans = bin(int(_ans, 16))[2:]
                    _status = '{0:>8}'.format(_ans).replace(' ', '0')
                else:
                    print('Register number out of range.')
                return _status
            except Exception:
                return None

        def read_dac_value(self, wait=0.05):
            """Return current internal 12-bit DAC value (from 0 to 4095)."""
            try:
                self.send_command(self.commands.status + '4')
                _time.sleep(wait)
                _ans = self.read_from_device().strip('S\r\n')
                _ans = int(_ans, 16)
                return _ans
            except Exception:
                return None

        def scan(self, channel, dac, speed, wait=0.05):
            """Scan the selected probe to determine the magnetic field value.

            Times out after 30 seconds if the probe does not lock.

            Args:
            ----
                channel (str): selects the probe channel (from 'A' to 'H').
                dac (int): selects intial 12 bits DAC value (from 0 to 4095).
                speed (int): search time (from 1 to 6). 1 is the fastest value
            (9 seconds to scan over the entire probe range). Each unit increase
            in the speed increases the search time by 3 seconds.

            Return:
            ------
                (B value, DAC value, dt)
                B value (str): measured magnetic field in Tesla.
                DAC value (int): current DAC value.
                dt (float): measurement duration in seconds or -1 if timed out.

            """
            try:
                with self.rlock:
                    self.send_command(self.commands.channel + channel)
                    _time.sleep(wait)

                    self.send_command(self.commands.aquisition + '1')
                    _time.sleep(wait)

                    self.send_command(self.commands.display_unit + '1')
                    _time.sleep(wait)

                    self.send_command(self.commands.search_time + str(speed))
                    _time.sleep(wait)

                    self.send_command(self.commands.search + str(dac))
                    _t0 = _time.time()
                    _time.sleep(wait)

                    _dt = 0
                    while not self.locked:
                        _time.sleep(0.1)
                        _dt = _time.time() - _t0
                        if _dt > 30:
                            _dt = -1
                            break

                    _b = self.read_b_value()
                    _dac = self.read_dac_value()
                    self.send_command(self.commands.quit_search)

                return _b, _dac, _dt

            except Exception:
                return None, None, None

    return NMR


NMRGPIB = NMR_factory(_utils.GPIBInterface)
NMRSerial = NMR_factory(_utils.SerialInterface)
