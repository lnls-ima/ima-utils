# -*- coding: utf-8 -*-
"""Balance Ohaus PA413P communication.

Created on 26/11/2020
@author: Luana Vilela
"""

import time as _time

from . import utils as _utils


class BalanceOhausCommands(object):
    """Balance Ohaus PA413P Commands."""

    def __init__(self):
        """Ohaus PA413P commands."""
        self.immediate_weight = 'IP'
        self.weight = 'P'
        self.zero = 'T'
        self.on = 'ON'
        self.off = 'OFF'
        self.unit = 'PU'


def BalanceOhaus_factory(baseclass):
    """Create BalanceOhaus class."""
    class BalanceOhaus(baseclass):
        """Balance Ohaus PA413P."""

        def __init__(self, log=False):
            """Initiaze all variables and prepare log.

            Args:
                log (bool): True to use event logging, False otherwise.

            """
            self.commands = BalanceOhausCommands()
            super().__init__(log=log)

        def zero_display(self):
            try:
                self.send_command(self.commands.zero)
            except Exception:
                if self.logger is not None:
                    self.logger.error('exception', exc_info=True)

        def turn_on(self):
            try:
                self.send_command(self.commands.on)
            except Exception:
                if self.logger is not None:
                    self.logger.error('exception', exc_info=True)

        def turn_off(self):
            try:
                self.send_command(self.commands.off)
            except Exception:
                if self.logger is not None:
                    self.logger.error('exception', exc_info=True)

        def read_weight(self, wait=0.3):
            try:
                self.send_command(self.commands.weight)
                _time.sleep(wait)

                resp = self.read_from_device()
                weight = float(
                    resp.split('\r\n')[6].replace('g', '').strip())
                return weight
            except Exception:
                if self.logger is not None:
                    self.logger.error('exception', exc_info=True)
                return ''

    return BalanceOhaus


BalanceOhausSerial = BalanceOhaus_factory(_utils.SerialInterface)
