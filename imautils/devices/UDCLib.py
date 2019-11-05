"""UDC communication.

Created on 9 de out de 2018
@author: Vitor Soares
"""

from . import utils as _utils


def UDC_factory(baseclass):
    """Create UDC class."""
    class UDC(baseclass):
        """Honeywell UDC control class."""

        def __init__(self, log=False):
            """Honeywell UDC control class.

            Args:
            ----
                log (bool): True to use event logging, False otherwise.

            """
            self.output1_register_address = 70
            self.output2_register_address = 382
            self.pv1_register_address = 72
            self.pv2_register_address = 74
            super().__init__(log=log)

        def read_output1(self):
            """Return controller output 1."""
            _output = None
            if self.output1_register_address is not None:
                _output = self.read_from_device(self.output1_register_address)
            return _output

        def read_output2(self):
            """Return controller output 2."""
            _output = None
            if self.output2_register_address is not None:
                _output = self.read_from_device(self.output2_register_address)
            return _output

        def read_pv1(self):
            """Return process variable."""
            _pv = None
            if self.pv1_register_address is not None:
                _pv = self.read_from_device(self.pv1_register_address)
            return _pv

        def read_pv2(self):
            """Return process variable 2."""
            _pv = None
            if self.pv2_register_address is not None:
                _pv = self.read_from_device(self.pv2_register_address)
            return _pv

    return UDC


UDCModBus = UDC_factory(_utils.ModBusInterface)
