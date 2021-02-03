# -*- coding: utf-8 -*-
"""
Created on 11/01/2013
Update on 23/11/2017
Versao 1.0
@author: James Citadini
@coauthor: Vitor Soares
"""

import time as _time
import numpy as _np

from . import utils as _utils


class FDI2056Commands():
    """Commands of FDI2056 Integrator."""

    def __init__(self):
        """Load commands."""
        # reads encoder position
        self.read_encoder = "CONTR:ENC:POS?"
        # configures encoder
        self.config_encoder = "CONTR:ENC:CONF 'DIFF,/A:/B:IND,ROT:"
        # returns lengh of flux buffer
        self.data_count = "DATA:COUN?"
        # configures encoder as trigger_ref source
        self.arm_encoder = "ARM:SOUR ENC"
        # configures trigger_ref
        self.arm_ref = "ARM:ENC "
        # configures partial integrals (integrates only between triggers)
        self.calc_flux = "CALC:FLUX 0"
        # disable timestamp
        self.disable_timestamp = "FORM:TIMESTAMP:ENABLE 0"
        # reads flux buffer
        self.fetch_array = "FETC:ARR? "
        # Short circuit on
        self.short_circuit_on = "INP:COUP GND"
        # Short circuit off
        self.short_circuit_off = "INP:COUP DC"
        # configure gain
        self.gain = "INP:GAIN "
        # saves current configuration in hd
        self.store_config = "MEM:STOR"
        # deletes current configuration in hd
        self.del_config = "MEM:DEL"
        # same as FetchArray but sends ABORt;INIT before fetching the array
        self.read_array = "READ:ARR? "
        # calibrates offset and slope for all gains
        self.calibrate = "SENS:CORR:ALL"
        # configures encoder as trigger source
        self.trigger_source_enc = "TRIG:SOUR ENC"
        # configures external trigger as trigger source
        self.trigger_source_ext = "TRIG:SOUR EXT"
        # configures timer as trigger source
        self.trigger_source_tim = "TRIG:SOUR TIM"
        # configures the frequency of an internal periodic signal source [kHz]
        self.trigger_tim_rate = "TRIG:TIM "
        # configures number of triggers to complete a measurement
        self.trigger_count = "TRIG:COUN "
        # number of events to generate a trigger
        self.trigger_ecount = "TRIG:ECO "
        # configures trigger direction as FORward or BACKward
        self.trigger_dir = "TRIG:ENC "
        # returns system errors until all errors are read
        self.error = "SYST:ERR?"
        # aborts ongoing commands
        self.stop = "ABORT"
        # starts measurement
        self.start = "INIT"
        # resets to default configurations
        self.reset = "*RST"
        # returns identification and firmware version
        self.idn = "*IDN?"
        # clears status registers
        self.clear_status = "*CLS"
        # enable bits in Status Byte
        self.status_en = "*SRE "
        # read Status Byte
        self.read_status = "*STB?"
        # enable bits in Standard Event Status Register
        self.event_en = "*ESE "
        # read Standard Event Status Register
        self.read_event = "*ESR?"
        # enable bits in OPERation Status
        self.oper_en = "STAT:OPER:ENAB "
        # read OPERation Status
        self.read_oper = "STAT:OPER?"
        # enable bits in QUEStionable Status
        self.ques_en = "STAT:QUES:ENAB "
        # read QUEStionable Status
        self.read_ques = "STAT:QUES?"
        # set operation complete bit
        self.opc = "*OPC"
        # properly shuts the system down
        self.power_off = "SYST:POW OFF"
        # search encoder index
        self.search_index = "IND,"


def FDI2056_factory(baseclass):
    """Create FDI2056 Integrator class."""
    class FDI2056(baseclass):
        """FDI2056 Integrator."""

        def __init__(self, log=False):
            """Initiaze variables and prepare log.

            Args:
                log (bool): True to use event logging, False otherwise.
            """
            self.commands = FDI2056Commands()
            self.conversion_factor = 1
            super().__init__(log=log)

        def connect(self, address, board=0, host="FDI2056"):
            """Connects to FDI2056 integrator.

            Args:
                address (int): device address,
                board (int): TCPIP board (default 0),
                host (str): host name (default 'FDI2056')

            Returns:
                True if successful, False otherwise.
            """
            try:
                if super().connect(address, board=board, host=host):
                    self.configure_status()
                    self.send_command(self.commands.short_circuit_off)
                    return True
                else:
                    return False

            except Exception:
                return False

        def read_status(self, register):
            """Reads status register.

            Returns:
                ans (str): a binary str containg the status register data.
            """
            if not self.connected:
                return False

            if register == 0:
                self.send_command(self.commands.read_status)
            elif register == 1:
                self.send_command(self.commands.read_event)
            elif register == 2:
                self.send_command(self.commands.read_oper)
            elif register == 3:
                self.send_command(self.commands.read_ques)
            _ans = self.read_from_device()
            _ans = bin(int(_ans.strip('\n')))[2:]
            return _ans

        def read_encoder(self):
            """Reads encoder position.

            Returns:
                ans (str): string contaning the encoder position.
            """
            if not self.connected:
                return ''

            try:
                self.send_command(self.commands.read_encoder)
                return self.read_from_device().strip('\n')

            except Exception:
                return ''

        def configure_gain(self, gain):
            """Configures integrator gain.

            Args:
                gain (int): integrator gain.
            """
            if not self.connected:
                return False

            self.send_command(self.commands.gain + str(gain))
            return True

        def configure_status(self):
            """Configures the status registers."""
            if not self.connected:
                return False
            
            self.send_command(self.commands.clear_status)
            self.send_command(self.commands.status_en + '255')
            self.send_command(self.commands.event_en + '255')
            self.send_command(self.commands.oper_en + '65535')
            self.send_command(self.commands.ques_en + '65535')

        def configure_encoder_reading(self, encoder_pulses):
            """Configures encoder.

            Args:
                encoder_pulses (int): nr of encoder pulses per measurement.
            """
            if not self.connected:
                return False

            pulses = str(int(encoder_pulses/4))
            self.send_command(self.commands.config_encoder + pulses + "'")
            self.send_command(self.commands.arm_encoder)
            self.send_command(self.commands.trigger_source_enc)
            return True

        def configure_homing(self, direction):
            if not self.connected:
                return False

            cmd = self.commands.search_index + str(direction)
            self.send_command(cmd)
            return True

        def configure_trig_timer(self, rate, npts):
            """Configures timer trigger.

            Args:
                rate (int): frequency of the internal periodic signal [kHz].
                npts (int): number of integration points per measurement.
            """
            if not self.connected:
                return False

            self.send_command(self.commands.trigger_source_tim)
            self.send_command(self.commands.trigger_tim_rate + str(rate))
            self.send_command(self.commands.trigger_count + str(npts+1))
            self.send_command(self.commands.trigger_ecount + "1")
            self.send_command(self.commands.calc_flux)
            self.send_command(self.commands.disable_timestamp)
            return True

        def configure_trig_external(self, npts):
            """Configures external trigger.

            Args:
                npts (int): number of integration points per measurement.
            """
            if not self.connected:
                return False

            self.send_command(self.commands.trigger_source_ext)
            self.send_command(self.commands.trigger_count + str(npts+1))
            self.send_command(self.commands.calc_flux)
            self.send_command(self.commands.disable_timestamp)
            return True

        def configure_trig_encoder(
                self, encoder_pulses, direction, trigger_ref,
                integration_points, nturns):
            """Configures a measurement.

            Args:
                encoder_pulses (int): number of encoder pulses per measurement.
                direction (bool): integrator direction
                    (False is backwards, True is forwards).
                trigger_ref (int): nr triggers before start the measurements.
                integration_points (int): nr of integration points per
                    measurement.
                nturns (int): number of turns per measurement.
            """
            if not self.connected:
                return False
            
            trig_count = str(integration_points*nturns + 1)
            trig_interval = str(round(encoder_pulses/integration_points))
            pulses = str(int(encoder_pulses/4))

            self.send_command(self.commands.config_encoder + pulses + "'")
            self.send_command(self.commands.trigger_source_enc)
            self.send_command(self.commands.arm_ref + str(trigger_ref))

            if direction in (0, '-', 'BACK'):
                self.send_command(self.commands.trigger_dir + 'BACK')
            else:
                self.send_command(self.commands.trigger_dir + 'FOR')

            self.send_command(self.commands.trigger_count + trig_count)
            self.send_command(self.commands.trigger_ecount + trig_interval)
            self.send_command(self.commands.calc_flux)
            self.send_command(self.commands.disable_timestamp)
            return True

        def start_measurement(self):
            """Starts measurement."""
            if not self.connected:
                return False

            self.send_command(self.commands.stop + ';' + self.commands.start)
            return True

        def calibrate(self, wait=1):
            """Calibrates the integrator."""
            if not self.connected:
                return False

            self.send_command(self.commands.short_circuit_on)
            self.send_command(self.commands.calibrate)
            _time.sleep(wait)
            self.send_command(self.commands.short_circuit_off)
            return True

        def get_data(self):
            """Gets data from the integrator.

            Returns:
                ans (str): string containg the flux data."""
            data_count = str(self.get_data_count())
            self.send_command(self.commands.fetch_array + data_count + ', 12')
            return self.read_from_device().strip('\n')

        def get_data_array(self):
            """Gets data array from the integrator.

            Returns:
                ans (array): array containg the flux data."""
            try:
                data = self.get_data()
                data_split = data.replace(' WB', '').replace(' V', '').split(',')
                return _np.array(data_split, dtype=float)
            except Exception:
                return _np.array([])

        def get_data_count(self):
            """Gets number of flux data stored in the integrator.

            Returns:
                ans (int): number of flux data stored in the integrator."""
            self.send_command(self.commands.data_count)
            return int(self.read_from_device().strip('\n'))

        def shut_down(self):
            """Properly shuts the system down."""
            self.send_command(self.commands.power_off)

    return FDI2056


FDI2056Ethernet = FDI2056_factory(_utils.EthernetInterface)
