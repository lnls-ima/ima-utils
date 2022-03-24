# -*- coding: utf-8 -*-
"""
OEM750X Parker Driver

Created on 28/08/2012
@author: James Citadini
"""

import time as _time

from . import utils as _utils


class ParkerDriverCommands(object):
    """Commands of Parker Driver."""

    def __init__(self):
        """Load commands."""
        # {Command to move motor}
        self.move = 'G'
        # {Command to stop motor}
        self.stop = 'S'
        # {Command to set distance}
        self.distance = 'D'
        # {motor speed}
        self.speed = 'V'
        # {motor acceleration}
        self.acceleration = 'A'
        # {motor status}
        self.status = 'CR'
        # {motor mode continuous}
        self.continuous_mode = 'MC'
        # {motor mode normal}
        self.normal_mode = 'MN'
        # {motor direction}
        self.direction = 'H'
        # {disable limit switch}
        self.limit_switch_off = 'LD3'
        # {enable limit switch}
        self.limit_switch_on = 'LD0'
        # {emergency stop}
        self.kill = 'K'
        # {set motor resolution}
        self.resolution = 'MR'
        # {read input status}
        self.i_status = 'IS'
        # {set output 1 high}
        self.output_1_high = 'O1X'
        # {set output 2 high}
        self.output_2_high = 'OX1'
        # {set output 1 low}
        self.output_1_low = 'O0X'
        # {set output 2 low}
        self.output_2_low = 'OX0'
        # {set absolute position to zero}
        self.zero_abs_position = 'PZ'


def ParkerDriver_factory(baseclass):
    """Create Parker Driver class."""
    class ParkerDriver(baseclass):
        """Parker Driver."""

        def __init__(self, log=False):
            """Initiaze variables and prepare log.

            Args:
                log (bool): True to use event logging, False otherwise.
            """
            self.commands = ParkerDriverCommands()
            super().__init__(log=log)

        def config_mode(self, address, mode, direction, wait=0.01):
            if not self.connected:
                return False

            # Adjust normal (0) or continuous mode (1)
            if mode == 0:
                cmd = str(address) + self.commands.normal_mode
            else:
                cmd = str(address) + self.commands.continuous_mode
            self.send_command(cmd)

            # Adjust clockwise (+) or counterclockwise direction (-)
            cmd = str(address) + self.commands.direction + direction
            self.send_command(cmd)

            return True

        def config_motor(
                self, address, mode, direction,
                resolution, speed, acceleration,
                steps, wait=0.01):
            if not self.connected:
                return False

            # Set resolution
            cmd = str(address) + self.commands.resolution + str(resolution)
            self.send_command(cmd)
            _time.sleep(wait)

            # Enable limit switch
            cmd = str(address) + self.commands.limit_switch_on
            self.send_command(cmd)
            _time.sleep(wait)

            # Adjust normal (0) or continuous mode (1)
            if mode == 0:
                cmd = str(address) + self.commands.normal_mode
            else:
                cmd = str(address) + self.commands.continuous_mode
            self.send_command(cmd)
            _time.sleep(wait)

            # Configure speed
            cmd = str(address) + self.commands.speed + str(speed)
            self.send_command(cmd)
            _time.sleep(wait)

            # Configure aceleration
            cmd = str(address) + self.commands.acceleration + str(acceleration)
            self.send_command(cmd)
            _time.sleep(wait)

            # Configure number of steps
            cmd = str(address) + self.commands.distance + str(int(steps))
            self.send_command(cmd)
            _time.sleep(wait)

            # Adjust clockwise (+) or counterclockwise direction (-)
            cmd = str(address) + self.commands.direction + direction
            self.send_command(cmd)
            _time.sleep(wait)

            return True

        def move_to_positive_limit(
                self, address, speed, acceleration, wait=0.01):
            if not self.connected:
                return False

            try:
                cmd = str(address) + self.commands.speed + str(speed)
                self.send_command(cmd)
                _time.sleep(wait)
                
                cmd = str(address) + self.commands.acceleration + str(acceleration)
                self.send_command(cmd)
                _time.sleep(wait)

                cmd = str(address) + self.commands.continuous_mode
                self.send_command(cmd)
                _time.sleep(wait)

                cmd = str(address) + self.commands.direction + '+'
                self.send_command(cmd)
                _time.sleep(wait)

                self.move_motor(address)
                return True

            except Exception:
                return False

        def move_to_negative_limit(
                self, address, speed, acceleration, wait=0.01):
            if not self.connected:
                return False

            try:
                cmd = str(address) + self.commands.speed + str(speed)
                self.send_command(cmd)
                _time.sleep(wait)
                
                cmd = str(address) + self.commands.acceleration + str(acceleration)
                self.send_command(cmd)
                _time.sleep(wait)

                cmd = str(address) + self.commands.continuous_mode
                self.send_command(cmd)
                _time.sleep(wait)

                cmd = str(address) + self.commands.direction + '-'
                self.send_command(cmd)
                _time.sleep(wait)

                self.move_motor(address)
                return True

            except Exception:
                return False

        def stop_motor(self, address):
            if not self.connected:
                return False

            cmd = str(address) + self.commands.stop
            return self.send_command(cmd)

        def move_motor(self, address):
            if not self.connected:
                return False

            cmd = str(address) + self.commands.move
            return self.send_command(cmd)

        def ready(self, address, wait=0.01):
            if not self.connected:
                return False

            cmd = str(address) + self.commands.status
            self.send_command(cmd)

            _time.sleep(wait)

            try:
                result = self.read_from_device()
            except UnicodeDecodeError:
                return False

            if result.find('\r\r') >= 0:
                return True
            else:
                return False

        def limits(self, address, wait=0.25):
            if not self.connected:
                return False

            cmd = str(address) + self.commands.i_status
            self.send_command(cmd)
            _time.sleep(wait)

            result = self.read_from_device()

            if result[10:12] == '11':
                return True
            else:
                return False

        def kill(self, address):
            cmd = str(address) + self.commands.kill
            return self.send_command(cmd)

        def set_output_1_high(self, address):
            if not self.connected:
                return False

            cmd = str(address) + self.commands.output_1_high
            return self.send_command(cmd)

        def set_output_2_high(self, address):
            if not self.connected:
                return False

            cmd = str(address) + self.commands.output_2_high
            return self.send_command(cmd)

        def set_output_1_low(self, address):
            if not self.connected:
                return False

            cmd = str(address) + self.commands.output_1_low
            return self.send_command(cmd)

        def set_output_2_low(self, address):
            if not self.connected:
                return False

            cmd = str(address) + self.commands.output_2_low
            return self.send_command(cmd)

        def all_input_status(self, address, wait=0.25):
            """ Read the status of all driver inputs.
                Input arguments:
                    address: the driver address
                    wait: period to wait after command is sent
                Output:
                    status: array of boolean representing each input
                            or None
                        idx 0: trigger bit 1
                        idx 1: triggerr bit 2
                        idx 2: trigger bit 3
                        idx 3: home enable
                        idx 4: FLT
                        idx 5: CCW limit
                        idx 6: CW limit
                        idx 7: Sequence Select 1
                        idx 8: Sequence Select 2
                        idx 9: Sequence Select 3 """
            if not self.connected:
                return None

            cmd = str(address) + self.commands.i_status
            self.send_command(cmd)
            _time.sleep(wait)

            result = self.read_from_device()

            result = result.replace('1IS','').replace('2IS','')
            result = result.replace('3IS','').replace('4IS','')
            result = result.replace('5IS','').replace('6IS','')
            result = result.replace('7IS','').replace('8IS','')
            result = result.replace('*','')
            result = result.replace('\r','').replace('\n','')

            status_list = [c == '1' for c in result]

            return status_list

        def zero_absolute_position(self, address):
            """ Set absolute position counter to zero.
                Input arguments:
                    address: the driver address
                Output:
                    status: True on success
                            False if failed """
            if not self.connected:
                return False

            cmd = str(address) + self.commands.zero_abs_position
            return self.send_command(cmd)

    return ParkerDriver


ParkerDriverSerial = ParkerDriver_factory(_utils.SerialInterface)
