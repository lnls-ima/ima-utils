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

        def set_parameter(
                self, address, command, value,
                sleep=0.1, tol=0.001):
            cmd = str(address) + command + str(value)
            self.send_command(cmd)

            cmd = str(address) + command
            self.send_command(cmd)        

            _time.sleep(sleep)

            try:
                result = self.read_from_device()
                result = result.replace('\n', '')
                result = result.replace('\r', '')
                result = result.replace('*', '')
                result = result.replace(command, '')

                if abs(float(result) - value) <= tol:
                    return True
                else:
                    return False

            except Exception:
                return False

        def config_mode(self, address, mode, direction):
            # Adjust normal (0) or continuous mode (1)
            if mode == 0:
                cmd = str(address) + self.commands.normal_mode
            else:
                cmd = str(address) + self.commands.continuous_mode
            self.send_command(cmd)

            # Adjust clockwise (0) or counterclockwise direction (1)
            if direction == 0:
                cmd = str(address) + self.commands.direction + '+'
            else:
                cmd = str(address) + self.commands.direction + '-'
            self.send_command(cmd)

        def config_motor(
                self, address, mode, direction,
                resolution, speed, acceleration, steps):
            # set resolution
            cmd = str(address) + self.commands.resolution + str(resolution)
            self.send_command(cmd)

            # Enable end of course
            cmd = str(address) + self.commands.limit_switch_on
            self.send_command(cmd)

            # Configure Driver
            if mode == 0:
                cmd = str(address) + self.commands.normal_mode
            else:
                cmd = str(address) + self.commands.continuous_mode
            self.send_command(cmd)

            cmd = str(address) + self.commands.speed + str(speed)
            self.send_command(cmd)

            cmd = str(address) + self.commands.acceleration + str(
                acceleration)
            self.send_command(cmd)

            cmd = str(address) + self.commands.distance + str(steps)
            self.send_command(cmd)

            # Adjust clockwise (0) or counterclockwise direction (1)
            if direction == 0:
                cmd = str(address) + self.commands.direction + '+'
            else:
                cmd = str(address) + self.commands.direction + '-'
            self.send_command(cmd)

        def move_to_positive_limit(self, address, speed, acceleration):
            try:
                cmd = str(address) + self.commands.speed + str(speed)
                self.send_command(cmd)

                cmd = str(address) + self.commands.acceleration + str(
                    acceleration)
                self.send_command(cmd)

                cmd = str(address) + self.commands.continuous_mode
                self.send_command(cmd)

                cmd = str(address) + self.commands.direction + '+'
                self.send_command(cmd)

                self.move_motor(address)
                return True

            except Exception:
                return False

        def move_to_negative_limit(self, address, speed, acceleration):
            try:
                cmd = str(address) + self.commands.speed + str(speed)
                self.send_command(cmd)

                cmd = str(address) + self.commands.acceleration + str(
                    acceleration)
                self.send_command(cmd)

                cmd = str(address) + self.commands.continuous_mode
                self.send_command(cmd)

                cmd = str(address) + self.commands.direction + '-'
                self.send_command(cmd)

                self.move_motor(address)
                return True

            except Exception:
                return False

        def stop_motor(self, address):
            cmd = str(address) + self.commands.stop
            return self.send_command(cmd)

        def move_motor(self, address):
            cmd = str(address) + self.commands.move
            return self.send_command(cmd)

        def ready(self, address):
            cmd = str(address) + self.commands.status
            self.send_command(cmd)

            _time.sleep(0.01)

            try:
                result = self.read_from_device()
            except UnicodeDecodeError:
                return False

            if result.find('\r\r') >= 0:
                return True
            else:
                return False

        def limits(self, address):
            cmd = str(address) + self.commands.i_status
            self.send_command(cmd)

            _time.sleep(0.25)

            result = self.read_from_device()

            if result[10:12] == '11':
                return True
            else:
                return False

        def kill(self, address):
            cmd = str(address) + self.commands.kill
            return self.send_command(cmd)

        def set_resolution(self, address, resolution):
            cmd = str(address) + self.commands.resolution + str(
                int(resolution))
            return self.send_command(cmd)

    return ParkerDriver


ParkerDriverSerial = ParkerDriver_factory(_utils.SerialInterface)
