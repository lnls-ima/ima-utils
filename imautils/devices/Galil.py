"""Galil control module.

Created on 15 de out de 2019

@author: VitorPS
"""

import sys as _sys
import traceback as _traceback
import socket as _socket
import numpy as _np


class Galil():
    """Galil control and communictations class."""

    def __init__(self):
        self.motors = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H']

    def connect(self, ip, port=23):
        """Connect to the controller.

        Returns:
            True if operation completed successfully;
            False otherwise."""
        try:
            self.socket = _socket.create_connection((ip, port),
                                                    timeout=1)
            return True
        except Exception:
            print(_traceback.print_exc(file=_sys.stdout))
            return False

    def disconnect(self):
        """Disconnect the controller.

        Returns:
            True if operation completed successfully;
            False otherwise."""

        try:
            self.socket.close()
            return True
        except Exception:
            print(_traceback.print_exc(file=_sys.stdout))
            return False

    def read(self):
        """Reads response from galil.

        Returns:
            ans (str): answer from the controller."""

        try:
            ans = self.socket.recv(1024).decode()
            return ans
        except Exception:
            print(_traceback.print_exc(file=_sys.stdout))
            return ''

    def write(self, msg):
        """Writes a string to the controller.

        Args:
            msg (str): string to be written.
        Returns:
            True if operation completed successfully;
            False otherwise."""

        try:
            msg = msg + '\r\n'
            self.socket.send(msg.encode())
            return True

        except Exception:
            print(_traceback.print_exc(file=_sys.stdout))
            return False

    def configure_controller(self, cn=[1, -1, -1, 1, 1]):
        """Configures controller parameters.

        Args:
            cn (list): list of configuration (cn[0]: 1 for limit switch active
                                                     high;
                                                     -1 for active low;
                                              cn[1]: 1 for homing forward;
                                                     -1 for homing backward;
                                              cn[2]: 1 for latch input active
                                                     high;
                                                     -1 for active low;
                                              cn[3]: 1 for abort inputs;
                                                     0 for general purpose
                                                     inputs;
                                              cn[4]: 1 abort input stops exec;
                                                     0 does not abort.)

        Returns:
            True if operation completed successfully;
            False otherwise."""

        try:
            msg = 'CN'
            for i in cn:
                msg = msg + str(i)

            if self.write(msg):
                if self.read()[-1] == ':':
                    return True
            return False

        except Exception:
            print(_traceback.print_exc(file=_sys.stdout))
            return False

    def configure_stepper(self, motor=0, mtype=-2, spd=0, ac=0, steps=200,
                          usteps=64, ld=3):
        """Configures stepper motors on the controller.

        Args:
            motor (int): motor to be configured selection (0 to 7);
            mtype (float): motor type (-2 for stepper, -2.5 for reverse
                           direction stepper;
            spd (int): motor speed value (counts/s);
            ac (int): motor acceleration/deceleration value (counts/s^2);
            steps (int): motor steps/rev;
            usteps (int): list of motor step division (1, 2, 4, 16 or 64
                          usteps/step).
            ld (int): 0 for both limit switches enabled;
                      1 for forward limit disabled;
                      2 for backward limit disabled;
                      3 for both limits disabled.

        Returns:
            True if operation completed successfully;
            False otherwise."""

        try:
            msg = ''
            motor = self.motors[motor]
            # MT configures motor type
            msg = msg + 'MT' + motor + '=' + str(mtype) + '; '
            # SP configures velocity
            msg = msg + 'SP' + motor + '=' + str(spd) + '; '
            # AC/DC configures acceleration/deceleration
            msg = msg + 'AC' + motor + '=' + str(ac) + '; '
            msg = msg + 'DC' + motor + '=' + str(ac) + '; '
            # YB configures step
            msg = msg + 'YB' + motor + '=' + str(steps) + '; '
            # YA configures ustep
            msg = msg + 'YA' + motor + '=' + str(usteps) + '; '
            # LD limit switch disalbe
            msg = msg + 'LD' + motor + '=' + str(ld) + '; '

            if self.write(msg):
                if self.read()[-1] == ':':
                    return True
            return False

        except Exception:
            print(_traceback.print_exc(file=_sys.stdout))
            return False

    def jog(self, motor=0, spd=0, pos_direction=True):
        """Configure jog on the selected motor(s) on positive direction.

        Args:
            motor (int): motor selection (0 to 7);
            spd (int): motor velocity during jog in counts/s;
            pos_direction (bool): selects positive (True) or negative (False)
                                  direction.

        Returns:
            True if operation completed successfully;
            False otherwise."""

        try:
            if not pos_direction:
                spd = -1*spd

            msg = 'JG'
            motor = self.motors[motor]
            msg = msg + motor + '=' + str(spd)
            if self.write(msg):
                if self.read()[-1] == ':':
                    return True
            return False

        except Exception:
            print(_traceback.print_exc(file=_sys.stdout))
            return False

    def cfg_relative_move(self, motor=0, r_position=0):
        """Configures relative movement on the selected motors.

        Args:
            motor (int): motor selection (0 to 7);
            r_postion (int): set relative position change (in counts).

        Returns:
            True if operation completed successfully;
            False otherwise."""

        try:
            msg = 'PR'
            motor = self.motors[motor]
            msg = msg + motor + '=' + str(r_position)

            if self.write(msg):
                if self.read()[-1] == ':':
                    return True
            return False

        except Exception:
            print(_traceback.print_exc(file=_sys.stdout))
            return False

    def cfg_absolute_move(self, motor=0, a_position=0):
        """Configures relative movement on the selected motors.

        Args:
            motor (int): motor selection (0 to 7);
            r_postion (int): set absolute position (in counts).

        Returns:
            True if operation completed successfully;
            False otherwise."""

        try:
            msg = 'PA'
            msg = msg + motor + '=' + str(a_position)

            if self.write(msg):
                if self.read()[-1] == ':':
                    return True
            return False

        except Exception:
            print(_traceback.print_exc(file=_sys.stdout))
            return False

    def enable_motor(self, motor=0):
        """Turns on the selected motor.

        Args:
            motor (int): motor selection from 0 to 7.

        Returns:
            True if operation completed successfully;
            False otherwise."""

        try:
            msg = 'SH'
            motor = self.motors[motor]
            msg = msg + motor

            if self.write(msg):
                if self.read()[-1] == ':':
                    return True
            return False

        except Exception:
            print(_traceback.print_exc(file=_sys.stdout))
            return False

    def disable_motor(self, motor=0):
        """Turns off the selected motors.

        Args:
            motors (list): list of motor selection (0 is not selected).

        Returns:
            True if operation completed successfully;
            False otherwise."""

        try:
            msg = 'MO'
            motor = self.motors[motor]
            msg = msg + motor

            if self.write(msg):
                if self.read()[-1] == ':':
                    return True
            return False

        except Exception:
            print(_traceback.print_exc(file=_sys.stdout))
            return False

    def stop(self, motor=0):
        """Stops selected motor(s) movement.

        Args:
            motors (int): motor selection from 0 to 7. Stops all motors this
                          parameter is set to -1.

        Returns:
            True if operation completed successfully;
            False otherwise."""

        try:
            msg = 'ST'
            if motor is not -1:
                motor = self.motors[motor]
            else:
                motor = ''

            msg = msg + motor

            if self.write(msg):
                if self.read()[-1] == ':':
                    return True
            return False

        except Exception:
            print(_traceback.print_exc(file=_sys.stdout))
            return False

    def begin_motion(self, motors=[0, 0, 0, 0, 0, 0, 0, 0], wait=True):
        """Begins motion and waits until motion is complete.

        Args:
            motors (list): list with 8 elements of motor selection
                           (0 is not selected);
            wait (bool): whether the controller waits for the motion to
                complete before executing the next commands.

        Returns:
            True if operation completed successfully;
            False otherwise."""

        try:
            msg0 = 'BG '
            msg1 = 'AM '
            for i in range(8):
                if motors[i]:
                    msg0 = msg0 + self.motors[i]
                    msg1 = msg1 + self.motors[i]

            if self.write(msg0):
                if self.read()[-1] == ':':
                    if wait:
                        if self.write(msg1):
                            if self.read()[-1] == ':':
                                return True
                    else:
                        return True
            return False

        except Exception:
            print(_traceback.print_exc(file=_sys.stdout))
            return False

    def read_positions(self):
        """Reads all the motors positions.

        Returns:
            numpy array containing all the postions (NaN if reading was
            unsucessful)."""

        try:
            pos = _np.zeros(8)
            pos[:] = _np.nan
            msg = 'RP '
            for i in range(8):
                msg = msg + self.motors[i]

            if not self.write(msg):
                self.read()  # clears buffer
                if not self.write(msg):
                    self.read()  # clears buffer
                    raise RuntimeError('Error during write operation.')

            ans = self.read()
            if ans[-1] is '?':
                self.write(msg)
                ans = self.read()
            if ans[-1] is not ':':
                raise RuntimeError('Error during write operation.')

            ans = ans.replace('\r\n:', '').split(',')
            for i in range(8):
                pos[i] = float(ans[i])
            return pos

        except Exception:
            print(_traceback.print_exc(file=_sys.stdout))
            return pos

    def set_position(self, motor=0, position=0):
        """Sets new values to the current positions.

        Args:
            motor (int): motor selection from 0 to 7.
            position (int): new position value.

        Returns:
            True if operation completed successfully;
            False otherwise."""

        msg = 'DP'
        msg = msg + self.motors[motor] + '=' + str(position)

        if self.write(msg):
            if self.read()[-1] == ':':
                return True
        return False

    def read_status(self, motors=[0, 0, 0, 0, 0, 0, 0, 0]):
        """Reads status from the selected motors.

        Args:
            motors (list): list of motor selection (0 is not selected).

        Returns:
        """
        pass
