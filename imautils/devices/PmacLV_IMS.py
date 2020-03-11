"""PowerBrick LV-IMS control module."""

import sys as _sys
import time as _time
import traceback as _traceback
import paramiko as _paramiko


class PmacCommands(object):
    """Commands of Pmac motion controller."""

    def __init__(self):
        """Load commands."""
        self._jogging()
        self._run_programs_commands()
        self._misc()

    def _jogging(self):
        """Jogging commands.
        jog_pos          - Jog motor indefinitely in positive direction
        jog_neg          - Jog motor indefinitely in negative direction
        jog_stop         - Stop jog
        jog_abs_position - Jog to absolute position
        jog_rel_position - Jog to relative position"""

        self.jog_pos = 'j+'
        self.jog_neg = 'j-'
        self.jog_stop = 'j/'
        self.jog_abs_position = 'j='
        self.jog_rel_position = 'j^'

    def _run_programs_commands(self):
        """Run programs.
        rp_measurement_x - Run routine to measure field integrals on x axis
        rp_measurement_y - Run routine to measure field integrals on y axis"""

        self.rp_measurement_x = '&1b1r'
        self.rp_measurement_y = '&1b2r'

    def _misc(self):
        """Miscellaneous."""

        self.jog_axis = '&1cx'
        self.abort = 'abort'
        self.kill = 'k'
        self.homez = 'hmz'
        self.enplchome = 'enable plc MotorHome'
        self.enplcsync = 'enable plc testPLC'
        self.enplcreadback = 'enable plc readbackCS1'
        self.gatherdata = 'system gather -u /var/ftp/gather.data.txt'
        self.firstintmeas = '&1#1->50000X#2->50000Y#3->50000X#4->50000Y'
        self.secondintmeas = '&1#1->50000X#2->50000Y#3->-50000X#4->-50000Y'


class EthernetCom(object):
    """PowerBrick LV control and communication class."""

    def __init__(self):
        self.user = 'root'
        self.password = 'deltatau'

        # load commands
        self.commands = PmacCommands()

    def connect(self, ip, port=22):
        """Connect to the controller.

        Returns:
            True if operation completed successfully;
            False otherwise."""

        try:
            self.ssh = _paramiko.SSHClient()
            self.ssh.load_system_host_keys()
            self.ssh.set_missing_host_key_policy(_paramiko.AutoAddPolicy())
            self.ssh.connect(ip, port, self.user, self.password, timeout=3)
            _time.sleep(0.3)
            self.ppmac = self.ssh.invoke_shell(term='vt100')
            _time.sleep(0.3)
            self.ppmac.send('gpascii -2\r\n')
            _time.sleep(0.4)
            self.read()
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
            self.ppmac.close()
            self.ssh.close()
            return True
        except Exception:
            print(_traceback.print_exc(file=_sys.stdout))
            return False

    def read(self, var=None):
        """Reads response from the controller.

        Returns:
            _ans (str): answer from the controller."""

        try:
            _ans = self.ppmac.recv(2048).decode()
            if var is not None:
                _ans = _ans.replace('\r', '')
                _ans = _ans.replace('\n', '')
                _ans = _ans.replace('\x06', '')
                _ans = _ans.replace(var, '')
                _ans = _ans.replace('=', '')
                if 'Coord[1].Q[87]' in _ans:
                    _ans = _ans.replace('Coord[1].Q[87]', '')
                elif 'Coord[1].Q[88]' in _ans:
                    _ans = _ans.replace('Coord[1].Q[88]', '')
                _ans = float(_ans)
            return _ans
        except Exception:
            print(_traceback.print_exc(file=_sys.stdout))
            return '0000000'

    def write(self, msg):
        """Writes a string to the controller.

        Args:
            _msg (str): string to be written.

        Returns:
            True if operation completed successfully;
            False otherwise."""

        try:
            _msg = msg + '\r\n'
            self.ppmac.send(_msg)
            return True
        except Exception:
            print(_traceback.print_exc(file=_sys.stdout))
            return False

    def set_par(self, input_par, value):
        """Create string with the desired value.

        Args:
            input_par (str): input parameter;
            value (int): value to the parameter.

        Returns:
            parameter (str): if operation completed successfully;
            input_par otherwise."""

        try:
            parameter = input_par + '=' + str(value)
            return parameter
        except Exception:
            return input_par

    def set_motor(self, input_motor):
        """Create string for the desired motor.

        Args:
            input_motor (int).

        Returns:
            motor (str): if operation completed successfully;
            input_motor otherwise."""

        try:
            motor = '#' + str(input_motor)
            return motor
        except Exception:
            return input_motor

    def cfg_motor(self, motor, ac, spd):
        """Configures motor speed and acceleration.

        Args:
            motor (int): motor to be configured;
            ac (float): motor acceleration/deceleration value (ms);
            spd (float): motor speed value (counts/ms).

        Returns:
            True if operation completed successfully;
            False otherwise."""

        try:
            _par_ac = 'Motor[' + str(motor) + '].JogTa'
            _par_spd = 'Motor[' + str(motor) + '].JogSpeed'

            _msg = self.set_par(_par_ac, ac)
            self.write(_msg)
            self.read()
            _msg = self.set_par(_par_spd, spd)
            self.write(_msg)
            self.read()
            return True
        except Exception:
            print(_traceback.print_exc(file=_sys.stdout))
            return False

    def cfg_measurement(self, dist, ac, n_scans):
        """Sets measurement parameters and
        enables gather synchronism errors.

        Args:
            dist (float): measurement distance (mm);
            ac (float): motor acceleration/deceleration value (ms);
            n_scans (int): number of measurements.

        Returns:
            True if operation completed successfully;
            False otherwise."""

        try:
            _msg = self.set_par('distance', dist)
            self.write(_msg)
            self.read()
            _msg = self.set_par('acce', ac)
            self.write(_msg)
            self.read()
            _msg = self.set_par('n_scans', n_scans)
            self.write(_msg)
            self.read()
            _msg = self.commands.enplcsync
            self.write(_msg)
            self.read()
            _msg = self.commands.enplcreadback
            self.write(_msg)
            self.read()
            return True
        except Exception:
            print(_traceback.print_exc(file=_sys.stdout))
            return False

    def cfg_trigger_signal(self, init_pos, add_pos):
        """Configures external trigger signal.

        Args:
            init_pos (int): trigger initial position (mm).
            add_pos (int): trigger add position (mm).

        Returns:
            True if operation completed successfully;
            False otherwise."""

        init_pos = init_pos*50000
        add_pos = add_pos*50000
        try:
            _msg = self.set_par('ComparePos', init_pos)
            self.write(_msg)
            self.read()
            _time.sleep(0.03)
            _msg = self.set_par('CompAddDist', add_pos)
            self.write(_msg)
            self.read()
            return True
        except Exception:
            print(_traceback.print_exc(file=_sys.stdout))
            return False

    def relative_move(self, motor, r_position):
        """Executes a relative position movement.

        Args:
            motor (int): motor to be moved;
            r_position (float): relative position change (mm).

        Returns:
            True if operation completed successfully;
            False otherwise."""

        try:
            _r_positon = r_position * 50000
            _msg = self.set_motor(motor)
            _msg = _msg + self.commands.jog_stop
            self.write(_msg)
            self.read()
            _msg = self.set_motor(motor)
            _msg = _msg + self.commands.jog_rel_position
            _msg = _msg + str(_r_positon)
            self.write(_msg)
            self.read()
            return True
        except Exception:
            print(_traceback.print_exc(file=_sys.stdout))
            return False

    def absolute_move(self, motor, a_position):
        """Executes an absolute position movement.

        Args:
            motor (int): motor to be moved;
            a_position (float): absolute position (mm).

        Returns:
            True if operation completed successfully;
            False otherwise."""

        try:
            _a_position = a_position * 50000
            _msg = self.set_motor(motor)
            _msg = _msg + self.commands.jog_stop
            self.write(_msg)
            self.read()
            _msg = self.set_motor(motor)
            _msg = _msg + self.commands.jog_abs_position
            _msg = _msg + str(_a_position)
            self.write(_msg)
            self.read()
            return True
        except Exception:
            print(_traceback.print_exc(file=_sys.stdout))
            return False

    def axis_move(self, axis, a_position):
        """Executes an axis (two motors) absolute position movement.

        Args:
            axis (str): axis to be moved;
            a_position (float): absolute position (mm).

        Returns:
            True if operation completed successfully;
            False otherwise."""

        try:
            _msg = self.set_motor('1,2,3,4')
            _msg = _msg + self.commands.jog_stop
            self.write(_msg)
            self.read()
            self.cfg_measurement_type('First Integral')
            _msg = self.commands.jog_axis
            _msg = _msg + axis + str(a_position)
            self.write(_msg)
            self.read()
            return True
        except Exception:
            print(_traceback.print_exc(file=_sys.stdout))
            return False

    def run_motion_prog(self, meas, axis):
        """Run motion program.

        Args:
            meas (str): type of measurement (first/second integral);
            axis (str): measurement axis.

        Returns:
            True if operation completed successfully;
            False otherwise."""

        try:
            self.cfg_measurement_type(meas)
            if axis == 'X':
                _msg = self.commands.rp_measurement_x
            else:
                _msg = self.commands.rp_measurement_y
            self.write(_msg)
            self.read()
            return True
        except Exception:
            print(_traceback.print_exc(file=_sys.stdout))
            return False

    def cfg_measurement_type(self, meas):
        """Configures coordinate system according
        to the measurement type.

        Args:
            meas (str): type of measurement (first/second integral).

        Returns:
            True if operation completed successfully;
            False otherwise."""

        try:
            if meas == 'First Integral':
                _msg = self.commands.firstintmeas
            else:
                _msg = self.commands.secondintmeas

            self.write(_msg)
            self.read()
            return True
        except Exception:
            print(_traceback.print_exc(file=_sys.stdout))
            return False

    def get_value(self, par):
        """Returns the value of some variable.

        Args:
            par (str): parameter to get the value.

        Returns:
            _ans (str) if operation completed successfully;
            0 otherwise."""

        try:
            self.write(par)
            _ans = self.read(par)
            return _ans
        except Exception:
            print(_traceback.print_exc(file=_sys.stdout))
            return 0

    def read_encoder(self, motor):
        """Reads motor's position in mm.

        Args:
            motor (int): selected motor.

        Returns:
            _position (str) if operation completed successfully;
            0 otherwise."""

        try:
            _msg = 'Motor[' + str(motor) + '].ActPos'
            _pos = self.get_value(_msg)
            _msg = 'Motor[' + str(motor) + '].HomePos'
            _homepos = self.get_value(_msg)
            _position = (_pos - _homepos)/50000
            _position = round(_position, 12)
            return _position
        except Exception:
            print(_traceback.print_exc(file=_sys.stdout))
            return '00000000'

    def home(self, motor):
        """Executes a home move.

        Args:
            motor (int): selected motor.

        Returns:
            True if operation completed successfully;
            False otherwise."""

        try:
            _msg = self.commands.enplchome
            _msg = _msg + str(motor)
            self.write(_msg)
            self.read()
            return True
        except Exception:
            print(_traceback.print_exc(file=_sys.stdout))
            return False

    def stop_motor(self, motor):
        """Stops the motor.

        Args:
            motor (int): motor to be stopped.

        Returns:
            True if operation completed successfully;
            False otherwise."""

        try:
            _msg = self.set_motor(motor)
            _msg = _msg + self.commands.jog_stop
            self.write(_msg)
            self.read()
            return True
        except Exception:
            print(_traceback.print_exc(file=_sys.stdout))
            return False

    def abort_motion_prog(self):
        """Stops motion program.

        Returns:
            True if operation completed successfully;
            False otherwise."""

        try:
            _msg = self.commands.abort
            self.write(_msg)
            self.read()
            return True
        except Exception:
            print(_traceback.print_exc(file=_sys.stdout))
            return False

    def in_position(self, motor):
        """Checks the motor's movement.

        Args:
            motor (int): motor to check the movement.

        Returns:
            1 if the motor is stopped.
            0 otherwise."""

        try:
            _msg = 'Motor[' + str(motor) + '].InPos'
            _in_pos = self.get_value(_msg)
            return _in_pos
        except Exception:
            print(_traceback.print_exc(file=_sys.stdout))
            return False

    def get_position(self, axis):
        """Get axis position.

        Args:
            axis (str): X or Y.

        Returns:
            axis position.
            False otherwise."""

        try:
            _msg = axis + '_POS'
            _pos = self.get_value(_msg)
            return _pos
        except Exception:
            print(_traceback.print_exc(file=_sys.stdout))
            return False

    def homez(self, motor):
        """Program zero-move homing.

        Args:
            motor (int): motor to perform zero-move homing.

        Returns:
            True if operation completed successfully;
            False otherwise."""

        try:
            _msg = self.set_motor(motor)
            _msg = _msg + self.commands.homez
            self.write(_msg)
            self.read()
            return True
        except Exception:
            print(_traceback.print_exc(file=_sys.stdout))
            return False

    def kill(self, motor):
        """Kill motor output.

        Args:
            motor (int): motor to kill output.

        Returns:
            True if operation completed successfully;
            False otherwise."""

        try:
            _msg = self.set_motor(motor)
            _msg = _msg + self.commands.kill
            self.write(_msg)
            self.read()
            return True
        except Exception:
            print(_traceback.print_exc(file=_sys.stdout))
            return False
