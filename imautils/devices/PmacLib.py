# -*- coding: utf-8 -*-
"""Pmac Lib."""

import win32com.client as _client
import win32com.shell.shell as _shell
import logging as _logging


class PmacCommands():
    """Commands of Pmac motion controller."""

    def __init__(self):
        """Load commands."""
        self._axis()
        self._constants()
        self._mvariables()
        self._qvariables()
        self._qvariables_trig()
        self._pvariables()
        self._ivariables()
        self._jogging()
        self._motor_reporting()
        self._run_programs_commands()
        self._misc()

    def _axis(self):
        self.list_of_axis = [1, 2, 3, 5, 6, 7, 8, 9]
        self.stop_all_axis = chr(1)
        self.kill_all_axis = chr(11)

    def _constants(self):
        """List of constants to convert counts to mm."""
        self.cts_mm_axis = [20000,
                            100000,
                            100000,
                            0,
                            8192,
                            400,
                            400,
                            400,
                            400]

    def _mvariables(self):
        """M-variables.

        di_estop_ok           - State of E-stop relays; 1 = OK, 0 = OFF
        di_input_pressure_ok  - Monitoring input pressure; 0 = fault, 1 = OK
        di_vacuum_ok          - Monitoring vac; 0 = fault, 1 = OK
        prog_running          - Check if there is a programming running
        """
        self.di_estop_ok = 'M7000'
        self.di_input_pressure_ok = 'M7004'
        self.di_vacuum_ok = 'M7012'
        self.prog_running = 'M5180'

    def _qvariables(self):
        """Q-variables.

        q_zaxis_manual_mode - Manual move mode for Z-axis is ON/0 = Normal mode
        q_motor_mask        - Bit mask to select the motors
        q_plc5_status       - Status of PLC 5
        q_plc10_status      - Status of PLC 10
        """
        self.q_zaxis_manual_mode = 'Q90'
        self.q_motor_mask = 'Q95'
        self.q_plc5_status = 'Q5500'
        self.q_plc10_status = 'Q6000'

    def _qvariables_trig(self):
        """Trigger Q-varibles.

        q_selected_motor = 'Q0'      - [1,2,3,5] trigger source, motor number
        q_incremment = 'Q1'          - [mm or deg] trigger pitch
                                       (negative numbers also possible)
        q_loop_count = 'Q2'          - [1] trigger counter
        q_plc0_status = 'Q3'         - Status of plc0
        q_plc0_run_control = 'Q9'    - [1] for starting stopping of plc0
        q_use_prog_start_pos = 'Q10' - [1/0] to use flexible start position
        q_start_pos = 'Q11'          - [mm or deg] position of first pulse
                                       if flexible start position is used
        q_pulse_width_perc = 'Q12'   - [0..100%] pulse width in %
                                       (limited to min 10% and max 75%)
        q_max_pulses = 'Q13'         - [0..] max number of pulses
                                       (0 for no limitation)
        q_falling_edge = 'Q14'       - [0/1] trigger edge
                                       1 = falling edge, 0 = rising edge
        """
        self.q_selected_motor = 'Q0'
        self.q_incremment = 'Q1'
        self.q_loop_count = 'Q2'
        self.q_plc0_status = 'Q3'
        self.q_plc0_run_control = 'Q9'
        self.q_use_prog_start_pos = 'Q10'
        self.q_start_pos = 'Q11'
        self.q_pulse_width_perc = 'Q12'
        self.q_max_pulses = 'Q13'
        self.q_falling_edge = 'Q14'

    def _pvariables(self):
        """P-variables.

        p_axis_mask - Bit mask to select the motors to be homed - b1200r
        p_homing_status - Homing status
        """
        self.p_axis_mask = 'P810'
        self.p_homing_status = 'P813'

    def _ivariables(self):
        """I-Variables.

        i_pos_scale_factor    - Ixx08 Motor xx Position Scale Factor
        i_soft_limit_pos_list - List of positive software position limit
                               [motor counts] - Ixx13
        i_soft_limit_neg_list - List of negative software position limit
                               [motor counts] - Ixx14
        i_axis_speed          - List of all axis speed - Ixx22 in counts/msec
        """
        self.i_pos_scale_factor = ['I'+str(i)+'08' for i in range(1, 10)]
        self.i_soft_limit_pos_list = ['I'+str(i)+'13' for i in range(1, 10)]
        self.i_soft_limit_neg_list = ['I'+str(i)+'14' for i in range(1, 10)]
        self.i_axis_speed = ['I'+str(i)+'22' for i in range(1, 10)]

    def _jogging(self):
        """Jogging commands.

        jog_pos          - Jog motor indefinitely in positive direction
        jog_neg          - Jog motor indefinitely in negative direction
        jog_stop         - Stop jog
        jog_abs_position - Jog to absolute position
        jog_rel_position - Jog to relative position
        """
        self.jog_pos = 'j+'
        self.jog_neg = 'j-'
        self.jog_stop = 'j/'
        self.jog_abs_position = 'j='
        self.jog_rel_position = 'j:'

    def _motor_reporting(self):
        """Motor reporting commands.

        current_position - Report position of motor in counts
        current_velocity - Report velocity of motor
        """
        self.current_position = 'p'
        self.current_velocity = 'v'

    def _run_programs_commands(self):
        """Run programs.

        align_axis - Run routine for alignment of actived axis
        """
        self.rp_align_axis = 'b1200r'

    def _misc(self):
        """Miscellaneous.

        enplc5   - Enable (run) PLC5
        enplc10  - Enable (run) PLC10
        displc5  - Disable (stop) PLC5
        displc10 - Disable (stop) PLC10
        enaplc2  - Enable (run) PLC2
        """
        self.enplc5 = 'enaplc5'
        self.enplc10 = 'enaplc10'
        self.displc5 = 'displc5'
        self.displc10 = 'displc10'
        self.enaplc2 = 'enaplc2'
        self.axis_status = '?'


class Pmac():
    """Implementation of the main commands to control the bench."""

    def __init__(self, log=False):
        """Initiaze all function variables."""
        self.commands = PmacCommands()
        self._value = ''
        self._com_obj = None
        self._dev_number = 0
        self._connected = False
        self.log = log
        self.logger = None
        self.log_events()

    @property
    def connected(self):
        """Return True if the device is connected, False otherwise."""
        return self._connected

    @property
    def dev_number(self):
        """Return the selected device number."""
        return self._dev_number

    def log_events(self):
        """Prepare logging file to save info, warning and error status."""
        if self.log:
            self.logger = _logging.getLogger()
            self.logger.setLevel(_logging.DEBUG)

    def create_com_object(self):
        """Create PcommServer.PmacDevice object.

        Returns:
            True if successful, False otherwise.

        """
        try:
            if not _shell.IsUserAnAdmin():
                if self.logger is not None:
                    self.logger.error(
                        'Fail to connect pmac: user is not a admin.')
                return False

            self._com_obj = _client.Dispatch('PcommServer.PmacDevice')
            return True
        except Exception:
            if self.logger is not None:
                self.logger.error('exception', exc_info=True)
            return False

    def select_device(self):
        """Select Pmac device number.

        Returns:
            True if successful, False otherwise.

        """
        if self._com_obj is None:
            return False

        self._dev_number, status = self._com_obj.SelectDevice(0)
        return status

    def connect(self):
        """Connect to Pmac device - Open(DeviceNumber).

        Returns:
            True if successful, False otherwise.

        """
        if self._com_obj is None:
            if not self.create_com_object():
                return False

        try:
            status = bool(self._com_obj.Open(self._dev_number))
            self._connected = status
            return status
        except Exception:
            if self.logger is not None:
                self.logger.error('exception', exc_info=True)
            return False

    def disconnect(self):
        """Disconnect Pmac device - Close(DeviceNumber).

        Returns:
            True if successful, False otherwise.

        """
        if self._com_obj is None:
            self._connected = False
            return True

        try:
            self._com_obj.Close(self._dev_number)
            self._com_obj = None
            self._connected = False
            return True
        except Exception:
            if self.logger is not None:
                self.logger.error('exception', exc_info=True)
            return None

    def set_par(self, input_par, value):
        """Create string with the desired value.

        Args:
            input_par (str): input parameter;
            value (int): value to the parameter.

        Returns:
            parameter (str): if operation completed successfully;
            input_par otherwise.

        """
        try:
            _parameter = input_par + '=' + str(value)
            return _parameter
        except Exception:
            if self.logger is not None:
                self.logger.error('exception', exc_info=True)
            return input_par

    def get_response(self, str_command):
        """Get response of the string command from Pmac device - GetResponseEx.

        Returns:
            True if successful, False otherwise.

        """
        try:
            if self._com_obj is None:
                self._value = ''
                return None

            MASK_STATUS = 0xF0000000
            COMM_EOT = 0x80000000
            # An acknowledge character (ACK ASCII 9) was received
            # indicating end of transmission from PMAC to Host PC.

            # send command and get pmac response
            response, retval = self._com_obj.GetResponseEx(
                self._dev_number,
                str_command.encode('utf-8'),
                False)

            # check the status and if it matches with the
            # acknowledge character COMM_EOT
            if retval & MASK_STATUS == COMM_EOT:
                result = response.encode('utf-8').decode('utf-8')
                self._value = result[0:result.find('\r')]
                return True

            else:
                self._value = ''
                return False
        except Exception:
            if self.logger is not None:
                self.logger.error('exception', exc_info=True)
            self._value = ''
            return None

    def read_response(self, str_command):
        """Get response of a variable.

        Returns:
            the pmac response (str).

        """
        try:
            if self.get_response(str_command):
                return self._value
            else:
                return ''
        except Exception:
            if self.logger is not None:
                self.logger.error('exception', exc_info=True)
            return None

    def activate_bench(self):
        """Activate the bench.

        Set the mask value to 503 in q95 and enable plcs 5 and 10.

        Returns:
            True if successful, False otherwise.

        """
        try:
            _cmd = self.set_par(self.commands.q_motor_mask, 503)
            if self.get_response(_cmd):
                if self.get_response(self.commands.enplc5):
                    if self.get_response(self.commands.enplc10):
                        return True
            return False
        except Exception:
            if self.logger is not None:
                self.logger.error('exception', exc_info=True)
            return None

    def axis_status(self, axis):
        """Get axis status.

        Args:
            axis (int): selected axis.

        Returns:
            the axis status (int).

        """
        try:
            _cmd = '#' + str(axis) + self.commands.axis_status
            if self.get_response(_cmd):
                status = int(self._value, 16)
                return status
            return None
        except Exception:
            if self.logger is not None:
                self.logger.error('exception', exc_info=True)
            return None

    def axis_homing_status(self, axis):
        """Get axis homing status.

        Args:
            axis (int): selected axis.

        Returns:
            the axis homing status (int).

        """
        try:
            status = self.axis_status(axis)
            if status is None:
                return None

            return status & 1024 != 0
        except Exception:
            if self.logger is not None:
                self.logger.error('exception', exc_info=True)
            return None

    def align_bench(self, axis_mask):
        """Set the mask of the axis to be aligned and run plc script.

        Args:
            axis_mask (int): mask of the axis to be aligned.

        Returns:
            True if successful, False otherwise.

        """
        try:
            _cmd = self.set_par(self.commands.p_axis_mask, axis_mask)
            if self.get_response(_cmd):
                if self.get_response(self.commands.p_axis_mask):
                    if int(self._value) == axis_mask:
                        if self.get_response(self.commands.rp_align_axis):
                            return True
                        else:
                            if self.logger is not None:
                                self.logger.warning('Fail to set P_axis_mask')
                    else:
                        if self.logger is not None:
                            self.logger.warning('Fail to set P_axis_mask')
            return False
        except Exception:
            if self.logger is not None:
                self.logger.error('exception', exc_info=True)
            return None

    def get_position(self, axis):
        """Read the current position in counter and convert to mm or deg.

        Args:
            axis (int): selected axis.

        Returns:
            the axis position (float).

        """
        try:
            _cmd = '#' + str(axis) + self.commands.current_position
            if self.get_response(_cmd):
                _pos = float(self._value) / self.commands.cts_mm_axis[axis-1]
                return _pos
            else:
                return None
        except Exception:
            if self.logger is not None:
                self.logger.error('exception', exc_info=True)
            return None

    def set_position(self, axis, value):
        """Move axis to defined position.

        Args:
            axis (int): selected axis,
            value (float): desired axis position [mm or deg].

        Returns:
            True if successful, False otherwise.

        """
        try:
            adj_value = value * self.commands.cts_mm_axis[axis-1]
            _cmd = '#' + str(axis) + self.set_par(
                self.commands.jog_abs_position, adj_value)
            if self.get_response(_cmd):
                return True
            return False
        except Exception:
            if self.logger is not None:
                self.logger.error('exception', exc_info=True)
            return None

    def get_velocity(self, axis):
        """Read the current velocity in mm/s or deg/s.

        Args:
            axis (int): selected axis.

        Returns:
            the axis velocity (float).

        """
        try:
            _cmd = self.commands.i_axis_speed[axis-1]
            if self.get_response(_cmd):
                _vel = float(
                    self._value)/self.commands.cts_mm_axis[axis-1]*1000
                return _vel
            else:
                return None
        except Exception:
            if self.logger is not None:
                self.logger.error('exception', exc_info=True)
            return None

    def set_velocity(self, axis, value):
        """Set the axis velocity.

        Args:
            axis (int): selected axis,
            value (float): desired axis velocity [mm/s or deg/s].

        Returns:
            True if successful, False otherwise.

        """
        try:
            # convert value from mm/sec to cts/msec
            adj_value = value * self.commands.cts_mm_axis[axis-1] / 1000

            # set speed
            _cmd = self.set_par(self.commands.i_axis_speed[axis-1], adj_value)
            if self.get_response(_cmd):
                if self._value != adj_value:
                    return True
            return False
        except Exception:
            if self.logger is not None:
                self.logger.error('exception', exc_info=True)
            return None

    def stop_axis(self, axis):
        """Stop axis.

        Args:
            axis (int): selected axis,

        Returns:
            True if successful, False otherwise.

        """
        try:
            if self.get_response('#' + str(axis) + self.commands.jog_stop):
                return True
            return False
        except Exception:
            if self.logger is not None:
                self.logger.error('exception', exc_info=True)
            return None

    def stop_all_axis(self):
        """Stop all axis."""
        try:
            if self.get_response(self.commands.stop_all_axis):
                return True
            return False
        except Exception:
            if self.logger is not None:
                self.logger.error('exception', exc_info=True)
            return None

    def kill_all_axis(self):
        """Kill all axis."""
        try:
            if self.get_response(self.commands.kill_all_axis):
                return True
            return False
        except Exception:
            if self.logger is not None:
                self.logger.error('exception', exc_info=True)
            return None

    def set_trigger(
            self, axis, start_pos, increments,
            pulse_width, max_pulses, edge=1):
        """Set the trigger parameters and enable plc2.

        Args:
            axis (int): axis for triggering,
            start_pos (float): trigger start position [mm or deg],
            increments (float): increments [mm or deg],
            pulse_width (float): pulse width in %,
            max_pulses (int): maximum number of pulses to trigger,
            edge (int): pulse edge [1 - falling (default), 0 - raising].

        Returns:
            True if successful, False otherwise.

        """
        try:
            cmds = [
                self.set_par(self.commands.q_plc0_run_control, 0),
                self.set_par(self.commands.q_selected_motor, axis),
                self.set_par(self.commands.q_incremment, increments),
                self.set_par(self.commands.q_use_prog_start_pos, 1),
                self.set_par(self.commands.q_start_pos, start_pos),
                self.set_par(self.commands.q_pulse_width_perc, pulse_width),
                self.set_par(self.commands.q_max_pulses, max_pulses),
                self.set_par(self.commands.q_falling_edge, edge),
                self.commands.enaplc2,
            ]
            for cmd in cmds:
                if not self.get_response(cmd):
                    return False
            return True

        except Exception:
            if self.logger is not None:
                self.logger.error('exception', exc_info=True)
            return None

    def stop_trigger(self):
        """Stop trigerring."""
        try:
            _cmd = self.set_par(self.commands.q_plc0_run_control, 0)
            if self.get_response(_cmd):
                return True
            return False
        except Exception:
            if self.logger is not None:
                self.logger.error('exception', exc_info=True)
            return None
