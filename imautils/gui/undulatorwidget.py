"""Database tables widgets."""

import os as _os
import sys as _sys
import numpy as _np
import time as _time
import traceback as _traceback
from threading import Thread as _Thread
import qtpy.uic as _uic
from qtpy.QtCore import (
    QTimer as _QTimer,
    )
from qtpy.QtWidgets import (
    QWidget as _QWidget,
    QApplication as _QApplication,
    QTableWidgetItem as _QTableWidgetItem,
    QMessageBox as _QMessageBox,
    )

from sqlite3 import (
    IntegrityError as _IntegrityError,
    )

from imautils.devices.DeltaControl import (
    UndulatorControl as _UndulatorControl,
    Utils as _UndUtils)
from imautils.gui.utils import get_ui_file as _get_ui_file


class UndulatorWidget(_QWidget):
    """Database widget class for the control application."""

    def __init__(self, parent=None):
        """Set up the ui."""
        super().__init__(parent)

        # setup the ui
        uifile = _get_ui_file(self)
        self.ui = _uic.loadUi(uifile, self)

        default_speed = 0.1  # [mm/s]
        self.ui.dsb_speed.setValue(default_speed)

        self.und_utils = _UndUtils()

        self.upd_status_timer = _QTimer()
        self.update_flag = True
        self.und = _UndulatorControl(virtual=False)
        self.status = {}

        self.connect_signal_slots()
        # self.upd_status_timer.start(1000)

    @property
    def database_name(self):
        """Database name."""
        return _QApplication.instance().database_name

    @property
    def mongo(self):
        """MongoDB database."""
        return _QApplication.instance().mongo

    @property
    def server(self):
        """Server for MongoDB database."""
        return _QApplication.instance().server

    @property
    def directory(self):
        """Return the default directory."""
        return _QApplication.instance().directory

    def init_tab(self):
        self.tw_configurations.setColumnWidth(0, 50)
        self.tw_configurations.setColumnWidth(1, 350)
        self.und.cfg.create_database()
        self.update_cfg_list()

    def clear(self):
        """Clear."""
        try:
            self.twg_database.delete_widgets()
            self.twg_database.clear()
        except Exception:
            _traceback.print_exc(file=_sys.stdout)

    def connect_signal_slots(self):
        """Create signal/slot connections."""
        self.ui.upd_status_timer.timeout.connect(self.update_status)
        self.ui.pbt_move.clicked.connect(self.move)
        self.ui.pbt_stop.clicked.connect(self.stop)
        self.ui.pbt_home.clicked.connect(self.home)
        self.ui.pbt_save.clicked.connect(self.save_cfg)
        self.ui.pbt_load.clicked.connect(self.load_cfg)
        self.ui.pbt_add_cfg.clicked.connect(
            lambda: self.add_cfg_to_table(self.ui.tw_configurations))
        self.ui.pbt_remove_cfg.clicked.connect(
            lambda: self.remove_cfg_from_table(self.ui.tw_configurations))
        self.ui.pbt_clear_table.clicked.connect(self.clear_table)
        self.ui.chb_update_status.stateChanged.connect(self.update_timer)

    def update_cfg_list(self):
        """Updates configuration name list in combobox."""
        try:
            self.und.cfg.update_db_name_list(self.ui.cmb_cfg_name)
        except Exception:
            _traceback.print_exc(file=_sys.stdout)

    def update_timer(self):
        """Starts and stops update status timer."""
        if self.ui.chb_update_status.isChecked():
            self.upd_status_timer.start(1000)
        else:
            self.upd_status_timer.stop()

    def update_status(self):
        """Updates status on widget."""
        try:

            if not self.und.check_connection():
                self.ui.chb_update_status.setChecked(False)
                _QMessageBox.warning(self, 'Warining',
                                     'Could not connect to the undulator IOC.'
                                     ' Try again later.',
                                     _QMessageBox.Ok)
                return False

            if all([self.update_flag,
                    self.parent().currentWidget() == self]):

                self.status = self.und.get_status()
                self.ui.lcd_state.display(self.status['state_idx'])
                self.ui.lbl_state.setText(self.und.state_idx_dict[
                    self.status['state_idx']])
                self.ui.lbl_und_en.setEnabled(self.status['enable_mon'])

                self.ui.lbl_csd_en.setEnabled(self.status['CSD_enable'])
                self.ui.lbl_cse_en.setEnabled(self.status['CSE_enable'])
                self.ui.lbl_cie_en.setEnabled(self.status['CIE_enable'])
                self.ui.lbl_cid_en.setEnabled(self.status['CID_enable'])

                self.ui.lbl_csd_moving.setEnabled(
                    self.status['CSD_motion_state'] == 1)
                self.ui.lbl_cse_moving.setEnabled(
                    self.status['CSE_motion_state'] == 1)
                self.ui.lbl_cie_moving.setEnabled(
                    self.status['CIE_motion_state'] == 1)
                self.ui.lbl_cid_moving.setEnabled(
                    self.status['CID_motion_state'] == 1)

                self.ui.lbl_csd_lim_p.setDisabled(self.status['CSD_pos_lim'])
                self.ui.lbl_cse_lim_p.setDisabled(self.status['CSE_pos_lim'])
                self.ui.lbl_cie_lim_p.setDisabled(self.status['CIE_pos_lim'])
                self.ui.lbl_cid_lim_p.setDisabled(self.status['CID_pos_lim'])

                self.ui.lbl_csd_lim_n.setDisabled(self.status['CSD_neg_lim'])
                self.ui.lbl_cse_lim_n.setDisabled(self.status['CSE_neg_lim'])
                self.ui.lbl_cie_lim_n.setDisabled(self.status['CIE_neg_lim'])
                self.ui.lbl_cid_lim_n.setDisabled(self.status['CID_neg_lim'])

                self.ui.lbl_csd_kill_p.setEnabled(self.status['CSD_pos_kill'])
                self.ui.lbl_cse_kill_p.setEnabled(self.status['CSE_pos_kill'])
                self.ui.lbl_cie_kill_p.setEnabled(self.status['CIE_pos_kill'])
                self.ui.lbl_cid_kill_p.setEnabled(self.status['CID_pos_kill'])

                self.ui.lbl_csd_kill_n.setEnabled(self.status['CSD_neg_kill'])
                self.ui.lbl_cse_kill_n.setEnabled(self.status['CSE_neg_kill'])
                self.ui.lbl_cie_kill_n.setEnabled(self.status['CIE_neg_kill'])
                self.ui.lbl_cid_kill_n.setEnabled(self.status['CID_neg_kill'])

                self.positions = self.und.read_encoder()
                self.ui.lcd_csd.display(self.positions[0])
                self.ui.lcd_cse.display(self.positions[1])
                self.ui.lcd_cie.display(self.positions[2])
                self.ui.lcd_cid.display(self.positions[3])
        except Exception:
            _traceback.print_exc(file=_sys.stdout)

    def move(self):
        """Moves the undulator."""
        self.update_flag = True #False

        _coupling = self.ui.cmb_coupling.currentIndex()
        _rel_pos = self.ui.dsb_rel_pos.value()
        _speed = self.ui.dsb_speed.value()

        if _coupling == 0:
            thread = _Thread(target=self.und.move_phase,
                             args=(_rel_pos, _speed), daemon=True)
        elif _coupling == 1:
            thread = _Thread(
                target=self.und.move_counterphase,
                             args=(_rel_pos, _speed), daemon=True)
        elif _coupling == 2:
            thread = _Thread(target=self.und.move_gv,
                             args=(_rel_pos, _speed), daemon=True)
        elif _coupling == 3:
            thread = _Thread(target=self.und.move_gh,
                             args=(_rel_pos, _speed), daemon=True)
        elif _coupling == 4:
            thread = _Thread(target=self.und.move_csd,
                             args=(_rel_pos, _speed), daemon=True)
        elif _coupling == 5:
            thread = _Thread(target=self.und.move_cse,
                             args=(_rel_pos, _speed), daemon=True)
        elif _coupling == 6:
            thread = _Thread(target=self.und.move_cie,
                             args=(_rel_pos, _speed), daemon=True)
        elif _coupling == 7:
            thread = _Thread(target=self.und.move_cid,
                             args=(_rel_pos, _speed), daemon=True)
        elif _coupling == 8:
            thread = _Thread(target=self.und.move_all,
                             args=(_rel_pos, _speed), daemon=True)
        thread.start()

        self.update_flag = True

    def stop(self):
        """Stops the undulator."""
        self.update_flag = False
        self.und.stop()
        self.update_flag = True

    def home(self):
        """Sends all the cassettes to position zero."""
        self.update_flag = False
        self.und.home_motors()
        self.update_flag = True

    def add_cfg_to_table(self, tw):
        """Adds row into tableWidget."""
        try:
            _tw = tw
            _idx = _tw.rowCount()
            _tw.insertRow(_idx)
            pos_list = []
            pos_list.append('Phase=' +
                            str(self.ui.dsb_ph_pos.value()))
            pos_list.append('CounterPhase=' +
                            str(self.ui.dsb_cph_pos.value()))
            pos_list.append('GV=' +
                            str(self.ui.dsb_gv_pos.value()))
            pos_list.append('GH=' +
                            str(self.ui.dsb_gh_pos.value()))
            pos_str = ';'.join(pos_list)
            _tw.setItem(_idx, 0, _QTableWidgetItem(str(_idx)))
            _tw.setItem(_idx, 1, _QTableWidgetItem(pos_str))
        except Exception:
            _traceback.print_exc(file=_sys.stdout)

    def remove_cfg_from_table(self, tw):
        """Removes selected row from tableWidget."""
        try:
            _tw = tw
            _idx = _tw.currentRow()
            _tw.removeRow(_idx)
        except Exception:
            _traceback.print_exc(file=_sys.stdout)

    def clear_table(self):
        """Clears tableWidget."""
        try:
            self.und_utils.clear_table(self.ui.tw_configurations)
        except Exception:
            raise
            # _traceback.print_exc(file=_sys.stdout)

    def update_cfg_from_ui(self):
        """Updates current power supply configuration from ui widgets.

        Returns:
            True if successfull;
            False otherwise."""
        try:
            self.und.cfg.name = self.ui.cmb_cfg_name.currentText()
            self.und.cfg.positions = self.und_utils.table_to_str(
                self.ui.tw_configurations)
            return True
        except Exception:
            _traceback.print_exc(file=_sys.stdout)
            return False

    def load_cfg_into_ui(self):
        """Loads database configuration into ui widgets."""
        try:
            self.ui.cmb_cfg_name.setCurrentText(self.und.cfg.name)
            self.und_utils.str_to_table(self.und.cfg,
                                        self.ui.tw_configurations)
            _QApplication.processEvents()
        except Exception:
            _traceback.print_exc(file=_sys.stdout)

    def save_cfg(self):
        """Saves current ui configuration into database."""
        try:
            self.update_cfg_from_ui()
            self.und_utils.save_cfg(self.und.cfg)
            self.update_cfg_list()
            return True
        except _IntegrityError:
            _QMessageBox.warning(self, 'Information',
                                 'Failed to save this configuration. Check if'
                                 ' the name is unique.',
                                 _QMessageBox.Ok)
        except Exception:
            _QMessageBox.warning(self, 'Information',
                                 'Failed to save this configuration.',
                                 _QMessageBox.Ok)
            raise
            # _traceback.print_exc(file=_sys.stdout)
            return False

    def load_cfg(self):
        """Load configuration from database."""
        try:
            name = self.ui.cmb_cfg_name.currentText()
            self.und_utils.load_cfg(self.und.cfg, name)
            self.load_cfg_into_ui()
            _QMessageBox.information(self, 'Information',
                                     'Configuration Loaded.',
                                     _QMessageBox.Ok)
            return True
        except Exception:
            _QMessageBox.warning(self, 'Information',
                                 'Failed to load this configuration.',
                                 _QMessageBox.Ok)
            raise
            #_traceback.print_exc(file=_sys.stdout)
            return False
