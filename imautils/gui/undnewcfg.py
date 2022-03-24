"""Database tables widgets."""

import os as _os
import sys as _sys
import numpy as _np
import time as _time
import traceback as _traceback
import qtpy.uic as _uic
from qtpy.QtCore import (
    QTimer as _QTimer,
    )
from qtpy.QtWidgets import (
    QWidget as _QWidget,
    QDialog as _QDialog,
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


class UndNewCfg(_QDialog):
    """Database widget class for the control application."""

    def __init__(self, parent=None):
        """Set up the ui."""
        super().__init__(parent)

        # setup the ui
        uifile = _get_ui_file(self)
        self.ui = _uic.loadUi(uifile, self)

        self.und_utils = _UndUtils()

        self.upd_status_timer = _QTimer()
        self.update_flag = True
        self.und = _UndulatorControl(virtual=True)
        self.status = {}

        self.connect_signal_slots()
        self.und.cfg.create_database()
        # self.update_cfg_list()
        self.tw_configurations.setColumnWidth(1, 270)
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
        self.ui.pbt_save.clicked.connect(self.save_cfg)
        self.ui.pbt_add_cfg.clicked.connect(
            lambda: self.add_cfg_to_table(self.ui.tw_configurations))
        self.ui.pbt_remove_cfg.clicked.connect(
            lambda: self.remove_cfg_from_table(self.ui.tw_configurations))
        self.ui.pbt_clear_table.clicked.connect(self.clear_table)

    def update_cfg_list(self):
        """Updates configuration name list in combobox."""
        try:
            self.und.cfg.update_db_name_list(self.ui.cmb_cfg_name)
        except Exception:
            _traceback.print_exc(file=_sys.stdout)

    def update_cfg_from_ui(self):
        """Updates current power supply configuration from ui widgets.

        Returns:
            True if successfull;
            False otherwise."""
        try:
            self.und.cfg.name = self.ui.le_cfg_name.text()
            self.und.cfg.positions = self.und_utils.table_to_str(
                self.ui.tw_configurations)
            return True
        except Exception:
            _traceback.print_exc(file=_sys.stdout)
            return False

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

    def save_cfg(self):
        """Saves current ui configuration into database."""
        try:
            self.update_cfg_from_ui()
            self.und_utils.save_cfg(self.und.cfg)
            # self.update_cfg_list()
            _QMessageBox.information(self, 'Information',
                                     'Configuration saved.',
                                     _QMessageBox.Ok)
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
