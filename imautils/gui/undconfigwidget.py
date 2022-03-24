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
from imautils.gui.undulatorwidget import (
    UndulatorWidget as _UndulatorWidget,
    )
from imautils.gui.undnewcfg import (
    UndNewCfg as _UndNewCfg)
from imautils.gui.utils import get_ui_file as _get_ui_file


class UndConfigWidget(_QWidget):
    """Widget class to embed undulator control in measurement software."""

    def __init__(self, parent=None):
        """Set up the ui."""
        super().__init__(parent)

        # setup the ui
        uifile = _get_ui_file(self)
        self.ui = _uic.loadUi(uifile, self)
        self.wdg_und = self.ui.horizontalLayout

        self.und_utils = _UndUtils()

        # self.upd_status_timer = _QTimer()
        # self.update_flag = True
        self.und = _UndulatorControl(virtual=True)
        self.status = {}

        self.connect_signal_slots()

        self.und.cfg.create_database()
        self.update_cfg_list()
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

    def clear(self):
        """Clear."""
        try:
            self.twg_database.delete_widgets()
            self.twg_database.clear()
        except Exception:
            _traceback.print_exc(file=_sys.stdout)

    def connect_signal_slots(self):
        """Create signal/slot connections."""
        self.ui.pbt_view.clicked.connect(self.view_cfg)
        self.ui.pbt_new.clicked.connect(self.new_cfg)

    def update_cfg_list(self):
        """Updates configuration name list in combobox."""
        try:
            self.und.cfg.update_db_name_list(self.ui.cmb_cfg_name)
        except Exception:
            _traceback.print_exc(file=_sys.stdout)
    
    def new_cfg(self):
        """Add new configuration to the database."""
        self.new_cfg_dialog = _UndNewCfg()
        self.new_cfg_dialog.show()
        self.new_cfg_dialog.finished.connect(self.update_cfg_list)

    def load_cfg(self):
        """Load configuration from database."""
        try:
            name = self.ui.cmb_cfg_name.currentText()
            self.und_utils.load_cfg(self.und.cfg, name)
            # self.load_cfg_into_ui()
            # _QMessageBox.information(self, 'Information',
            #                          'Configuration Loaded.',
            #                          _QMessageBox.Ok)
            return True
        except Exception:
            _QMessageBox.warning(self, 'Information',
                                 'Failed to load the undulator configuration.',
                                 _QMessageBox.Ok)
            raise
            #_traceback.print_exc(file=_sys.stdout)
            return False

    def view_cfg(self):
        """Displays undulator positions configuration."""
        if self.load_cfg():
            _QMessageBox.information(self, 'Undulator Configuration',
                                     self.und.cfg.positions,
                                     _QMessageBox.Ok)
