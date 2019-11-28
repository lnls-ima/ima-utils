"""Database widgets."""

import sys as _sys
import numpy as _np
import traceback as _traceback
from qtpy.QtCore import Qt as _Qt
from qtpy.QtWidgets import (
    QWidget as _QWidget,
    QApplication as _QApplication,
    QLabel as _QLabel,
    QTabWidget as _QTabWidget,
    QTableWidget as _QTableWidget,
    QTableWidgetItem as _QTableWidgetItem,
    QMessageBox as _QMessageBox,
    QVBoxLayout as _QVBoxLayout,
    QHBoxLayout as _QHBoxLayout,
    QSpinBox as _QSpinBox,
    QAbstractItemView as _QAbstractItemView,
    QMainWindow as _QMainWindow,
    )

from imautils.db import database as _database


class DatabaseTabWidget(_QTabWidget):
    """Database tab widget class."""

    def __init__(
            self, parent=None, database_name=None, mongo=None, server=None,
            number_rows=100, max_number_rows=1000, max_str_size=100):
        """Set up the ui."""
        super().__init__(parent)

        self._number_rows = number_rows
        self._max_number_rows = max_number_rows
        self._max_str_size = max_str_size

        self.database_name = database_name
        self.mongo = mongo
        self.server = server
        self.database = None
        self.database_widgets = []
        self.clear()
        self.delete_widgets()
        self.load_database()

    def delete_widgets(self):
        """Delete tables."""
        try:
            for widget in self.database_widgets:
                widget.deleteLater()
            self.database_widgets = []
        except Exception:
            _traceback.print_exc(file=_sys.stdout)

    def get_current_database_widget(self):
        """Get current databse widget."""
        try:
            idx = self.currentIndex()
            if len(self.database_widgets) > idx and idx != -1:
                current_widget = self.database_widgets[idx]
                return current_widget
            else:
                return None
        except Exception:
            _traceback.print_exc(file=_sys.stdout)
            return None

    def get_current_table_name(self):
        """Get current table name."""
        try:
            current_widget = self.get_current_database_widget()
            if current_widget is not None:
                current_table_name = current_widget.table_name
            else:
                current_table_name = None
            return current_table_name
        except Exception:
            _traceback.print_exc(file=_sys.stdout)
            return None

    def get_table_selected_id(self, table_name):
        """Get table selected ID."""
        current_widget = self.get_current_database_widget()
        if current_widget is None:
            return None

        if current_widget.table_name != table_name:
            return None

        idns = current_widget.get_selected_ids()

        if len(idns) == 0:
            return None

        if len(idns) > 1:
            msg = 'Select only one entry of the database table.'
            _QMessageBox.critical(self, 'Failure', msg, _QMessageBox.Ok)
            return None

        idn = idns[0]
        return idn

    def get_table_selected_ids(self, table_name):
        """Get table selected IDs."""
        current_widget = self.get_current_database_widget()
        if current_widget is None:
            return []

        if current_widget.table_name != table_name:
            return []

        return current_widget.get_selected_ids()

    def load_database(self):
        """Load database."""
        try:
            self.database = _database.Database(
                database_name=self.database_name,
                mongo=self.mongo,
                server=self.server)
            self.database_widgets = []
            table_names = self.database.get_collections()

            for table_name in table_names:
                tab = DatabaseWidget(
                    database_name=self.database_name,
                    collection_name=table_name,
                    mongo=self.mongo,
                    server=self.server,
                    number_rows=self._number_rows,
                    max_number_rows=self._max_number_rows,
                    max_str_size=self._max_str_size)
                self.database_widgets.append(tab)
                self.addTab(tab, table_name)

        except Exception:
            _traceback.print_exc(file=_sys.stdout)
            msg = 'Failed to load database.'
            _QMessageBox.critical(self, 'Failure', msg, _QMessageBox.Ok)
            return

    def scroll_down_tables(self):
        """Scroll down all tables."""
        for idx in range(len(self.database_widgets)):
            self.setCurrentIndex(idx)
            self.database_widgets[idx].scroll_down()

    def update_database_tables(self):
        """Update database tables."""
        if not self.isVisible():
            return

        try:
            self.blockSignals(True)
            _QApplication.setOverrideCursor(_Qt.WaitCursor)

            idx = self.currentIndex()
            self.clear()
            self.load_database()
            self.scroll_down_tables()
            self.setCurrentIndex(idx)

            self.blockSignals(False)
            _QApplication.restoreOverrideCursor()

        except Exception:
            _traceback.print_exc(file=_sys.stdout)
            self.blockSignals(False)
            _QApplication.restoreOverrideCursor()
            _QMessageBox.critical(
                self, 'Failure', 'Failed to update database.', _QMessageBox.Ok)


class DatabaseWidget(_QWidget):
    """Database widget."""

    _hidden_columns = []

    def __init__(
            self, parent=None, database_name=None, collection_name=None,
            mongo=None, server=None, number_rows=100,
            max_number_rows=1000, max_str_size=100):
        """Set up the ui."""
        super().__init__(parent)

        self.database_name = database_name
        self.collection_name = collection_name
        self.mongo = mongo
        self.server = server
        self.database_collection = _database.DatabaseCollection(
            database_name=self.database_name,
            collection_name=self.collection_name,
            mongo=self.mongo,
            server=self.server)

        self._number_rows = number_rows
        self._max_number_rows = max_number_rows
        self._max_str_size = max_str_size

        vlayout = _QVBoxLayout()
        hlayout = _QHBoxLayout()

        self.table = _QTableWidget()
        self.table.setAlternatingRowColors(True)
        self.table.verticalHeader().hide()
        self.table.horizontalHeader().setStretchLastSection(True)
        self.table.horizontalHeader().setDefaultSectionSize(120)
        self.table.setSelectionBehavior(_QAbstractItemView.SelectRows)

        self.sb_initial_id = _QSpinBox()
        self.sb_initial_id.setMinimumWidth(100)
        self.sb_initial_id.setButtonSymbols(2)
        self.sb_initial_id.editingFinished.connect(self.change_initial_id)
        hlayout.addStretch(0)
        hlayout.addWidget(_QLabel("Initial ID:"))
        hlayout.addWidget(self.sb_initial_id)

        self.sb_max_number_rows = _QSpinBox()
        self.sb_max_number_rows.setMinimumWidth(100)
        self.sb_max_number_rows.setButtonSymbols(2)
        self.sb_max_number_rows.setMaximum(self._max_number_rows)
        self.sb_max_number_rows.setValue(self._number_rows)
        self.sb_max_number_rows.editingFinished.connect(self.change_max_rows)
        hlayout.addWidget(_QLabel("Maximum number of rows:"))
        hlayout.addWidget(self.sb_max_number_rows)

        self.sb_number_rows = _QSpinBox()
        self.sb_number_rows.setMinimumWidth(100)
        self.sb_number_rows.setButtonSymbols(2)
        self.sb_number_rows.setReadOnly(True)
        self.sb_number_rows.setMaximum(self._max_number_rows)
        self.sb_number_rows.setValue(self._number_rows)
        hlayout.addWidget(_QLabel("Current number of rows:"))
        hlayout.addWidget(self.sb_number_rows)

        vlayout.addWidget(self.table)
        vlayout.addLayout(hlayout)
        self.setLayout(vlayout)

        self.column_names = []
        self.data_types = []
        self.data = []
        self.update_table()

    def change_initial_id(self):
        """Change initial ID."""
        self.filter_data(initial_id=self.sb_initial_id.value())

    def change_max_rows(self):
        """Change maximum number of rows."""
        self.filter_data()

    def update_table(self):
        """Update table."""
        if self.database_name is None or self.collection_name is None:
            return

        self.blockSignals(True)
        self.table.setColumnCount(0)
        self.table.setRowCount(0)

        self.column_names = self.database_collection.get_field_names()
        self.data_types = self.database_collection.get_field_types()

        self.table.setColumnCount(len(self.column_names))
        self.table.setHorizontalHeaderLabels(self.column_names)

        self.table.setRowCount(1)
        for j in range(len(self.column_names)):
            self.table.setItem(0, j, _QTableWidgetItem(''))

        max_rows = self.sb_max_number_rows.value()
        data = self.database_collection.search_collection(
            self.column_names, max_nr_lines=max_rows)

        if len(data) > 0:
            min_idn = self.database_collection.get_first_id()
            self.sb_initial_id.setMinimum(min_idn)

            max_idn = self.database_collection.get_last_id()
            self.sb_initial_id.setMaximum(max_idn)

            self.sb_max_number_rows.setValue(len(data))
            self.data = data[:]
            self.add_rows_to_table(data)
        else:
            self.sb_initial_id.setMinimum(0)
            self.sb_initial_id.setMaximum(0)
            self.sb_max_number_rows.setValue(0)

        self.table.itemChanged.connect(self.filter_changed)
        self.table.itemSelectionChanged.connect(self.select_line)
        self.blockSignals(False)

    def add_rows_to_table(self, data):
        """Add rows to table."""
        if len(self.column_names) == 0:
            return

        self.table.setRowCount(1)

        if len(data) > self.sb_max_number_rows.value():
            tabledata = data[-self.sb_max_number_rows.value()::]
        else:
            tabledata = data

        if len(tabledata) == 0:
            return

        self.sb_initial_id.setValue(int(tabledata[0]['id']))
        self.sb_number_rows.setValue(len(tabledata))
        self.table.setRowCount(len(tabledata) + 1)

        for j, col in enumerate(self.column_names):
            for i, row in enumerate(tabledata):
                item_str = str(row[col])
                if len(item_str) > self._max_str_size:
                    item_str = item_str[:10] + '...'
                item = _QTableWidgetItem(item_str)
                item.setFlags(_Qt.ItemIsSelectable | _Qt.ItemIsEnabled)
                self.table.setItem(i + 1, j, item)

    def scroll_down(self):
        """Scroll down."""
        vbar = self.table.verticalScrollBar()
        vbar.setValue(vbar.maximum())

    def select_line(self):
        """Select the entire line."""
        if (self.table.rowCount() == 0
                or self.table.columnCount() == 0
                or len(self.column_names) == 0
                or len(self.data_types) == 0):
            return

        selected = self.table.selectedItems()
        rows = [s.row() for s in selected]

        if 0 in rows:
            self.table.setSelectionBehavior(_QAbstractItemView.SelectItems)
        else:
            self.table.setSelectionBehavior(_QAbstractItemView.SelectRows)

    def filter_changed(self, item):
        """Apply column filter to data."""
        if item.row() == 0:
            self.filter_data()

    def filter_data(self, initial_id=None):
        """Apply column filter to data."""
        if (self.table.rowCount() == 0
                or self.table.columnCount() == 0
                or len(self.column_names) == 0
                or len(self.data_types) == 0):
            return

        try:
            max_rows = self.sb_max_number_rows.value()

            filters = []
            for idx in range(len(self.column_names)):
                filters.append(self.table.item(0, idx).text())

            self.data = self.database_collection.search_collection(
                self.column_names,
                filters=filters,
                initial_idn=initial_id,
                max_nr_lines=max_rows)

            self.add_rows_to_table(self.data)
        except Exception:
            _traceback.print_exc(file=_sys.stdout)

    def get_selected_ids(self):
        """Get selected IDs."""
        selected = self.table.selectedItems()
        rows = [s.row() for s in selected if s.row() != 0]
        rows = _np.unique(rows)

        selected_ids = []
        for row in rows:
            idn = int(self.table.item(row, 0).text())
            selected_ids.append(idn)

        return selected_ids
