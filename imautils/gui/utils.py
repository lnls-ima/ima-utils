# -*- coding: utf-8 -*-

"""Utils."""

import numpy as _np
import os.path as _path
from qtpy.QtWidgets import (
    QComboBox as _QComboBox,
    QListView as _QListView,
    QVBoxLayout as _QVBoxLayout,
)
from qtpy.QtCore import Qt as _Qt
from qtpy.QtGui import QStandardItemModel as _QStandardItemModel


BASEPATH = _path.dirname(
    _path.dirname(_path.dirname(_path.abspath(__file__))))

class CheckableComboBox(_QComboBox):
    """Combo box with checkable items."""

    def __init__(self, parent=None):
        """Initialize object."""
        super().__init__(parent)
        self.setView(_QListView(self))
        self.view().pressed.connect(self.handle_item_pressed)
        self.setModel(_QStandardItemModel(self))

    def addItem(self, text, userData=None, checked=False):
        """Add item to combo box (Overriding ComboBox.addItem)."""
        super().addItem(text, userData=userData)
        item = self.model().item(self.count()-1)
        if checked:
            item.setCheckState(_Qt.Checked)
        else:
            item.setCheckState(_Qt.Unchecked)

    def addItems(self, texts, checked=False):
        """Add items to combo box (Overriding ComboBox.addItems)."""
        super().addItems(texts)
        for index in range(self.count()):
            item = self.model().item(index)
            if checked:
                item.setCheckState(_Qt.Checked)
            else:
                item.setCheckState(_Qt.Unchecked)

    def checked_items(self):
        """Get checked items."""
        checkedItems = []
        for index in range(self.count()):
            item = self.model().item(index)
            if item.checkState() == _Qt.Checked:
                checkedItems.append(item)
        return checkedItems

    def checked_indexes(self):
        """Get checked indexes."""
        checkedIndexes = []
        for index in range(self.count()):
            item = self.model().item(index)
            if item.checkState() == _Qt.Checked:
                checkedIndexes.append(index)
        return checkedIndexes

    def handle_item_pressed(self, index):
        """Change item check state."""
        item = self.model().itemFromIndex(index)
        if item.checkState() == _Qt.Checked:
            item.setCheckState(_Qt.Unchecked)
        else:
            item.setCheckState(_Qt.Checked)

    def set_all_checked(self):
        """Check all items."""
        for index in range(self.count()):
            item = self.model().item(index)
            item.setCheckState(_Qt.Checked)

    def set_all_unchecked(self):
        """Unchecked all items."""
        for index in range(self.count()):
            item = self.model().item(index)
            item.setCheckState(_Qt.Unchecked)


class DraggableText():
    """Draggable text annotation."""

    def __init__(self, canvas, ax, x, y, string, tol=50, **kwargs):
        """Initialize variables and set callbacks."""
        self.canvas = canvas
        self.ax = ax
        self.tol = tol
        self.text = self.ax.text(x, y, string, **kwargs)
        self.change_position = False
        self._set_callbacks()

    def _get_distance_from_point(self, x, y):
        bb = self.text.get_window_extent(renderer=self.canvas.renderer)
        xm = (bb.x1 + bb.x0)/2
        ym = (bb.y1 + bb.y0)/2
        return _np.sqrt((xm - x)**2 + (ym - y)**2)

    def _set_center_position(self, x, y):
        bb = self.text.get_window_extent(renderer=self.canvas.renderer)
        bb = bb.transformed(self.ax.transData.inverted())
        dx = bb.x1 - bb.x0
        dy = bb.y1 - bb.y0
        self.text.set_position((x - dx/2, y - dy/2))

    def _set_callbacks(self):
        def button_press_callback(event):
            if event.x is not None and event.y is not None:
                dist = self._get_distance_from_point(event.x, event.y)
                if dist < self.tol:
                    self.change_position = True

        def button_release_callback(event):
            self.change_position = False

        def motion_notify_callback(event):
            if not self.change_position:
                return
            if event.xdata is None or event.ydata is None:
                return
            try:
                self._set_center_position(event.xdata, event.ydata)
                self.canvas.draw()
            except Exception:
                pass

        self.canvas.mpl_connect(
            'button_press_event', button_press_callback)
        self.canvas.mpl_connect(
            'button_release_event', button_release_callback)
        self.canvas.mpl_connect(
            'motion_notify_event', motion_notify_callback)


class DraggableLegend():
    """Draggable legend."""

    def __init__(self, canvas, ax, tol=50, **kwargs):
        """Initialize variables and set callbacks."""
        self.canvas = canvas
        self.ax = ax
        self.tol = tol
        self.legend = self.ax.legend(**kwargs)
        self.change_position = False
        self._set_callbacks()

    def _get_distance_from_point(self, x, y):
        bb = self.legend.get_window_extent(renderer=self.canvas.renderer)
        xm = (bb.x1 + bb.x0)/2
        ym = (bb.y1 + bb.y0)/2
        return _np.sqrt((xm - x)**2 + (ym - y)**2)

    def _set_center_position(self, x, y):
        bb = self.legend.get_window_extent(renderer=self.canvas.renderer)
        bb = bb.transformed(self.ax.transData.inverted())
        dx = bb.x1 - bb.x0
        dy = bb.y1 - bb.y0
        bb.x0 = x - dx/2
        bb.y0 = y - dy/2
        bb.x1 = bb.x0 + dx
        bb.y1 = bb.y0 + dy
        self.legend.set_bbox_to_anchor(bb, transform=self.ax.transData)

    def _set_callbacks(self):
        def button_press_callback(event):
            if event.x is not None and event.y is not None:
                dist = self._get_distance_from_point(event.x, event.y)
                if dist < self.tol:
                    self.change_position = True

        def button_release_callback(event):
            self.change_position = False

        def motion_notify_callback(event):
            if not self.change_position:
                return
            if event.xdata is None or event.ydata is None:
                return
            try:
                self._set_center_position(event.xdata, event.ydata)
                self.canvas.draw()
            except Exception:
                pass

        self.canvas.mpl_connect(
            'button_press_event', button_press_callback)
        self.canvas.mpl_connect(
            'button_release_event', button_release_callback)
        self.canvas.mpl_connect(
            'motion_notify_event', motion_notify_callback)


def get_ui_file(widget):
    """Get the ui file path.

    Args:
        widget  (QWidget or class)
    """
    if isinstance(widget, type):
        basename = '%s.ui' % widget.__name__.lower()
    else:
        basename = '%s.ui' % widget.__class__.__name__.lower()
    ui_path = _path.join(
        BASEPATH, _path.join('imautils', _path.join('gui', 'ui')))
    ui_file = _path.join(ui_path, basename)

    return ui_file
