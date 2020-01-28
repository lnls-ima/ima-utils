# -*- coding: utf-8 -*-

"""Utils."""

import time as _time
import numpy as _np


EMPTY_STR = '--'
COMMENT_CHAR = "#"
DEFAULT_DTYPE = str
DEFAULT_NOT_NULL = False
DEFAULT_UNIQUE = False


def read_file(filename):
    """Read file and return the list of non-empty lines.

    Args:
        filename (str): file path.

    Returns:
        list of non-empty file lines.

    """
    with open(filename, mode='r') as f:
        fdata = f.read()
    data = [
        line for line in fdata.splitlines() if len(line) != 0 and
        not line.strip().startswith(COMMENT_CHAR)]
    return data


def find_value(data, variable, vtype=str, raise_error=True):
    """Find variable value in file data.

    Args:
        data (list): list of file lines.
        variable (str): string to search in file lines.
        vtype (type): variable type
        raise_error (bool): raise error flag.

    Returns:
        the variable value.

    Raises:
        ValueError: if raise_error is True and the value was not found.

    """
    file_line = None
    for line in data:
        if line.split('\t')[0].strip() == variable:
            file_line = line
            break

    try:
        value = file_line.split()[1]
        value = vtype(value)
    except Exception:
        if raise_error:
            message = 'Invalid value for "%s"' % variable
            raise ValueError(message)

        value = None

    return value


def find_index(data, variable):
    """Find index of line with the specified variable.

    Args:
        data (list): list of file lines.
        variable (str): string to search in file lines.

    Returns:
        the line index.

    """
    index = None
    for i, line in enumerate(data):
        if line.split('\t')[0].strip() == variable:
            index = i
            break

    if index is None:
        for i, line in enumerate(data):
            if variable in line:
                index = i
                break

    return index


def get_timestamp():
    """Get timestamp (format: Year-month-day_hour-min-sec)."""
    timestamp = _time.strftime('%Y-%m-%d_%H-%M-%S', _time.localtime())
    return timestamp


def get_date_hour():
    """Get date and hour (format: Year-month-day, hour:min:sec)."""
    timestamp = get_timestamp()
    date, hour = timestamp_to_date_hour(timestamp)
    return date, hour


def timestamp_to_date_hour(timestamp):
    """Convert timestamp to date and hour."""
    date = timestamp.split('_')[0]
    hour = timestamp.split('_')[1].replace('-', ':')
    return date, hour


def date_hour_to_timestamp(date, hour):
    """Convert date and hour to timestamp."""
    hour = hour.replace(':', '-')
    timestamp = '{0:s}_{1:s}'.format(date, hour)
    return timestamp


def to_array(value):
    """Return a numpy.ndarray or None."""
    if value is not None:
        if not isinstance(value, _np.ndarray):
            value = _np.array(value)
        if len(value.shape) == 0:
            value = _np.array([value])
    else:
        value = _np.array([])
    return value


def to_float(value, precision=None):
    """Return a float number or None."""
    if value is None:
        return None

    elif isinstance(value, str):
        if len(value.strip()) == 0 or value == EMPTY_STR:
            return None
        else:
            if precision is not None:
                v = _np.around(float(value), precision)
            else:
                v = float(value)
            return v

    elif isinstance(value, (float, int)):
        if precision is not None:
            v = _np.around(value, precision)
        else:
            v = float(value)
        return v

    else:
        return None
