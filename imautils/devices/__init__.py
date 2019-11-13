"""Subpackage for devices communication."""

from . import Agilent3458ALib
from . import Agilent34401ALib
from . import Agilent34970ALib
from . import ElcomatLib
from . import HeidenhainLib
from . import NMRLib
from . import UDCLib
from . import Parker_Drivers
from . import PmacLV_IMS
from . import FDI2056
from . import F1000DRSLib
from . import pydrs
from . import utils


import sys as _sys
if _sys.platform == 'win32':
    from . import PmacLib
