# -*- coding: utf-8 -*-
"""
Created on Fri Sep 28 16:38:12 2018

@author: koshnick
"""

import mypy
import datetime

import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib as mpl
import matplotlib.pyplot as plt

from math import floor
from matplotlib.backends.backend_pdf import PdfPages

sns.set()

import schedule_monitor as sm

# =============================================================================
# --- Tests
# =============================================================================

def test_inflate_schedules():
    testDict = {'ARC.AHU.AHU05': {('weekstart', 'Start'): 0.0,
                  ('Monday', 'Start'): datetime.datetime(1990, 1, 1, 5, 0),
                  ('Monday', 'End'): datetime.datetime(1990, 1, 2, 0, 30),
                  ('Tuesday', 'Start'): datetime.datetime(1990, 1, 2, 5, 0),
                  ('Tuesday', 'End'): datetime.datetime(1990, 1, 3, 0, 30),
                  ('Wednesday', 'Start'): datetime.datetime(1990, 1, 3, 5, 0),
                  ('Wednesday', 'End'): datetime.datetime(1990, 1, 4, 0, 30),
                  ('Thursday', 'Start'): datetime.datetime(1990, 1, 4, 5, 0),
                  ('Thursday', 'End'): datetime.datetime(1990, 1, 5, 0, 30),
                  ('Friday', 'Start'): datetime.datetime(1990, 1, 5, 5, 0),
                  ('Friday', 'End'): datetime.datetime(1990, 1, 5, 23, 30),
                  ('Saturday', 'Start'): datetime.datetime(1990, 1, 6, 7, 0),
                  ('Saturday', 'End'): datetime.datetime(1990, 1, 6, 23, 30),
                  ('Sunday', 'Start'): datetime.datetime(1990, 1, 7, 7, 0),
                  ('Sunday', 'End'): datetime.datetime(1990, 1, 7, 23, 30)},
                 'ARC.AHU.AHU02': {('weekstart', 'Start'): 0.0,
                  ('Monday', 'Start'): datetime.datetime(1990, 1, 1, 5, 0),
                  ('Monday', 'End'): datetime.datetime(1990, 1, 2, 0, 30),
                  ('Tuesday', 'Start'): datetime.datetime(1990, 1, 2, 5, 0),
                  ('Tuesday', 'End'): datetime.datetime(1990, 1, 3, 0, 30),
                  ('Wednesday', 'Start'): datetime.datetime(1990, 1, 3, 5, 0),
                  ('Wednesday', 'End'): datetime.datetime(1990, 1, 4, 0, 30),
                  ('Thursday', 'Start'): datetime.datetime(1990, 1, 4, 5, 0),
                  ('Thursday', 'End'): datetime.datetime(1990, 1, 5, 0, 30),
                  ('Friday', 'Start'): datetime.datetime(1990, 1, 5, 5, 0),
                  ('Friday', 'End'): datetime.datetime(1990, 1, 5, 23, 30),
                  ('Saturday', 'Start'): datetime.datetime(1990, 1, 6, 7, 0),
                  ('Saturday', 'End'): datetime.datetime(1990, 1, 6, 23, 30),
                  ('Sunday', 'Start'): datetime.datetime(1990, 1, 7, 7, 0),
                  ('Sunday', 'End'): datetime.datetime(1990, 1, 7, 23, 30)},
                 'value': {('weekstart', 'Start'): 1,
                  ('Monday', 'Start'): 1,
                  ('Monday', 'End'): 0,
                  ('Tuesday', 'Start'): 1,
                  ('Tuesday', 'End'): 0,
                  ('Wednesday', 'Start'): 1,
                  ('Wednesday', 'End'): 0,
                  ('Thursday', 'Start'): 1,
                  ('Thursday', 'End'): 0,
                  ('Friday', 'Start'): 1,
                  ('Friday', 'End'): 0,
                  ('Saturday', 'Start'): 1,
                  ('Saturday', 'End'): 0,
                  ('Sunday', 'Start'): 1,
                  ('Sunday', 'End'): 0}}

    testStandard = pd.DataFrame.from_dict(testDict)

    inflated =  sm.inflate_schedules(testStandard)

    trueResult = pd.read_excel('test_inflated.xlsx', index_col=[0,1,2])


    pd.testing.assert_frame_equal(inflated, trueResult, check_dtype=False)

    print("test_inflate_scheudles_passed")

    return inflated


def _test_combine_standard_pi():

    pass


def _test_type_aggregate_mismatches():

    pass


def _test_find_differences_in_columns():

    pass


def _test_update_weekly_archive():

    pass


if __name__ == "__main__":
    A = test_inflate_schedules()