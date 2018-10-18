# -*- coding: utf-8 -*-
"""
Created on Thu Oct 18 09:16:11 2018

@author: koshnick
"""

import pandas as pd
import numpy as np

df = pd.DataFrame([{'var1': 'a,b,c', 'var2': 1},
               {'var1': 'd,e,f', 'var2': 2},
               {'var1':'b','var2':5}])

b = pd.DataFrame([{'var1': 'a', 'var2': 1},
               {'var1': 'b', 'var2': 1},
               {'var1': 'c', 'var2': 1},
               {'var1': 'd', 'var2': 2},
               {'var1': 'e', 'var2': 2},
               {'var1': 'f', 'var2': 2}])


def explode_str(df, col):

    for index, row in df.iterrows():

        print(index, row)
        if row[col].find(',') > 0:

            for item in row[col].split(','):

                newRow = row
                newRow[col] = item

                df = df.append(newRow)

    return df



A = explode_str(df, 'var1')
print(A)