import os
from statistics import mode
import time
import argparse
import math
import os 
import gc
import numpy as np
import pandas as pd
from numpy.random import default_rng
import shutil

dest='/N/u/d/dnperera/temp'
tmp = '/scratch_ssd/dnperera/'

parser = argparse.ArgumentParser(description='generate random data')

parser.add_argument('-r', dest='rows', type=int, help='number of rows', required=True, nargs='+')
parser.add_argument('-t', dest='tables', type=int, help='number tables', required=True)
parser.add_argument('-u', dest='unique', type=float, default=1.0, help="unique factor")

args = parser.parse_args()
args = vars(args)

rng = default_rng()
rows = args['rows']


for r in rows:
    max_val = r * args['unique']
    for i in range(args['tables']):
        frame_data = rng.integers(0, max_val, size=(r, 2))
        df_tmp_path = f'{tmp}/df{i}_{r}.csv' 
        df_dst_path = f'{dest}/df{i}_{r}.csv'
        df = pd.DataFrame(frame_data, columns=["col0", "col1"])
        df.to_csv(df_tmp_path, index=False)

        shutil.move(df_tmp_path, df_dst_path)
