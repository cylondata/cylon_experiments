from ast import arg
import os
import subprocess
import sys
import argparse

import numpy as np
import pandas as pd
from numpy.random import default_rng

parser = argparse.ArgumentParser(description='generate random data')
parser.add_argument('-r',
                    dest='rows',
                    type=int,
                    nargs='+',
                    help='row cases in millions',
                    required=True)
parser.add_argument('-w',
                    dest='world',
                    type=int,
                    nargs='+',
                    help='world sizes',
                    default=[1, 2, 4, 8, 16, 32, 64, 128])
parser.add_argument('-o',
                    dest='out',
                    type=str,
                    help='out dir',
                    default='/tmp/cylon')

args = parser.parse_args()
args = vars(args)

rows = [int(ii * 1000000) for ii in args['rows']]
world = args['world']
unique = 0.9
cols = 2

print("args: ", args, flush=True)

out_dir = args['out']

for r in rows:
    dir_path = f'{out_dir}/{r}'
    os.makedirs(dir_path, exist_ok=True)

    max_val = int(r * unique)
    rng = default_rng()

    df0 = pd.DataFrame(rng.integers(0, max_val,
                                    size=(r, cols))).add_prefix('col')
    df1 = pd.DataFrame(rng.integers(0, max_val,
                                    size=(r, cols))).add_prefix('col')

    for w in world:
        for i, df in enumerate([df0, df1]):
            df.to_parquet(f'{dir_path}/df{i}_{w}.parquet',
                          engine='pyarrow',
                          index=False,
                          compression='snappy',
                          row_group_size=r / 10,
                          flavor='spark')
print(f"done!\n ====================================== \n", flush=True)
