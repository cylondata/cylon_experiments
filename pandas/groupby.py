import os
import time
import argparse
import math
import os 
import gc
import numpy as np
import pandas as pd
from numpy.random import default_rng


parser = argparse.ArgumentParser(description='generate random data')

parser.add_argument('-r', dest='rows', type=int, help='number of rows', required=True, nargs='+')
parser.add_argument('-i', dest='it', type=int, help='iterations', default=1)
parser.add_argument('-u', dest='unique', type=float, default=1.0, help="unique factor")

args = parser.parse_args()
args = vars(args)
print(args, flush=True)

rows = args['rows']
it = args['it']
script = os.path.basename(__file__).replace('.py', '')
    

if __name__ == "__main__":
    for r in rows:
        max_val = r * args['unique']
        rng = default_rng()
        frame_data = rng.integers(0, max_val, size=(r, 2)) 
        print(f"data generated", flush=True)

        timing = {'rows': [], 'world':[], 'it':[], 'time':[]}

        try:                 
            for i in range(it):
                df = pd.DataFrame(frame_data).add_prefix("col")
                # df_r = pd.DataFrame(frame_data1).add_prefix("col")
                print(f"data loaded", flush=True)


                t1 = time.time()
                out = df.groupby(by='col0').agg({'col1': ["sum", "mean", "std"]})
                t2 = time.time()

                timing['rows'].append(r)
                timing['world'].append(1)
                timing['it'].append(i)
                timing['time'].append((t2 - t1) * 1000)
                print(f"timings {r} 1 {i} {(t2 - t1) * 1000:.0f} ms, {out.shape[0]}", flush=True)
                
                del df 
                del out 
                gc.collect()
        finally:
            pd.DataFrame(timing).to_csv(f'pandas_{script}.csv', mode='a', index=False, header=False)
