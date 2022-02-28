from pycylon import DataFrame, CylonEnv
from pycylon.net import MPIConfig
import argparse
import pandas as pd 
import numpy as np
import time
import gc
import os
from numpy.random import default_rng


parser = argparse.ArgumentParser(description='run cylon join')
parser.add_argument('-r', dest='rows', type=int, required=True)
parser.add_argument('-w', dest='world', type=int, required=True)
parser.add_argument('-i', dest='it', type=int, required=True)
parser.add_argument('-o', dest='out', type=str, required=True)
parser.add_argument('-u', dest='unique', type=float, default=1.0, help="unique factor")


script = os.path.basename(__file__).replace('.py', '')

args = vars(parser.parse_args())

w = args['world']
global_r = args['rows']
r = int(global_r/w)
cols = 2
max_val = int(global_r * args['unique'])

rng = default_rng()
# frame_data = rng.integers(0, max_val, size=(r, cols))

print(f"data generated", flush=True)

env = CylonEnv(config=MPIConfig())
rank = env.rank

timing = {'rows': [], 'world':[], 'it':[], 'time':[]}

# try:
#     for i in range(args['it']):
#         data = pd.DataFrame(frame_data).add_prefix('col') 
#         # df1 = DataFrame(data)

#         t11 = time.time()
#         x = data.sum()
#         t22 = time.time()

#         # t1 = time.time()
#         # tb3 = df1.to_table().sum(0)
#         # tb4 = df1.to_table().sum(1)
#         # env.barrier()
#         # t2 = time.time()

#         # timing['rows'].append(global_r)
#         # timing['world'].append(w)
#         # timing['it'].append(i)
#         # timing['time'].append((t2 - t1) * 1000)

        
#         # print(tb3)
#         # print(tb4)

#         # t11 = time.time()
#         # x = data.sum()
#         # t22 = time.time()

#         # print(x, (t22 - t11) * 1000, timing['time'][-1],timing['rows'][-1], r )
#         print(x, (t22 - t11) * 1000)
        
#         del data
#         # del df1
#         # del tb3
#         gc.collect()
# finally:
#     if rank == 0:
#         pd.DataFrame(timing).to_csv(f"{args['out']}/{script}.csv", mode='a', index=False, header=False)
#     env.finalize()

# for r in rows:
max_val = r * args['unique']
rng = default_rng()
frame_data = rng.integers(0, max_val, size=(r, 2)) 
print(f"data generated", flush=True)

timing = {'rows': [], 'world':[], 'it':[], 'time':[]}

try:                 
    for i in range(args['it']):
        df = pd.DataFrame(frame_data).add_prefix("col")
        print(f"data loaded", flush=True)


        t1 = time.time()
        out = df.sum()
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