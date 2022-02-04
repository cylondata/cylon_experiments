from pycylon import DataFrame, CylonEnv
from pycylon.net import MPIConfig
import argparse
import pandas as pd 
import numpy as np
import time
import gc
import os
from mpi4py import MPI

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


data = pd.DataFrame(np.random.randint(0, max_val, size=(r, cols))).add_prefix('col') 
print(f"data generated", flush=True)

env = CylonEnv(config=MPIConfig())
rank = env.rank

timing = {'rows': [], 'world':[], 'it':[], 'time':[]}

comm = MPI.COMM_WORLD
rank = comm.Get_rank()

# dummy allreduce for mpi to build the trees etc
val = np.random.randint(0, max_val)
val = comm.allreduce(val)

try:
    for i in range(args['it']):
        df1 = DataFrame(data)

        t1 = time.time()
        # sum is not exopsed in df :( 
        tb3 = df1.to_table().sum(0)
        tb4 = df1.to_table().sum(1)
        env.barrier()
        t2 = time.time()

        timing['rows'].append(global_r)
        timing['world'].append(w)
        timing['it'].append(i)
        timing['time'].append((t2 - t1) * 1000)
        
        del df1
        del tb3
        del tb4
        gc.collect()
finally:
    if rank == 0:
        pd.DataFrame(timing).to_csv(f"{args['out']}/{script}.csv", mode='a', index=False, header=False)
    env.finalize()

