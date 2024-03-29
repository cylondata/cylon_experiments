import os
import time
import argparse
import math
import os 
import gc
import numpy as np
from numpy.random import default_rng

from util import start_cluster, stop_cluster, TOTAL_NODES, MAX_PROCS, HEAD_IP, RAY_PW, SCHED_IP

parser = argparse.ArgumentParser(description='generate random data')

# parser.add_argument('-s', dest='scale', type=str, help='number of rows', required=True)
parser.add_argument('-w', dest='world', type=int, help='processes', required=True, nargs='+')
parser.add_argument('-r', dest='rows', type=int, help='number of rows', required=True, nargs='+')
parser.add_argument('-i', dest='it', type=int, help='iterations', default=1)
parser.add_argument('-e', dest='engine', type=str, help='ray/ dask', default='ray')
parser.add_argument('-u', dest='unique', type=float, default=1.0, help="unique factor")

args = parser.parse_args()
args = vars(args)
print(args, flush=True)

world = args['world']
rows = args['rows']
it = args['it']
engine = args['engine']
script = os.path.basename(__file__).replace('.py', '')
if engine == 'dask':
    script = 'dask_' + script 

home_dir = os.path.expanduser("~")
    
if __name__ == "__main__":
    os.environ["MODIN_ENGINE"] = engine

    for r in rows:
        # max_val = r * args['unique']
        # rng = default_rng()
        # frame_data = rng.integers(0, max_val, size=(r, 2)) 
        # print(f"data generated", flush=True)
        
        f0 = f'{home_dir}/data/cylon/{r}/df0_512.parquet'
        f1 = f'{home_dir}/data/cylon/{r}/df1_512.parquet'

        for w in world:
            procs = int(math.ceil(w / TOTAL_NODES))
            print(f"world sz {w} procs per worker {procs} iter {it}", flush=True)

            assert procs <= MAX_PROCS

            timing = {'rows': [], 'world':[], 'it':[], 'time':[]}

            try:
                stop_cluster(engine)
                start_cluster(engine, procs, min(w, TOTAL_NODES))
                
                client = None
                if engine == 'ray':
                    import ray
                    ray.init(address=f'{HEAD_IP}:6379', _redis_password=RAY_PW, _node_ip_address=HEAD_IP)
                elif engine == 'dask':
                    from dask.distributed import Client
                    client = Client(f"{SCHED_IP}:8786")
                    
                    if client is None:
                        print(f"unable to connect dask client", flush=True)
                        exit(1) 
                
                import modin.config as cfg
                # pd.DEFAULT_NPARTITIONS = w
                cfg.NPartitions.put(w)
                cfg.Engine.put(engine)
                cfg.StorageFormat.put('pandas')

                if engine=='ray':
                    cfg.RayRedisAddress.put(f'{HEAD_IP}:6379')
                    cfg.RayRedisPassword.put(RAY_PW)
                
                import modin.pandas as pd
            
                for i in range(it):

                    # df = pd.DataFrame(frame_data, columns=["col0", "col1"])
                    # df_r = pd.DataFrame(frame_data1).add_prefix("col")
                    df = pd.read_parquet(f0)
                    print(f"data loaded", flush=True)


                    t1 = time.time()
                    out = df.sort_values(by='col0')
                    t2 = time.time()

                    # timing = {'rows': [], 'world':[], 'it':[], 'time':[]}

                    timing['rows'].append(r)
                    timing['world'].append(w)
                    timing['it'].append(i)
                    timing['time'].append((t2 - t1) * 1000)
                    print(f"timings {r} {w} {i} {(t2 - t1) * 1000:.0f} ms, {len(out)}", flush=True)
                    
                    del df 
                    del out 
                    gc.collect()

                if engine == 'ray':
                    import ray
                    ray.shutdown()
                elif engine == 'dask':
                    client.close()
            finally:
                stop_cluster(engine)
                import pandas as pd
                pd.DataFrame(timing).to_csv(f'{script}.csv', mode='a', index=False, header=False)
