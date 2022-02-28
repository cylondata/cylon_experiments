import os
import time
import argparse
import math
import os 
import gc
import numpy as np
import pandas as pd
from numpy.random import default_rng

from util import start_cluster, stop_cluster, TOTAL_NODES, MAX_PROCS, HEAD_IP, RAY_PW, SCHED_IP

parser = argparse.ArgumentParser(description='generate random data')

# parser.add_argument('-s', dest='scale', type=str, help='number of rows', required=True)
parser.add_argument('-w', dest='world', type=int, help='processes', required=True, nargs='+')
parser.add_argument('-r', dest='rows', type=int, help='number of rows', required=True, nargs='+')
parser.add_argument('-i', dest='it', type=int, help='iterations', default=1)
parser.add_argument('-u', dest='unique', type=float, default=1.0, help="unique factor")

args = parser.parse_args()
args = vars(args)
print(args, flush=True)

engine = 'ray'
world = args['world']
rows = args['rows']
it = args['it']

script = os.path.basename(__file__).replace('.py', '')


if __name__ == "__main__":
    os.environ["MODIN_ENGINE"] = engine

    for r in rows:
        max_val = r * args['unique']
        rng = default_rng()
        frame_data = rng.integers(0, max_val, size=(r, 2)) 
        # frame_data1 = rng.integers(0, max_val, size=(r, 2)) 
        print(f"data generated", flush=True)
        
        for w in world:
            procs = int(math.ceil(w / TOTAL_NODES))
            print(f"world sz {w} procs per worker {procs} iter {it}", flush=True)

            assert procs <= MAX_PROCS

            timing = {'rows': [], 'world':[], 'it':[], 'time':[]}

            try:
                stop_cluster(engine)
                start_cluster(engine, procs, min(w, TOTAL_NODES))
                
                import ray
                ray.init(address=f'{HEAD_IP}:6379', _redis_password=RAY_PW, _node_ip_address=HEAD_IP)

            
                for i in range(it):

                    pdf_l = pd.DataFrame(frame_data).add_prefix("col")
                    # pdf_r = pd.DataFrame(frame_data1).add_prefix("col")

                    df_l = ray.data.from_pandas(pdf_l).repartition(w)
                    # df_r = ray.data.from_pandas(pdf_r).repartition(w)

                    print(f"data loaded", flush=True)


                    t1 = time.time()
                    out = df_l.sum()
                    t2 = time.time()

                    # timing = {'rows': [], 'world':[], 'it':[], 'time':[]}

                    timing['rows'].append(r)
                    timing['world'].append(w)
                    timing['it'].append(i)
                    timing['time'].append((t2 - t1) * 1000)
                    print(f"timings {r} {w} {i} {(t2 - t1) * 1000:.0f} ms, {out}", flush=True)
                    
                    del pdf_l 
                    del df_l 
                    del out 
                    gc.collect()

                ray.shutdown()
            finally:
                stop_cluster(engine)
                pd.DataFrame(timing).to_csv(f'{script}.csv', mode='a', index=False, header=False)
