import os
import time
import argparse
import math
import os 
import gc
import numpy as np
import pandas as pd

from numpy.random import default_rng

from pyspark.sql import SparkSession


parser = argparse.ArgumentParser(description='generate random data')

# parser.add_argument('-r', dest='rows', type=int, help='number of rows', required=True, nargs='+')
parser.add_argument('-w', dest='world', type=int, help='processes', required=True, nargs='+')
parser.add_argument('-i', dest='it', type=int, help='iterations', default=1)
# parser.add_argument('-u', dest='unique', type=float, default=1.0, help="unique factor")


args = parser.parse_args()
args = vars(args)
print(args, flush=True)

# rows = args['rows']
it = args['it']
script = os.path.basename(__file__).replace('.py', '')
world = args['world']

TOTAL_MEM = 240
TOTAL_NODES = 14

f0 = '/N/u/d/dnperera/data/cylon/df0_512.parquet'
f1 = '/N/u/d/dnperera/data/cylon/df1_512.parquet'
r = 1000000000


if __name__ == "__main__":
    # for r in rows:
        # max_val = r * args['unique']
        # rng = default_rng()
        # frame_data = rng.integers(0, max_val, size=(r, 2)) 
        # frame_data1 = rng.integers(0, max_val, size=(r, 2)) 
        # print(f"data generated", flush=True)

        # df_l = pd.DataFrame(frame_data).add_prefix("col")
        # df_r = pd.DataFrame(frame_data1).add_prefix("col")
        # print(f"data loaded", flush=True)
        
    for w in world:
        procs = int(math.ceil(w / TOTAL_NODES))
        # mem = int(TOTAL_MEM*0.75/procs)
        mem = int(TOTAL_MEM*0.9)
        print(f"world sz {w} procs per worker {procs} mem {mem} iter {it}", flush=True)

        timing = {'rows': [], 'world':[], 'it':[], 'time':[]}

        # spark = SparkSession\
        #     .builder\
        #     .appName(f'join {r} {w}')\
        #     .master('spark://v-001:7077')\
        #     .config('spark.executor.memory', f'{int(mem*0.6)}g')\
        #     .config('spark.executor.pyspark.memory', f'{int(mem*0.4)}g')\
        #     .config('spark.executor.pyspark.memory', f'{int(mem*0.4)}g')\
        #     .config('spark.cores.max', w)\
        #     .config('spark.driver.memory', '100g')\
        #     .getOrCreate()
            # .config('spark.executor.cores', '1')\
            # .config('spark.executor.instances', f'{w}')\
        spark = SparkSession\
            .builder\
            .appName(f'join {r} {w}')\
            .master('spark://v-001:7077')\
            .config('spark.driver.memory', '100g')\
            .config('spark.executor.memory', f'{int(mem*0.6)}g')\
            .config('spark.executor.pyspark.memory', f'{int(mem*0.4)}g')\
            .config('spark.rpc.message.maxSize', 2047)\
            .config('spark.cores.max', w)\
            .config('spark.sql.execution.arrow.pyspark.enabled', 'true')\
            .getOrCreate()


        sdf0 = spark.read.parquet(f0).repartition(w).cache()
        sdf1 = spark.read.parquet(f1).repartition(w).cache()
        print(f"data loaded to spark {sdf0.count()} {sdf1.count()}", flush=True)

        try:           
            for i in range(it):
                t1 = time.time()
                out = sdf0.join(sdf1, on='col0', how='inner')
                count = out.count()
                t2 = time.time()

                timing['rows'].append(r)
                timing['world'].append(w)
                timing['it'].append(i)
                timing['time'].append((t2 - t1) * 1000)
                print(f"timings {r} {w} {i} {(t2 - t1) * 1000:.0f} ms, {count}", flush=True)
                
                del out
                del count
                gc.collect()
        finally:
            file_path = f'spark_{script}.csv'
            if os.path.exists(file_path):
                pd.DataFrame(timing).to_csv(file_path, mode='a', index=False, header=False)
            else:
                pd.DataFrame(timing).to_csv(file_path, mode='w', index=False, header=True)
            
            spark.stop()
