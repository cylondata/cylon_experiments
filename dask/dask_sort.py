import os
import sys

import time
import argparse
import math
import subprocess
import gc
import traceback
import numpy as np
import pandas as pd 
from numpy.random import default_rng
from dask.dataframe import from_pandas, read_parquet
from dask.distributed import Client, wait

parser = argparse.ArgumentParser(description='generate random data')

# parser.add_argument('-s', dest='scale', type=str, help='number of rows', required=True)
parser.add_argument('-w', dest='world', type=int, help='processes', required=True, nargs='+')
parser.add_argument('-r', dest='rows', type=int, help='number of rows', required=True, nargs='+')
parser.add_argument('-i', dest='it', type=int, help='iterations', default=1)
parser.add_argument('-u', dest='unique', type=float, default=1.0, help="unique factor")

args = parser.parse_args()
args = vars(args)
print(args, flush=True)

# scale = args['scale']
world = args['world']
rows = args['rows']
it = args['it']

TOTAL_NODES = 14
MAX_PROCS = 40
TOTAL_MEM = 240

python_exec = sys.executable
prefix = sys.prefix
home_dir = os.path.expanduser("~")

DASK_SCHED = f"{prefix}/bin/dask-scheduler"
SCHED_FILE = f"{home_dir}/sched.json"
DASK_WORKER = f"{prefix}/bin/dask-worker"
SCHED_IP = "v-001"


script = os.path.basename(__file__).replace('.py', '')

nodes_file = "nodes.txt"
ips = []

with open(nodes_file, 'r') as fp:
    for l in fp.readlines():
        ips.append(l.split(' ')[0])

assert len(ips) == TOTAL_NODES


def start_dask(procs, nodes):
    print("starting scheduler", flush=True)
    # q = f"{DASK_SCHED} --interface enp175s0f0 --scheduler-file {SCHED_FILE}"
    q = ["ssh", SCHED_IP, DASK_SCHED, "--interface", "enp175s0f0", "--scheduler-file", SCHED_FILE]
    subprocess.Popen(q, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    
    time.sleep(3)   

    
    for ip in ips[0:nodes]:
        # print("starting worker", ip, flush=True)
        # q = f"ssh {ip} {DASK_WORKER} v-001:8786 --interface enp175s0f0 --nthreads 1 --nprocs {str(procs)} \
        #         --local-directory /scratch/dnperera/dask/ --scheduler-file {SCHED_FILE}"
        q = ["ssh", ip, DASK_WORKER, f"{ips[0]}:8786", "--interface", "enp175s0f0", \
                "--nthreads", "1", "--nprocs", str(procs), f"--memory-limit=\"{int(TOTAL_MEM)} GiB\"", \
                "--local-directory", "/scratch_hdd/dnperera1/dask/", "--scheduler-file", SCHED_FILE]
        # print(f"running {' '.join(q)}", flush=True)
        subprocess.Popen(q, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    
    time.sleep(5)   

def stop_dask():   
    print("stopping dask", flush=True) 
    for ip in ips:
        # print("stopping worker", ip, flush=True)
        q= ["ssh", ip, "pkill", "-f", "dask-worker"]
        # print(f"running {' '.join(q)}", flush=True)
        subprocess.Popen(q, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)      
    
    time.sleep(5)           
    
    # print("stopping scheduler", flush=True)
    subprocess.Popen(["ssh", SCHED_IP, "pkill", "-f", "dask-scheduler"], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    time.sleep(3) 

    if os.path.exists(SCHED_FILE):
        os.remove(SCHED_FILE)

    
if __name__ == "__main__":
    

    for r in rows:
        max_val = r * args['unique']
        rng = default_rng()
        frame_data = rng.integers(0, max_val, size=(r, 2)) 

        pdf = pd.DataFrame(frame_data).add_prefix('col')
        print(f"data generated", flush=True)

        f0 = f'/N/u/d/dnperera/data/cylon/{r}/df0_512.parquet'
        f1 = f'/N/u/d/dnperera/data/cylon/{r}/df1_512.parquet'
        
        for w in world:
            procs = int(math.ceil(w / TOTAL_NODES))
            print(f"world sz {w} procs per worker {procs} iter {it}", flush=True)

            assert procs <= MAX_PROCS
            timing = {'rows': [], 'world':[], 'it':[], 'time':[]}

            try:
                stop_dask()
                start_dask(procs, min(w, TOTAL_NODES))

                client = Client(scheduler_file=SCHED_FILE)
                client.restart()

                # df_l = read_parquet(f0).repartition(npartitions=w)
                df_l = from_pandas(pdf, npartitions=w)
                df_l = client.persist(df_l)
                wait([df_l])
                print(f"data loaded", flush=True)
           
                for i in range(it):
                    t1 = time.time()
                    out = df_l.sort_values(by='col0')
                    out = client.persist(out)
                    wait([df_l, out])
                    count = out.shape[0].compute()
                    t2 = time.time()

                    timing['rows'].append(r)
                    timing['world'].append(w)
                    timing['it'].append(i)
                    timing['time'].append((t2 - t1) * 1000)
                    print(f"timings {r} {w} {i} {(t2 - t1) * 1000:.0f} ms, {count}", flush=True)
                    
                    client.cancel([out], asynchronous=False, force=True)
                    # client.cancel(out)
                    # gc.collect()
                
                client.cancel([df_l], asynchronous=False, force=True)
                # gc.collect()
            except Exception:
                print(f"Exception occured!")
                traceback.print_exception(*sys.exc_info())
                client.restart()
            finally:
                stop_dask()
                pd.DataFrame(timing).to_csv(f'{script}.csv', mode='a', index=False, header=False)
