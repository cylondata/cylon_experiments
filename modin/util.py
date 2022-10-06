import os
from platform import node
import subprocess
import sys
import time

python_exec = sys.executable
prefix = sys.prefix
home_dir = os.path.expanduser("~")

TOTAL_NODES = 14
MAX_PROCS = 40
TOTAL_MEM = 240
RAY_PW = '1234'
RAY_EXEC = f"{prefix}/bin/ray"
HEAD_IP="v-001"



DASK_SCHED = f"{prefix}/bin/dask-scheduler"
SCHED_FILE = f"{home_dir}/sched.json"
DASK_WORKER = f"{prefix}/bin/dask-worker"
SCHED_IP = "v-001"

nodes_file = "nodes.txt"
ips = []

with open(nodes_file, 'r') as fp:
    for l in fp.readlines():
        ips.append(l.split(' ')[0])

assert len(ips) == TOTAL_NODES

def start_ray(procs, nodes):
    print("starting head", flush=True)
    query = f"ssh {HEAD_IP} {RAY_EXEC} start --head --port=6379 --node-ip-address={HEAD_IP} --redis-password={RAY_PW} \
        --num-cpus={min(2, procs)}  --object-store-memory={int(240/2*0.9*10**9)}"           
    # print(f"running: {query}", flush=True)
    subprocess.run(query, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True, check=True)


    for ip in ips[0:nodes]:
        # print(f"starting worker {ip}", flush=True)
        query = f"ssh {ip} {RAY_EXEC} start --address=\'{HEAD_IP}:6379\' --node-ip-address={ip} --redis-password={RAY_PW} \
            --num-cpus={procs} --object-store-memory={int(240/2*0.9*10**9)}"
        # print(f"running: {query}", flush=True)
        subprocess.run(query, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True, check=True)



def stop_ray():
    import ray
    ray.shutdown()

    print("stopping workers", flush=True)
    for ip in ips:
        subprocess.run(f"ssh {ip} {RAY_EXEC} stop -f", stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)
    
    
    print("stopping head", flush=True)
    subprocess.run(f"{RAY_EXEC} stop -f", stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True) 
    


def start_dask(procs, nodes):
    print("starting dask", flush=True)
    q = ["ssh", SCHED_IP, DASK_SCHED, "--interface", "enp175s0f0", "--scheduler-file", SCHED_FILE]
    subprocess.Popen(q, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
   
    time.sleep(3)   

    
    for ip in ips[0:nodes]:
        # print("starting worker", ip, flush=True)
        q = ["ssh", ip, DASK_WORKER, f"{SCHED_IP}:8786", "--interface", "enp175s0f0", \
                "--nthreads", "1", "--nprocs", str(procs), f"--memory-limit=\"{int(TOTAL_MEM)} GiB\"", \
                "--local-directory", "/scratch_hdd/dnperera1/dask/", "--scheduler-file", SCHED_FILE]
        # print(f"running {' '.join(q)}", flush=True)
        subprocess.Popen(q, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    
    time.sleep(3)


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


def start_cluster(engine, procs, nodes):
    if engine == 'ray':
        start_ray(procs, nodes)
    elif engine == 'dask':
        start_dask(procs, nodes)
    else:
        raise Exception(f"{engine} not supported")


def stop_cluster(engine):
    if engine == 'ray':
        stop_ray()
    elif engine == 'dask':
        stop_dask()
    else:
        raise Exception(f"{engine} not supported")