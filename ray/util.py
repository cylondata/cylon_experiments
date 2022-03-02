import argparse
import math
import subprocess
from sys import prefix
import time

TOTAL_NODES = 14
MAX_PROCS = 40
TOTAL_MEM = 240
RAY_PW = '1234'
# RAY_EXEC = "/N/u2/d/dnperera/victor/MODIN/bin/ray"
RAY_EXEC = "/N/u2/d/dnperera/victor/modin_env/bin/ray"
HEAD_IP="v-001"

DASK_SCHED = "/N/u2/d/dnperera/victor/modin_env/bin/dask-scheduler"
SCHED_FILE = "/N/u2/d/dnperera/dask-sched.json"
DASK_WORKER = "/N/u2/d/dnperera/victor/modin_env/bin/dask-worker"
SCHED_IP = "v-001"

nodes_file = "nodes.txt"
ips = []

with open(nodes_file, 'r') as fp:
    for l in fp.readlines():
        ips.append(l.split(' ')[0])

assert len(ips) == TOTAL_NODES

def start_ray(procs, nodes, head, prefix):
    ray_exec = f'{prefix}/bin/ray' if prefix else RAY_EXEC
    if not head:
        head = HEAD_IP

    print("starting head", flush=True)
    query = f"ssh {head} {ray_exec} start --head --port=6379 --node-ip-address={head} --redis-password={RAY_PW} \
        --num-cpus={min(2, procs)}"           
    print(f"running: {query}", flush=True)
    subprocess.run(query, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True, check=True)

    time.sleep(3)

    for ip in ips[0:nodes]:
        # print(f"starting worker {ip}", flush=True)
        query = f"ssh {ip} {ray_exec} start --address=\'{head}:6379\' --node-ip-address={ip} --redis-password={RAY_PW} \
            --num-cpus={procs}"
        print(f"running: {query}", flush=True)
        subprocess.run(query, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True, check=True)

    time.sleep(3)


def stop_ray(head, prefix):
    ray_exec = f'{prefix}/bin/ray' if prefix else RAY_EXEC
    if not head:
        head = HEAD_IP

    import ray
    ray.shutdown()

    print("stopping workers", flush=True)
    for ip in ips:
        subprocess.run(f"ssh {ip} {ray_exec} stop -f", stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)
    
    time.sleep(3)
    
    print("stopping head", flush=True)
    subprocess.run(f"ssh {head} {ray_exec} stop -f", stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True) 
    
    time.sleep(3)


def start_dask(procs, nodes):
    print("starting dask", flush=True)
    q = ["ssh", SCHED_IP, DASK_SCHED, "--interface", "enp175s0f0", "--scheduler-file", SCHED_FILE]
    subprocess.Popen(q, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
   
    time.sleep(3)   

    
    for ip in ips[0:nodes]:
        # print("starting worker", ip, flush=True)
        q = ["ssh", ip, DASK_WORKER, f"{ips[0]}:8786", "--interface", "enp175s0f0", \
                "--nthreads", "1", "--nprocs", str(procs), f"--memory-limit=\"{int(TOTAL_MEM/procs)} GiB\"", \
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


def start_cluster(engine, procs, nodes, head, prefix):
    if engine == 'ray':
        start_ray(procs, nodes, head, prefix)
    elif engine == 'dask':
        start_dask(procs, nodes)
    else:
        raise Exception(f"{engine} not supported")


def stop_cluster(engine, head, prefix):
    if engine == 'ray':
        stop_ray(head, prefix)
    elif engine == 'dask':
        stop_dask()
    else:
        raise Exception(f"{engine} not supported")

def cluster_status(engine, head, prefix):
    if engine== 'ray':
        ray_exec = f'{prefix}/bin/ray' if prefix else RAY_EXEC
        if not head:
            head = HEAD_IP
        p = subprocess.run(f"ssh {head} {ray_exec} status --redis_password={RAY_PW}", stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True, text=True) 
        print(p.stdout)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='manage ray')


    parser.add_argument('-n', dest='start', type=int, help='processes', required=False, default=0)
    parser.add_argument('-e', dest='engine', type=str, help='engine', required=False, default='ray')
    parser.add_argument('--head', dest='head', type=str, help='head ip', required=False, default=HEAD_IP)
    parser.add_argument('--prefix', dest='prefix', type=str, help='exec prefix', required=False, default=None)
    parser.add_argument('--stop', dest='stop', help='stop', required=False, default=False, action='store_true')
    parser.add_argument('--status', dest='status', help='status', required=False, default=False, action='store_true')

    
    args = parser.parse_args()
    args = vars(args)

    head = args['head']
    prefix = args['prefix']

    if args['start']:
        w = args['start']
        procs = int(math.ceil(w / TOTAL_NODES))
        start_cluster(args['engine'], procs, min(w, TOTAL_NODES), head, prefix)
    elif args['stop']:
        stop_cluster(args['engine'], head, prefix)
    elif args['status']:
        cluster_status(args['engine'], head, prefix)
