import argparse
import math
import os
import subprocess
import sys
import tempfile
import time
import traceback

from cylon_experiments.cylonflow import CFlowRunner

from cylonflow.dask.executor import CylonDaskExecutor

python_exec = sys.executable
prefix = sys.prefix
home_dir = os.path.expanduser("~")
temp_dir = tempfile.gettempdir()

sched_file = f"{home_dir}/sched.json"
dask_sched_exec = f"{prefix}/bin/dask-scheduler"
dask_worker_exec = f"{prefix}/bin/dask-worker"

sched_node = 'v-001'
worker_nodes = [
    'v-002',
    'v-003',
    'v-004',
    'v-005',
    'v-006',
    'v-007',
    'v-008',
    'v-009',
    'v-010',
    'v-011',
    'v-012',
    'v-013',
    'v-014',
    'v-015'
]

TOTAL_NODES = len(worker_nodes)
MAX_PROCS = 40
TOTAL_MEM = 240

fds = []


def start_dask(procs, nodes):
    global fds

    os.makedirs('/scratch_hdd/dnperera1/dask', exist_ok=True)
    
    fds = [open(f'/scratch_hdd/dnperera1/dask/scheduler.log', mode='a')]
    print("starting scheduler", flush=True)
    # q = f"{DASK_SCHED} --interface enp175s0f0 --scheduler-file {SCHED_FILE}"
    q = ["ssh", sched_node, dask_sched_exec, "--interface", "enp175s0f0", "--scheduler-file", sched_file]
    subprocess.Popen(q, stdout=fds[-1], stderr=fds[-1])

    time.sleep(2)

    print("starting workers", flush=True)
    for ip in worker_nodes[0:nodes]:
        fds.append(open(f'/scratch_hdd/dnperera1/dask/worker-{ip}.log', mode='a'))
        # q = f"ssh {ip} {DASK_WORKER} v-001:8786 --interface enp175s0f0 --nthreads 1 --nprocs {str(procs)} \
        #         --local-directory /scratch/dnperera/dask/ --scheduler-file {SCHED_FILE}"
        q = ["ssh", ip, dask_worker_exec, f"{sched_node}:8786", "--interface", "enp175s0f0",
             "--nthreads", "1", "--nworkers", str(procs), f"--memory-limit=\"{int(TOTAL_MEM / procs)} GiB\"",
             "--local-directory", "/scratch_hdd/dnperera1/dask/", "--scheduler-file", sched_file]
        # print(f"running {' '.join(q)}", flush=True)
        subprocess.Popen(q, stdout=fds[-1], stderr=fds[-1])

    time.sleep(2)


def stop_dask():
    global fds 

    print("stopping dask", flush=True)
    for ip in worker_nodes:
        # print("stopping worker", ip, flush=True)
        q = f"ssh {ip} pkill -f dask-worker"
        # print(f"running {' '.join(q)}", flush=True)
        subprocess.run(q, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)

    # print("stopping scheduler", flush=True)
    subprocess.run(f"ssh {sched_node} pkill -f dask-scheduler", stdout=subprocess.PIPE,
                   stderr=subprocess.STDOUT, shell=True)
    time.sleep(3)

    for fd in fds:
        fd.close()    
    fds = []


def get_generic_args(description):
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('-R', dest='row_cases', type=int, required=True, nargs='+')
    parser.add_argument('-W', dest='world_cases', type=int, required=True, nargs='+')
    # parser.add_argument('-w', dest='world_sz', type=int, required=True)
    # parser.add_argument('-r', dest='rows', type=int, required=True)
    parser.add_argument('-i', dest='it', type=int, required=True)
    parser.add_argument('-o', dest='out', type=str, required=True)
    parser.add_argument('-u', dest='unique', type=float, default=1.0, help="unique factor")
    parser.add_argument('--cols', dest='cols', type=int, default=2)
    parser.add_argument('-c', dest='comm', type=str, default='gloo', choices=['gloo'])

    return parser


class DaskRunner(CFlowRunner):
    def __init__(self, world_size, name, config, experiment_cls, args, address=None,
                 scheduler_file=None, tag='') -> None:
        self.config = config
        self.address = address
        self.scheduler_file = scheduler_file
        super().__init__(world_size, name, experiment_cls, args, tag=tag)

    def initialize_executor(self):
        self.executor = CylonDaskExecutor(self.world_size, self.config, address=self.address,
                                          scheduler_file=self.scheduler_file)

        self.executor.start(self.experiment_cls, executable_args=[self.args])

    def shutdown(self):
        self.executor.shutdown()


def run_dask(args, config, experiment_cls, name, tag):
    print('dask scheduler file', sched_file)
    print('args', args)

    row_cases = args.pop('row_cases')
    world_cases = args.pop('world_cases')

    config.timeout = 1800

    for r in row_cases:
        for w in world_cases:
            print(f'----------------- starting {name} case {r} {w}')
            dask_runner = None
            try:
                stop_dask()

                procs = int(math.ceil(w / TOTAL_NODES))
                start_dask(procs, min(w, TOTAL_NODES))

                print(f"world sz {w} procs per worker {procs} iter {args['it']}", flush=True)

                args['rows'] = r
                args['world_sz'] = w

                dask_runner = DaskRunner(args['world_sz'], name, config, experiment_cls, args,
                                         scheduler_file=sched_file, tag=tag)
                dask_runner.run()
                
                print(f'----------------- {name} complete case {r} {w}')
            
            except Exception:
                traceback.print_exception(*sys.exc_info())
                print(f'----------------- {name} case {r} {w} ERROR occured!')
            finally:
                dask_runner.shutdown()
                del dask_runner
                # stop_dask()
