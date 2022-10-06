import argparse
import math
import os
import shutil
import subprocess
import sys
import tempfile
import traceback

import ray
from cylon_experiments.cylonflow import CFlowRunner

from cylonflow.api.config import GlooFileStoreConfig
from cylonflow.ray.executor import CylonRayExecutor

python_exec = sys.executable
prefix = sys.prefix
home_dir = os.path.expanduser("~")
temp_dir = tempfile.gettempdir()
log_dir = '/scratch_hdd/dnperera1/ray/'

ray_exec = f"{prefix}/bin/ray"
ray_pw = '1234'

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


def start_ray(procs, nodes):
    global fds

    os.makedirs(log_dir, exist_ok=True)

    fds = [open(f'{log_dir}/scheduler.log', mode='a')]
    print("starting head", flush=True)
    q = f"ssh {sched_node} {ray_exec} start --head --port=6379 --node-ip-address={sched_node} " \
        f"--redis-password={ray_pw} --num-cpus=0" #{min(2, procs)}
    subprocess.run(q, stdout=fds[-1], stderr=fds[-1], shell=True, check=True)

    print(f"starting workers with {procs} cores pre node", flush=True)
    for ip in worker_nodes[0:nodes]:
        fds.append(open(f'{log_dir}/worker-{ip}.log', mode='a'))
        # print(f"starting worker {ip}", flush=True)
        q = f"ssh {ip} {ray_exec} start --address=\'{sched_node}:6379\' --node-ip-address={ip} " \
            f"--redis-password={ray_pw} --num-cpus={procs}"
        # print(f"running: {query}", flush=True)
        subprocess.run(q, stdout=fds[-1], stderr=fds[-1], shell=True, check=True)


def stop_ray():
    global fds
    ray.shutdown()

    print("stopping workers", flush=True)
    for ip in worker_nodes:
        subprocess.run(f"ssh {ip} {ray_exec} stop -f",
                       stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)

    print("stopping head", flush=True)
    subprocess.run(f"ssh {sched_node} {ray_exec} stop -f",
                   stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True)

    for fd in fds:
        fd.close()
    fds = []


def get_generic_args(description):
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('-R', dest='row_cases', type=int,
                        required=True, nargs='+')
    parser.add_argument('-W', dest='world_cases',
                        type=int, required=True, nargs='+')
    parser.add_argument('-i', dest='it', type=int, required=True)
    parser.add_argument('-o', dest='out', type=str, required=True)
    parser.add_argument('-u', dest='unique', type=float,
                        default=1.0, help="unique factor")
    parser.add_argument('--cols', dest='cols', type=int, default=2)
    parser.add_argument('-c', dest='comm', type=str,
                        default='gloo', choices=['gloo'])

    return parser


class RayRunner(CFlowRunner):
    def __init__(self, world_size, name, config, experiment_cls, args, tag='') -> None:
        self.config = config
        super().__init__(world_size, name, experiment_cls, args, tag=tag)

    def initialize_executor(self):
        self.executor = CylonRayExecutor(self.world_size, self.config)
        self.executor.start(self.experiment_cls, executable_args=[self.args])

    def shutdown(self):
        self.executor.shutdown()
        del self.executor


def run_ray(args, experiment_cls, name, tag):
    print('args', args)

    row_cases = args.pop('row_cases')
    world_cases = args.pop('world_cases')

    config = GlooFileStoreConfig()
    config.tcp_iface = 'enp175s0f0'
    config.file_store_path = f'/N/u/d/dnperera/gloo'
    config.timeout = 180000

    for r in row_cases:
        for w in world_cases:
            print(f'----------------- starting {name} case {r} {w}')
            runner = None
            try:
                stop_ray()

                procs = int(math.ceil(w / TOTAL_NODES))
                start_ray(procs, min(w, TOTAL_NODES))

                if os.path.exists(config.file_store_path):
                    shutil.rmtree(config.file_store_path)
                os.makedirs(config.file_store_path)

                print(f"world sz {w} procs per worker {procs} iter {args['it']}", flush=True)

                args['rows'] = r
                args['world_sz'] = w

                ray.init(address=f'{sched_node}:6379', _redis_password=ray_pw)

                print('resources:', ray.available_resources())

                runner = RayRunner(args['world_sz'], name, config, experiment_cls, args, tag=tag)
                runner.run()

                print(f'----------------- {name} complete case {r} {w}')

            except Exception:
                traceback.print_exception(*sys.exc_info())
                print(f'----------------- {name} case {r} {w} ERROR occured!')
            finally:
                runner.shutdown()
                ray.shutdown()
                del runner
                stop_ray()

                if os.path.exists(config.file_store_path):
                    shutil.rmtree(config.file_store_path)
