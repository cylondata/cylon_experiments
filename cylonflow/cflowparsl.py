import argparse
import time

import parsl
from parsl.config import Config
from parsl.providers import LocalProvider

from cylonflow.parsl.executor import CylonEnvExecutor

from cylon_experiments.cylonflow import CFlowRunner


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
                        default='mpi', choices=['mpi', 'ucx'])

    return parser


host_file_txt = """v-001 slots=40
v-002 slots=40
v-003 slots=40
v-004 slots=40
v-005 slots=40
v-006 slots=40
v-007 slots=40
v-008 slots=40
v-009 slots=40
v-010 slots=40
v-011 slots=40
v-012 slots=40
v-013 slots=40
v-014 slots=40
v-015 slots=40
"""

mpi_params = "--mca btl \"vader,tcp,openib,self\" "\
    "--mca btl_tcp_if_include enp175s0f0 "\
    "--mca btl_openib_allow_ib 1 " \
    "--mca mpi_preconnect_mpi 1 "\
    "--map-by node --bind-to core --bind-to socket "


class ParslRunner(CFlowRunner):
    def __init__(self, world_size, name, cylon_app, args, tag='') -> None:
        super().__init__(world_size, name, None, args, tag)
        self.cylon_app = cylon_app

    def initialize_executor(self):
        self.cylon_exec = CylonEnvExecutor(
            comm_type=self.args['comm'],
            label="cylon_parsl",
            address="172.29.200.201",
            ranks_per_node=self.world_size + 1,
            worker_debug=True,
            heartbeat_threshold=3600,
            hostfile=host_file_txt,
            mpi_params=mpi_params,
            provider=LocalProvider(),
        )

        self.config = Config(executors=[self.cylon_exec],
                             run_dir='/tmp/parsl')
        
        parsl.load(self.config)

    def execute_experiment(self):
        result = self.cylon_app(self.args).result()
        return result.payloads

    def shutdown(self):
        self.cylon_exec.shutdown()
        parsl.clear()
        del self.config, self.cylon_exec

        time.sleep(1)  # let executor shutdown cluster


def run_parsl(cylon_app, args, name, tag):
    print('args', args)

    row_cases = args.pop('row_cases')
    world_cases = args.pop('world_cases')

    for r in row_cases:
        for w in world_cases:
            print(f'----------------- starting {name} case {r} {w}')

            parsl_runner = None
            try:
                args['rows'] = r
                args['world_sz'] = w

                parsl_runner = ParslRunner(w, name, cylon_app, args, tag)
                parsl_runner.run()

                print(f'----------------- {name} complete case {r} {w}')
            finally:
                parsl_runner.shutdown()
