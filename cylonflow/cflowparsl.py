import argparse

from parsl.config import Config
from cylonflow.parsl.executor import CylonEnvExecutor
from parsl.providers import LocalProvider

from cylon_experiments.cylonflow import CFlowRunner


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


class ParslRunner(CFlowRunner):
    executors: CylonEnvExecutor
    config: Config

    def __init__(self, world_size, name, experiment_cls, args, tag):
        super().__init__(world_size, name, None, args, tag)

    def initialize_executor(self):
        pass

    def shutdown(self):
        self.executor.shutdown()


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


def run_parsl(cylon_app, args, name, tag):
    print('args', args)

    row_cases = args.pop('row_cases')
    world_cases = args.pop('world_cases')

    for r in row_cases:
        for w in world_cases:
            print(f'----------------- starting {name} case {r} {w}')

            executor = CylonEnvExecutor(
                label="cylon_parsl",
                address="127.0.0.1",
                ranks_per_node=7,
                worker_debug=True,
                heartbeat_threshold=10,
                # hostfile="nodes.txt"
                hostfile=host_file_txt,
                mpi_params="--oversubscribe",
                provider=LocalProvider(),
            )
            config: Config

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
