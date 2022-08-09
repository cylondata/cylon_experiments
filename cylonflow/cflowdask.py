import argparse
import os
from pathlib import Path

from cylon_experiments.cylonflow import CFlowRunner
from cylonflow.dask.executor import CylonDaskExecutor


def get_generic_args(description):
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('-w', dest='world_sz', type=int, required=True)
    parser.add_argument('-r', dest='rows', type=int, required=True)
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


def run_dask(args, config, experiment_cls, name, tag):
    home = str(Path.home())
    sched_file = os.path.join(home, 'sched.json')
    print('dask scheduler file', sched_file)
    print('args', args)

    dask_runner = DaskRunner(args['world_sz'], name, config, experiment_cls, args,
                             scheduler_file=sched_file, tag=tag)
    dask_runner.run()
