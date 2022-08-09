from cylon_experiments.cylonflow import CFlowRunner
from cylonflow.dask.executor import CylonDaskExecutor


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
