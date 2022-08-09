from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Tuple

import pandas as pd
from numpy.random import default_rng
from pycylon import CylonEnv


@dataclass
class ExperimentArgs:
    rows: int
    out: str

    comm: str = 'gloo'
    cols: int = 2
    unique: float = 1.0
    it: int = 1


class CFlowExperiment(ABC):
    args: ExperimentArgs = None

    def __init__(self, args: ExperimentArgs) -> None:
        self.args = args

    def generate_data(self, rng, tot_rows, world_sz, cols=2, unique_fac=1) -> Any:
        rows = int(tot_rows / world_sz)

        max_val = int(tot_rows * unique_fac)
        return pd.DataFrame(rng.integers(0, max_val,
                                         size=(rows, cols))).add_prefix('col')

    @abstractmethod
    def tag(self) -> str:
        raise NotImplementedError()

    @abstractmethod
    def experiment(self, env, data) -> Tuple[int, float]:
        raise NotImplementedError()

    def run(self, env: CylonEnv = None):
        rank = env.rank
        w = env.world_size
        global_r = self.args.rows
        cols = self.args.cols

        # generate data
        rng = default_rng(seed=rank)
        data = self.generate_data(rng, global_r, w, cols, self.args['unique'])

        t = self.tag()

        timing = {
            'rows': [],
            'world': [],
            'it': [],
            'time': [],
            'tag': [],
            'out': []
        }

        try:
            for i in range(self.args.it):
                out_len, tm = self.experiment(env, data)
                env.barrier()

                send = np.array([out_len, tm], np.float64)
                sums = np.array([0, 0], np.float64)
                comm.Reduce([send, MPI.DOUBLE], [sums, MPI.DOUBLE])

                if rank == 0:
                    timing['rows'].append(global_r)
                    timing['world'].append(w)
                    timing['it'].append(i)
                    timing['time'].append(sums[1] / w)
                    timing['tag'].append(t)
                    timing['out'].append(int(sums[0]))

                    print(f"{i} done", flush=True)

                gc.collect()
                self.env.barrier()
        finally:
            if rank == 0:
                file_path = f"{out}/{self.name}.csv"
                if os.path.exists(file_path):
                    pd.DataFrame(timing).to_csv(file_path,
                                                mode='a',
                                                index=False,
                                                header=False)
                else:
                    pd.DataFrame(timing).to_csv(file_path,
                                                index=False,
                                                header=True)
