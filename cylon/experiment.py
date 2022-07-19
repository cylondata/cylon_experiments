from abc import abstractmethod
import gc
import os
from typing import List, Tuple, Union
from numpy.random import default_rng

from pycylon.net import MPIConfig
from pycylon.net.gloo_config import GlooMPIConfig
from pycylon.net.ucx_config import UCXConfig
from pycylon import CylonEnv

from mpi4py import MPI
import pandas as pd
import numpy as np


class CylonExperiment:

    def __init__(self, name, args) -> None:
        self.name = name
        self.env = None

        # init env
        self.reinitialize_env(args)

        rank = self.env.rank
        w = self.env.world_size
        global_r = args['rows']
        cols = 2

        #generate data 
        rng = default_rng(seed=rank)
        self.data = self.generate_data(rng, global_r, w, cols, args['unique'])

    def reinitialize_env(self, args):        
        com = args['comm']
        if com == 'mpi':
            config = MPIConfig()
        elif com == 'gloo':
            config = GlooMPIConfig()
        elif com == 'ucx':
            config = UCXConfig()
        else:
            raise ValueError('unsupported comm ' + self.comm)

        self.env = CylonEnv(config)

    def generate_data(self,
                      rng,
                      tot_rows,
                      world_sz,
                      cols=2,
                      unique_fac=1) -> Union[pd.DataFrame, List[pd.DataFrame]]:
        rows = int(tot_rows / world_sz)

        max_val = int(tot_rows * unique_fac)
        return pd.DataFrame(rng.integers(0, max_val,
                                         size=(rows, cols))).add_prefix('col')

    @abstractmethod
    def experiment(self, env, data) -> Tuple[int, float]:
        pass

    @abstractmethod
    def tag(self, args) -> str:
        pass

    def finalize(self):
        self.env.finalize()

    def run(self, args):
        rank = self.env.rank
        w = self.env.world_size

        t = self.tag(args)
        it = args['it']
        out = args['out']

        global_r = args['rows']

        timing = {
            'rows': [],
            'world': [],
            'it': [],
            'time': [],
            'tag': [],
            'out': []
        }

        # print("rank ", rank, flush=True)
        comm = MPI.COMM_WORLD

        try:
            for i in range(it):
                out_len, tm = self.experiment(self.env, self.data)
                self.env.barrier()

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
