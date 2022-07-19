import os
from typing import List, Tuple

from pycylon import DataFrame, CylonEnv
from pycylon.net import MPIConfig
import argparse
import pandas as pd 
import numpy as np
import time
import gc
from numpy.random import default_rng
from mpi4py import MPI

from experiment import CylonExperiment


parser = argparse.ArgumentParser(description='run cylon join')
parser.add_argument('-r', dest='rows', type=int, required=True)
# parser.add_argument('-w', dest='world', type=int, required=True)
parser.add_argument('-i', dest='it', type=int, required=True)
parser.add_argument('-o', dest='out', type=str, required=True)
parser.add_argument('-u', dest='unique', type=float, default=1.0, help="unique factor")
parser.add_argument('-a', dest='algo', type=str, default="sort")
parser.add_argument('-c', dest='comm', type=str, default="mpi")


script = os.path.basename(__file__).replace('.py', '')

args = vars(parser.parse_args())

class JoinExp(CylonExperiment):
    def __init__(self. args) -> None:
        super().__init__('join', args)

    def tag(self, args) -> str:
        return f"a={args['algo']} u={args['unique']} c={args['comm']}"

    def generate_data(self, rng, tot_rows, world_sz, cols, unique_fac):
        data = super().generate_data(rng, tot_rows, world_sz, cols, unique_fac) 
        data1 = super().generate_data(rng, tot_rows, world_sz, cols, unique_fac) 
        # print(f"data generated", flush=True)

        return [data, data1]

    def experiment(self, env, data) -> Tuple[int, float]:
        df1 = DataFrame(data[0])
        df2 = DataFrame(data[1])
        # print(f"df loaded", flush=True)
        env.barrier()

        t1 = time.time()
        df3 = df1.merge(df2, on=[0], algorithm=args['algo'], env=env)
        env.barrier()
        t2 = time.time()

        l_len = len(df3)
        del df1
        del df2
        del df3

        return l_len, (t2 - t1) * 1000

exp = JoinExp(args)
exp.run(args)
exp.finalize()
