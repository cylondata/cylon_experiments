import time
from typing import Tuple, Any

import numpy as np
from pycylon import DataFrame

from cylon_experiments.experiment import CylonExperiment, get_generic_args, execute_experiment

parser = get_generic_args(description='run cylon scalar')

args = vars(parser.parse_args())


class ScalarExp(CylonExperiment):

    def __init__(self, args) -> None:
        super().__init__('scalar', args)

    def generate_data(self, rng, tot_rows, world_sz, cols=2, unique_fac=1) -> Any:
        df = super().generate_data(rng, tot_rows, world_sz, cols, unique_fac)
        val = np.random.randint(0, len(df))

        return df, val

    def experiment(self, env, data) -> Tuple[int, float]:
        df1 = DataFrame(data[0])
        val = data[1]

        t1 = time.time()
        df2 = df1 + val
        env.barrier()
        t2 = time.time()

        l_len = len(df2)
        del df1
        del df2

        return l_len, (t2 - t1) * 1000

    def tag(self, args) -> str:
        return f"u={args['unique']} c={args['comm']}"


execute_experiment(ScalarExp, args)
