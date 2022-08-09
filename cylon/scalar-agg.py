import gc
import time
from typing import Tuple

from pycylon import DataFrame

from cylon_experiments.experiment import get_generic_args, CylonExperiment, execute_experiment

parser = get_generic_args(description='run cylon scalar')

args = vars(parser.parse_args())


class ScalarAggExp(CylonExperiment):
    def __init__(self, args) -> None:
        super().__init__('scalar-agg', args)

    def experiment(self, env, data) -> Tuple[int, float]:
        df1 = DataFrame(data)

        t1 = time.time()
        # sum is not exopsed in df :(
        tb3 = df1.to_table().sum(0)
        tb4 = df1.to_table().sum(1)
        env.barrier()
        t2 = time.time()

        l_len = (len(tb3) == 1) and (len(tb4) == 1)
        del df1
        del tb3
        del tb4
        gc.collect()

        return l_len, (t2 - t1) * 1000

    def tag(self, args) -> str:
        return f"u={args['unique']} c={args['comm']}"


execute_experiment(ScalarAggExp, args)
