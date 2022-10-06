import time
from typing import Tuple

from pycylon import DataFrame

from cylon_experiments.experiment import CylonExperiment, get_generic_args, execute_experiment


parser = get_generic_args('run cylon composite')

args = vars(parser.parse_args())


class CompositeExp(CylonExperiment):
    def __init__(self, args) -> None:
        super().__init__('composite', args)

    def tag(self, args) -> str:
        return f"u={args['unique']} c={args['comm']}"

    def generate_data(self, rng, tot_rows, world_sz, cols=2, unique_fac=1):
        data = super().generate_data(rng, tot_rows, world_sz, cols, unique_fac)
        data1 = super().generate_data(rng, tot_rows, world_sz, cols, unique_fac)
        val = rng.integers(0, tot_rows)
        return [data, data1, val]

    def experiment(self, env, data) -> Tuple[int, float]:
        w = env.world_size
        df1 = DataFrame(data[0])
        df2 = DataFrame(data[1])
        env.barrier()

        t1 = time.time()
        df3 = df1.merge(df2, on=[0], env=env) \
                    .groupby(by=0, env=env).agg({1: "sum"}) \
                    .sort_values([0], sampling='initial', env=env) + data[2]
        env.barrier()
        t2 = time.time()

        l_len = len(df3)
        del df1, df2, df3

        return l_len, (t2 - t1) * 1000

execute_experiment(CompositeExp, args)
