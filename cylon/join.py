import time
from typing import Tuple

from pycylon import DataFrame

from experiment import CylonExperiment, get_generic_args, execute_experiment

parser = get_generic_args('run cylon join')
parser.add_argument('-a', dest='algos', nargs='+', type=str, default=['sort', 'hash'])

args = vars(parser.parse_args())


class JoinExp(CylonExperiment):
    def __init__(self, args) -> None:
        super().__init__('join', args)

    def tag(self, args) -> str:
        return f"a={args['algo']} u={args['unique']} c={args['comm']}"

    def generate_data(self, rng, tot_rows, world_sz, cols=2, unique_fac=1):
        data = super().generate_data(rng, tot_rows, world_sz, cols, unique_fac)
        data1 = super().generate_data(rng, tot_rows, world_sz, cols, unique_fac)
        return [data, data1]

    def experiment(self, env, data) -> Tuple[int, float]:
        df1 = DataFrame(data[0])
        df2 = DataFrame(data[1])
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


def run_fn(exp, args):
    for algo in args['algos']:
        args['algo'] = algo
        exp.run(args)


execute_experiment(JoinExp, args, run_fn=run_fn)
