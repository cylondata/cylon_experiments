import time
from typing import Tuple

from pycylon import DataFrame

from cylon_experiments.experiment import CylonExperiment, get_generic_args, execute_experiment

parser = get_generic_args('run cylon sort')
parser.add_argument('-a', dest='algos', nargs='+', type=str, default=['initial', 'regular'])

args = vars(parser.parse_args())


class SortExp(CylonExperiment):

    def __init__(self, args) -> None:
        super().__init__('sort', args)

    def tag(self, args) -> str:
        return f"a={args['algo']} u={args['unique']} c={args['comm']}"

    def experiment(self, env, data) -> Tuple[int, float]:
        w = env.world_size
        df1 = DataFrame(data)
        env.barrier()

        t1 = time.time()
        df2 = df1.sort_values(by=0, env=env, sampling=args['algo'], num_bins=w*100, num_samples=w*1000)
        env.barrier()
        t2 = time.time()

        l_len = len(df2)
        del df1
        del df2

        return l_len, (t2 - t1) * 1000


def run_fn(exp, args):
    for algo in args['algos']:
        args['algo'] = algo
        exp.run(args)


execute_experiment(SortExp, args, run_fn=run_fn)
