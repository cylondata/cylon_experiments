import time
from typing import Tuple

from pycylon import DataFrame

from cylon_experiments.experiment import CylonExperiment, get_generic_args, execute_experiment

parser = get_generic_args('run cylon join')
parser.add_argument('-a', dest='algos', nargs='+', type=str, default=['hash', 'mapred_hash'])

args = vars(parser.parse_args())


class GroupbyExp(CylonExperiment):

    def __init__(self, args) -> None:
        super().__init__('groupby', args)

    def tag(self, args) -> str:
        return f"a={args['algo']} u={args['unique']} c={args['comm']}"

    def experiment(self, env, data) -> Tuple[int, float]:
        df1 = DataFrame(data)
        env.barrier()

        t1 = time.time()
        df2 = df1.groupby(by=0, env=env, groupby_type=args['algo']).agg(
            {1: ["sum", "mean", "std"]})
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


execute_experiment(GroupbyExp, args, run_fn=run_fn)
