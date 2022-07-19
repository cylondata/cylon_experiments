from typing import Tuple
from pycylon import DataFrame, CylonEnv
from pycylon.net import MPIConfig
import argparse
import time

from mpi4py import MPI

from experiment import CylonExperiment

parser = argparse.ArgumentParser(description='run cylon join')
parser.add_argument('-r', dest='rows', type=int, required=True)
parser.add_argument('-i', dest='it', type=int, required=True)
parser.add_argument('-o', dest='out', type=str, required=True)
parser.add_argument('-a', dest='algo', type=str, default="hash")  # mapred_hash
parser.add_argument('-u',
                    dest='unique',
                    type=float,
                    default=1.0,
                    help="unique factor")
parser.add_argument('-c', dest='comm', type=str, default="mpi")

args = vars(parser.parse_args())

# script = os.path.basename(__file__).replace('.py', '')

# w = args['world']
# global_r = args['rows']
# r = int(global_r/w)
# cols = 2
# max_val = int(global_r * args['unique'])
# algo = args['algo']
# tag = f"a={algo} u={args['unique']}"

# env = CylonEnv(config=MPIConfig(), distributed=True)
# rank = env.rank

# rng = default_rng()
# data = pd.DataFrame(rng.integers(0, max_val, size=(r, cols))).add_prefix('col')
# # if rank == 0:
# #     print(f"data generated {r}", flush=True)

# timing = {'rows': [], 'world':[], 'it':[], 'time':[], 'tag':[], 'out':[]}

# comm = MPI.COMM_WORLD

# try:
#     for i in range(args['it']):
#         # if rank == 0:
#         #     print(f"start {i}", flush=True)

#         data = pd.DataFrame(rng.integers(0, max_val, size=(r, cols))).add_prefix('col')
#         df1 = DataFrame(data)
#         env.barrier()

#         t1 = time.time()
#         df2 = df1.groupby(by=0, env=env, groupby_type=algo).agg({1: ["sum", "mean", "std"]})
#         env.barrier()
#         t2 = time.time()

#         l_len = len(df2)
#         g_len = comm.reduce(l_len)

#         timing['rows'].append(global_r)
#         timing['world'].append(w)
#         timing['it'].append(i)
#         timing['time'].append((t2 - t1) * 1000)
#         timing['tag'].append(tag)
#         timing['out'].append(g_len)

#         del df1
#         del df2
#         del data
#         gc.collect()
#         env.barrier()

#         # if rank == 0:
#             # print(f"done {i} {g_len}", flush=True)
#         # time.sleep(5)
# finally:
#     if rank == 0:
#         pd.DataFrame(timing).to_csv(f"{args['out']}/{script}.csv", mode='a', index=False, header=False)
#     env.finalize()


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

exp = GroupbyExp(args)

for algo in ['hash', 'mapred_hash']:
    args['algo'] = algo
    exp.run(args)

exp.finalize()
