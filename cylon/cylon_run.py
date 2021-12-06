import os
from os.path import expanduser

from multiprocessing import Process
import argparse
from time import sleep

parser = argparse.ArgumentParser(description='generate random data')
parser.add_argument('-s', dest='script', type=str, help='script', required=True)
parser.add_argument('-r', dest='rows', type=int, nargs='+', help='row cases in millions',
                    required=True)
parser.add_argument('-w', dest='world', type=int, nargs='+', help='world sizes',
                    default=[1, 2, 4, 8, 16, 32, 64, 128])
parser.add_argument('-i', dest='it', type=int, help='number of repetitions', default=1)
parser.add_argument('--sargs', dest='sargs', type=str, help='script args', default="")

args = parser.parse_args()
args = vars(args)


rows = [int(ii * 1000000) for ii in args['rows']]
it = args['it']
world = args['world']
script = f"{os.getcwd()}/{args['script']}"

print("args: ", args, flush=True)

PYTHON_EXEC = "~/.conda/envs/cylon_dev/bin/python"

TOTAL_NODES = 15


for r in rows:
    for w in world:
        print(f"rows {r} world_size {w} starting!", flush=True)           
        hostfile = "" if w == 1 else "--hostfile nodes.txt"
        join_exec = f"mpirun --map-by node --report-bindings -mca btl vader,tcp,openib," \
                    f"self -mca btl_tcp_if_include enp175s0f0 --mca btl_openib_allow_ib 1 " \
                    f"{hostfile} --bind-to core --bind-to socket -np {w} " \
                    f"{PYTHON_EXEC} {script} -r {r} -w {w} -i {it} {args['sargs']}"
        print("running", join_exec, flush=True)
                              
        print(f"rows {r} world_size {w} done!\n-----------------------------------------", flush=True)
    print(f"rows {r} done!\n ====================================== \n", flush=True)
