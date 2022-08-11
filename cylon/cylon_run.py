import os
import subprocess
import sys
import argparse

parser = argparse.ArgumentParser(description='generate random data')
parser.add_argument('-s', dest='script', type=str, nargs='+', help='scripts', required=True)
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

print("args: ", args, flush=True)

TOTAL_NODES = 15

out_dir = f"{os.getcwd()}/results"
os.makedirs(out_dir, exist_ok=True)

for s in args['script']:
    script = f"{os.getcwd()}/{s}"
    for r in rows:
        for w in world:
            print(f"{s} rows {r} world_size {w} starting!", flush=True)           
            hostfile = "" if w == 1 else "--hostfile nodes.txt"
            exec = f"mpirun --map-by node --report-bindings --mca btl \"vader,tcp,openib," \
                        f"self\" --mca btl_tcp_if_include enp175s0f0 --mca btl_openib_allow_ib 1 " \
                        f"{hostfile} --bind-to core --bind-to socket --mca mpi_preconnect_mpi 1 -np {w} " \
                        f"{sys.executable} {script} -r {r} -i {it} -o {out_dir} {args['sargs']}"
            print("running", exec, flush=True)

            out = subprocess.run(exec, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, text=True)
            if out.returncode != 0:
                sys.exit("Failed: " + out.stderr)
                                
            print(f"{s} rows {r} world_size {w} done!", flush=True)
    print(f"{s} done!\n ====================================== \n", flush=True)
