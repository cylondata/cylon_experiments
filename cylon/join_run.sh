#!/bin/bash

source /opt/Anaconda3-2020.11/etc/profile.d/conda.sh

conda activate cylon_dev

rows=1000000000
# rows=10000
iter=4
u=0.9
# world=(32 128 512)
world=(256)
join_exec="$HOME/victor/git/cylon/build/bin/join_example"
algo=""

for t in n o; do
    for w in ${world[@]}; do 
        for i in $(seq 1 $iter);do
            echo "***************** start $t $w $i"
            mpirun --map-by node --mca btl "vader,tcp,openib,self" \
                --mca btl_tcp_if_include enp175s0f0 --mca btl_openib_allow_ib 1 --hostfile nodes.txt \
                --bind-to core --bind-to socket --mca mpi_preconnect_mpi 1 \
                -np $w $join_exec m $t $(( rows/w )) $u $algo || exit 1
        done
    done
done

algo="hash"
for t in n o; do
    for w in ${world[@]}; do 
        for i in $(seq 1 $iter);do
            echo "***************** start $t $w $i"
            mpirun --map-by node --mca btl "vader,tcp,openib,self" \
                --mca btl_tcp_if_include enp175s0f0 --mca btl_openib_allow_ib 1 --hostfile nodes.txt \
                --bind-to core --bind-to socket --mca mpi_preconnect_mpi 1 \
                -np $w $join_exec m $t $(( rows/w )) $u $algo || exit 1
        done
    done
done

          