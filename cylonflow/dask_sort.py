from cflowdask import get_generic_args, run_dask
from cylon_experiments.cylonflow import SortExperiment
from cylonflow.api.config import GlooFileStoreConfig

if __name__ == "__main__":
    parser = get_generic_args('run cylonflow dask sort')
    parser.add_argument('-a', dest='algo', type=str, default='initial', choices=['initial', 'regular'])

    args = vars(parser.parse_args())

    config = GlooFileStoreConfig()
    config.tcp_iface = 'enp175s0f0'
    config.file_store_path = '/N/u/d/dnperera/gloo'
    
    run_dask(args,
             config,
             SortExperiment,
             'cylon_dask_sort',
             tag=f"a={args['algo']} u={args['unique']} c={args['comm']}")
