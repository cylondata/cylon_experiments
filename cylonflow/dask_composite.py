from cflowdask import run_dask, get_generic_args
from cylon_experiments.cylonflow import CompositeExperiment
from cylonflow.api.config import GlooFileStoreConfig

if __name__ == "__main__":
    parser = get_generic_args('run cylon composite')

    args = vars(parser.parse_args())

    config = GlooFileStoreConfig()
    config.tcp_iface = 'enp175s0f0'
    config.file_store_path = '/N/u/d/dnperera/gloo'

    run_dask(args,
             config,
             CompositeExperiment,
             'cylon_dask_composite',
             f"u={args['unique']} c={args['comm']}")
