from cflowdask import get_generic_args, run_dask
from cylon_experiments.cylonflow import ScalarExperiment
from cylonflow.api.config import GlooFileStoreConfig

if __name__ == "__main__":
    parser = get_generic_args('run cylonflow dask scalar')

    args = vars(parser.parse_args())

    config = GlooFileStoreConfig()
    config.tcp_iface = 'enp175s0f0'
    config.file_store_path = '/N/u/d/dnperera/gloo'

    run_dask(args,
             config,
             ScalarExperiment,
             'cylon_dask_scalar',
             tag=f"u={args['unique']} c={args['comm']}")
