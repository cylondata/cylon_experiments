from cflowdask import get_generic_args, run_dask
from cylon_experiments.cylonflow import ScalarAggExp
from cylonflow.api.config import GlooFileStoreConfig

if __name__ == "__main__":
    parser = get_generic_args('run cylonflow dask scalar agg')

    args = vars(parser.parse_args())

    config = GlooFileStoreConfig()
    run_dask(args,
             config,
             ScalarAggExp,
             'cylon_dask_scalar_agg',
             tag=f"u={args['unique']} c={args['comm']}")
