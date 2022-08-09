from cflowdask import get_generic_args, run_dask
from cylon_experiments.cylonflow import GroupByExperiment
from cylonflow.api.config import GlooFileStoreConfig

if __name__ == "__main__":
    parser = get_generic_args('run cylonflow dask groupby')
    parser.add_argument('-a', dest='algo', type=str, default='hash', choices=['hash', 'mapred_hash'])

    args = vars(parser.parse_args())

    config = GlooFileStoreConfig()
    run_dask(args,
             config,
             GroupByExperiment,
             'cylon_dask_groupby',
             tag=f"a={args['algo']} u={args['unique']} c={args['comm']}")
