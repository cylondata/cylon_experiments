from cflowdask import run_dask, get_generic_args
from cylon_experiments.cylonflow import JoinExperiment
from cylonflow.api.config import GlooFileStoreConfig

if __name__ == "__main__":
    parser = get_generic_args('run cylon join')
    parser.add_argument('-a', dest='algo', type=str, default='sort', choices=['sort', 'hash'])

    args = vars(parser.parse_args())

    config = GlooFileStoreConfig()
    run_dask(args,
             config,
             JoinExperiment,
             'cylon_dask_join',
             f"a={args['algo']} u={args['unique']} c={args['comm']}")
