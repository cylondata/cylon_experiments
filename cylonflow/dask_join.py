from cflowdask import DaskRunner
from cylon_experiments.cylonflow import JoinExperiment
from cylon_experiments.experiment import get_generic_args
from cylonflow.api.config import GlooFileStoreConfig

if __name__ == "__main__":
    parser = get_generic_args('run cylon join')
    parser.add_argument('-a', dest='algo', nargs='+', type=str, default='sort')

    args = vars(parser.parse_args())
    args['comm'] = 'gloo'

    print('args', args)

    config = GlooFileStoreConfig()
    dask_runner = DaskRunner(4, 'dask_join', config, JoinExperiment, args,
                             scheduler_file='/home/nira/sched.json',
                             tag=f"a={args['algo']} u={args['unique']} c={args['comm']}")

    dask_runner.run()
