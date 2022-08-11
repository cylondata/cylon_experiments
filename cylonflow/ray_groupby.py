from cflowray import get_generic_args, run_ray
from cylon_experiments.cylonflow import GroupByExperiment

if __name__ == "__main__":
    parser = get_generic_args('run cylonflow ray groupby')
    parser.add_argument('-a', dest='algo', type=str, default='hash', choices=['hash', 'mapred_hash'])

    args = vars(parser.parse_args())

    run_ray(args,
            GroupByExperiment,
            'cylon_ray_groupby',
            tag=f"a={args['algo']} u={args['unique']} c={args['comm']}")
