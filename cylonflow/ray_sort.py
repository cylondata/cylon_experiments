from cflowray import get_generic_args, run_ray
from cylon_experiments.cylonflow import SortExperiment

if __name__ == "__main__":
    parser = get_generic_args('run cylonflow ray groupby')
    parser.add_argument('-a', dest='algo', type=str, default='initial', choices=['initial', 'regular'])

    args = vars(parser.parse_args())

    run_ray(args,
            SortExperiment,
            'cylon_ray_sort',
            tag=f"a={args['algo']} u={args['unique']} c={args['comm']}")
