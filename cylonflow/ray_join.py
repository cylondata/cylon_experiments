from cflowray import run_ray, get_generic_args
from cylon_experiments.cylonflow import JoinExperiment

if __name__ == "__main__":
    parser = get_generic_args('run cylon join')
    parser.add_argument('-a', dest='algo', type=str, default='sort', choices=['sort', 'hash'])

    args = vars(parser.parse_args())

    run_ray(args,
            JoinExperiment,
            'cylon_ray_join',
            f"a={args['algo']} u={args['unique']} c={args['comm']}")
