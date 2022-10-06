from cflowray import run_ray, get_generic_args
from cylon_experiments.cylonflow import CompositeExperiment

if __name__ == "__main__":
    parser = get_generic_args('run cylon composite')

    args = vars(parser.parse_args())

    run_ray(args,
            CompositeExperiment,
            'cylon_ray_composite',
            f"u={args['unique']} c={args['comm']}")
