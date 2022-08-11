from cflowray import get_generic_args, run_ray
from cylon_experiments.cylonflow import ScalarExperiment

if __name__ == "__main__":
    parser = get_generic_args('run cylonflow ray scalar')

    args = vars(parser.parse_args())

    run_ray(args,
            ScalarExperiment,
            'cylon_ray_scalar',
            tag=f"u={args['unique']} c={args['comm']}")
