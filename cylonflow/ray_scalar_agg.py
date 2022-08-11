from cflowray import get_generic_args, run_ray
from cylon_experiments.cylonflow import ScalarAggExp

if __name__ == "__main__":
    parser = get_generic_args('run cylonflow ray scalar agg')

    args = vars(parser.parse_args())

    run_ray(args,
            ScalarAggExp,
            'cylon_ray_scalar_agg',
            tag=f"u={args['unique']} c={args['comm']}")
