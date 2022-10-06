from cflowparsl import run_parsl, get_generic_args
from cylonflow.parsl.app.cylon import cylon_env_app


@cylon_env_app
def parsl_scalar_agg(exp_args, cylon_env=None, **kwargs):
    from cylon_experiments.cylonflow import ScalarAggExp
    return ScalarAggExp(exp_args).run_experiment(cylon_env=cylon_env)


if __name__ == "__main__":
    parser = get_generic_args('run cylon scalar agg')

    args = vars(parser.parse_args())

    run_parsl(parsl_scalar_agg, args, 'cylon_parsl_scalar_agg',
              f"u={args['unique']} c={args['comm']}")
