from cflowparsl import run_parsl, get_generic_args
from cylonflow.parsl.app.cylon import cylon_env_app


@cylon_env_app
def parsl_join(exp_args=None, cylon_env=None):
    from cylon_experiments.cylonflow import JoinExperiment
    return JoinExperiment(exp_args).run_experiment(cylon_env=cylon_env)


if __name__ == "__main__":
    parser = get_generic_args('run cylon join')
    parser.add_argument('-a', dest='algo', type=str, default='sort', choices=['sort', 'hash'])

    args = vars(parser.parse_args())

    run_parsl(args,
              parsl_join,
              'cylon_parsl_join',
              f"a={args['algo']} u={args['unique']} c={args['comm']}")
