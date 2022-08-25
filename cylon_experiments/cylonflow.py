import gc
import os.path
import time
from abc import ABC, abstractmethod
from typing import Tuple, Any

import numpy as np
import pandas as pd
from numpy.random import default_rng
from pycylon import DataFrame


class CFlowExperiment(ABC):
    args: dict = None
    bypass_len_check: bool = False

    def __init__(self, args, bypass_len_check=False) -> None:
        self.args = args
        self.bypass_len_check = bypass_len_check

    def generate_data(self, rng, tot_rows, world_sz, cols=2, unique_fac=1.) -> Any:
        rows = int(tot_rows / world_sz)

        max_val = int(tot_rows * unique_fac)
        return pd.DataFrame(rng.integers(0, max_val, size=(rows, cols))).add_prefix('col')

    @abstractmethod
    def experiment(self, env, data) -> Tuple[int, float]:
        raise NotImplementedError()

    def run_experiment(self, cylon_env=None) -> Tuple[int, np.ndarray]:
        rank = cylon_env.rank
        w = cylon_env.world_size
        global_r = self.args['rows']
        cols = self.args['cols']

        # generate data
        rng = default_rng(seed=rank)
        data = self.generate_data(rng, global_r, w, cols, self.args['unique'])

        exec_times = np.zeros(self.args['it'], np.float32)
        out_len = -1

        for i in range(self.args['it']):
            cylon_env.barrier()
            out_len_i, tm = self.experiment(cylon_env, data)
            cylon_env.barrier()

            if i == 0 or self.bypass_len_check:
                out_len = out_len_i
            elif out_len != out_len_i:
                raise ValueError('experiment iterations result sizes are different')
            exec_times[i] = tm

            gc.collect()

        return out_len, exec_times


class JoinExperiment(CFlowExperiment):
    def experiment(self, env, data) -> Tuple[int, float]:
        df1 = DataFrame(data[0])
        df2 = DataFrame(data[1])
        env.barrier()

        t1 = time.time()
        df3 = df1.merge(df2, on=[0], algorithm=self.args['algo'], env=env)
        env.barrier()
        t2 = time.time()

        l_len = len(df3)
        del df1
        del df2
        del df3

        return l_len, (t2 - t1) * 1000

    def generate_data(self, rng, tot_rows, world_sz, cols=2, unique_fac=1):
        data = super().generate_data(rng, tot_rows, world_sz, cols, unique_fac)
        data1 = super().generate_data(rng, tot_rows, world_sz, cols, unique_fac)
        return data, data1


class GroupByExperiment(CFlowExperiment):
    def experiment(self, env, data) -> Tuple[int, float]:
        df1 = DataFrame(data)
        env.barrier()

        t1 = time.time()
        df2 = df1.groupby(by=0, env=env, groupby_type=self.args['algo']).agg({1: ["sum", "mean", "std"]})
        env.barrier()
        t2 = time.time()

        l_len = len(df2)
        del df1
        del df2

        return l_len, (t2 - t1) * 1000


class ScalarExperiment(CFlowExperiment):
    def experiment(self, env, data) -> Tuple[int, float]:
        df1 = DataFrame(data[0])
        val = data[1]

        t1 = time.time()
        df2 = df1 + val
        env.barrier()
        t2 = time.time()

        l_len = len(df2)
        del df1
        del df2

        return l_len, (t2 - t1) * 1000

    def generate_data(self, rng, tot_rows, world_sz, cols=2, unique_fac=1) -> Any:
        return super().generate_data(rng, tot_rows, world_sz, cols, self.args['unique']), \
               rng.integers(0, tot_rows)


class ScalarAggExp(CFlowExperiment):
    def experiment(self, env, data) -> Tuple[int, float]:
        df1 = DataFrame(data)

        t1 = time.time()
        # sum is not exopsed in df :(
        tb3 = df1.to_table().sum(0)
        tb4 = df1.to_table().sum(1)
        env.barrier()
        t2 = time.time()

        l_len = (len(tb3) == 1) and (len(tb4) == 1)
        del df1
        del tb3
        del tb4
        gc.collect()

        return l_len, (t2 - t1) * 1000


class SortExperiment(CFlowExperiment):
    def __init__(self, args):
        super().__init__(args, bypass_len_check=True)

    def experiment(self, env, data) -> Tuple[int, float]:
        w = env.world_size
        df1 = DataFrame(data)
        env.barrier()

        t1 = time.time()
        df2 = df1.sort_values(by=0, env=env, sampling=self.args['algo'], num_bins=w*100, num_samples=w*1000)
        env.barrier()
        t2 = time.time()

        l_len = len(df2)
        del df1
        del df2

        return l_len, (t2 - t1) * 1000


class CFlowRunner(ABC):
    def __init__(self, world_size, name, experiment_cls, args, tag='') -> None:
        self.world_size = world_size
        self.name = name
        self.experiment_cls = experiment_cls
        self.args = args
        self.tag = tag

        self.output_dir = args['out']
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir, exist_ok=True)

        self.executor = None

        self.initialize_executor()

    @abstractmethod
    def initialize_executor(self):
        raise NotImplementedError()

    @abstractmethod
    def shutdown(self):
        raise NotImplementedError()

    def run(self):
        result = self.executor.execute_cylon(lambda exp, cylon_env: exp.run_experiment(cylon_env=cylon_env))
        if len(result) != self.world_size:
            raise ValueError('len(result) != world_size')
        result0 = result[0]

        it = self.args['it']

        if len(result0[1]) != it or not isinstance(result0[1], np.ndarray):
            raise ValueError('received results dont match iterations')

        total_length = 0
        times = []
        for ln, tm in result:
            total_length += ln
            times.append(tm)

        times_arr = np.mean(np.stack(times), axis=0)

        timing = {
            'rows': [self.args['rows']] * it,
            'world': [self.world_size] * it,
            'it': list(range(0, it)),
            'time': times_arr.tolist(),
            'tag': [self.tag] * it,
            'out': [total_length] * it
        }

        file_path = os.path.join(self.output_dir, f"{self.name}.csv")
        if os.path.exists(file_path):
            pd.DataFrame(timing).to_csv(file_path,
                                        mode='a',
                                        index=False,
                                        header=False)
        else:
            pd.DataFrame(timing).to_csv(file_path,
                                        index=False,
                                        header=True)
