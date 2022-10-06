import os
import shutil
import tempfile
from multiprocessing import Process

import pandas as pd
from numpy.random import default_rng

from pycylon import CylonEnv, DataFrame
from pycylon.net.gloo_config import GlooMPIConfig

ROWS = 10


conf = GlooMPIConfig()
env = CylonEnv(config=conf)

rng = default_rng(seed=ROWS)
data1 = rng.integers(0, ROWS, size=(ROWS, 2))
data2 = rng.integers(0, ROWS, size=(ROWS, 2))

df1 = DataFrame(pd.DataFrame(data1).add_prefix("col"))
df2 = DataFrame(pd.DataFrame(data2).add_prefix("col"))

print("Distributed Merge")
df3 = df1.merge(right=df2, on=[0], env=env)
print(f'res len {len(df3)}')

env.finalize()
