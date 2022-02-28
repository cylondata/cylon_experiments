import pandas as pd 
import numpy as np
from numpy.random import default_rng
import time

rng = default_rng()

r = 1000000000
data = np.asfortranarray(rng.integers(0, r, size=(r, 2)))
# data = rng.integers(0, r, size=(r, 2))
df = pd.DataFrame(data).add_prefix("col")

t1 = time.time()
df.sum()
t2 = time.time()
print((t2-t1)*1000)

df = pd.DataFrame(data, columns=["col0", "col1"])
t1 = time.time()
df.sum()
t2 = time.time()
print((t2-t1)*1000)

t1 = time.time()
np.sum(data, axis=0)
t2 = time.time()
print((t2-t1)*1000)

