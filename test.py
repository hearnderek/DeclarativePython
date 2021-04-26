import pandas as pd
import numpy as np
import sketch


df = pd.read_csv('test_funcs_input.csv')

print(df)

run = sketch.Run('2021-04-26')
run.init_df(df)
run.process_module('test_funcs')
df = run.df

print(df)

print(run.calculate())
